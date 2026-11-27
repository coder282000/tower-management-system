"""Milestone 8: distributed system models for the tower fault-management system, measured on the real cluster.

Same workload, same seed, five models (cfg["correlation_tier"] selects the model; nodes/node.py implements them):

  host_based      core         sites are dumb terminals: every reading goes to the core, the core correlates
  server_based    manager      each site's manager (server) correlates and escalates to the core   [original design]
  workstation     site         each site correlates its own readings and escalates straight to the core
  processor_pool  pool         readings go to the core, which hands each one to the least-loaded manager of the pool
                               (managers are interchangeable, stateless workers; state is kept at the core)
  integrated      integrated   hybrid: sites report by exception (workstation filtering), managers correlate (server),
                               and the Milestone 7 fault tolerance is on (backup manager, replication, outbox)

Two CONTROLS separate the model from the fault-tolerance service (integrated is the only model that has FT switched on):
  integrated_noft   the integrated model with Milestone 7 fault tolerance in baseline mode
  server_based_ft   the server-based model WITH Milestone 7 fault tolerance
If integrated beats server_based on reliability but server_based_ft matches it, the gain came from FT, not from the model.

Part A  Performance and scalability   3 / 6 / 9 sites, 2 managers, measured for DURATION seconds after a warm-up
          latency       fault at the site -> core knows (mean, p95), share detected
          throughput    incidents detected per second (completed work / time) next to the offered fault rate
          utilisation   CPU % and memory MB per tier; messages/s into the core (all, and the fault-management "data" part
                        without resource updates and registration), into each manager, and in total (bandwidth)
Part B  Reliability                   at 6 sites mgr1 is killed, then (separate run) the core is killed, for 8 s each;
          share of faults raised during the outage that still reach the core. Repeated TRIALS times.
Part C  Cost                          an ASSUMED unit-cost model (see COST) applied to the measured message rates
Part D  Decision matrix               min-max normalised scores under several weight profiles, with a sensitivity
          analysis (weights, unit costs, cluster size). A conclusion that survives all of them is a robust one.
Part E  Stress (saturation)           9 sites, sensor interval 0.3 / 0.1 / 0.05 s (about 30 / 90 / 180 readings/s):
          where does each model stop keeping up? throughput, share detected, latency, core CPU.

Hypotheses
  H8.1 Host-based has the lowest hardware cost but the highest core load, and its core is a single point of failure.
  H8.2 Workstation and integrated models send the fewest messages into the core (they filter at the edge).
  H8.3 Server-based balances core load but loses the sites of a crashed manager; the pool and integrated models do not.
  H8.4 Load on the core grows with the number of sites for host-based and pool, much less for the others.
  H8.5 (control) The reliability advantage of the integrated model comes from the fault-tolerance service, not from the model.
Controlled: sensor_interval 0.3 s (Part A, B), link 5 ms +-2 ms, seed 42, 2 managers, same fault process at every site.

    python3 -m experiments.week8_models --demo      6 sites, 10 s per model, no crash runs (about 3 min)
    (also: --sizes 3,9   choose the cluster sizes;  --controls   force the control models on, e.g. with --quick)
    python3 -m experiments.week8_models --quick     3 and 9 sites, 1 trial, crash runs, no stress (about 14 min)
    python3 -m experiments.week8_models             3/6/9 sites, 2 trials, controls, crash runs, stress (about 50 min)
    options: --duration 30   measure each model for 30 s instead of 15 s (more faults, tighter numbers)
             --trials N      trials per configuration
             --part perf,rel,stress   run only some parts (default: all)
             --no-controls   skip the two control models
Writes results/week8_models.csv, week8_summary.csv, week8_reliability.csv, week8_stress.csv, week8_cost.csv,
week8_decision.csv, week8_hypotheses.txt and results/environment.txt.

Faults are matched to what the core learned by FAULT ID (fid): every fault episode at a site gets an id that travels
with its readings into the incident, so a later, unrelated incident is never counted as a late detection.
"""
import asyncio
import csv
import json
import os
import shutil
import statistics
import subprocess
import sys
import time

from cluster import launch, shutdown, wait_registered
from experiments._common import acquire_lock, ci95, mean_sd, write_env
from kernel.topology import build
from nodes.node import Node, load_config

TMP, LOGS = "config/_week8_tmp.json", "logs/week8"
FT_CFG = {"miss_limit": 3, "detect_timeout": 1.5, "sync_interval": 0.5}
MODELS = {"host_based": ("core", None), "server_based": ("manager", None), "workstation": ("site", None),
          "processor_pool": ("pool", None), "integrated": ("integrated", "proposed")}
CONTROLS = {"integrated_noft": ("integrated", "baseline"), "server_based_ft": ("manager", "proposed")}
ALL = {**MODELS, **CONTROLS}
BASE_OF = {"integrated_noft": "integrated", "server_based_ft": "server_based"}
WARMUP, MATCH_S = 3.0, 3.0
# ASSUMED relative cost units (replace with real quotes): hardware per node + bandwidth per message/s.
COST = {"site_terminal": 1.0, "site_workstation": 2.0, "manager": 3.0, "core_base": 5.0,
        "core_per_msg_s": 0.05, "bandwidth_per_msg_s": 0.02}
COST_VARIANTS = {"assumed costs": {}, "cheap workstations": {"site_workstation": 1.2},
                 "expensive workstations": {"site_workstation": 4.0}, "expensive managers": {"manager": 6.0},
                 "expensive bandwidth": {"bandwidth_per_msg_s": 0.1}, "expensive core": {"core_base": 15.0}}
# weights over: latency, core_load, network, cost, reliability, throughput (throughput only if the stress part ran)
PROFILES = {"equal weights": dict(latency=1, core_load=1, network=1, cost=1, reliability=1, throughput=1),
            "telecom (reliability, latency first)": dict(latency=.25, core_load=.10, network=.10, cost=.10, reliability=.35, throughput=.10),
            "cost-driven": dict(latency=.10, core_load=.10, network=.15, cost=.45, reliability=.10, throughput=.10),
            "performance-driven": dict(latency=.30, core_load=.15, network=.10, cost=.05, reliability=.10, throughput=.30)}
CRITERIA = ("latency", "core_load", "network", "cost", "reliability", "throughput")


async def ask(client, name, typ, payload=None, timeout=3.0):
    return await client.send(name, client.msg(name, typ, payload or {}), wait_reply=True, emulate=False, timeout=timeout)


class Cluster:
    def __init__(self, model, n_sites, interval=0.3, base_port=29000):
        tier, ft = ALL[model]
        self.cfg = build(load_config("config/cluster.json"), n_sites, 2, base_port=base_port)
        self.cfg.update({"log_dir": LOGS, "sensor_interval": interval, "trace_faults": True, "log_append": True,
                         "correlation_tier": tier})
        if ft:
            self.cfg["ft"] = {"mode": ft, **FT_CFG}
        self.model = model
        self.names = [n["name"] for n in self.cfg["nodes"] if n["role"] != "client"]
        self.sites = [n["name"] for n in self.cfg["nodes"] if n["role"] == "site"]

    async def __aenter__(self):
        shutil.rmtree(LOGS, ignore_errors=True)
        with open(TMP, "w") as f:
            json.dump(self.cfg, f)
        self.procs = launch(TMP, self.names)
        self.client = Node(self.cfg, "client")
        await self.client.start()
        await wait_registered(self.client, [n for n in self.names if n != "core"], timeout=60)
        return self

    async def __aexit__(self, *exc):
        await shutdown(self.client, self.names)
        self.client.tp.server.close()
        await self.client.tp.server.wait_closed()
        for p in self.procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()

    def kill(self, name):
        p = self.procs[self.names.index(name)]
        p.kill()
        p.wait()

    def restart(self, name):
        self.procs[self.names.index(name)] = subprocess.Popen(
            [sys.executable, "-m", "nodes.run", "--config", TMP, "--name", name])

    async def all_stats(self):
        rs = await asyncio.gather(*[ask(self.client, n, "STATS", timeout=2.0) for n in self.names])
        return {n: r.payload for n, r in zip(self.names, rs) if r}


def events(node, kinds):
    out = []
    try:
        with open(os.path.join(LOGS, f"{node}.jsonl")) as f:
            for line in f:
                try:
                    e = json.loads(line)
                except ValueError:
                    continue
                if e.get("event") in kinds:
                    out.append(e)
    except OSError:
        pass
    return out


def detections(cl, lo, hi, match_s):
    """For each fault episode that starts at a site in [lo, hi): seconds until the core knows (None = it never did).
    A fault is matched to the core event that carries the same fault id; match_s limits how late that may be."""
    faults = sorted((e["ts"], e["fid"]) for s in cl.sites for e in events(s, {"sensor_fault"}) if lo <= e["ts"] < hi)
    first = {}
    for e in sorted(events("core", {"incident_escalated", "correlated_incident"}), key=lambda e: e["ts"]):
        if e.get("fid") is not None and e["fid"] not in first:
            first[e["fid"]] = e["ts"]
    out = []
    for fts, fid in faults:
        c = first.get(fid)
        out.append(None if c is None or c < fts or (match_s is not None and c - fts > match_s) else c - fts)
    return out


async def performance(model, n_sites, duration, interval=0.3):
    async with Cluster(model, n_sites, interval) as cl:
        await asyncio.sleep(WARMUP)
        a = await cl.all_stats()
        t0 = time.time()
        await asyncio.sleep(duration)
        t1 = time.time()
        b = await cl.all_stats()
        await asyncio.sleep(0.5)
        lat = detections(cl, t0, t1 - MATCH_S, MATCH_S)
        got = [x for x in lat if x is not None]
        eff = max(duration - MATCH_S, 1.0)                 # faults are judged in the window that leaves MATCH_S to finish
        recv = lambda node, types=None: sum(1 for e in events(node, {"recv"})
                                            if t0 <= e["ts"] < t1 and (types is None or e.get("type") in types)) / duration
        DATA_CORE = {"SENSOR", "CORRELATED_INCIDENT", "INCIDENT_RESOLVED"}    # fault-management traffic into the core
        DATA_MGR = {"SENSOR", "POOL_SENSOR"}                                  # (the rest is resource updates, registration...)
        role_of = {x["name"]: x["role"] for x in cl.cfg["nodes"]}
        by = lambda role: [n for n in cl.names if role_of[n] == role]
        cpu = lambda role: round(statistics.mean(b[n]["cpu_mean"] for n in by(role) if n in b), 1)
        mem = lambda role: round(statistics.mean(b[n]["rss_max_mb"] for n in by(role) if n in b), 1)
        net = sum(b[n]["link_sent"] - a.get(n, {}).get("link_sent", 0) for n in b) / duration
        mgrs = by("manager")
        return {"model": model, "sites": n_sites, "interval_s": interval, "faults": len(lat), "detected": len(got),
                "detect_ratio": round(len(got) / len(lat), 3) if lat else "",
                "detect_mean_ms": round(1000 * statistics.mean(got), 1) if got else "",
                "detect_p95_ms": round(1000 * sorted(got)[int(0.95 * (len(got) - 1))], 1) if got else "",
                "offered_faults_s": round(len(lat) / eff, 2), "throughput_inc_s": round(len(got) / eff, 2),
                "core_in_msgs_s": round(recv("core"), 1), "core_data_msgs_s": round(recv("core", DATA_CORE), 1),
                "mgr_in_msgs_s": round(statistics.mean(recv(m) for m in mgrs), 1),
                "mgr_data_msgs_s": round(statistics.mean(recv(m, DATA_MGR) for m in mgrs), 1),
                "net_msgs_s": round(net, 1),
                "cpu_site_pct": cpu("site"), "cpu_mgr_pct": cpu("manager"), "cpu_core_pct": cpu("core"),
                "mem_site_mb": mem("site"), "mem_mgr_mb": mem("manager"), "mem_core_mb": mem("core")}


async def reliability(model, target):
    t_fault, t_repair, t_end = 6.0, 14.0, 30.0
    async with Cluster(model, 6) as cl:
        await asyncio.sleep(WARMUP)
        await asyncio.sleep(t_fault)
        ts_f = time.time()
        cl.kill(target)
        await asyncio.sleep(t_repair - t_fault)
        ts_r = time.time()
        cl.restart(target)
        await asyncio.sleep(t_end - t_repair)
        await asyncio.sleep(0.5)
        lat = detections(cl, ts_f, ts_r, None)
        got = [x for x in lat if x is not None]
        return {"model": model, "killed": target, "faults_in_outage": len(lat), "detected": len(got),
                "detect_ratio": round(len(got) / len(lat), 3) if lat else "",
                "detect_mean_s": round(statistics.mean(got), 2) if got else ""}


def cost(model, n_sites, row, c=COST):
    m = BASE_OF.get(model, model)
    sites = n_sites * (c["site_workstation"] if m in ("workstation", "integrated") else c["site_terminal"])
    managers = 0 if m in ("host_based", "workstation") else 2 * c["manager"]
    load = c["core_per_msg_s"] * row["core_in_msgs_s"]
    bandwidth = c["bandwidth_per_msg_s"] * row["net_msgs_s"]
    return {"model": model, "sites": n_sites, "hardware": round(sites + managers + c["core_base"], 2),
            "core_load_cost": round(load, 2), "bandwidth_cost": round(bandwidth, 2),
            "total_cost": round(sites + managers + c["core_base"] + load + bandwidth, 2)}


def write_csv(path, rows):
    if not rows:
        return
    os.makedirs("results", exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print("saved", path)


def mean_rows(rows, keys):
    out = {}
    for r in rows:
        out.setdefault(tuple(r[k] for k in keys), []).append(r)
    res = []
    for k, g in out.items():
        row = dict(zip(keys, k))
        for f in g[0]:
            if f in keys or f == "trial":
                continue
            xs = [x[f] for x in g if x[f] != ""]
            row[f] = round(statistics.mean(xs), 2) if xs else ""
        res.append(row)
    return res


def summary_rows(rows, keys, fields):
    """Mean, sample sd and 95% CI of every field per group: what the report should quote."""
    groups = {}
    for r in rows:
        groups.setdefault(tuple(r[k] for k in keys), []).append(r)
    out = []
    for k, g in groups.items():
        row = {**dict(zip(keys, k)), "n": len(g)}
        for f in fields:
            xs = [x[f] for x in g if x[f] != ""]
            m, sd = mean_sd(xs)
            c = ci95(xs)
            row[f + "_mean"] = "" if m is None else round(m, 3)
            row[f + "_sd"] = "" if sd is None else round(sd, 3)
            row[f + "_ci95"] = "" if c is None else round(c, 3)
        out.append(row)
    return out


# ---------------------------------------------------------------- decision matrix and sensitivity
def raw_table(perf, rel, stress, n_sites, cvar):
    """One row per main model: the raw numbers behind each criterion (None = not measured)."""
    c = {**COST, **cvar}
    raw = {}
    for m in MODELS:
        p = next((r for r in perf if r["model"] == m and r["sites"] == n_sites), None)
        if p is None:
            return None
        rl = [r["detect_ratio"] for r in rel if r["model"] == m and r["detect_ratio"] != ""]
        st = [r for r in stress if r["model"] == m]
        top = max(st, key=lambda r: 1 / r["interval_s"]) if st else None      # the highest offered load
        raw[m] = {"latency": p["detect_mean_ms"], "core_load": p["core_data_msgs_s"], "network": p["net_msgs_s"],
                  "cost": cost(m, n_sites, p, c)["total_cost"],
                  "reliability": statistics.mean(rl) if rl else None,
                  "throughput": top["detect_ratio"] if top and top["detect_ratio"] != "" else None}
    return raw


def score(raw, weights):
    crit = [k for k in CRITERIA if all(raw[m][k] not in (None, "") for m in raw)]
    wsum = sum(weights.get(k, 0) for k in crit) or 1.0
    sc = {m: 0.0 for m in raw}
    for k in crit:
        vals = [raw[m][k] for m in raw]
        lo, hi = min(vals), max(vals)
        for m in raw:
            x = 0.5 if hi == lo else (raw[m][k] - lo) / (hi - lo)
            sc[m] += weights.get(k, 0) * (x if k in ("reliability", "throughput") else 1 - x)    # 1 = best
    return {m: round(sc[m] / wsum, 3) for m in sc}, crit


def decision(perf, rel, stress, sizes, n_ref):
    rows, table = [], []
    scenarios = [(f"{pn} @ {n_ref} sites", n_ref, {}, w) for pn, w in PROFILES.items()]
    scenarios += [(f"{cn} (equal weights) @ {n_ref} sites", n_ref, cv, PROFILES["equal weights"]) for cn, cv in COST_VARIANTS.items() if cv]
    scenarios += [(f"equal weights @ {n} sites", n, {}, PROFILES["equal weights"]) for n in sizes if n != n_ref]
    crit_used = []
    for name, n, cv, w in scenarios:
        raw = raw_table(perf, rel, stress, n, cv)
        if raw is None:
            continue
        sc, crit_used = score(raw, w)
        ranked = sorted(sc, key=lambda m: -sc[m])
        table.append((name, ranked, sc))
        for i, m in enumerate(ranked):
            rows.append({"scenario": name, "model": m, "score": sc[m], "rank": i + 1})
    return rows, table, crit_used


def print_decision(table, crit_used):
    if not table:
        return
    print(f"\nDecision matrix (criteria used: {', '.join(crit_used)}; 1.0 = best on every criterion)")
    wins = {}
    for name, ranked, sc in table:
        print(f"  {name:55} " + "  >  ".join(f"{m} {sc[m]}" for m in ranked[:3]))
        wins[ranked[0]] = wins.get(ranked[0], 0) + 1
    best = max(wins, key=wins.get)
    print(f"\n  Winner: {', '.join(f'{m} in {n}' for m, n in sorted(wins.items(), key=lambda kv: -kv[1]))} of {len(table)} scenarios."
          f"\n  {'The choice is robust.' if wins[best] == len(table) else 'The choice depends on the weights/costs: say so in the report and name what it depends on.'}")


def verdicts(perf, rel, costs, n_lo, n_hi, n_ref, rel_ctrl):
    L = []
    g = lambda m, n, k: next((r[k] for r in perf if r["model"] == m and r["sites"] == n), None)
    ok = lambda b: "SUPPORTED" if b else "NOT SUPPORTED"
    ms = list(MODELS)
    core_in = {m: g(m, n_ref, "core_data_msgs_s") for m in ms}
    hw = {m: next((c["hardware"] for c in costs if c["model"] == m and c["sites"] == n_ref), None) for m in ms}
    if None not in core_in.values() and None not in hw.values():
        core_rel = [r["detect_ratio"] for r in rel if r["model"] == "host_based" and r["killed"] == "core" and r["detect_ratio"] != ""]
        cr = statistics.mean(core_rel) if core_rel else None
        L.append(f"H8.1 host_based: hardware cost {hw['host_based']} (lowest of {sorted(hw.values())[:2]}...), core data-plane in {core_in['host_based']:.1f} msgs/s "
                 f"(highest: {max(core_in, key=core_in.get)}), share detected when the core is killed {'n/a' if cr is None else round(cr, 2)}: "
                 f"{ok(hw['host_based'] == min(hw.values()) and core_in['host_based'] == max(core_in.values()))}")
        low2 = sorted(core_in, key=core_in.get)[:2]
        L.append(f"H8.2 fault-management msgs/s into the core { {m: round(v, 1) for m, v in sorted(core_in.items(), key=lambda kv: kv[1])} }: two lowest = {low2}: "
                 f"{ok(set(low2) == {'workstation', 'integrated'})}")
    rm = lambda m: [r["detect_ratio"] for r in rel if r["model"] == m and r["killed"] == "mgr1" and r["detect_ratio"] != ""]
    if all(rm(m) for m in ("server_based", "processor_pool", "integrated")):
        a, b, c = (statistics.mean(rm(m)) for m in ("server_based", "processor_pool", "integrated"))
        L.append(f"H8.3 share detected after mgr1 crash: server_based {a:.2f}, processor_pool {b:.2f}, integrated {c:.2f}: {ok(a < b and a < c)}")
    if n_lo != n_hi:
        grow = {m: (g(m, n_hi, "core_data_msgs_s") / g(m, n_lo, "core_data_msgs_s")) if g(m, n_lo, "core_data_msgs_s") else None for m in ms}
        if None not in grow.values():
            hi_g = statistics.mean(grow[m] for m in ("host_based", "processor_pool"))
            lo_g = statistics.mean(grow[m] for m in ms if m not in ("host_based", "processor_pool"))
            L.append(f"H8.4 core data-plane load growth {n_lo}->{n_hi} sites: " + ", ".join(f"{m} x{v:.1f}" for m, v in grow.items()) +
                     f": {ok(hi_g > lo_g)}")
    if rel_ctrl:
        def r_(m, t="mgr1"):
            xs = [r["detect_ratio"] for r in rel_ctrl if r["model"] == m and r["killed"] == t and r["detect_ratio"] != ""]
            return statistics.mean(xs) if xs else None
        a, b, c, d = r_("integrated"), r_("integrated_noft"), r_("server_based"), r_("server_based_ft")
        if None not in (a, b, c, d):
            L.append(f"H8.5 share detected after mgr1 crash: integrated {a:.2f}, integrated without FT {b:.2f}, server_based {c:.2f}, "
                     f"server_based + FT {d:.2f}: FT is the cause: {ok(d > c and a > b)}")
    return L


# ---------------------------------------------------------------- main
async def main():
    acquire_lock()
    demo, quick = "--demo" in sys.argv, "--quick" in sys.argv
    arg = lambda name, default: sys.argv[sys.argv.index(name) + 1] if name in sys.argv else default
    parts = set(arg("--part", "perf,rel,stress").split(","))
    if quick and "--part" not in sys.argv:
        parts.discard("stress")
    if demo:
        parts = {"perf"}
    sizes = (6,) if demo else ((3, 9) if quick else (3, 6, 9))
    if "--sizes" in sys.argv:                              # e.g. --sizes 3,9
        sizes = tuple(int(x) for x in arg("--sizes", "").split(","))
    trials = int(arg("--trials", 1 if (demo or quick) else 2))
    duration = float(arg("--duration", 10.0 if demo else 15.0))
    controls = "--controls" in sys.argv or not (demo or quick or "--no-controls" in sys.argv)
    n_ref = 9 if 9 in sizes else sizes[-1]
    n_lo, n_hi = sizes[0], sizes[-1]
    write_env({"experiment": "week8", "sizes": sizes, "trials": trials, "duration_s": duration, "sensor_interval_s": 0.3,
               "parts": sorted(parts), "controls": controls, "cost_model": COST})

    perf, rel, stress = [], [], []
    if "perf" in parts:
        for n in sizes:
            for t in range(trials):
                for model in MODELS:
                    r = await performance(model, n, duration)
                    perf.append({"trial": t, **r})
                    print(f"  {model:15} {n} sites  detect {r['detect_mean_ms']} ms ({r['detected']}/{r['faults']})  thr {r['throughput_inc_s']}/s  "
                          f"core in {r['core_in_msgs_s']}/s (data {r['core_data_msgs_s']})  mgr in {r['mgr_in_msgs_s']}/s  net {r['net_msgs_s']}/s  "
                          f"cpu site/mgr/core {r['cpu_site_pct']}/{r['cpu_mgr_pct']}/{r['cpu_core_pct']}", flush=True)
        if controls:
            n_c = 6 if 6 in sizes else n_ref
            for t in range(trials):
                for model in CONTROLS:
                    r = await performance(model, n_c, duration)
                    perf.append({"trial": t, **r})
                    print(f"  control {model:15} {n_c} sites  detect {r['detect_mean_ms']} ms ({r['detected']}/{r['faults']})  "
                          f"core in {r['core_in_msgs_s']}/s  net {r['net_msgs_s']}/s", flush=True)
    avg = mean_rows(perf, ["model", "sites"]) if perf else []
    costs = [cost(r["model"], r["sites"], r) for r in avg]
    if perf:
        print(f"\n{'model':16}{'sites':>6}{'detect ms':>11}{'thr inc/s':>11}{'core in/s':>11}{'core data/s':>12}{'mgr in/s':>10}{'net/s':>9}{'cpu core %':>11}{'cost':>8}")
        for r in avg:
            c = next(x for x in costs if x["model"] == r["model"] and x["sites"] == r["sites"])
            print(f"{r['model']:16}{r['sites']:>6}{r['detect_mean_ms']:>11}{r['throughput_inc_s']:>11}{r['core_in_msgs_s']:>11}"
                  f"{r['core_data_msgs_s']:>12}{r['mgr_in_msgs_s']:>10}{r['net_msgs_s']:>9}{r['cpu_core_pct']:>11}{c['total_cost']:>8}")
        if not demo:
            write_csv("results/week8_models.csv", perf)
            write_csv("results/week8_summary.csv", summary_rows(
                perf, ["model", "sites"], ["detect_mean_ms", "detect_p95_ms", "detect_ratio", "throughput_inc_s", "core_in_msgs_s",
                                           "core_data_msgs_s", "mgr_in_msgs_s", "mgr_data_msgs_s", "net_msgs_s", "cpu_site_pct", "cpu_mgr_pct", "cpu_core_pct",
                                           "mem_site_mb", "mem_mgr_mb", "mem_core_mb"]))
            write_csv("results/week8_cost.csv", costs)
    if demo:
        return

    if "rel" in parts:
        names = list(MODELS) + (list(CONTROLS) if controls else [])
        for t in range(trials):
            for model in names:
                for target in ("mgr1", "core"):
                    r = await reliability(model, target)
                    rel.append({"trial": t, **r})
                    print(f"  reliability t{t} {model:15} kill {target:5} -> detected {r['detected']}/{r['faults_in_outage']} "
                          f"({r['detect_ratio']})", flush=True)
        write_csv("results/week8_reliability.csv", rel)
        print(f"\n{'model':16}{'killed':>8}{'share detected (mean±sd)':>28}")
        for r in summary_rows(rel, ["model", "killed"], ["detect_ratio"]):
            print(f"{r['model']:16}{r['killed']:>8}{r['detect_ratio_mean']:>16}±{r['detect_ratio_sd']}")

    if "stress" in parts:
        for model in MODELS:
            for interval in (0.3, 0.1, 0.05):
                r = await performance(model, n_ref, duration, interval)
                r["offered_readings_s"] = round(n_ref / interval, 1)
                stress.append(r)
                print(f"  stress {model:15} {n_ref} sites {r['offered_readings_s']:>6} readings/s  detected {r['detected']}/{r['faults']} "
                      f"({r['detect_ratio']})  thr {r['throughput_inc_s']}/s  latency {r['detect_mean_ms']} ms  core cpu {r['cpu_core_pct']}%", flush=True)
        write_csv("results/week8_stress.csv", stress)
        print(f"\nSaturation: first load at which fewer than 90% of faults are detected within {MATCH_S:.0f} s")
        for model in MODELS:
            rows = sorted((r for r in stress if r["model"] == model), key=lambda r: r["interval_s"], reverse=True)
            sat = next((r["offered_readings_s"] for r in rows if r["detect_ratio"] != "" and r["detect_ratio"] < 0.9), None)
            print(f"  {model:15} {'saturates at about ' + str(sat) + ' readings/s' if sat else 'did not saturate up to ' + str(rows[-1]['offered_readings_s']) + ' readings/s'}")

    if perf and (rel or stress):
        drows, table, crit_used = decision(avg, [r for r in rel if r["model"] in MODELS], stress, sizes, n_ref)
        write_csv("results/week8_decision.csv", drows)
        print_decision(table, crit_used)
    if perf and rel:
        L = verdicts(avg, rel, costs, n_lo, n_hi, n_ref, [r for r in rel if r["model"] in ALL])
        print("\nHypothesis check (means over trials; sd/CI in results/week8_summary.csv)")
        for line in L:
            print("  " + line)
        with open("results/week8_hypotheses.txt", "w") as f:
            f.write("\n".join(L) + "\n")
        print("saved results/week8_hypotheses.txt")


if __name__ == "__main__":
    asyncio.run(main())
