"""Milestone 7: fault tolerance and recovery on the real cluster (3 sites, 2 managers, core, cloud).

Real faults, real recovery: nodes are killed with SIGKILL and relaunched, message loss, a stopped service and a lying
manager are injected with FT_INJECT. Every scenario runs for BOTH systems on the same cluster with the same seeds:
   baseline  = the original system (fire-and-forget, a site always talks to its own manager, nobody watches the sites)
   proposed  = failure detection + backup-manager failover + incident replication + store-and-forward outbox +
               soft-state re-registration + site-down detection (services/faulttolerance.py)

Scenarios (fault at T_FAULT, repaired at T_REPAIR, run ends at T_END). Fault model = brief section "Consider":
   manager_crash     crash / node failure           mgr1 is killed, relaunched at T_REPAIR
   core_crash        crash / node failure           the core is killed, relaunched at T_REPAIR
   site_crash        crash / node failure (edge)    site1 is killed, relaunched at T_REPAIR
   omission          omission failure               mgr1 loses 70% of its outgoing messages between T_FAULT and T_REPAIR
   partition         communication failure          mgr1 loses 100% of its outgoing messages (cut off from core and backup)
   service_failure   service failure                mgr1's correlation service stops (node alive, acks "unhealthy")
   byzantine         arbitrary (Byzantine) failure  mgr1 fabricates incidents for its healthy sites
   none              control                        no fault: used by the sweep to count FALSE failovers / suspicions

Measured (the brief's formulas)
   availability   A = T_up / T_total of the faulted component, probed every 0.25 s.
                  Sites: "up" = the site got a healthy acknowledgement < 0.5 s ago (acks arrive about every 0.25 s). core_crash: the core answers a probe.
                  site_crash: site1 answers a probe. avail_basis says which one was used.
   MTTR           mean length of a contiguous down interval (mean time to recovery).
   downtime_s     total down time per watched component. Unlike A it does not depend on the length of the run.
   coverage       GROUND TRUTH: share of fault episodes that began at a site during the fault window and that the core
                  learned about before the run ended (the fault id travels with the reading: trace_faults).
   delivery       share of escalations generated in the window that reached the core, and their mean delay
   dup_processed  escalations the core processed twice (its duplicate filter is in memory and is lost in a core crash)
   registry_restore_s  core_crash: time after the restart until the core knows every node again (soft state)
   failover_s / detect_s  fault -> first site fails over / fault -> core suspects the node (manager or site)
   bogus_gen / bogus_accepted  byzantine: fabricated incidents sent and accepted by the core

Hypotheses
   H7.1 Without fault tolerance a manager crash or manager service failure takes its sites out for the whole outage;
        with it the outage shrinks to about the detection time (a few acknowledgement periods).
   H7.2 A core crash, a partition or message loss on the manager->core path lose escalations in the baseline; the
        outbox delivers them (late). The duplicate filter keeps them "never twice" ONLY while the core stays up.
   H7.3 Availability = MTBF / (MTBF + MTTR): cutting MTTR is what raises availability.
   H7.4 A crashed tower site goes unnoticed in the baseline; the proposed system detects it from missing re-registrations.
   H7.5 The proposed system tolerates crash and omission faults but NOT Byzantine faults (needs 3f+1 managers).
Controlled: sensor_interval 0.25 s, link 5 ms +-2 ms, seed 42, timeline 5 s / 13 s / 24 s, same cluster for every run.
Independent variable: fault scenario x system (baseline/proposed). In the sweep: miss_limit, detect_timeout.
To stop order effects, odd-numbered trials run the proposed system first.

    python3 -m experiments.week7_faulttolerance --demo              manager crash, baseline vs proposed (about 1.5 min)
    python3 -m experiments.week7_faulttolerance --only omission     one or more scenarios, comma separated
    python3 -m experiments.week7_faulttolerance                     all scenarios, 3 trials (about 25 min)
    python3 -m experiments.week7_faulttolerance --trials 10         more trials for tighter numbers (about 80 min)
    python3 -m experiments.week7_faulttolerance --quick             1 trial
    python3 -m experiments.week7_faulttolerance --sweep             detection-parameter sweep (about 13 min)
Writes results/week7_faults.csv (every run), results/week7_summary.csv (mean, sd, 95% CI) and, with --sweep,
results/week7_sweep.csv. results/environment.txt records the hardware and software.
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

BASE, TMP, LOGS = "config/cluster.json", "config/_week7_tmp.json", "logs/week7"
T_FAULT, T_REPAIR, T_END = 5.0, 13.0, 24.0
PROBE_S, UP_AGE = 0.25, 0.5          # a site is "up" if its last healthy acknowledgement is younger than 0.5 s
SCENARIOS = ("manager_crash", "core_crash", "site_crash", "omission", "partition", "service_failure", "byzantine")
DEFAULTS = {"miss_limit": 3, "detect_timeout": 1.5, "sync_interval": 0.5, "ack_timeout": 0.4,
            "rereg_interval": 1.0, "site_timeout": 3.0}
SWEEP = (("miss_limit", 1), ("miss_limit", 2), ("miss_limit", 3), ("miss_limit", 5),
         ("detect_timeout", 0.75), ("detect_timeout", 1.5), ("detect_timeout", 3.0))
METRICS = ("availability", "downtime_s", "mttr_s", "worst_gap_s", "failover_s", "detect_s", "coverage", "delivery_ratio",
           "delivery_delay_mean_s", "dup_processed", "registry_restore_s", "bogus_gen", "bogus_accepted")


async def ask(client, name, typ, payload=None, timeout=3.0, emulate=False):
    return await client.send(name, client.msg(name, typ, payload or {}), wait_reply=True, emulate=emulate, timeout=timeout)


class Cluster:
    def __init__(self, base_port=28000):
        self.cfg = build(load_config(BASE), 3, 2, base_port=base_port)
        self.cfg["log_dir"], self.cfg["sensor_interval"] = LOGS, 0.25
        self.cfg["log_append"], self.cfg["trace_faults"] = True, True     # relaunched nodes keep their logs; fault ids
        self.cfg["ft"] = {"mode": "baseline", **DEFAULTS}
        self.names = [n["name"] for n in self.cfg["nodes"] if n["role"] != "client"]
        self.sites = [n["name"] for n in self.cfg["nodes"] if n["role"] == "site"]
        self.site_mgr = {n["name"]: n["manager"] for n in self.cfg["nodes"] if n["role"] == "site"}
        self.mode = "baseline"

    async def __aenter__(self):
        shutil.rmtree(LOGS, ignore_errors=True)
        with open(TMP, "w") as f:
            json.dump(self.cfg, f)
        self.procs = launch(TMP, self.names)
        self.client = Node(self.cfg, "client")
        await self.client.start()
        await wait_registered(self.client, [n for n in self.names if n != "core"], timeout=40)
        t0 = time.time()
        while time.time() - t0 < 40:                       # wait until mgr2 leads
            sts = [await ask(self.client, m, "STATS", timeout=1.0) for m in ("mgr1", "mgr2")]
            if all(sts) and all(s.payload["leader"] == "mgr2" for s in sts):
                return self
            await asyncio.sleep(0.3)
        raise RuntimeError("no leader elected")

    async def __aexit__(self, *exc):
        await shutdown(self.client, self.names)
        self.client.tp.server.close()
        await self.client.tp.server.wait_closed()
        for p in self.procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()

    # ----- fault-injection framework (driver side) -----
    def kill(self, name):                                  # node crash: SIGKILL, no goodbye
        p = self.procs[self.names.index(name)]
        p.kill()
        p.wait()

    def restart(self, name):                               # node recovery: relaunch the process in the current mode
        self.procs[self.names.index(name)] = subprocess.Popen(
            [sys.executable, "-m", "nodes.run", "--config", TMP, "--name", name],
            env={**os.environ, "FT_MODE": self.mode})

    async def inject(self, name, **payload):
        return await ask(self.client, name, "FT_INJECT", payload)

    async def configure(self, mode, **params):
        self.mode = mode
        payload = {"mode": mode, "reset": True, **{**DEFAULTS, **params}}
        await asyncio.gather(*[ask(self.client, n, "FT_CONFIG", payload) for n in self.names])


def read_events(node, kind):
    out = []
    try:
        with open(os.path.join(LOGS, f"{node}.jsonl")) as f:
            for line in f:
                try:
                    e = json.loads(line)
                except ValueError:
                    continue
                if e.get("event") == kind:
                    out.append(e)
    except OSError:
        pass
    return out


async def probe(cl, series, stop):
    """Every 0.25 s ask each site how long ago it last got a healthy acknowledgement, and ask the core who it knows."""
    while not stop.is_set():
        t = time.time()
        rs = await asyncio.gather(*[ask(cl.client, s, "STATS", timeout=0.4) for s in cl.sites],
                                  ask(cl.client, "core", "REGISTRY", timeout=0.4))
        for s, r in zip(cl.sites, rs[:-1]):
            series[s].append((t, r.payload["ft_ok_age"] if r else 99.0))
        core = rs[-1]
        series["_core"].append((t, core is not None, core.payload["nodes"] if core else []))
        await asyncio.sleep(max(0.0, PROBE_S - (time.time() - t)))


async def run_one(cl, scen, mode, params=None):
    await cl.configure(mode, **(params or {}))
    await asyncio.sleep(3.0)                               # warm-up: sensors flowing, replicas filled
    series, stop = {s: [] for s in cl.sites}, asyncio.Event()
    series["_core"] = []
    pt = asyncio.create_task(probe(cl, series, stop))
    t0 = time.time()
    ts = {"start": t0, "fault": t0 + T_FAULT, "repair": t0 + T_REPAIR}
    await asyncio.sleep(T_FAULT)
    ts["fault"] = time.time()
    if scen == "manager_crash":
        cl.kill("mgr1")
    elif scen == "core_crash":
        cl.kill("core")
    elif scen == "site_crash":
        cl.kill("site1")
    elif scen == "omission":
        await cl.inject("mgr1", kind="omission", loss=0.7, seconds=T_REPAIR - T_FAULT)
    elif scen == "partition":
        await cl.inject("mgr1", kind="omission", loss=1.0, seconds=T_REPAIR - T_FAULT)
    elif scen == "service_failure":
        await cl.inject("mgr1", kind="service_stop", service="correlation")
    elif scen == "byzantine":
        await cl.inject("mgr1", kind="byzantine", seconds=T_REPAIR - T_FAULT)
    await asyncio.sleep(T_REPAIR - T_FAULT)
    ts["repair"] = time.time()
    if scen == "manager_crash":
        cl.restart("mgr1")
    elif scen == "core_crash":
        cl.restart("core")
    elif scen == "site_crash":
        cl.restart("site1")
    elif scen == "service_failure":
        await cl.inject("mgr1", kind="service_start", service="correlation")
    await asyncio.sleep(T_END - T_REPAIR)
    stop.set()
    await pt
    await asyncio.sleep(1.0)
    return analyse(cl, scen, mode, series, ts)


def scan(points):
    """points = [(t, up)] -> (up samples, total samples, list of outage lengths)."""
    up_n = tot = 0
    outages, down_since, last_t = [], None, None
    for t, up in points:
        tot += 1
        up_n += up
        if not up and down_since is None:
            down_since = t
        if up and down_since is not None:
            outages.append(t - down_since)
            down_since = None
        last_t = t
    if down_since is not None:                             # still down when the run ended
        outages.append(last_t - down_since)
    return up_n, tot, outages


def analyse(cl, scen, mode, series, ts):
    fault, repair, start = ts["fault"], ts["repair"], ts["start"]
    # ---- availability / MTTR of the faulted component ----
    if scen == "core_crash":
        watch, basis = ["_core"], "core"
        pts = {"_core": [(t, up) for t, up, _ in series["_core"]]}
    elif scen == "site_crash":
        watch, basis = ["site1"], "site1"
        pts = {"site1": [(t, age < UP_AGE) for t, age in series["site1"]]}
    else:
        watch = [s for s in cl.sites if cl.site_mgr[s] == "mgr1"] if scen in ("manager_crash", "service_failure") else cl.sites
        basis = "sites of mgr1" if scen in ("manager_crash", "service_failure") else "all sites"
        pts = {s: [(t, age < UP_AGE) for t, age in series[s]] for s in watch}
    up_n = tot = 0
    outages = []
    for k in watch:
        u, n, o = scan(pts[k])
        up_n, tot, outages = up_n + u, tot + n, outages + o
    gap_sites = [s for s in cl.sites if not (scen == "site_crash" and s == "site1")]    # a dead site has no gap to report
    worst = max([min(age, 60.0) for s in gap_sites for t, age in series[s] if t >= fault] or [0.0])
    # ---- soft-state recovery of the core ----
    restore = ""
    if scen == "core_crash":
        need = set(cl.names) - {"core"}
        hit = [t for t, up, nodes in series["_core"] if t >= repair and up and need <= set(nodes)]
        restore = round(min(hit) - repair, 2) if hit else ""
    # ---- detection / failover ----
    fo = [e["ts"] for s in cl.sites for e in read_events(s, "ft_failover") if e["ts"] >= fault]
    if scen == "site_crash":
        susp = [e["ts"] for e in read_events("core", "ft_site_down") if e.get("target") == "site1" and e["ts"] >= fault]
    else:
        susp = [e["ts"] for e in read_events("core", "ft_suspect") if e.get("target") == "mgr1" and e["ts"] >= fault]
    # ---- coverage: fault episodes at the sites (ground truth) vs incidents the core learned about ----
    eps = {e["fid"] for s in cl.sites for e in read_events(s, "sensor_fault") if fault <= e["ts"] < repair}
    known = {e.get("fid") for e in read_events("core", "incident_escalated") if e["ts"] >= fault}
    coverage = round(len(eps & known) / len(eps), 3) if eps else ""
    # ---- delivery of escalations ----
    hi = repair if scen in ("core_crash", "omission", "partition", "byzantine") else float("inf")
    senders = ("mgr1",) if scen in ("omission", "partition", "byzantine") else ("mgr1", "mgr2")
    gen, bogus_gen = {}, 0
    for m in senders:
        for e in read_events(m, "ft_escalate"):
            if fault <= e["ts"] <= hi:
                if e.get("bogus"):
                    bogus_gen += 1
                else:
                    gen[e["eid"]] = e["ts"]
    rec = [e for e in read_events("core", "ft_receive") if e["ts"] >= start]     # this run only (eids restart every run)
    recv = {e["eid"]: e["delay"] for e in rec if e["ts"] >= fault and not e.get("bogus")}
    bogus_acc = sum(1 for e in rec if e["ts"] >= fault and e.get("bogus"))
    cnt = {}
    for e in rec:
        cnt[e["eid"]] = cnt.get(e["eid"], 0) + 1
    dup = sum(c - 1 for c in cnt.values() if c > 1)
    delivered = [recv[k] for k in gen if k in recv]
    return {
        "scenario": scen, "mode": mode, "avail_basis": basis,
        "availability": round(up_n / tot, 4) if tot else "",
        "outages": len(outages), "mttr_s": round(statistics.mean(outages), 2) if outages else 0.0,
        "downtime_s": round(sum(outages) / len(watch), 2),
        "worst_gap_s": round(worst, 2),
        "failover_s": round(min(fo) - fault, 2) if fo else "",
        "detect_s": round(min(susp) - fault, 2) if susp else "",
        "n_failovers": len([1 for s in cl.sites for e in read_events(s, "ft_failover") if e["ts"] >= start]),
        "n_suspects": len([1 for e in read_events("core", "ft_suspect") if e["ts"] >= start]),
        "faults_in_window": len(eps), "coverage": coverage,
        "esc_generated": len(gen), "esc_delivered": len(delivered),
        "delivery_ratio": round(len(delivered) / len(gen), 3) if gen else "",
        "delivery_delay_mean_s": round(statistics.mean(delivered), 2) if delivered else "",
        "dup_processed": dup, "registry_restore_s": restore,
        "bogus_gen": bogus_gen, "bogus_accepted": bogus_acc,
    }


def say(r, trial, order):
    v = lambda x: "-" if x == "" else f"{x}s"
    print(f"  t{trial} {r['scenario']:15} {r['mode']:9} A={r['availability']}  mttr {r['mttr_s']}s  down {r['downtime_s']}s  "
          f"detect {v(r['detect_s'])}  failover {v(r['failover_s'])}  coverage {r['coverage']}  "
          f"delivery {r['delivery_ratio']} ({r['esc_delivered']}/{r['esc_generated']})", flush=True)


def narrate(base, prop):
    """Demo only: the Failure -> Detection -> Recovery -> Service restoration story, in numbers."""
    print("\n  Failure -> Detection -> Recovery -> Service restoration (manager crash: mgr1 killed, relaunched at +8 s)")
    print(f"    baseline  FAILURE mgr1 killed -> DETECTION none (nobody notices) -> RECOVERY only when mgr1 is relaunched"
          f" -> its sites are down for {base['mttr_s']} s, availability {base['availability']}, coverage {base['coverage']}")
    print(f"    proposed  FAILURE mgr1 killed -> DETECTION core suspects mgr1 after {prop['detect_s']} s"
          f" -> RECOVERY sites fail over to mgr2 after {prop['failover_s']} s"
          f" -> RESTORATION outage {prop['mttr_s']} s, availability {prop['availability']}, coverage {prop['coverage']}")


def fmt(xs, nd=2):
    m, sd = mean_sd(xs)
    return "-" if m is None else f"{m:.{nd}f}±{sd:.{nd}f}"


def summarise(rows, scens):
    out = []
    for scen in scens:
        for mode in ("baseline", "proposed"):
            g = [r for r in rows if r["scenario"] == scen and r["mode"] == mode]
            if not g:
                continue
            row = {"scenario": scen, "mode": mode, "n": len(g), "avail_basis": g[0]["avail_basis"]}
            for k in METRICS:
                xs = [r[k] for r in g if r[k] != ""]
                m, sd = mean_sd(xs)
                c = ci95(xs)
                row[k + "_mean"] = "" if m is None else round(m, 3)
                row[k + "_sd"] = "" if sd is None else round(sd, 3)
                row[k + "_ci95"] = "" if c is None else round(c, 3)
            out.append(row)
    return out


def get(summ, scen, mode, k):
    r = next((x for x in summ if x["scenario"] == scen and x["mode"] == mode), None)
    v = r.get(k + "_mean", "") if r else ""
    return None if v == "" else v


def verdicts(summ, rows):
    print("\nHypothesis check (means over trials; compare with the sd/CI in results/week7_summary.csv)")
    have = {r["scenario"] for r in summ}
    ok = lambda b: "SUPPORTED" if b else "NOT SUPPORTED"
    for sc in ("manager_crash", "service_failure"):
        if sc in have:
            bm, pm = get(summ, sc, "baseline", "mttr_s"), get(summ, sc, "proposed", "mttr_s")
            ba, pa = get(summ, sc, "baseline", "availability"), get(summ, sc, "proposed", "availability")
            bc, pc = get(summ, sc, "baseline", "coverage"), get(summ, sc, "proposed", "coverage")
            if None not in (bm, pm, ba, pa):
                print(f"  H7.1 {sc:16} MTTR {bm}s -> {pm}s, availability {ba} -> {pa}, coverage {bc} -> {pc}: {ok(pm < bm and pa > ba)}")
    for sc in ("core_crash", "partition", "omission"):
        if sc in have:
            bd, pd = get(summ, sc, "baseline", "delivery_ratio"), get(summ, sc, "proposed", "delivery_ratio")
            bc, pc = get(summ, sc, "baseline", "coverage"), get(summ, sc, "proposed", "coverage")
            dup = get(summ, sc, "proposed", "dup_processed")
            if None not in (bd, pd):
                print(f"  H7.2 {sc:16} delivery {bd} -> {pd}, coverage {bc} -> {pc}, duplicates processed (proposed) {dup}: {ok(pd > bd)}")
    if "core_crash" in have:
        rr, rb = get(summ, "core_crash", "proposed", "registry_restore_s"), get(summ, "core_crash", "baseline", "registry_restore_s")
        print(f"  soft state: after a core restart the core knows every node again after {rr} s (proposed) vs "
              f"{'never within the run' if rb is None else str(rb) + ' s'} (baseline)")
    pairs = []
    for sc in have:
        bm, pm = get(summ, sc, "baseline", "mttr_s"), get(summ, sc, "proposed", "mttr_s")
        ba, pa = get(summ, sc, "baseline", "availability"), get(summ, sc, "proposed", "availability")
        if None not in (bm, pm, ba, pa) and abs(bm - pm) > 0.2:
            pairs.append((sc, pm - bm, pa - ba))
    if pairs:
        good = all((dm < 0) == (da > 0) for _, dm, da in pairs)
        print("  H7.3 " + "; ".join(f"{sc}: dMTTR {dm:+.2f}s -> dA {da:+.3f}" for sc, dm, da in pairs) + f": {ok(good)}")
    if "site_crash" in have:
        bd, pd = get(summ, "site_crash", "baseline", "detect_s"), get(summ, "site_crash", "proposed", "detect_s")
        print(f"  H7.4 site_crash        core detects the dead site after: baseline {'never' if bd is None else str(bd) + ' s'}, proposed {pd} s: "
              f"{ok(bd is None and pd is not None)}")
    if "byzantine" in have:
        g, a = get(summ, "byzantine", "proposed", "bogus_gen"), get(summ, "byzantine", "proposed", "bogus_accepted")
        gb, ab = get(summ, "byzantine", "baseline", "bogus_gen"), get(summ, "byzantine", "baseline", "bogus_accepted")
        print(f"  H7.5 byzantine         fabricated incidents accepted by the core: baseline {ab}/{gb}, proposed {a}/{g}: "
              f"{ok(bool(a) and bool(ab))}  (both modes trust the manager: crash-tolerant, not Byzantine-tolerant)")


def print_table(summ):
    keys = ["availability", "mttr_s", "worst_gap_s", "detect_s", "failover_s", "coverage", "delivery_ratio"]
    print(f"\nFailure comparison, mean±sd over trials (A and MTTR refer to the faulted component, see avail_basis)")
    print(f"{'scenario':16}{'mode':10}{'n':>3}" + "".join(f"{k[:14]:>15}" for k in keys))
    for r in summ:
        vals = []
        for k in keys:
            m, sd = r[k + "_mean"], r[k + "_sd"]
            vals.append("-" if m == "" else f"{m:.2f}±{sd:.2f}")
        print(f"{r['scenario']:16}{r['mode']:10}{r['n']:>3}" + "".join(f"{v:>15}" for v in vals))


def write_csv(path, rows):
    os.makedirs("results", exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print("saved", path)


async def sweep(cl, trials):
    """Detection speed against false alarms. none = no fault: every failover or suspicion there is a FALSE one."""
    rows = []
    for k, v in SWEEP:
        for t in range(trials):
            for scen in ("none", "manager_crash"):
                r = await run_one(cl, scen, "proposed", {k: v})
                rows.append({"param": k, "value": v, "trial": t, "scenario": scen, "failover_s": r["failover_s"],
                             "detect_s": r["detect_s"], "n_failovers": r["n_failovers"], "n_suspects": r["n_suspects"],
                             "availability": r["availability"], "coverage": r["coverage"]})
                print(f"  {k}={v:<5} t{t} {scen:14} failover {r['failover_s']}  detect {r['detect_s']}  "
                      f"failovers {r['n_failovers']}  suspicions {r['n_suspects']}  availability {r['availability']}", flush=True)
    write_csv("results/week7_sweep.csv", rows)
    print(f"\n{'parameter':16}{'value':>7}{'failover s':>12}{'detect s':>10}{'false failovers':>17}{'false suspicions':>18}")
    for k, v in SWEEP:
        crash = [r for r in rows if r["param"] == k and r["value"] == v and r["scenario"] == "manager_crash"]
        none = [r for r in rows if r["param"] == k and r["value"] == v and r["scenario"] == "none"]
        f = lambda g, c: fmt([x[c] for x in g], 2)
        print(f"{k:16}{v:>7}{f(crash, 'failover_s'):>12}{f(crash, 'detect_s'):>10}{f(none, 'n_failovers'):>17}{f(none, 'n_suspects'):>18}")


async def main():
    acquire_lock()
    demo, quick, do_sweep = "--demo" in sys.argv, "--quick" in sys.argv, "--sweep" in sys.argv
    trials = 1 if (demo or quick) else 3
    if "--trials" in sys.argv:
        trials = int(sys.argv[sys.argv.index("--trials") + 1])
    scens = ("manager_crash",) if demo else SCENARIOS
    if "--only" in sys.argv:                               # e.g. --only omission,partition
        scens = tuple(sys.argv[sys.argv.index("--only") + 1].split(","))
    write_env({"experiment": "week7", "trials": trials, "timeline_s": f"{T_FAULT}/{T_REPAIR}/{T_END}",
               "ft_defaults": DEFAULTS, "sensor_interval_s": 0.25})
    rows = []
    async with Cluster() as cl:
        if do_sweep:
            await sweep(cl, 2 if "--trials" not in sys.argv else trials)
            return
        for scen in scens:
            for t in range(trials):
                order = ("baseline", "proposed") if t % 2 == 0 else ("proposed", "baseline")   # alternate: no order effect
                for pos, mode in enumerate(order):
                    r = await run_one(cl, scen, mode)
                    rows.append({"trial": t, "order": pos + 1, **r})
                    say(r, t, pos + 1)
                if demo:
                    narrate(*[next(x for x in rows if x["mode"] == m and x["trial"] == t) for m in ("baseline", "proposed")])
    if not demo:
        write_csv("results/week7_faults.csv", rows)
        summ = summarise(rows, scens)
        write_csv("results/week7_summary.csv", summ)
        print_table(summ)
        verdicts(summ, rows)


if __name__ == "__main__":
    asyncio.run(main())
