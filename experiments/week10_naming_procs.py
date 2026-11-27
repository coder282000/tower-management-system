"""Milestone 10: naming and distributed process management, measured on the real cluster.

NAMING   Name -> Name Resolution -> Resource Location.  Six resolution schemes behind one interface (services/transparency.py):
           static            a table in the client                           0 messages, stale after any change
           broadcast         ask every holder "who has this name?"           2 x holders messages, no name state anywhere
           nameserver        one request to the master (core)                2 messages
           nameserver+cache  the same with a time-to-live cache              0 messages on a hit, stale until the TTL ends
           prefix            DNS/Sprite prefix table: "r1/" -> zone server   2 messages when warm, +2 for the first referral
           prefix+cache      the same with a TTL cache
         The master holds every record and changes them; each manager is a read-only ZONE server for its prefix ("r1/", "r2/", ...).
PROCESS  Create -> Locate -> Schedule -> Coordinate -> Terminate  (services/procmgmt.py): workflows of dependent processes, run by the
         elected leader, state kept in the replicated store (Milestone 9) so a new leader can carry on, placement policies
         round_robin | random | registry (stale loads, the Milestone 2 policy) | probe2 (two fresh probes), names proc/<pid> resolved by the
         schemes above.

Experiments (--part)
  resolution   A  latency and messages per resolution, cold and warm, for every scheme
  staleness    B  20% of the names are migrated: wrong answers right after, and time until every scheme answers correctly
  scale        C  scalability: (1) number of names, (2) concurrent resolvers and load per server, (3) number of nodes
  nsfailure    D  the master (name server and root) is killed: which schemes keep resolving?
  lifecycle    E  one workflow, step by step: create, schedule, locate (every scheme), coordinate, terminate
  policy       F  four placement policies under load: completion time, balance, messages
  procscale    G  1 / 8 / 32 concurrent workflows, with and without durable state
  coordfail    H  the coordinator (leader) is killed mid-run: durable state vs none
  workerfail   I  a worker node is killed mid-run: processes are rescheduled
  terminate    J  group termination: time, resources released, names removed

Hypotheses
  H10.1 Messages per resolution: static 0, nameserver 2, prefix 2 (warm), broadcast 2 x holders; every scheme resolves correctly
        when nothing changes.
  H10.2 After a migration static and cached resolvers answer wrongly (a static map for ever, a cache until its TTL); nameserver and
        broadcast never; prefix converges within a fraction of a second (the master pushes to the zone servers).
  H10.3 Scalability: broadcast cost grows linearly with the number of nodes, nameserver and prefix cost does not; the static table grows
        linearly with the number of names; prefix spreads the resolution load over the zone servers (the master serves a small share).
  H10.4 When the master dies, an uncached nameserver client cannot resolve any more, while broadcast, prefix and cached clients can.
  H10.5 Probing two random workers (probe2) balances processes better and finishes workflows sooner than the stale registry policy,
        for two more messages per process.
  H10.6 With the process table in the replicated store, killing the coordinator loses no workflow and runs no process twice;
        with in-memory state the unfinished workflows are lost.
  H10.7 When a worker dies its processes are rescheduled elsewhere and every workflow still completes.
  H10.8 Terminating a workflow kills all its processes, frees their resources and removes their names quickly.
  H10.9 Throughput saturates as concurrent workflows grow (the coordinator is the bottleneck); durable state costs latency per phase.
Controlled: link 5 ms +-2 ms, seed 42, one machine (1 vCPU): client-side load generation shares the CPU, so absolute throughput is a lower bound.

    python3 -m experiments.week10_naming_procs --demo           narrated, about 2.5 min
    python3 -m experiments.week10_naming_procs --quick          1 trial of everything, about 12 min
    python3 -m experiments.week10_naming_procs                  3 trials, about 40 min
    options: --part resolution,staleness,...   --trials N
Writes results/week10_*.csv, week10_summary.csv (mean, sd, CI), week10_hypotheses.txt, environment.txt.
"""
import asyncio
import csv
import json
import os
import random
import statistics
import sys
import time

from experiments._common import acquire_lock, ci95, mean_sd, write_env
from experiments._lab import Lab, drive, ms, pct
from services.procmgmt import ProcessClient
from services.transparency import make_resolver

PARTS = ("resolution", "staleness", "scale", "nsfailure", "lifecycle", "policy", "procscale", "coordfail", "workerfail", "terminate")
SCHEMES = {"static": ("static", 0), "broadcast": ("broadcast", 0), "nameserver": ("nameserver", 0),
           "nameserver+cache": ("nameserver", 2.0), "prefix": ("prefix", 0), "prefix+cache": ("prefix", 2.0)}
NAMING_PARTS = ("resolution", "staleness", "scale", "nsfailure")


def mk(lab, label, table=None, ttl=None):
    sch, t = SCHEMES[label]
    return make_resolver(sch, lab.client, lab.cfg, ttl=t if ttl is None else (ttl if t else 0), table=table)


def names_for(n, regions):
    return [f"r{i % regions + 1}/site{i // regions + 1}/status" for i in range(n)]


def f(x, d=1):
    return "n/a" if x is None else f"{x:.{d}f}"


def write_csv(path, rows):
    if not rows:
        return
    os.makedirs("results", exist_ok=True)
    cols = list(dict.fromkeys(k for r in rows for k in r))              # rows of different sub-tests have different columns
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, restval="")
        w.writeheader()
        w.writerows(rows)
    print("saved", path)


def summarise(rows, keys, fields):
    groups = {}
    for r in rows:
        groups.setdefault(tuple(r[k] for k in keys), []).append(r)
    out = []
    for k, g in groups.items():
        row = {**dict(zip(keys, k)), "n": len(g)}
        for fld in fields:
            xs = [x[fld] for x in g if x.get(fld) not in ("", None)]
            m, sd = mean_sd(xs)
            c = ci95(xs)
            row[fld + "_mean"] = "" if m is None else round(m, 3)
            row[fld + "_sd"] = "" if sd is None else round(sd, 3)
            row[fld + "_ci95"] = "" if c is None else round(c, 3)
        out.append(row)
    return out


def counters(res):
    return res.sent + res.recv


async def setup_names(lab, n, rf=2):
    await lab.reset()
    names = names_for(n, len(lab.managers))
    r = await lab.admin.create(names, rf=rf, val={"s": "ok"}, want_recs=True)
    return names, {x["name"]: x for x in r["recs"]}


# ============================================================================ A. resolution
async def part_resolution(lab, trials, n_names=300, m=600):
    rows = []
    holders = len(lab.cfg["lrs"]["storage"])
    for trial in range(trials):
        names, table = await setup_names(lab, n_names)
        rng = random.Random(100 + trial)
        for label in SCHEMES:
            cold = []
            for nm in rng.sample(names, 12):                       # a fresh client's very first resolution
                r = mk(lab, label, table)
                t0 = time.time()
                await r.resolve(nm)
                cold.append(time.time() - t0)
            res = mk(lab, label, table)
            for nm in rng.sample(names, 30):                       # warm-up: prefix table learned, caches partly filled
                await res.resolve(nm)
            await asyncio.sleep(0.2)                               # replies of earlier broadcasts still in flight
            c0, l0, h0 = counters(res), res.lookups, res.hits
            lat, wrong = [], 0
            for _ in range(m):
                nm = rng.choice(names)
                t0 = time.time()
                rec = await res.resolve(nm)
                lat.append(time.time() - t0)
                wrong += (rec is None or rec["primary"] != table[nm]["primary"])
            await asyncio.sleep(0.3)
            rows.append({"trial": trial, "scheme": label, "holders": holders, "cold_first_ms": ms(statistics.mean(cold)),
                         "warm_mean_ms": ms(statistics.mean(lat)), "warm_p95_ms": ms(pct(lat, 0.95)),
                         "msgs_per_res": round((counters(res) - c0) / m, 2), "hit_rate": round((res.hits - h0) / m, 3),
                         "wrong_pct": round(100 * wrong / m, 2)})
            r = rows[-1]
            print(f"  resolution t{trial} {label:17} cold {r['cold_first_ms']} ms  warm {r['warm_mean_ms']} ms  msgs {r['msgs_per_res']}  hit {r['hit_rate']}  wrong {r['wrong_pct']}%", flush=True)
    return rows


# ============================================================================ B. staleness
async def part_staleness(lab, trials, n_names=100, n_move=20):
    rows = []
    for trial in range(trials):
        names, table = await setup_names(lab, n_names)
        resolvers = {label: mk(lab, label, table, ttl=3.0) for label in SCHEMES}
        for label in sorted(resolvers, key=lambda x: "cache" in x):    # the cached ones are warmed LAST so their entries are fresh at the move
            for nm in names:
                await resolvers[label].resolve(nm)                 # everybody knows every name
        moved = names[:n_move]
        async def move(nm):
            rec = table[nm]
            dest = next(h for h in lab.cfg["lrs"]["storage"] if h != rec["primary"] and h not in rec["backups"])
            return nm, (await lab.admin.migrate(nm, dest))["rec"]["primary"]

        truth = dict(await asyncio.gather(*[move(nm) for nm in moved]))
        t_mig = time.time()
        for label, res in sorted(resolvers.items(), key=lambda kv: "cache" not in kv[0]):    # the TTL clock is running: check cached ones first
            wrong_moved = wrong_other = 0
            for nm in names:
                rec = await res.resolve(nm)
                bad = rec is None or rec["primary"] != (truth.get(nm) or table[nm]["primary"])
                if nm in truth:
                    wrong_moved += bad
                else:
                    wrong_other += bad
            conv = 0.0 if wrong_moved == 0 else None
            if conv is None:
                while time.time() - t_mig < 5.0:                       # how long until every moved name is answered correctly?
                    await asyncio.sleep(0.05)
                    recs = [await res.resolve(nm) for nm in moved]
                    if all(rc is not None and rc["primary"] == truth[nm] for rc, nm in zip(recs, moved)):
                        conv = time.time() - t_mig
                        break
            rows.append({"trial": trial, "scheme": label, "wrong_moved_pct": round(100 * wrong_moved / n_move, 1),
                         "wrong_unmoved_pct": round(100 * wrong_other / (n_names - n_move), 1), "converge_s": None if conv is None else round(conv, 3)})
            r = rows[-1]
            print(f"  staleness t{trial} {label:17} wrong right after the move: {r['wrong_moved_pct']}% of the moved names  consistent after {r['converge_s']} s", flush=True)
    return rows


# ============================================================================ C. scalability
async def resolve_load(lab, label, table, names, conc, dur=4.0):
    res = [mk(lab, label, table) for _ in range(conc)]
    for r in res:
        await r.resolve(names[0])
    s0 = {nd: await lab.stats(nd) for nd in lab.names}
    lat, stop = [], time.time() + dur
    rng = random.Random(5)

    async def worker(r):
        while time.time() < stop:
            t0 = time.time()
            await r.resolve(rng.choice(names))
            lat.append(time.time() - t0)

    t0 = time.time()
    await asyncio.gather(*[worker(r) for r in res])
    el = time.time() - t0
    s1 = {nd: await lab.stats(nd) for nd in lab.names}
    d = {nd: s1[nd]["ns_resolve"] - s0[nd]["ns_resolve"] for nd in lab.names}
    who = sum(s1[nd]["lrs_who"] - s0[nd]["lrs_who"] for nd in lab.names)
    tot = sum(d.values())
    return {"throughput_per_s": round(len(lat) / el, 1), "p95_ms": ms(pct(lat, 0.95)), "mean_ms": ms(statistics.mean(lat)),
            "core_share_pct": round(100 * d["core"] / tot, 1) if tot else None, "who_per_res": round(who / len(lat), 2) if who else 0}


async def part_scale(lab, trials):
    rows = []
    for trial in range(trials):
        for n in (50, 500, 2000):                                   # C1: number of names
            t0 = time.time()
            names, table = await setup_names(lab, n)
            create_s = time.time() - t0
            core = await lab.stats("core")
            for label in ("static", "nameserver", "prefix", "broadcast"):
                res = mk(lab, label, table)
                rng = random.Random(7)
                for nm in rng.sample(names, 20):
                    await res.resolve(nm)
                lat = []
                for _ in range(200):
                    t1 = time.time()
                    await res.resolve(rng.choice(names))
                    lat.append(time.time() - t1)
                rows.append({"trial": trial, "test": "names", "x": n, "scheme": label, "mean_ms": ms(statistics.mean(lat)), "p95_ms": ms(pct(lat, 0.95)),
                             "static_table_kb": round(len(json.dumps(table)) / 1024, 1) if label == "static" else None,
                             "ns_records": core["ns_records"], "create_s": round(create_s, 2)})
                r = rows[-1]
                print(f"  scale t{trial} names={n:5} {label:11} {r['mean_ms']} ms  records {r['ns_records']}  table {r['static_table_kb']} KB  create {r['create_s']} s", flush=True)
        names, table = await setup_names(lab, 500)                  # C2: concurrent resolvers
        for conc in (1, 8, 32):
            for label in ("nameserver", "prefix", "broadcast"):
                r = await resolve_load(lab, label, table, names, conc)
                rows.append({"trial": trial, "test": "load", "x": conc, "scheme": label, **r})
                print(f"  scale t{trial} resolvers={conc:2} {label:11} {r['throughput_per_s']:7} res/s  p95 {r['p95_ms']} ms  core serves {r['core_share_pct']}%  who/res {r['who_per_res']}", flush=True)
    for m in (2, 4, 8):                                             # C3: number of nodes
        async with Lab(f"m10nodes{m}", managers=m, base_port=42000 + 100 * m, extra=()) as l2:
            names, table = await setup_names(l2, 200)
            holders = len(l2.cfg["lrs"]["storage"])
            for trial in range(trials):
                for label in ("nameserver", "prefix", "broadcast"):
                    res = mk(l2, label, table)
                    rng = random.Random(9)
                    for nm in rng.sample(names, 60):
                        await res.resolve(nm)
                    await asyncio.sleep(0.3)
                    c0, lat = counters(res), []
                    for _ in range(200):
                        t1 = time.time()
                        await res.resolve(rng.choice(names))
                        lat.append(time.time() - t1)
                    await asyncio.sleep(0.3)
                    rows.append({"trial": trial, "test": "nodes", "x": holders, "scheme": label, "mean_ms": ms(statistics.mean(lat)), "p95_ms": ms(pct(lat, 0.95)),
                                 "msgs_per_res": round((counters(res) - c0) / 200, 2)})
                    r = rows[-1]
                    print(f"  scale t{trial} holders={holders:2} {label:11} {r['mean_ms']} ms  msgs/res {r['msgs_per_res']}", flush=True)
    return rows


# ============================================================================ D. naming failure
async def part_nsfailure(lab, trials, n_names=60, rate=30, dur=9.0, t_kill=2.0, t_back=7.0):
    rows = []
    for trial in range(trials):
        names, table = await setup_names(lab, n_names)
        rec_before = (await lab.stats("core"))["ns_records"]
        for label in SCHEMES:
            for temp in ("warm", "cold"):
                res = mk(lab, label, table, ttl=60.0)
                if temp == "warm":
                    for nm in names:
                        await res.resolve(nm)
                rng = random.Random(trial + 3)
                ev = {}

                async def chaos():
                    await asyncio.sleep(t_kill)
                    ev["kill"] = lab.kill("core")
                    await asyncio.sleep(t_back - t_kill)
                    ev["back"] = lab.restart("core")

                async def one(i, t):
                    nm = rng.choice(names)
                    t0 = time.time()
                    rec = await res.resolve(nm)
                    return {"ok": rec is not None and rec["primary"] == table[nm]["primary"], "lat": time.time() - t0, "kind": "res"}

                ch = asyncio.create_task(chaos())
                out = await drive(rate, dur, one)
                await ch
                during = [r for r in out if t_kill + 0.3 <= r["t"] < t_back]
                await asyncio.sleep(1.5)
                rec_after = (await lab.stats("core"))["ns_records"]
                on_core = round(100 * sum(1 for nm in names if table[nm]["primary"] == "core") / len(names), 1)
                rows.append({"trial": trial, "scheme": label, "client": temp, "names_on_core_pct": on_core, "success_during_outage_pct": round(100 * sum(r["ok"] for r in during) / len(during), 1),
                             "mean_ms_during": ms(statistics.mean([r["lat"] for r in during])), "records_before": rec_before, "records_after_restart": rec_after})
                r = rows[-1]
                print(f"  nsfailure t{trial} {label:17} {temp:4} resolutions that succeed while the core is down: {r['success_during_outage_pct']}%  (records {rec_before} -> {rec_after} after restart)", flush=True)
                await lab.reset()
                await lab.admin.create(names, rf=2, val={"s": "ok"}, want_recs=False)
    return rows


# ============================================================================ process management helpers
def template(kind="incident", i=0):
    if kind == "mixed":                                              # independent tasks of very different size: placement cannot rely on rotation
        rng = random.Random(1000 + i)
        return {"tasks": [{"id": f"t{j}", "burst": rng.choice([0.1, 0.1, 0.1, 0.2, 1.5])} for j in range(4)]}
    if kind == "chain":
        return {"tasks": [{"id": "a", "burst": 0.5}, {"id": "b", "burst": 0.5, "after": ["a"]}, {"id": "c", "burst": 0.5, "after": ["b"]}]}
    return {"tasks": [{"id": "diagnose", "burst": 0.3, "prio": 1}, {"id": "dispatch", "burst": 0.4, "after": ["diagnose"]},
                      {"id": "fuel", "burst": 0.4, "after": ["diagnose"]}, {"id": "verify", "burst": 0.2, "after": ["dispatch", "fuel"]}]}


async def status_any(lab, wid):
    for m in reversed(lab.managers):
        r = await lab.client.send(m, lab.client.msg(m, "PM_STATUS", {"wid": wid}), wait_reply=True, emulate=False, timeout=1.0)
        if r is not None and r.payload.get("found"):
            return r.payload["wf"]
    return None


async def wait_done(lab, wids, timeout=45.0):
    t0, pending, out = time.time(), set(wids), {}
    while pending and time.time() - t0 < timeout:
        for w in list(pending):
            st = await status_any(lab, w)
            if st and st["state"] != "RUNNING":
                out[w] = st
                pending.discard(w)
        await asyncio.sleep(0.25)
    return out


async def configure_pm(lab, **kw):
    for m in lab.managers:
        await lab.client.send(m, lab.client.msg(m, "PM_CONFIG", kw), wait_reply=True, emulate=False, timeout=2.0)


def pm_events(lab, kinds, t0):
    out = []
    for m in lab.managers:
        out += [e for e in lab.events(m, kinds) if e["ts"] >= t0]
    return out


def executions(lab, t0):
    """pid -> [times started, times completed] over every worker."""
    cnt = {}
    for nd in lab.workers:
        for e in lab.events(nd, {"pm_exec_start", "pm_exec_end"}):
            if e["ts"] >= t0:
                c = cnt.setdefault(e["pid"], [0, 0])
                if e["event"] == "pm_exec_start":
                    c[0] += 1
                elif e.get("status") == "DONE":
                    c[1] += 1
    return cnt


def run_twice(ex):                                                # a genuine fault: the same process COMPLETED more than once
    return sum(1 for s, e in ex.values() if e > 1)


def restarted(ex):                                                # lost with a dead node (or never finished there) and run again
    return sum(1 for s, e in ex.values() if s > 1 and e <= 1)


def pm_metrics(lab, t0, wids, results):
    ev = pm_events(lab, {"pm_wf_created", "pm_task_scheduled", "pm_task_done", "pm_wf_done"}, t0)
    by = lambda k: [e for e in ev if e["event"] == k]
    mk_s = [e["makespan_s"] for e in by("pm_wf_done") if e["state"] == "DONE"]
    sched = by("pm_task_scheduled")
    cnt = {}
    for e in sched:
        cnt[e["node"]] = cnt.get(e["node"], 0) + 1
    vals = [cnt.get(w, 0) for w in lab.workers]
    mean = statistics.mean(vals) if vals else 0
    tdone = [e["ts"] for e in by("pm_task_done")]
    avg = lambda k, rows: round(1000 * statistics.mean([e[k] for e in rows]), 2) if rows else None
    return {"workflows": len(wids), "completed": sum(1 for w in wids if results.get(w, {}).get("state") == "DONE"),
            "makespan_mean_s": round(statistics.mean(mk_s), 3) if mk_s else None, "makespan_p95_s": round(pct(mk_s, 0.95), 3) if mk_s else None,
            "create_ms": avg("create_s", by("pm_wf_created")), "wait_ms": avg("wait_s", sched), "place_ms": avg("place_s", sched), "start_ms": avg("start_s", sched),
            "register_ms": avg("register_s", sched), "schedule_ms": avg("schedule_s", sched), "run_ms": avg("run_s", by("pm_task_done")),
            "balance_cv": round(statistics.pstdev(vals) / mean, 3) if mean else None, "max_share": round(max(vals) / sum(vals), 3) if sum(vals) else None,
            "tasks_per_s": round(len(tdone) / (max(tdone) - t0), 2) if tdone else None}


async def run_load(lab, pc, n, rate, kind="incident"):
    t0, wids = time.time(), []

    async def submit(i, t):
        r = await pc.run(template(kind, i))
        if r:
            wids.append(r["wid"])
        return {"ok": bool(r)}

    await drive(rate, n / rate, submit, t0, count=n)
    return t0, wids, await wait_done(lab, wids)


# ============================================================================ E. lifecycle
async def part_lifecycle(lab, trials):
    rows = []
    pc = ProcessClient(lab.client, mk(lab, "nameserver"), lab.managers)
    for trial in range(trials):
        await lab.reset()
        t0 = time.time()
        r = await pc.run({"tasks": [{"id": "diagnose", "burst": 0.4}, {"id": "dispatch", "burst": 2.0, "after": ["diagnose"]},
                                    {"id": "fuel", "burst": 2.0, "after": ["diagnose"]}, {"id": "verify", "burst": 0.3, "after": ["dispatch", "fuel"]}]})
        wid = r["wid"]
        while True:                                                  # wait until dispatch runs
            st = await pc.status(wid)
            if st["tasks"]["dispatch"]["state"] == "RUNNING":
                break
            await asyncio.sleep(0.05)
        pid = f"{wid}.dispatch"
        truth = st["tasks"]["dispatch"]["node"]
        for label in SCHEMES:
            res = mk(lab, label, None) if label != "static" else None
            if res is None:
                continue                                              # a static table cannot know a process created a moment ago
            pcl = ProcessClient(lab.client, res, lab.managers)
            c0 = counters(res)
            node, lat = await pcl.locate(pid)
            await asyncio.sleep(0.3)                                # broadcast replies still in flight
            rows.append({"trial": trial, "phase": "locate", "what": label, "ms": ms(lat), "msgs": counters(res) - c0, "correct": int(node == truth)})
        while (await pc.status(wid))["state"] == "RUNNING":
            await asyncio.sleep(0.1)
        m = pm_metrics(lab, t0, [wid], {wid: await pc.status(wid)})
        for k, lbl in (("create_ms", "create"), ("schedule_ms", "schedule"), ("wait_ms", "coordinate (dependency wait)"), ("register_ms", "name registration"), ("run_ms", "run")):
            rows.append({"trial": trial, "phase": lbl, "what": "workflow of 4 processes", "ms": m[k], "msgs": None, "correct": None})
        r2 = await pc.run({"tasks": [{"id": f"t{i}", "burst": 5.0} for i in range(6)]})
        await asyncio.sleep(0.8)
        tt = await pc.terminate(r2["wid"])
        rows.append({"trial": trial, "phase": "terminate", "what": "6 running processes", "ms": ms(tt["terminate_s"]), "msgs": None, "correct": int(tt["killed"] == 6)})
        for x in [y for y in rows if y["trial"] == trial]:
            print(f"  lifecycle t{trial} {x['phase']:30} {x['what']:24} {x['ms']} ms  msgs {x['msgs']}  correct {x['correct']}", flush=True)
    return rows


# ============================================================================ F. placement policies
async def part_policy(lab, trials, n=40, rate=4):
    rows = []
    pc = ProcessClient(lab.client, mk(lab, "nameserver"), lab.managers)
    for trial in range(trials):
        for kind in ("incident", "mixed"):                           # identical tasks, then tasks of very different size
            for pol in ("round_robin", "random", "registry", "probe2"):
                await lab.reset()
                await configure_pm(lab, policy=pol, durable=True)
                s0 = await lab.stats(lab.managers[-1])
                t0, wids, res = await run_load(lab, pc, n, rate, kind)
                s1 = await lab.stats(lab.managers[-1])
                m = pm_metrics(lab, t0, wids, res)
                ntasks = sum(len(template(kind, i)["tasks"]) for i in range(len(wids)))
                rows.append({"trial": trial, "workload": kind, "policy": pol, **m, "probes_per_task": round((s1["pm_probes"] - s0["pm_probes"]) / max(1, ntasks), 2)})
                print(f"  policy t{trial} {kind:8} {pol:12} completed {m['completed']}/{n}  makespan {m['makespan_mean_s']} s (p95 {m['makespan_p95_s']})  balance cv {m['balance_cv']}  place {m['place_ms']} ms", flush=True)
    return rows


# ============================================================================ G. process scalability
async def part_procscale(lab, trials):
    rows = []
    pc = ProcessClient(lab.client, mk(lab, "nameserver"), lab.managers)
    for trial in range(trials):
        for durable in (True, False):
            for conc in (1, 8, 32):
                await lab.reset()
                await configure_pm(lab, policy="round_robin", durable=durable)
                t0, wids, res = await run_load(lab, pc, conc, 1000.0)
                m = pm_metrics(lab, t0, wids, res)
                rows.append({"trial": trial, "durable": int(durable), "concurrent": conc, **m})
                print(f"  procscale t{trial} durable={int(durable)} workflows={conc:2} completed {m['completed']}  makespan {m['makespan_mean_s']} s  {m['tasks_per_s']} tasks/s  create {m['create_ms']} ms  schedule {m['schedule_ms']} ms  wait {m['wait_ms']} ms", flush=True)
    return rows


# ============================================================================ H. coordinator failure
async def part_coordfail(lab, trials, n=12):
    rows = []
    pc = ProcessClient(lab.client, mk(lab, "nameserver"), lab.managers)
    for trial in range(trials):
        for durable in (True, False):
            await lab.reset()
            await configure_pm(lab, policy="round_robin", durable=durable)
            t0 = time.time()
            wids = [(await pc.run(template("chain")))["wid"] for _ in range(n)]
            await asyncio.sleep(1.0)
            t_kill = lab.kill(lab.managers[-1])
            res = {}
            t1 = time.time()
            while len(res) < n and time.time() - t1 < 15:
                for w in wids:
                    if w not in res:
                        st = await status_any(lab, w)
                        if st and st["state"] != "RUNNING":
                            res[w] = st
                await asyncio.sleep(0.3)
            t_done = time.time()
            rec = [e["ts"] - t_kill for e in pm_events(lab, {"pm_recovered"}, t_kill)]
            ex = executions(lab, t0)
            rows.append({"trial": trial, "durable": int(durable), "workflows": n, "completed": sum(1 for s in res.values() if s["state"] == "DONE"),
                         "lost": n - len(res), "takeover_s": round(min(rec), 2) if rec else None,
                         "all_done_s": round(t_done - t_kill, 2) if len(res) == n else None,
                         "tasks_run_twice": run_twice(ex), "tasks_restarted": restarted(ex), "tasks_run": len(ex)})
            r = rows[-1]
            print(f"  coordfail t{trial} durable={int(durable)}  completed {r['completed']}/{n}  lost {r['lost']}  new coordinator after {r['takeover_s']} s  completed twice {r['tasks_run_twice']}  restarted after losing their node {r['tasks_restarted']}", flush=True)
    return rows


# ============================================================================ I. worker failure
async def part_workerfail(lab, trials, n=16):
    rows = []
    pc = ProcessClient(lab.client, mk(lab, "nameserver"), lab.managers)
    for trial in range(trials):
        for fault in (False, True):
            await lab.reset()
            await configure_pm(lab, policy="round_robin", durable=True)
            t0 = time.time()
            ev = {}

            async def chaos():
                await asyncio.sleep(0.3)
                ev["kill"] = lab.kill(lab.sites[0])

            ch = asyncio.create_task(chaos()) if fault else None
            t0, wids, res = await run_load(lab, pc, n, 1000.0, "chain")
            if ch:
                await ch
            m = pm_metrics(lab, t0, wids, res)
            resch = len(pm_events(lab, {"pm_task_rescheduled"}, t0))
            ex = executions(lab, t0)
            rows.append({"trial": trial, "fault": int(fault), "workflows": n, "completed": m["completed"], "makespan_mean_s": m["makespan_mean_s"],
                         "makespan_p95_s": m["makespan_p95_s"], "rescheduled": resch, "tasks_run_twice": run_twice(ex), "tasks_restarted": restarted(ex)})
            r = rows[-1]
            print(f"  workerfail t{trial} fault={int(fault)}  completed {r['completed']}/{n}  rescheduled {resch}  makespan mean {r['makespan_mean_s']} s p95 {r['makespan_p95_s']} s", flush=True)
    return rows


# ============================================================================ J. terminate
async def part_terminate(lab, trials, n_tasks=8):
    rows = []
    pc = ProcessClient(lab.client, mk(lab, "nameserver"), lab.managers)
    for trial in range(trials):
        await lab.reset()
        idle = {}
        for w in lab.workers:                                        # sites keep CPU reserved for their sensor/heartbeat services: measure the idle level
            x = await lab.client.send(w, lab.client.msg(w, "PROC_LOAD"), wait_reply=True, emulate=False, timeout=1.0)
            idle[w] = x.payload["free"]
        r = await pc.run({"tasks": [{"id": f"t{i}", "burst": 8.0} for i in range(n_tasks)]})
        await asyncio.sleep(1.2)
        before = await lab.admin.list("proc/")
        t0 = time.time()
        tt = await pc.terminate(r["wid"])
        t_ret = time.time() - t0
        t_free = None
        while time.time() - t0 < 5.0:
            free = []
            for w in lab.workers:
                x = await lab.client.send(w, lab.client.msg(w, "PROC_LOAD"), wait_reply=True, emulate=False, timeout=0.5)
                free.append(x.payload["free"] if x else -1)
            if all(v >= idle[w] for v, w in zip(free, lab.workers)) and not await lab.admin.list("proc/"):
                t_free = time.time() - t0
                break
            await asyncio.sleep(0.05)
        rows.append({"trial": trial, "processes": n_tasks, "names_before": len(before), "killed": tt["killed"], "terminate_call_ms": ms(t_ret),
                     "all_released_ms": ms(t_free), "names_after": len(await lab.admin.list("proc/"))})
        x = rows[-1]
        print(f"  terminate t{trial} killed {x['killed']}/{n_tasks}  call returned in {x['terminate_call_ms']} ms  resources and names released after {x['all_released_ms']} ms  names left {x['names_after']}", flush=True)
    return rows


# ============================================================================ verdicts
def gm(rows, k, **flt):
    xs = [r[k] for r in rows if all(r.get(a) == b for a, b in flt.items()) and r.get(k) not in ("", None)]
    return statistics.mean(xs) if xs else None


def verdicts(R):
    L = []
    ok = lambda b: "SUPPORTED" if b else "NOT SUPPORTED"
    a = R.get("resolution")
    if a:
        hold = a[0]["holders"]
        mg = {s: gm(a, "msgs_per_res", scheme=s) for s in SCHEMES}
        wrong = max(gm(a, "wrong_pct", scheme=s) for s in SCHEMES)
        good = mg["static"] == 0 and abs(mg["nameserver"] - 2) < 0.1 and abs(mg["prefix"] - 2) < 0.1 and abs(mg["broadcast"] - 2 * hold) < 0.1 and wrong == 0
        L.append("H10.1 messages per resolution (warm): " + ", ".join(f"{s} {mg[s]:.2f}" for s in SCHEMES) + f"; holders {hold} so broadcast expects {2 * hold}; "
                 f"cold first lookup ms: " + ", ".join(f"{s} {f(gm(a, 'cold_first_ms', scheme=s))}" for s in ("nameserver", "prefix", "broadcast")) + f"; wrong answers {wrong}%: {ok(good)}")
    b = R.get("staleness")
    if b:
        w = {s: gm(b, "wrong_moved_pct", scheme=s) for s in SCHEMES}
        cv = {s: gm(b, "converge_s", scheme=s) for s in SCHEMES}
        good = w["static"] == 100 and w["nameserver+cache"] > 0 and w["nameserver"] == 0 and w["broadcast"] == 0 and (cv["prefix"] is not None and cv["prefix"] < 0.5)
        L.append("H10.2 wrong answers for the moved names right after the move (%): " + ", ".join(f"{s} {w[s]:.0f}" for s in SCHEMES) +
                 "; time until correct (s): " + ", ".join(f"{s} {f(cv[s], 2)}" for s in SCHEMES) + f": {ok(good)}")
    c = R.get("scale")
    if c:
        nodes = {s: [(x, gm(c, "msgs_per_res", test="nodes", scheme=s, x=x)) for x in sorted({r["x"] for r in c if r["test"] == "nodes"})] for s in ("nameserver", "prefix", "broadcast")}
        names_kb = [(x, gm(c, "static_table_kb", test="names", scheme="static", x=x)) for x in sorted({r["x"] for r in c if r["test"] == "names"})]
        share = {s: gm(c, "core_share_pct", test="load", scheme=s, x=32) for s in ("nameserver", "prefix")}
        bl = nodes["broadcast"]
        lin = len(bl) > 1 and all(abs(m - 2 * x) < 0.5 for x, m in bl)
        flat = all(abs(m - 2) < 0.2 for s in ("nameserver", "prefix") for _, m in nodes[s])
        good = lin and flat and names_kb[-1][1] > 10 * names_kb[0][1] * 0.5 and share["prefix"] is not None and share["prefix"] < 50 and share["nameserver"] > 90
        L.append("H10.3 messages per resolution vs number of holders: " + "; ".join(f"{s} " + ", ".join(f"{x}:{m:.1f}" for x, m in nodes[s]) for s in nodes) +
                 "; static table KB vs names: " + ", ".join(f"{x}:{k:.0f}" for x, k in names_kb) + f"; share of resolutions the core serves at 32 resolvers: nameserver {f(share['nameserver'], 0)}%, prefix {f(share['prefix'], 0)}%: {ok(good)}")
    d = R.get("nsfailure")
    if d:
        s = {(sc, cl): gm(d, "success_during_outage_pct", scheme=sc, client=cl) for sc in SCHEMES for cl in ("warm", "cold")}
        on_core = gm(d, "names_on_core_pct", scheme="broadcast", client="cold")      # no holder can answer for names whose primary IS the dead master
        good = (s[("nameserver", "warm")] == 0 and s[("nameserver", "cold")] == 0 and s[("broadcast", "cold")] >= 100 - on_core - 3
                and s[("prefix", "cold")] >= 95 and s[("nameserver+cache", "warm")] >= 95)
        rec = gm(d, "records_after_restart", scheme="nameserver", client="cold"), gm(d, "records_before", scheme="nameserver", client="cold")
        L.append("H10.4 resolutions that succeed while the master is down (%), warm/cold client: " + ", ".join(f"{sc} {f(s[(sc, 'warm')], 0)}/{f(s[(sc, 'cold')], 0)}" for sc in SCHEMES) +
                 f"; ({f(on_core, 0)}% of the names have their primary copy on the dead master itself: nobody can answer for those by broadcast); "
                 f"records after the master restarted {f(rec[0], 0)} of {f(rec[1], 0)} (journal): {ok(good)}")
    p = R.get("policy")
    if p:
        pols = ("round_robin", "random", "registry", "probe2")
        good = True
        parts = []
        for kind in ("incident", "mixed"):
            ms_ = {pl: gm(p, "makespan_mean_s", policy=pl, workload=kind) for pl in pols}
            p95 = {pl: gm(p, "makespan_p95_s", policy=pl, workload=kind) for pl in pols}
            parts.append(f"{kind} tasks: makespan mean s " + ", ".join(f"{pl} {f(ms_[pl], 2)}" for pl in pols) + " (p95 " + ", ".join(f"{f(p95[pl], 1)}" for pl in pols) + ")")
            good &= ms_["probe2"] < ms_["registry"] and ms_["probe2"] < ms_["random"]
        mixed = {pl: gm(p, "makespan_mean_s", policy=pl, workload="mixed") for pl in pols}
        L.append("H10.5 " + "; ".join(parts) + f"; probes per task (probe2) {f(gm(p, 'probes_per_task', policy='probe2'), 2)}; "
                 f"round robin is best only while tasks are identical (it ignores size): on mixed tasks round robin {f(mixed['round_robin'], 2)} vs probe2 {f(mixed['probe2'], 2)} s: {ok(good)}")
    h = R.get("coordfail")
    if h:
        dc, dl, dt = gm(h, "completed", durable=1), gm(h, "lost", durable=1), gm(h, "tasks_run_twice", durable=1)
        nc, nl = gm(h, "completed", durable=0), gm(h, "lost", durable=0)
        L.append(f"H10.6 coordinator killed: durable state completes {f(dc)} of {h[0]['workflows']} workflows (lost {f(dl)}, tasks completed twice {f(dt)}, {f(gm(h, 'tasks_restarted', durable=1))} restarted because they ran on the killed node), new coordinator after {f(gm(h, 'takeover_s', durable=1), 2)} s; "
                 f"in-memory state completes {f(nc)} (lost {f(nl)}): {ok(dl == 0 and dt == 0 and nl > 0)}")
    i = R.get("workerfail")
    if i:
        L.append(f"H10.7 worker killed: completed {f(gm(i, 'completed', fault=1))} of {i[0]['workflows']} (no fault {f(gm(i, 'completed', fault=0))}), rescheduled {f(gm(i, 'rescheduled', fault=1))} processes, "
                 f"makespan mean {f(gm(i, 'makespan_mean_s', fault=0), 2)} -> {f(gm(i, 'makespan_mean_s', fault=1), 2)} s (p95 {f(gm(i, 'makespan_p95_s', fault=1), 2)} s): "
                 f"{ok(gm(i, 'completed', fault=1) == i[0]['workflows'])}")
    j = R.get("terminate")
    if j:
        L.append(f"H10.8 terminate: killed {f(gm(j, 'killed'), 0)} of {j[0]['processes']}, call returns in {f(gm(j, 'terminate_call_ms'))} ms, resources and names released after {f(gm(j, 'all_released_ms'))} ms, names left {f(gm(j, 'names_after'), 0)}: "
                 f"{ok(gm(j, 'killed') == j[0]['processes'] and gm(j, 'names_after') == 0 and gm(j, 'all_released_ms') is not None)}")
    g = R.get("procscale")
    if g:
        tp = {c: gm(g, "tasks_per_s", durable=1, concurrent=c) for c in (1, 8, 32)}
        cr = {d: gm(g, "create_ms", durable=d, concurrent=32) for d in (1, 0)}
        sc = {d: gm(g, "schedule_ms", durable=d, concurrent=32) for d in (1, 0)}
        L.append("H10.9 tasks/s vs concurrent workflows (durable): " + ", ".join(f"{c}:{f(v, 1)}" for c, v in tp.items()) + f"; per-phase at 32 workflows durable vs in-memory: create {f(cr[1])} vs {f(cr[0])} ms, schedule {f(sc[1])} vs {f(sc[0])} ms: "
                 f"{ok(tp[32] is not None and tp[1] is not None and tp[32] < 32 * tp[1] * 0.8 and (cr[1] or 0) > (cr[0] or 0))}")
    return L


# ============================================================================ demo
async def demo():
    print("Milestone 10 demo: naming and distributed process management.\n")
    async with Lab("m10demo", managers=3, extra=(), base_port=37000) as lab:
        names, table = await setup_names(lab, 120)
        holders = len(lab.cfg["lrs"]["storage"])
        print(f"NAMING     {len(names)} names such as {names[0]!r}; {holders} holders; zones {lab.cfg['lrs']['zones']}")
        print("           one name resolved by every scheme (warm):")
        for label in SCHEMES:
            res = mk(lab, label, table)
            for nm in names[:10]:
                await res.resolve(nm)
            c0, t0 = counters(res), time.time()
            for nm in names[10:60]:
                rec = await res.resolve(nm)
            print(f"             {label:17} {1000 * (time.time() - t0) / 50:5.1f} ms/resolution   {(counters(res) - c0) / 50:4.1f} messages/resolution   -> {rec['primary']}")
        rec = table[names[0]]
        dest = next(h for h in lab.cfg["lrs"]["storage"] if h != rec["primary"] and h not in rec["backups"])
        resolvers = {label: mk(lab, label, table) for label in SCHEMES}
        for r in resolvers.values():
            await r.resolve(names[0])
        new = (await lab.admin.migrate(names[0], dest))["rec"]["primary"]
        print(f"\nSTALENESS  {names[0]!r} migrated {rec['primary']} -> {new}; what each scheme answers right now:")
        for label, r in resolvers.items():
            got = (await r.resolve(names[0]))["primary"]
            print(f"             {label:17} {got:6} {'correct' if got == new else 'STALE'}")
    async with Lab("m10demo2", managers=3, sites=4, pm={"policy": "probe2", "rf": 2, "durable": True, "check_s": 0.5}, extra=(), base_port=37500) as lab:
        pc = ProcessClient(lab.client, mk(lab, "nameserver"), lab.managers)
        t0 = time.time()
        r = await pc.run({"tasks": [{"id": "diagnose", "burst": 0.4}, {"id": "dispatch", "burst": 1.5, "after": ["diagnose"]},
                                    {"id": "fuel", "burst": 1.5, "after": ["diagnose"]}, {"id": "verify", "burst": 0.3, "after": ["dispatch", "fuel"]}]})
        wid = r["wid"]
        print("\nPROCESS    workflow diagnose -> (dispatch, fuel) -> verify, placement policy probe2:")
        print(f"  CREATE     workflow {wid} stored as a replicated object in {1000 * r['create_s']:.1f} ms; coordinator = the elected leader ({pc.coord})")
        while True:
            st = await pc.status(wid)
            if st["tasks"]["dispatch"]["state"] == "RUNNING":
                break
            await asyncio.sleep(0.05)
        for tid, t in st["tasks"].items():
            print(f"  SCHEDULE   {tid:9} {t['state']:8} on {t['node']}")
        node, lat = await pc.locate(f"{wid}.dispatch")
        print(f"  LOCATE     proc/{wid}.dispatch -> {node}   ({1000 * lat:.1f} ms through the name server)")
        while (await pc.status(wid))["state"] == "RUNNING":
            await asyncio.sleep(0.1)
        print(f"  COORDINATE verify started only after dispatch and fuel were done; workflow finished in {time.time() - t0:.2f} s")
        r2 = await pc.run({"tasks": [{"id": f"t{i}", "burst": 6.0} for i in range(6)]})
        await asyncio.sleep(0.8)
        tt = await pc.terminate(r2["wid"])
        print(f"  TERMINATE  {tt['killed']} running processes killed in {1000 * tt['terminate_s']:.0f} ms, names removed: {await lab.admin.list('proc/') == []}")
        print("\nCOORDINATOR FAILURE   12 workflows running, the leader is killed:")
        for durable in (True, False):
            await lab.reset()
            await configure_pm(lab, policy="round_robin", durable=durable)
            wids = [(await pc.run(template("chain")))["wid"] for _ in range(12)]
            await asyncio.sleep(1.0)
            tk = lab.kill(lab.managers[-1])
            res = {}
            while len(res) < 12 and time.time() - tk < 20:
                for w in wids:
                    if w not in res:
                        st = await status_any(lab, w)
                        if st and st["state"] != "RUNNING":
                            res[w] = st
                await asyncio.sleep(0.3)
            rec = [e["ts"] - tk for e in pm_events(lab, {"pm_recovered"}, tk)]
            print(f"   state {'in the replicated store' if durable else 'in memory only       '}: {sum(1 for s in res.values() if s['state'] == 'DONE')}/12 workflows completed, "
                  f"new coordinator after {f(min(rec), 1) if rec else 'n/a'} s, processes completed twice: {run_twice(executions(lab, tk - 3))}")
    print("\nCreate -> Locate -> Schedule -> Coordinate -> Terminate, and a coordinator crash: with durable state nothing is lost.")


# ============================================================================ main
async def main():
    acquire_lock()
    if "--demo" in sys.argv:
        return await demo()
    quick = "--quick" in sys.argv
    trials = int(sys.argv[sys.argv.index("--trials") + 1]) if "--trials" in sys.argv else (1 if quick else 3)
    parts = sys.argv[sys.argv.index("--part") + 1].split(",") if "--part" in sys.argv else list(PARTS)
    write_env({"experiment": "week10", "trials": trials, "parts": parts, "naming_cluster": "3 managers + cloud + core",
               "process_cluster": "3 managers + 4 sites + cloud + core", "zones": "one per manager + proc/ wf/ ptab/ on mgr1"})
    R = {}
    fns = {"resolution": part_resolution, "staleness": part_staleness, "scale": part_scale, "nsfailure": part_nsfailure, "lifecycle": part_lifecycle,
           "policy": part_policy, "procscale": part_procscale, "coordfail": part_coordfail, "workerfail": part_workerfail, "terminate": part_terminate}
    naming = [p for p in parts if p in NAMING_PARTS]
    if naming:
        async with Lab("m10n", managers=3, extra=(), base_port=35000) as lab:
            for p in naming:
                print(f"\n== {p}", flush=True)
                R[p] = await fns[p](lab, trials)
                write_csv(f"results/week10_{p}.csv", R[p])
    proc = [p for p in parts if p not in NAMING_PARTS]
    if proc:
        async with Lab("m10p", managers=3, sites=4, extra=(), pm={"policy": "round_robin", "rf": 2, "durable": True, "check_s": 0.5}, base_port=38000) as lab:
            for p in proc:
                print(f"\n== {p}", flush=True)
                R[p] = await fns[p](lab, trials)
                write_csv(f"results/week10_{p}.csv", R[p])
    keys = {"resolution": ["scheme"], "staleness": ["scheme"], "scale": ["test", "x", "scheme"], "nsfailure": ["scheme", "client"], "lifecycle": ["phase", "what"],
            "policy": ["policy"], "procscale": ["durable", "concurrent"], "coordfail": ["durable"], "workerfail": ["fault"], "terminate": []}
    summ = []
    for p, rows in R.items():
        fields = sorted({k for r in rows for k, v in r.items() if k not in keys[p] + ["trial", "n"] and isinstance(v, (int, float)) and not isinstance(v, bool)})
        for s in summarise(rows, keys[p], fields):
            summ.append({"part": p, **s})
    allkeys = sorted({k for s in summ for k in s}, key=lambda k: (k != "part", k))
    write_csv("results/week10_summary.csv", [{k: s.get(k, "") for k in allkeys} for s in summ])
    L = verdicts(R)
    print("\nHypothesis check (means over trials; sd and 95% CI in results/week10_summary.csv)")
    for line in L:
        print("  " + line)
    with open("results/week10_hypotheses.txt", "w") as fh:
        fh.write("\n".join(L) + "\n")
    print("saved results/week10_hypotheses.txt")


if __name__ == "__main__":
    asyncio.run(main())
