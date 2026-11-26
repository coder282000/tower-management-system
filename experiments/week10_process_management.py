"""Milestone 10 (part 2 of 2): DISTRIBUTED PROCESS MANAGEMENT.   Owner: Tebby Manyala.

Split out of the original week10_naming_procs.py.   Create -> Locate -> Schedule -> Coordinate -> Terminate  (services/procmgmt.py):
workflows of dependent processes, run by the elected leader, state kept in the replicated store (Milestone 9) so a new leader can
carry on; placement policies round_robin | random | registry (stale loads) | probe2 (two fresh probes); process names proc/<pid>
resolved by the naming schemes (week10_naming_schemes.py).

Experiments (--part)
  lifecycle    E  one workflow, step by step: create, schedule, locate (every scheme), coordinate, terminate        -> report 2.7
  policy       F  four placement policies under load: completion time, balance, messages
  procscale    G  1 / 8 / 32 concurrent workflows, with and without durable state
  coordfail    H  the coordinator (leader) is killed mid-run: durable state vs none                                  -> report 2.8
  workerfail   I  a worker node is killed mid-run: processes are rescheduled
  terminate    J  group termination: time, resources released, names removed                                         -> report 2.7

Hypotheses
  H10.5 Probing two random workers (probe2) balances processes better and finishes workflows sooner than the stale registry policy,
        for two more messages per process.
  H10.6 With the process table in the replicated store, killing the coordinator loses no workflow and runs no process twice;
        with in-memory state the unfinished workflows are lost.
  H10.7 When a worker dies its processes are rescheduled elsewhere and every workflow still completes.
  H10.8 Terminating a workflow kills all its processes, frees their resources and removes their names quickly.
  H10.9 Throughput saturates as concurrent workflows grow (the coordinator is the bottleneck); durable state costs latency per phase.
Controlled: link 5 ms +-2 ms, seed 42, one machine (1 vCPU): absolute throughput is a lower bound.

    python3 -m experiments.week10_process_management --demo           narrated, about 1.5 min
    python3 -m experiments.week10_process_management --quick          1 trial of everything
    python3 -m experiments.week10_process_management                  3 trials
    options: --part lifecycle,policy,procscale,coordfail,workerfail,terminate   --trials N
Writes results/week10_<part>.csv, results/week10_process_summary.csv (mean, sd, CI), results/week10_process_hypotheses.txt.
The naming parts (resolution, staleness, scale, nsfailure) are in week10_naming_schemes.py.
"""
import asyncio
import csv
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


def gm(rows, k, **flt):
    xs = [r[k] for r in rows if all(r.get(a) == b for a, b in flt.items()) and r.get(k) not in ("", None)]
    return statistics.mean(xs) if xs else None




# ============================================================================ verdicts
def verdicts(R):
    L = []
    ok = lambda b: "SUPPORTED" if b else "NOT SUPPORTED"
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
    print("Milestone 10 demo (process management).\n")
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
PARTS = ("lifecycle", "policy", "procscale", "coordfail", "workerfail", "terminate")
KEYS = {"lifecycle": ["phase", "what"], "policy": ["policy"], "procscale": ["durable", "concurrent"], "coordfail": ["durable"],
        "workerfail": ["fault"], "terminate": []}


async def main():
    acquire_lock()
    if "--demo" in sys.argv:
        return await demo()
    quick = "--quick" in sys.argv
    trials = int(sys.argv[sys.argv.index("--trials") + 1]) if "--trials" in sys.argv else (1 if quick else 3)
    parts = sys.argv[sys.argv.index("--part") + 1].split(",") if "--part" in sys.argv else list(PARTS)
    write_env({"experiment": "week10_process_management", "trials": trials, "parts": parts,
               "process_cluster": "3 managers + 4 sites + cloud + core", "zones": "one per manager + proc/ wf/ ptab/ on mgr1"})
    R = {}
    fns = {"lifecycle": part_lifecycle, "policy": part_policy, "procscale": part_procscale, "coordfail": part_coordfail,
           "workerfail": part_workerfail, "terminate": part_terminate}
    async with Lab("m10p", managers=3, sites=4, extra=(), pm={"policy": "round_robin", "rf": 2, "durable": True, "check_s": 0.5}, base_port=38000) as lab:
        for p in parts:
            print(f"\n== {p}", flush=True)
            R[p] = await fns[p](lab, trials)
            write_csv(f"results/week10_{p}.csv", R[p])
    summ = []
    for p, rows in R.items():
        fields = sorted({k for r in rows for k, v in r.items() if k not in KEYS[p] + ["trial", "n"] and isinstance(v, (int, float)) and not isinstance(v, bool)})
        for s in summarise(rows, KEYS[p], fields):
            summ.append({"part": p, **s})
    allkeys = sorted({k for s in summ for k in s}, key=lambda k: (k != "part", k))
    write_csv("results/week10_process_summary.csv", [{k: s.get(k, "") for k in allkeys} for s in summ])
    L = verdicts(R)
    print("\nHypothesis check (means over trials; sd and 95% CI in results/week10_process_summary.csv)")
    for line in L:
        print("  " + line)
    with open("results/week10_process_hypotheses.txt", "w") as fh:
        fh.write("\n".join(L) + "\n")
    print("saved results/week10_process_hypotheses.txt")


if __name__ == "__main__":
    asyncio.run(main())
