"""Milestone 9 (part 2 of 2): REPLICATION, FAILURE transparency and the RELIABILITY model.   Owner: Bathshua Miheso.

Split out of the original week9_transparency.py. One logical object is kept as rf copies (primary-backup, sync or async); when a
node dies the application must only see a slower answer. Every transparent system is tested against the non-transparent baseline
(DirectClient) on the same cluster.

Experiments (--part), each repeated TRIALS times
  replication   B  write latency, read latency and balance, staleness and convergence, versus rf and sync/async     -> report 1.5
  failure       D  the primary node is killed (and relaunched): errors, client-visible outage, lost writes, recovery  -> report 1.6
  reliability   E  repeated failures: single and double node failures vs rf, measured availability vs the model     -> report 1.7
  detection     F  failure detection by the name server's monitor vs only by the clients' suspicion (crash and hang)  -> report 1.8
  fencing       G  a hung, replaced primary that resumes must not accept a stale client's write (split brain check)   -> report 1.8
  (detection and fencing reuse failure_run(), which is why they live in this file)

Hypotheses
  H9.2  Replication transparency costs write latency with synchronous replication (a bounded factor, since backups are written in
        parallel); asynchronous replication removes that cost but exposes stale reads; read-any balances reads.
  H9.4  Failure transparency: with rf >= 2 a primary crash costs the application a latency spike, not errors; the baseline is down
        until the node returns. Synchronous replication loses no acknowledged write, asynchronous may.
  H9.5  Reliability: with rf copies the object survives rf-1 simultaneous node failures; availability matches the model
        "unavailable only while every copy is down".
  H9.6  The name server's monitor shortens the outage seen by the application compared with client-side suspicion alone.
  H9.7  Fencing: a primary that hung, was replaced and then resumed cannot acknowledge a write from a client with a stale location
        cache (the backups reject its old epoch), so there is no split brain and no lost write.
Controlled: 3 managers + cloud + core (+ the client node), link 5 ms +-2 ms, seed 42, no sensor traffic.

    python3 -m experiments.week9_replication_consistency --demo             narrated, about 1.5 min
    python3 -m experiments.week9_replication_consistency --quick            1 trial of everything
    python3 -m experiments.week9_replication_consistency                    3 trials
    options: --part replication,failure,reliability,detection,fencing   --trials N
Writes results/week9_<part>.csv, results/week9_replication_summary.csv (mean, sd, CI), results/week9_replication_hypotheses.txt.
The other M9 parts (overhead, migration) are in week9_transparency.py.
"""
import asyncio
import csv
import os
import statistics
import sys
import time

from experiments._common import acquire_lock, ci95, mean_sd, write_env
from experiments._lab import Lab, drive, ms, pct
from services.transparency import DirectClient, LogicalClient, make_resolver

def logical(lab, scheme="nameserver", ttl=1e9, policy="primary", deadline=6.0, seed=1, table=None):
    return LogicalClient(lab.client, make_resolver(scheme, lab.client, lab.cfg, ttl=ttl, table=table),
                         read_policy=policy, deadline=deadline, seed=seed)


def msgs(lc):
    return lc.m["sent"] + lc.m["recv"] + lc.resolver.sent + lc.resolver.recv


def f(x, d=1):
    return "n/a" if x is None else f"{x:.{d}f}"


def write_csv(path, rows):
    if not rows:
        return
    os.makedirs("results", exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
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
        for f in fields:
            xs = [x[f] for x in g if x.get(f) not in ("", None)]
            m, sd = mean_sd(xs)
            c = ci95(xs)
            row[f + "_mean"] = "" if m is None else round(m, 3)
            row[f + "_sd"] = "" if sd is None else round(sd, 3)
            row[f + "_ci95"] = "" if c is None else round(c, 3)
        out.append(row)
    return out


# ============================================================================ B. replication
async def part_replication(lab, trials, n=120):
    rows = []
    for trial in range(trials):
        for rf, mode in ((1, "sync"), (2, "sync"), (3, "sync"), (2, "async"), (3, "async")):
            await lab.reset()
            name = f"r1/rep/{rf}{mode}"
            nodes = lab.managers[:rf]
            await lab.admin.create([name], rf=rf, sync=(mode == "sync"), val=0, where=nodes)
            lp, la = logical(lab, policy="primary"), logical(lab, policy="any", seed=trial + 7)
            await lp.read(name), await la.read(name)
            before = {m: await lab.stats(m) for m in nodes}
            wl = []
            for i in range(n):
                wl.append((await lp.write(name, i + 1))["lat"])
            after = {m: await lab.stats(m) for m in nodes}
            repl_msgs = sum(after[m]["lrs_repl"] - before[m]["lrs_repl"] for m in nodes)
            repl_items = sum(after[m]["lrs_repl_items"] - before[m]["lrs_repl_items"] for m in nodes)
            await asyncio.sleep(0.3)
            rp = [(await lp.read(name))["lat"] for _ in range(n)]
            reads = [await la.read(name) for _ in range(n)]
            ra = [r["lat"] for r in reads]
            cnt = {}
            for r in reads:
                cnt[r["node"]] = cnt.get(r["node"], 0) + 1
            stale_any = stale_pri = 0
            for i in range(100):
                w = await lp.write(name, 1000 + i)
                stale_any += (await la.read(name))["v"] < w["v"]
                stale_pri += (await lp.read(name))["v"] < w["v"]
            t0, want = time.time(), (await lp.read(name))["v"]
            while time.time() - t0 < 2.0:
                st = [await lab.admin.state(m, name) for m in nodes]
                if all(s and s["v"] == want for s in st):
                    break
                await asyncio.sleep(0.005)
            rows.append({"trial": trial, "rf": rf, "mode": mode, "write_ms": ms(statistics.mean(wl)), "write_p95_ms": ms(pct(wl, 0.95)),
                         "read_primary_ms": ms(statistics.mean(rp)), "read_any_ms": ms(statistics.mean(ra)),
                         "any_max_share": round(max(cnt.values()) / n, 3), "stale_any": round(stale_any / 100, 3),
                         "stale_primary": round(stale_pri / 100, 3), "converge_ms": ms(time.time() - t0),
                         "repl_msgs_per_write": round(repl_msgs / n, 2), "repl_items_per_write": round(repl_items / n, 2)})
        print(f"  replication trial {trial}: done", flush=True)
    return rows


# ============================================================================ D. failure
SYSTEMS = {  # name: (client kind, rf, sync, nodes, node to kill)
    "direct_mem": ("direct", 1, True, ["mgr1"], "mgr1"),
    "direct_file": ("direct", 1, True, ["cloud"], "cloud"),
    "transparent_rf1": ("logical", 1, True, ["mgr1"], "mgr1"),
    "transparent_rf2_sync": ("logical", 2, True, ["mgr1", "mgr2"], "mgr1"),
    "transparent_rf2_async": ("logical", 2, False, ["mgr1", "mgr2"], "mgr1"),
    "transparent_rf3_sync": ("logical", 3, True, ["mgr1", "mgr2", "mgr3"], "mgr1"),
}


async def failure_run(lab, system, rate=80, t_kill=3.0, t_restart=9.0, dur=15.0, fault="kill"):
    kind, rf, sync, nodes, victim = SYSTEMS[system]
    await lab.reset()
    name = f"r1/fail/{system}"
    await lab.admin.create([name], rf=rf, sync=sync, val=0, where=nodes)
    if kind == "direct":
        dc = DirectClient(lab.client)
        write = lambda i: dc.op("write", nodes[0], name, i)
        read = lambda: dc.op("read", nodes[0], name)
    else:
        lc = logical(lab, deadline=3.0)
        write = lambda i: lc._op("write", name, i)
        read = lambda: lc._op("read", name)
    op = lambda i, t: write(i) if i % 5 else read()           # 80% writes
    ev = {}

    async def chaos():
        await asyncio.sleep(t_kill)
        ev["kill"] = lab.kill(victim) if fault == "kill" else lab.stop(victim)       # kill = crash, hang = alive but silent
        await asyncio.sleep(t_restart - t_kill)
        ev["restart"] = lab.restart(victim) if fault == "kill" else lab.cont(victim)

    t0 = time.time()
    ch = asyncio.create_task(chaos())
    res = await drive(rate, dur, op, t0)
    await ch
    await asyncio.sleep(2.5)                                  # let the rejoin finish
    ok = [r for r in res if r["ok"]]
    after = [r for r in ok if r["t"] >= t_kill]
    done = sorted(r["t"] + r["lat"] for r in ok)                      # the outage is the longest gap between COMPLETED operations
    pre = [c for c in done if c < t_kill][-1:]
    post = [c for c in done if c >= t_kill]
    gaps = [b - a for a, b in zip(pre + post, post)] if post else []
    lost = lost_writes(res)
    prom = [e["ts"] - ev["kill"] for e in lab.events("core", {"ns_promoted"}) if e["ts"] >= ev["kill"]]
    rej = [e["ts"] - ev["restart"] for e in lab.events("core", {"ns_rejoined"}) if e["ts"] >= ev["restart"]]
    rec = await lab.admin.resolve(name)
    copies, consistent = None, None
    if rec:
        sts = [await lab.admin.state(nd, name) for nd in [rec["primary"]] + rec["backups"]]
        copies = len(sts)
        consistent = int(all(s and s["v"] == sts[0]["v"] and s["val"] == sts[0]["val"] for s in sts))
    return {"system": system, "ops": len(res), "errors": len(res) - len(ok), "ok_pct": round(100 * len(ok) / len(res), 2),
            "max_ms": ms(max(r["lat"] for r in ok)) if ok else None, "p99_ms": ms(pct([r["lat"] for r in ok], 0.99)),
            "client_gap_s": round(max(gaps), 3) if gaps else None, "lost_writes": lost,
            "promotion_s": round(min(prom), 3) if prom else None, "rejoin_s": round(min(rej), 3) if rej else None,
            "copies_after": copies, "consistent_after": consistent}


async def part_failure(lab, trials, systems=tuple(SYSTEMS)):
    rows = []
    for trial in range(trials):
        for system in systems:
            rows.append({"trial": trial, **await failure_run(lab, system)})
            r = rows[-1]
            print(f"  failure t{trial} {system:22} errors {r['errors']:4}  max {r['max_ms']} ms  gap {r['client_gap_s']} s  lost {r['lost_writes']}  promoted after {r['promotion_s']} s", flush=True)
    return rows


# ============================================================================ E. reliability
def all_down_intervals(per_node, nodes, t_end):
    """Intervals in [0, t_end] when EVERY node of `nodes` is down: the object has no copy left."""
    pts = sorted({0.0, t_end} | {x for n in nodes for iv in per_node.get(n, []) for x in iv if 0 <= x <= t_end})
    out = []
    for a, b in zip(pts, pts[1:]):
        mid = (a + b) / 2
        if all(any(s <= mid < e for s, e in per_node.get(n, [])) for n in nodes):
            if out and abs(out[-1][1] - a) < 1e-9:
                out[-1] = (out[-1][0], b)
            else:
                out.append((a, b))
    return out


def lost_writes(res):
    """Acknowledged writes lost: a later acknowledgement carries an OLDER version (the state went back in time)."""
    w = sorted((r["t"] + r["lat"], r["v"]) for r in res if r["ok"] and r["kind"] == "write" and r["v"])
    top = lost = 0
    for _, v in w:
        if v < top - 5:                                       # (a margin of 5 absorbs acknowledgements that arrive out of order)
            lost += top - v + 1
            top = v
        else:
            top = max(top, v)
    return lost


async def reliability_run(lab, scenario, rate=20):
    await lab.reset()
    objs = {"direct": ("direct", ["mgr1"]), "rf1": ("logical", ["mgr1"]), "rf2": ("logical", ["mgr1", "mgr2"]),
            "rf3": ("logical", ["mgr1", "mgr2", "mgr3"])}
    sched = {"single": [("mgr1", 3, 6), ("mgr2", 11, 14), ("mgr1", 18, 21)], "double": [("mgr1", 4, 10), ("mgr2", 5, 10)]}[scenario]
    dur = {"single": 24.0, "double": 16.0}[scenario]
    clients = {}
    for k, (kind, nodes) in objs.items():
        await lab.admin.create([f"r1/rel/{k}"], rf=len(nodes), val=0, where=nodes)
        clients[k] = DirectClient(lab.client) if kind == "direct" else logical(lab, deadline=1.0, seed=3)
    intervals, t0 = {}, time.time()

    async def fail(node, ts, te):
        await asyncio.sleep(max(0, t0 + ts - time.time()))
        tk = time.time() - t0
        lab.kill(node)
        await asyncio.sleep(max(0, t0 + te - time.time()))
        lab.restart(node)
        while not await lab.stats(node, timeout=0.3):
            await asyncio.sleep(0.1)
        intervals.setdefault(node, []).append((tk, time.time() - t0))

    async def load(k):
        c, name = clients[k], f"r1/rel/{k}"
        if k == "direct":
            return await drive(rate, dur, lambda i, t: c.op("write", "mgr1", name, i) if i % 2 else c.op("read", "mgr1", name), t0)
        return await drive(rate, dur, lambda i, t: c._op("write", name, i) if i % 2 else c._op("read", name), t0)

    fails = [asyncio.create_task(fail(*f)) for f in sched]
    results = dict(zip(objs, await asyncio.gather(*[load(k) for k in objs])))
    await asyncio.gather(*fails)
    await asyncio.sleep(2.5)
    out = []
    for k, (kind, nodes) in objs.items():
        res = results[k]
        okr = [r for r in res if r["ok"]]
        window = 0.0 if kind == "direct" else 1.0                  # a logical client retries for 1 s: shorter outages are masked
        down = sum(max(0.0, b - a - window) for a, b in all_down_intervals(intervals, nodes, dur))
        lost = lost_writes(res)
        out.append({"scenario": scenario, "object": k, "ops": len(res), "errors": len(res) - len(okr),
                    "availability": round(len(okr) / len(res), 4), "predicted": round(1 - down / dur, 4),
                    "max_ms": ms(max(r["lat"] for r in okr)) if okr else None, "lost_writes": lost, "data_lost": int(lost > 0)})
    return out


async def part_reliability(lab, trials):
    rows = []
    for trial in range(trials):
        for scenario in ("single", "double"):
            for r in await reliability_run(lab, scenario):
                rows.append({"trial": trial, **r})
                print(f"  reliability t{trial} {scenario:6} {r['object']:6} availability {r['availability']}  predicted {r['predicted']}  errors {r['errors']}  data_lost {r['data_lost']}", flush=True)
    return rows


# ============================================================================ F. detection  /  G. fencing
async def fencing_run(lab):
    """A primary that was hung, replaced, and then resumed must not accept a write from a client with a stale cache."""
    await lab.reset()
    name = "r1/fence/obj"
    await lab.admin.create([name], rf=2, val=0, where=["mgr1", "mgr2"])
    stale, fresh = logical(lab, deadline=3.0), logical(lab, deadline=6.0, seed=5)
    await stale.write(name, 1)                                    # the stale client caches primary=mgr1, epoch 1
    lab.stop("mgr1")                                              # the primary hangs
    w2 = await fresh.write(name, 2)                               # another client forces the failover to mgr2 (epoch 2)
    lab.cont("mgr1")                                              # the old primary wakes up, still believing it is primary
    w3 = await stale.write(name, 3)                               # the stale client writes to the old location first
    rec = await lab.admin.resolve(name)                           # the AUTHORITATIVE copy is the primary the name server names
    st = await lab.admin.state(rec["primary"], name)
    return {"zombie_acked": int(w3["ok"] and w3["node"] == "mgr1"), "write_survived": int(bool(st and st["val"] == 3)),
            "redirects": w3["redirects"], "served_by": w3["node"], "attempts": w3["attempts"]}


async def part_detection(trials):
    rows = []
    for monitor in (0.5, 0):
        async with Lab("m9det", lrs={"monitor_s": monitor, "rf": 2}, base_port=32000) as lab:
            for fault in ("kill", "hang"):
                for trial in range(trials):
                    r = await failure_run(lab, "transparent_rf2_sync", fault=fault)
                    rows.append({"trial": trial, "monitor_s": monitor, "fault": fault, **{k: r[k] for k in ("errors", "max_ms", "p99_ms", "client_gap_s", "promotion_s", "lost_writes")}})
                    print(f"  detection t{trial} monitor={monitor} {fault}: max {r['max_ms']} ms  gap {r['client_gap_s']} s  promoted after {r['promotion_s']} s  errors {r['errors']}", flush=True)
    return rows


async def part_fencing(trials):
    rows = []
    async with Lab("m9fence", lrs={"monitor_s": 0, "rf": 2}, base_port=32500) as lab:
        for trial in range(trials):
            r = await fencing_run(lab)
            rows.append({"trial": trial, **r})
            print(f"  fencing t{trial}: zombie acknowledged the stale write: {r['zombie_acked']}, write survived: {r['write_survived']}, served by {r['served_by']} after {r['redirects']} redirect(s)", flush=True)
    return rows


def gm(rows, k, **flt):
    xs = [r[k] for r in rows if all(r.get(a) == b for a, b in flt.items()) and r.get(k) not in ("", None)]
    return statistics.mean(xs) if xs else None




# ============================================================================ verdicts
def verdicts(R):
    L = []
    ok = lambda b: "SUPPORTED" if b else "NOT SUPPORTED"
    r = R.get("replication")
    if r:
        w1, w3s, w3a = gm(r, "write_ms", rf=1, mode="sync"), gm(r, "write_ms", rf=3, mode="sync"), gm(r, "write_ms", rf=3, mode="async")
        s_sync, s_async = gm(r, "stale_any", rf=3, mode="sync"), gm(r, "stale_any", rf=3, mode="async")
        share = gm(r, "any_max_share", rf=3, mode="sync")
        L.append(f"H9.2 write ms rf1 {w1:.1f}, rf3 sync {w3s:.1f} (x{w3s / w1:.2f}), rf3 async {w3a:.1f} (x{w3a / w1:.2f}); stale reads from any copy: sync {s_sync:.2f}, "
                 f"async {s_async:.2f}; busiest copy serves {share:.2f} of read-any (ideal 0.33): {ok(w3s / w1 < 4 and w3a / w1 < 1.4 and s_sync == 0 and s_async > 0 and share < 0.5)}")
    if R.get("failure"):
        t2, d1 = "transparent_rf2_sync", "direct_mem"
        sync_lost, async_lost = gm(R["failure"], "lost_writes", system="transparent_rf2_sync"), gm(R["failure"], "lost_writes", system="transparent_rf2_async")
        fr = R["failure"]
        L.append(f"H9.4 primary killed: errors direct {f(gm(fr, 'errors', system=d1), 0)}, rf2 sync {f(gm(fr, 'errors', system=t2))}; client-visible gap direct {f(gm(fr, 'client_gap_s', system=d1), 2)} s "
                 f"vs rf2 {f(gm(fr, 'client_gap_s', system=t2), 2)} s; worst latency rf2 {f(gm(fr, 'max_ms', system=t2), 0)} ms; acknowledged writes lost: sync {f(sync_lost)}, async {f(async_lost)}, "
                 f"direct_mem {f(gm(fr, 'lost_writes', system=d1))}: {ok((gm(fr, 'errors', system=t2) or 0) <= 2 and (gm(fr, 'client_gap_s', system=t2) or 9) < 1.5 and (sync_lost or 0) == 0)}")
    e = R.get("reliability")
    if e:
        parts = []
        good = True
        for sc in ("single", "double"):
            for ob in ("direct", "rf1", "rf2", "rf3"):
                a, p = gm(e, "availability", scenario=sc, object=ob), gm(e, "predicted", scenario=sc, object=ob)
                parts.append(f"{sc}/{ob} {a:.3f} (model {p:.3f})")
                good &= abs(a - p) < 0.1
        lost = {ob: gm(e, "data_lost", scenario="double", object=ob) for ob in ("rf1", "rf2", "rf3")}
        L.append("H9.5 availability measured (model = unavailable only while every copy is down, minus the client's 1 s retry window): " + "; ".join(parts) + f"; data lost in the double failure: {lost}: {ok(good and lost['rf3'] == 0)}")
    d = R.get("detection")
    if d:
        g = lambda mon, fl, k="client_gap_s": gm(d, k, monitor_s=mon, fault=fl)
        L.append(f"H9.6 outage seen by the application (s) / worst operation (ms): kill with monitor {f(g(0.5, 'kill'), 2)}/{f(g(0.5, 'kill', 'max_ms'), 0)}, kill suspicion only {f(g(0, 'kill'), 2)}/{f(g(0, 'kill', 'max_ms'), 0)}; "
                 f"hang with monitor {f(g(0.5, 'hang'), 2)}/{f(g(0.5, 'hang', 'max_ms'), 0)}, hang suspicion only {f(g(0, 'hang'), 2)}/{f(g(0, 'hang', 'max_ms'), 0)}: "
                 f"{ok(None not in (g(0.5, 'hang'), g(0, 'hang')) and g(0.5, 'hang') < g(0, 'hang') * 0.9)}")
    fe = R.get("fencing")
    if fe:
        za, ws = sum(r["zombie_acked"] for r in fe), sum(r["write_survived"] for r in fe)
        L.append(f"H9.7 a resumed, replaced primary acknowledged {za} of {len(fe)} writes from a stale client; the write survived on the new primary in {ws} of {len(fe)}: {ok(za == 0 and ws == len(fe))}")
    return L



# ============================================================================ demo
async def demo():
    print("Milestone 9 demo (replication and failure transparency).\n")
    async with Lab("m9demo", lrs={"rf": 2}, base_port=33000) as lab:
        await lab.reset()
        names = ["r1/site1/status", "r1/site2/status", "r2/site3/status"]
        await lab.admin.create(names[:2], rf=2, val={"grid": "ok"})
        await lab.admin.create(["r1/site9/local"], rf=1, val={"grid": "ok"}, where=["client"])
        print("\nREPLICATION         the same object read from every copy, versions identical:")
        rec = await lab.admin.resolve(names[0])
        for nd in [rec["primary"]] + rec["backups"]:
            print("  ", nd, await lab.admin.state(nd, names[0]))
        print("\nFAILURE             the node holding the primary is killed (SIGKILL) while the client keeps working:")
        for system in ("transparent_rf2_sync", "direct_mem"):
            r = await failure_run(lab, system, dur=12.0, t_restart=8.0)
            print(f"   {system:21} errors {r['errors']:4} of {r['ops']}, outage seen by the application {r['client_gap_s']} s, slowest op {r['max_ms']} ms, "
                  f"backup promoted after {r['promotion_s'] if r['promotion_s'] is not None else 'n/a (no backup)'} s, acknowledged writes lost {r['lost_writes']}, copies after recovery {r['copies_after']} consistent={r['consistent_after']}")
    print("\nFailure -> Detection -> Recovery -> Service Restoration: the transparent client saw a latency spike; the baseline was down until the node came back.")


# ============================================================================ main
PARTS = ("replication", "failure", "reliability", "detection", "fencing")
KEYS = {"replication": ["rf", "mode"], "failure": ["system"], "reliability": ["scenario", "object"], "detection": ["monitor_s", "fault"], "fencing": []}


async def main():
    acquire_lock()
    if "--demo" in sys.argv:
        return await demo()
    quick = "--quick" in sys.argv
    trials = int(sys.argv[sys.argv.index("--trials") + 1]) if "--trials" in sys.argv else (1 if quick else 3)
    parts = sys.argv[sys.argv.index("--part") + 1].split(",") if "--part" in sys.argv else list(PARTS)
    write_env({"experiment": "week9_replication_consistency", "trials": trials, "parts": parts, "managers": 3,
               "storage": "mgr1,mgr2,mgr3,cloud,core(+client)", "monitor_s": 0.5, "async_flush_s": 0.05})
    R = {}
    main_parts = [p for p in parts if p not in ("detection", "fencing")]
    if main_parts:
        async with Lab("m9", lrs={"rf": 2}, base_port=30000) as lab:
            for p in main_parts:
                print(f"\n== {p}", flush=True)
                R[p] = await {"replication": part_replication, "failure": part_failure, "reliability": part_reliability}[p](lab, trials)
                write_csv(f"results/week9_{p}.csv", R[p])
    if "detection" in parts:
        print("\n== detection", flush=True)
        R["detection"] = await part_detection(trials)
        write_csv("results/week9_detection.csv", R["detection"])
    if "fencing" in parts:
        print("\n== fencing", flush=True)
        R["fencing"] = await part_fencing(trials)
        write_csv("results/week9_fencing.csv", R["fencing"])
    summ = []
    for p, rows in R.items():
        fields = [k for k in rows[0] if k not in KEYS[p] + ["trial", "n"] and isinstance(rows[0][k], (int, float))]
        for s in summarise(rows, KEYS[p], fields):
            summ.append({"part": p, **s})
    allkeys = sorted({k for s in summ for k in s}, key=lambda k: (k != "part", k))
    write_csv("results/week9_replication_summary.csv", [{k: s.get(k, "") for k in allkeys} for s in summ])
    L = verdicts(R)
    print("\nHypothesis check (means over trials; sd and 95% CI in results/week9_replication_summary.csv)")
    for line in L:
        print("  " + line)
    with open("results/week9_replication_hypotheses.txt", "w") as fh:
        fh.write("\n".join(L) + "\n")
    print("saved results/week9_replication_hypotheses.txt")


if __name__ == "__main__":
    asyncio.run(main())
