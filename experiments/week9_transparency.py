"""Milestone 9 (part 1 of 2): ACCESS, LOCATION and MIGRATION transparency.   Owner: Bernadette Weloba.

Split out of the original week9_transparency.py. The application sees a LOGICAL resource ("r1/site1/status"); the system hides
the physical one (services/store.py, naming.py, transparency.py). Each transparency is tested against a NON-transparent baseline
(DirectClient: the application names the physical node itself) on the same cluster.

Experiments (--part), each repeated TRIALS times
  overhead    A  cost of the layer: latency and messages per operation, direct vs transparent (warm cache, cold, local copy)   -> report 1.3
  migration   C  a client at 50 ops/s while the primary moves: errors, latency spike, lost writes                                -> report 1.4

Hypotheses
  H9.1  Access/location transparency is almost free: with a warm location cache an operation costs the same one round trip as
        direct access (< 15% overhead); a cold lookup adds exactly one more round trip; a copy on the client's own node is reached
        by a function call.
  H9.3  Migration transparency: no error and no lost write while the primary moves; the price is a short latency spike
        (redirect + freeze); a hard-coded or static location fails for ever after the move.
Controlled: 3 managers + cloud + core (+ the client node), link 5 ms +-2 ms, seed 42, no sensor traffic.

    python3 -m experiments.week9_transparency --demo             narrated, about 1 min
    python3 -m experiments.week9_transparency --quick            1 trial of everything
    python3 -m experiments.week9_transparency                    3 trials
    options: --part overhead,migration   --trials N
Writes results/week9_overhead.csv, results/week9_migration.csv, results/week9_transparency_summary.csv (mean, sd, CI),
results/week9_transparency_hypotheses.txt, results/environment.txt.
The other M9 parts (replication, failure, reliability, detection, fencing) are in week9_replication_consistency.py.
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


# ============================================================================ A. overhead
async def part_overhead(lab, trials, n=150):
    rows = []
    for trial in range(trials):
        await lab.reset()
        await lab.admin.create(["r1/ovh/mem"], rf=1, val=0, where=["mgr1"])      # memory backend, remote
        await lab.admin.create(["r1/ovh/file"], rf=1, val=0, where=["core"])     # file backend, remote
        await lab.admin.create(["r1/ovh/local"], rf=1, val=0, where=["client"])  # a copy on the client's own node
        cases = [("direct", "mem", "mgr1"), ("direct", "file", "core"), ("warm", "mem", None), ("warm", "file", None),
                 ("cold", "mem", None), ("cold", "file", None), ("local", "local", None)]
        for case, res, where in cases:
            name = f"r1/ovh/{res}"
            if case == "direct":
                dc = DirectClient(lab.client)
                call = lambda kind, v, dc=dc, w=where, nm=name: dc.op(kind, w, nm, v)
                counters = lambda dc=dc: dc.m["sent"] + dc.m["recv"]
            else:
                lc = logical(lab, ttl=0 if case == "cold" else 1e9)
                call = lambda kind, v, lc=lc, nm=name: lc._op(kind, nm, v)
                counters = lambda lc=lc: msgs(lc)
                await call("read", None)                                           # prime the cache
            for kind in ("write", "read"):
                lat, ok_all, last = [], True, None
                m0 = counters()
                ns0 = (await lab.stats("core"))["ns_resolve"]
                for i in range(n):
                    r = await call(kind, i + 1 if kind == "write" else None)
                    lat.append(r["lat"])
                    ok_all &= r["ok"]
                    last = r
                ns1 = (await lab.stats("core"))["ns_resolve"]
                c1 = counters()
                rd = await call("read", None)                                      # uniformity: same call, same result
                uniform = bool(rd["ok"]) and (kind != "write" or rd["val"] == n)
                rows.append({"trial": trial, "case": case, "resource": res, "kind": kind, "n": n,
                             "mean_ms": ms(statistics.mean(lat)), "p50_ms": ms(pct(lat, 0.5)), "p95_ms": ms(pct(lat, 0.95)),
                             "msgs_per_op": round((c1 - m0) / n, 2),
                             "ns_resolves_per_op": round((ns1 - ns0) / n, 2), "uniform_ok": int(ok_all and uniform)})
        print(f"  overhead trial {trial}: done", flush=True)
    return rows


# ============================================================================ C. migration
async def migration_run(lab, system, rate=50, dur=8.0, t_mig=3.0):
    await lab.reset()
    name = "r1/mig/obj"
    await lab.admin.create([name], rf=2, val=0, where=["mgr1", "mgr2"])
    rec = await lab.admin.resolve(name)
    if system == "transparent":
        lc = logical(lab, deadline=1.0)
        op = lambda i, t: lc._op("write", name, i) if i % 2 == 0 else lc._op("read", name)
    elif system == "static":
        lc = logical(lab, scheme="static", deadline=1.0, table={name: rec})
        op = lambda i, t: lc._op("write", name, i) if i % 2 == 0 else lc._op("read", name)
    else:
        dc = DirectClient(lab.client)
        op = lambda i, t: dc.op("write", "mgr1", name, i) if i % 2 == 0 else dc.op("read", "mgr1", name)
    info = {}

    async def migrator():
        await asyncio.sleep(t_mig)
        info["t"] = time.time() - t0
        info["reply"] = await lab.admin.migrate(name, "mgr3")

    t0 = time.time()
    mig = asyncio.create_task(migrator())
    res = await drive(rate, dur, op, t0)
    await mig
    st = await lab.admin.state("mgr3", name)
    acked = sum(1 for r in res if r["kind"] == "write" and r["ok"])
    after = [r for r in res if r["t"] >= t_mig]
    lat = [r["lat"] for r in res if r["ok"]]
    return {"system": system, "ops": len(res), "errors": sum(1 for r in res if not r["ok"]),
            "errors_after_move": sum(1 for r in after if not r["ok"]),
            "ok_after_move_pct": round(100 * sum(1 for r in after if r["ok"]) / max(1, len(after)), 1),
            "max_ms": ms(max(lat)) if lat else None, "p99_ms": ms(pct(lat, 0.99)),
            "median_ms": ms(pct(lat, 0.5)), "redirected_ops": sum(1 for r in res if r["redirects"]),
            "retried_ops": sum(1 for r in res if r["retries"]),
            "freeze_ms": ms(info["reply"].get("freeze_s")) if info.get("reply") and info["reply"].get("ok") else None,
            "lost_writes": max(0, 1 + acked - st["v"]) if st else None}


async def part_migration(lab, trials):
    rows = []
    for trial in range(trials):
        for system in ("transparent", "static", "direct"):
            rows.append({"trial": trial, **await migration_run(lab, system)})
            r = rows[-1]
            print(f"  migration t{trial} {system:11} errors {r['errors']:3}  max {r['max_ms']} ms  redirected {r['redirected_ops']}  freeze {r['freeze_ms']} ms  lost {r['lost_writes']}", flush=True)
    return rows


def gm(rows, k, **flt):
    xs = [r[k] for r in rows if all(r.get(a) == b for a, b in flt.items()) and r.get(k) not in ("", None)]
    return statistics.mean(xs) if xs else None




# ============================================================================ verdicts
def verdicts(R):
    L = []
    ok = lambda b: "SUPPORTED" if b else "NOT SUPPORTED"
    o = R.get("overhead")
    if o:
        lines = []
        for res in ("mem", "file"):
            d = gm(o, "mean_ms", case="direct", resource=res, kind="read")
            w = gm(o, "mean_ms", case="warm", resource=res, kind="read")
            c = gm(o, "mean_ms", case="cold", resource=res, kind="read")
            cm, wm = gm(o, "msgs_per_op", case="cold", resource=res, kind="read"), gm(o, "msgs_per_op", case="warm", resource=res, kind="read")
            lines.append((res, d, w, c, wm, cm))
        loc = gm(o, "mean_ms", case="local", kind="read")
        uni = all(r["uniform_ok"] for r in o)
        good = all(w / d - 1 < 0.15 and abs((cm - wm) - 2) < 0.4 for _, d, w, c, wm, cm in lines) and loc is not None and loc < 1.0 and uni
        L.append("H9.1 read latency direct/warm/cold (ms): " + "; ".join(f"{res} {d:.1f}/{w:.1f}/{c:.1f} (msgs {wm:.1f}->{cm:.1f})" for res, d, w, c, wm, cm in lines) +
                 f"; local copy {loc:.2f} ms; identical results everywhere: {uni}: {ok(good)}")
    m = R.get("migration")
    if m:
        te, tm, tl = gm(m, "errors", system="transparent"), gm(m, "max_ms", system="transparent"), gm(m, "lost_writes", system="transparent")
        se, de = gm(m, "errors_after_move", system="static"), gm(m, "errors_after_move", system="direct")
        L.append(f"H9.3 transparent: errors {f(te)}, max latency {f(tm, 0)} ms, lost writes {f(tl)}, freeze {f(gm(m, 'freeze_ms', system='transparent'))} ms; "
                 f"errors after the move: static map {f(se, 0)}, hard-coded node {f(de, 0)}: {ok(te == 0 and tl == 0 and tm < 300 and se > 0 and de > 0)}")
    return L



# ============================================================================ demo
async def demo():
    print("Milestone 9 demo (access, location, migration): the application only ever says read/write(name).\n")
    async with Lab("m9demo", lrs={"rf": 2}, base_port=33000) as lab:
        await lab.reset()
        names = ["r1/site1/status", "r1/site2/status", "r2/site3/status"]
        await lab.admin.create(names[:2], rf=2, val={"grid": "ok"})
        await lab.admin.create(["r1/site9/local"], rf=1, val={"grid": "ok"}, where=["client"])
        lc = logical(lab)
        print("ACCESS + LOCATION   same call for a remote memory copy, a remote file copy and a copy on this very node:")
        for nm in names[:2] + ["r1/site9/local"]:
            r = await lc.write(nm, {"grid": "down"})
            rec = await lab.admin.resolve(nm)
            print(f"   write({nm!r}) ok={r['ok']} v={r['v']} lat={r['lat'] * 1000:.1f} ms   (physical: primary {rec['primary']}, backups {rec['backups']} - the application never saw this)")
        print("\nMIGRATION           a client writes 50 ops/s while the primary moves to another node:")
        r = await migration_run(lab, "transparent")
        print(f"   errors {r['errors']} of {r['ops']}, ops redirected {r['redirected_ops']}, writes frozen for {r['freeze_ms']} ms, slowest op {r['max_ms']} ms, lost writes {r['lost_writes']}")
        r2 = await migration_run(lab, "direct")
        print(f"   baseline (hard-coded node): errors {r2['errors']} of {r2['ops']}, {r2['ok_after_move_pct']}% of the calls after the move succeed")
    print("\nAccess, location and migration transparency: the application only ever named the resource, never the node.")


# ============================================================================ main
PARTS = ("overhead", "migration")
KEYS = {"overhead": ["case", "resource", "kind"], "migration": ["system"]}


async def main():
    acquire_lock()
    if "--demo" in sys.argv:
        return await demo()
    quick = "--quick" in sys.argv
    trials = int(sys.argv[sys.argv.index("--trials") + 1]) if "--trials" in sys.argv else (1 if quick else 3)
    parts = sys.argv[sys.argv.index("--part") + 1].split(",") if "--part" in sys.argv else list(PARTS)
    write_env({"experiment": "week9_transparency", "trials": trials, "parts": parts, "managers": 3,
               "storage": "mgr1,mgr2,mgr3,cloud,core(+client)", "monitor_s": 0.5, "async_flush_s": 0.05})
    R = {}
    async with Lab("m9", lrs={"rf": 2}, base_port=30000) as lab:
        for p in parts:
            print(f"\n== {p}", flush=True)
            R[p] = await {"overhead": part_overhead, "migration": part_migration}[p](lab, trials)
            write_csv(f"results/week9_{p}.csv", R[p])
    summ = []
    for p, rows in R.items():
        fields = [k for k in rows[0] if k not in KEYS[p] + ["trial", "n"] and isinstance(rows[0][k], (int, float))]
        for s in summarise(rows, KEYS[p], fields):
            summ.append({"part": p, **s})
    allkeys = sorted({k for s in summ for k in s}, key=lambda k: (k != "part", k))
    write_csv("results/week9_transparency_summary.csv", [{k: s.get(k, "") for k in allkeys} for s in summ])
    L = verdicts(R)
    print("\nHypothesis check (means over trials; sd and 95% CI in results/week9_transparency_summary.csv)")
    for line in L:
        print("  " + line)
    with open("results/week9_transparency_hypotheses.txt", "w") as fh:
        fh.write("\n".join(L) + "\n")
    print("saved results/week9_transparency_hypotheses.txt")


if __name__ == "__main__":
    asyncio.run(main())
