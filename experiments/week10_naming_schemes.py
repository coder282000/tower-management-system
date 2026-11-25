"""Milestone 10 (part 1 of 2): NAMING.   Owner: Kelvin Oluchina.

Split out of the original week10_naming_procs.py.   Name -> Name Resolution -> Resource Location.  Six resolution schemes behind one
interface (services/transparency.py):
   static            a table in the client                           0 messages, stale after any change
   broadcast         ask every holder "who has this name?"           2 x holders messages, no name state anywhere
   nameserver        one request to the master (core)                2 messages
   nameserver+cache  the same with a time-to-live cache              0 messages on a hit, stale until the TTL ends
   prefix            DNS/Sprite prefix table: "r1/" -> zone server   2 messages when warm, +2 for the first referral
   prefix+cache      the same with a TTL cache

Experiments (--part)
  resolution   A  latency and messages per resolution, cold and warm, for every scheme                          -> report 2.3
  staleness    B  20% of the names are migrated: wrong answers right after, and time until every scheme is right -> report 2.4
  scale        C  scalability: (1) number of names, (2) concurrent resolvers and load per server, (3) holders     -> report 2.5
  nsfailure    D  the master (name server and root) is killed: which schemes keep resolving?                     -> report 2.6

Hypotheses
  H10.1 Messages per resolution: static 0, nameserver 2, prefix 2 (warm), broadcast 2 x holders; every scheme resolves correctly
        when nothing changes.
  H10.2 After a migration static and cached resolvers answer wrongly (a static map for ever, a cache until its TTL); nameserver and
        broadcast never; prefix converges within a fraction of a second (the master pushes to the zone servers).
  H10.3 Scalability: broadcast cost grows linearly with the number of nodes, nameserver and prefix cost does not; the static table
        grows linearly with the number of names; prefix spreads the resolution load over the zone servers.
  H10.4 When the master dies, an uncached nameserver client cannot resolve any more, while broadcast, prefix and cached clients can.
Controlled: link 5 ms +-2 ms, seed 42, one machine (1 vCPU): client-side load generation shares the CPU.

    python3 -m experiments.week10_naming_schemes --demo           narrated, about 1 min
    python3 -m experiments.week10_naming_schemes --quick          1 trial of everything
    python3 -m experiments.week10_naming_schemes                  3 trials
    options: --part resolution,staleness,scale,nsfailure   --trials N
Writes results/week10_<part>.csv, results/week10_naming_summary.csv (mean, sd, CI), results/week10_naming_hypotheses.txt.
The process-management parts (lifecycle, policy, procscale, coordfail, workerfail, terminate) are in week10_process_management.py.
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
        async with Lab(f"m10nodes{m}", managers=m, base_port=36000 + 100 * m, extra=()) as l2:
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


def gm(rows, k, **flt):
    xs = [r[k] for r in rows if all(r.get(a) == b for a, b in flt.items()) and r.get(k) not in ("", None)]
    return statistics.mean(xs) if xs else None




# ============================================================================ verdicts
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
    return L



# ============================================================================ demo
async def demo():
    print("Milestone 10 demo (naming).\n")
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
    print("\nName -> Resolution -> Location: static is free but stale, broadcast costs 2 x holders, nameserver/prefix cost 2 messages; caches are fast and briefly wrong.")


# ============================================================================ main
PARTS = ("resolution", "staleness", "scale", "nsfailure")
KEYS = {"resolution": ["scheme"], "staleness": ["scheme"], "scale": ["test", "x", "scheme"], "nsfailure": ["scheme", "client"]}


async def main():
    acquire_lock()
    if "--demo" in sys.argv:
        return await demo()
    quick = "--quick" in sys.argv
    trials = int(sys.argv[sys.argv.index("--trials") + 1]) if "--trials" in sys.argv else (1 if quick else 3)
    parts = sys.argv[sys.argv.index("--part") + 1].split(",") if "--part" in sys.argv else list(PARTS)
    write_env({"experiment": "week10_naming_schemes", "trials": trials, "parts": parts, "naming_cluster": "3 managers + cloud + core",
               "zones": "one per manager + proc/ wf/ ptab/ on mgr1"})
    R = {}
    fns = {"resolution": part_resolution, "staleness": part_staleness, "scale": part_scale, "nsfailure": part_nsfailure}
    async with Lab("m10n", managers=3, extra=(), base_port=35000) as lab:
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
    write_csv("results/week10_naming_summary.csv", [{k: s.get(k, "") for k in allkeys} for s in summ])
    L = verdicts(R)
    print("\nHypothesis check (means over trials; sd and 95% CI in results/week10_naming_summary.csv)")
    for line in L:
        print("  " + line)
    with open("results/week10_naming_hypotheses.txt", "w") as fh:
        fh.write("\n".join(L) + "\n")
    print("saved results/week10_naming_hypotheses.txt")


if __name__ == "__main__":
    asyncio.run(main())
