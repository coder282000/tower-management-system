"""Transparency layer, client side (Milestone 9) and the name-resolution schemes (Milestone 10).

The application talks to a LOGICAL resource:

        client = LogicalClient(node, resolver)
        await client.write("r1/site1/status", {"grid": "down"})
        await client.read("r1/site1/status")

and never learns which node holds it, how many copies exist, whether it moved, or that a node died.

What the layer hides (the five transparencies)
  access        one read/write interface for every backend (memory / file) and for local and remote copies
                (a copy on the client's own node is reached by a function call, not a message)
  location      the name carries no location; a resolver maps it to the current primary (and caches the answer)
  replication   one logical object: reads/writes go to the primary, replication is invisible; read_policy="any"
                spreads reads over the copies (cheaper, possibly stale)
  migration     MOVED / NOT_HERE / STALE answers drop the cached location, the name is resolved again, the request
                is repeated; RETRY (a short freeze while the primary moves) is waited out
  failure       an unreachable primary is reported to the name server (NS_SUSPECT), which promotes a backup; the
                client then repeats the request. The application only sees a slower answer.

Resolution schemes (Milestone 10), all with the same interface  resolve(name) -> record | None
  StaticResolver     a table in the client (static map): no messages, never changes, stale after a migration
  BroadcastResolver  ask every holder "NS_WHO name": no name state at all, cost = number of holders per lookup
  NameServerResolver one request to the master name server (optionally cached with a time-to-live)
  PrefixResolver     DNS/Sprite style: a prefix table maps "r1/" to its zone server; a miss asks the root once
                     (or broadcasts if the root is down), afterwards one request to the zone server
Every resolver counts: sent, recv, lookups (calls), hits (served from cache), misses.

DirectClient is the NON-transparent baseline: the caller passes the physical node, one attempt, errors reach the caller.
AdminClient creates, migrates and lists names (talks to the master name server).
"""
import asyncio
import random
import time

from comms.message import Message


def default_lrs(cfg, rf=2, sync=True, monitor_s=0.5, zones=True, extra=(), **kw):
    """Add the lrs block to a cluster config: storage pool = managers + cloud + core, one zone per manager."""
    mgrs = [n["name"] for n in cfg["nodes"] if n["role"] == "manager"]
    cfg["lrs"] = {"enabled": True, "rf": rf, "sync": sync, "monitor_s": monitor_s, "extra": list(extra),
                  "storage": mgrs + ["cloud", "core"],
                  "zones": ({f"r{i + 1}/": m for i, m in enumerate(mgrs)} | {"proc/": mgrs[0], "wf/": mgrs[0], "ptab/": mgrs[0]}) if zones else {},
                  **kw}
    return cfg


# ===================================================================== resolvers
class BaseResolver:
    scheme = "base"

    def __init__(self, node, ttl=0, core="core"):
        self.node, self.ttl, self.core = node, ttl, core
        self.cache = {}
        self.sent = self.recv = self.lookups = self.hits = self.misses = 0
        self.last_error = None

    async def _rpc(self, dest, typ, payload, timeout=0.5):
        self.sent += 1
        r = await self.node.send(dest, self.node.msg(dest, typ, payload), wait_reply=True, timeout=timeout)
        if r is not None:
            self.recv += 1
        return r

    async def resolve(self, name):
        self.lookups += 1
        if self.ttl:
            e = self.cache.get(name)
            if e and e[1] > time.time():
                self.hits += 1
                return e[0]
        self.misses += 1
        rec = await self._lookup(name)
        if rec is not None and self.ttl:
            self.cache[name] = (rec, time.time() + self.ttl)
        return rec

    def invalidate(self, name):
        self.cache.pop(name, None)

    def learn(self, rec):
        if self.ttl and rec:
            self.cache[rec["name"]] = (rec, time.time() + self.ttl)

    async def suspect(self, name, node):
        """Failure transparency: tell the master a node does not answer; it verifies and promotes if needed."""
        r = await self._rpc(self.core, "NS_SUSPECT", {"name": name, "node": node}, timeout=3.0)
        if r is not None and r.payload.get("found"):
            self.learn(r.payload["rec"])
            return r.payload["rec"]
        return None

    def counters(self):
        return {"sent": self.sent, "recv": self.recv, "lookups": self.lookups, "hits": self.hits, "misses": self.misses}


class StaticResolver(BaseResolver):
    scheme = "static"

    def __init__(self, node, table, **kw):
        super().__init__(node, **kw)
        self.table = table

    async def resolve(self, name):
        self.lookups += 1
        return self.table.get(name)

    async def suspect(self, name, node):
        return None                                           # a static map cannot recover by itself


class NameServerResolver(BaseResolver):
    scheme = "nameserver"

    async def _lookup(self, name):
        r = await self._rpc(self.core, "NS_RESOLVE", {"name": name})
        self.last_error = None if r is not None else "ns_unreachable"
        return r.payload["rec"] if r is not None and r.payload.get("found") else None


class BroadcastResolver(BaseResolver):
    scheme = "broadcast"

    def __init__(self, node, holders, **kw):
        super().__init__(node, **kw)
        self.holders = list(holders)

    async def _lookup(self, name):
        async def ask(h):
            self.sent += 1
            r = await self.node.send(h, self.node.msg(h, "NS_WHO", {"name": name}), wait_reply=True, timeout=0.5)
            if r is not None:
                self.recv += 1                                # every holder answers: those replies are real traffic even after we have our answer
            return r
        tasks = [asyncio.ensure_future(ask(h)) for h in self.holders]
        self._bg = getattr(self, "_bg", set())
        found = None
        for fut in asyncio.as_completed(tasks):               # return at the FIRST positive answer; the others finish in the background
            r = await fut
            if r is not None and r.payload.get("found"):
                found = r.payload["rec"]
                break
        for t in tasks:
            if not t.done():
                self._bg.add(t)
                t.add_done_callback(self._bg.discard)
        return found


class PrefixResolver(BaseResolver):
    scheme = "prefix"

    def __init__(self, node, zone_servers, **kw):
        super().__init__(node, **kw)
        self.table = {}                                       # prefix -> zone server (learned)
        self.zone_servers = list(zone_servers)
        self.referrals = 0

    def _server_for(self, name):
        best = None
        for pre in self.table:
            if name.startswith(pre) and (best is None or len(pre) > len(best)):
                best = pre
        return best

    async def _learn_prefix(self, name):
        r = await self._rpc(self.core, "NS_ZONE", {"name": name})
        if r is None:                                         # root down: ask the zone servers directly
            for srv in self.zone_servers:
                r = await self._rpc(srv, "NS_ZONE", {"name": name})
                if r is not None:
                    break
        if r is not None and r.payload.get("found"):
            self.referrals += 1
            self.table[r.payload["prefix"]] = r.payload["server"]
            return r.payload["prefix"]
        return None

    async def _lookup(self, name):
        pre = self._server_for(name) or await self._learn_prefix(name)
        if pre is not None:
            r = await self._rpc(self.table[pre], "NS_RESOLVE", {"name": name})
            if r is None:
                self.table.pop(pre, None)                     # the zone server is gone: forget the prefix
            elif r.payload.get("found"):
                return r.payload["rec"]
        r = await self._rpc(self.core, "NS_RESOLVE", {"name": name})   # not in the zone copy (yet): ask the master
        return r.payload["rec"] if r is not None and r.payload.get("found") else None

    def counters(self):
        return {**super().counters(), "referrals": self.referrals, "prefixes": len(self.table)}


def make_resolver(scheme, node, cfg, ttl=0, table=None):
    lrs = cfg["lrs"]
    holders = lrs["storage"] + [h for h in lrs.get("extra", []) if h not in lrs["storage"]]
    holders += [n["name"] for n in cfg["nodes"] if n["role"] == "site"]          # workers run processes: they answer for their names too
    core = next(n["name"] for n in cfg["nodes"] if n["role"] == "core")
    if scheme == "static":
        return StaticResolver(node, table or {}, ttl=0, core=core)
    if scheme == "nameserver":
        return NameServerResolver(node, ttl=ttl, core=core)
    if scheme == "broadcast":
        return BroadcastResolver(node, holders, ttl=ttl, core=core)
    if scheme == "prefix":
        return PrefixResolver(node, sorted(set(lrs["zones"].values())), ttl=ttl, core=core)
    raise ValueError(scheme)


# ===================================================================== the transparent client
class LogicalClient:
    """read(name) / write(name, value): the whole interface an application sees."""

    def __init__(self, node, resolver, read_policy="primary", op_timeout=0.5, deadline=6.0, seed=1):
        self.node, self.resolver = node, resolver
        self.read_policy, self.op_timeout, self.deadline = read_policy, op_timeout, deadline
        self.rng = random.Random(seed)
        self.m = {"ops": 0, "sent": 0, "recv": 0, "local": 0, "redirects": 0, "failovers": 0, "retries": 0, "errors": 0}

    async def _call(self, target, typ, payload):
        if target == self.node.name:                          # access transparency: same call, no network
            self.m["local"] += 1
            return await self.node.store.local(typ, payload)
        self.m["sent"] += 1
        r = await self.node.send(target, self.node.msg(target, typ, payload), wait_reply=True, timeout=self.op_timeout)
        if r is None:
            return None
        self.m["recv"] += 1
        return r.payload

    async def read(self, name):
        return await self._op("read", name)

    async def write(self, name, val):
        return await self._op("write", name, val)

    async def _op(self, kind, name, val=None):
        t0 = time.time()
        self.m["ops"] += 1
        res = {"ok": False, "kind": kind, "name": name, "attempts": 0, "redirects": 0, "failovers": 0, "retries": 0,
               "node": None, "err": None, "v": None, "val": None, "stale": False}
        delay, avoid = 0.02, set()
        while time.time() - t0 < self.deadline:
            res["attempts"] += 1
            rec = await self.resolver.resolve(name)
            if rec is None or rec.get("unavailable"):
                res["err"] = "UNRESOLVED" if rec is None else "UNAVAILABLE"
                self.resolver.invalidate(name)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 0.3)
                continue
            target, any_ = rec["primary"], False
            if kind == "read" and self.read_policy == "any":
                cands = [x for x in [rec["primary"]] + rec.get("backups", []) if x not in avoid] or [rec["primary"]]
                target, any_ = self.rng.choice(cands), True
            if kind == "read":
                p = await self._call(target, "OBJ_READ", {"name": name, "any": any_})
            else:
                p = await self._call(target, "OBJ_WRITE", {"name": name, "val": val, "epoch": rec.get("epoch")})
            if p is None:                                     # no answer: node down (or message lost)
                res["failovers"] += 1
                self.m["failovers"] += 1
                self.resolver.invalidate(name)
                avoid.add(target)
                if target == rec["primary"] or kind == "write":
                    await self.resolver.suspect(name, target)  # the master verifies and promotes a backup
                await asyncio.sleep(delay)
                delay = min(delay * 2, 0.3)
                continue
            if p.get("ok"):
                res.update(ok=True, node=p.get("node"), v=p.get("v"), val=p.get("val"), err=None)
                res["stale"] = p.get("role") == "backup"
                break
            err = p.get("err")
            res["err"] = err
            if err == "RETRY":                                # the primary is being moved: wait the freeze out
                res["retries"] += 1
                self.m["retries"] += 1
                await asyncio.sleep(0.02)
            else:                                             # MOVED / NOT_HERE / NOT_PRIMARY / STALE: location is outdated
                res["redirects"] += 1
                self.m["redirects"] += 1
                self.resolver.invalidate(name)
                if err == "NOT_HERE":
                    avoid.add(target)
                await asyncio.sleep(0.005)
        res["lat"] = time.time() - t0
        if not res["ok"]:
            self.m["errors"] += 1
        return res


class DirectClient:
    """The non-transparent baseline: the application knows the physical node and handles failures itself (it does not)."""

    def __init__(self, node, op_timeout=0.5):
        self.node, self.op_timeout = node, op_timeout
        self.m = {"ops": 0, "sent": 0, "recv": 0, "errors": 0}

    async def op(self, kind, where, name, val=None):
        t0 = time.time()
        self.m["ops"] += 1
        self.m["sent"] += 1
        typ, payload = ("OBJ_READ", {"name": name}) if kind == "read" else ("OBJ_WRITE", {"name": name, "val": val})
        r = await self.node.send(where, self.node.msg(where, typ, payload), wait_reply=True, timeout=self.op_timeout)
        res = {"ok": False, "kind": kind, "name": name, "attempts": 1, "redirects": 0, "failovers": 0, "retries": 0,
               "node": where, "err": "TIMEOUT", "v": None, "val": None, "stale": False}
        if r is not None:
            self.m["recv"] += 1
            p = r.payload
            res.update(ok=bool(p.get("ok")), err=p.get("err"), v=p.get("v"), val=p.get("val"))
        res["lat"] = time.time() - t0
        if not res["ok"]:
            self.m["errors"] += 1
        return res


# ===================================================================== administration
class AdminClient:
    def __init__(self, node, core="core"):
        self.node, self.core = node, core

    async def _ns(self, typ, payload, timeout=10.0, dest=None):
        d = dest or self.core
        r = await self.node.send(d, self.node.msg(d, typ, payload), wait_reply=True, emulate=False, timeout=timeout)
        return r.payload if r is not None else None

    async def create(self, names, rf=2, sync=True, val=None, where=None, want_recs=False):
        return await self._ns("NS_CREATE", {"names": list(names), "rf": rf, "sync": sync, "val": val, "where": where,
                                            "want_recs": want_recs}, timeout=60.0)

    async def migrate(self, name, dest):
        return await self._ns("NS_MIGRATE", {"name": name, "dest": dest}, timeout=10.0)

    async def list(self, prefix, server=None):
        r = await self._ns("NS_LIST", {"prefix": prefix}, dest=server)
        return r["names"] if r else None

    async def put(self, recs):
        return await self._ns("NS_PUT", {"recs": recs})

    async def delete(self, name):
        return await self._ns("NS_DEL", {"name": name})

    async def reset(self):
        return await self._ns("NS_RESET", {}, timeout=30.0)

    async def resolve(self, name, server=None):
        r = await self._ns("NS_RESOLVE", {"name": name}, dest=server)
        return r["rec"] if r and r.get("found") else None

    async def state(self, node, name):
        """The version and value one specific copy holds (experiment instrumentation, bypasses the transparency layer)."""
        r = await self.node.send(node, self.node.msg(node, "OBJ_STATE", {"name": name}), wait_reply=True, emulate=False, timeout=1.0)
        return r.payload if r is not None else None
