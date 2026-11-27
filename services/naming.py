"""Naming subsystem, server side (Milestones 9 and 10).

    Name  ->  Name Resolution  ->  Resource Location          e.g.  r1/site1/status  ->  {primary: mgr1, backups: [mgr2]}

Names are hierarchical paths. What a name can denote (rec["kind"]):
    obj    a replicated object in the store   (location = primary + backups, epoch)
    proc   a running process                  (location = the node that runs it)

Two kinds of server share this one class
  MASTER  (the core)   holds every record. Only the master changes records: it creates objects, promotes a backup when a
                       primary dies, migrates a primary, and re-adds a restarted node. Its table is journalled to
                       <log_dir>/core.ns.jsonl, so a restarted core is back with all names.
  ZONE    (a manager)  holds a read-only copy of the records under the prefixes delegated to it in cfg["lrs"]["zones"]
                       (like a DNS zone). The master pushes every change to the zone server, so it is eventually
                       consistent; a zone server keeps answering when the master is down.

Messages (answered with NS_REPLY)
  NS_RESOLVE {name}                -> {found, rec}                           any server
  NS_ZONE {name}                   -> {found, prefix, server}                which zone server is responsible (prefix tables)
  NS_PUT {recs}                    -> ack                                    master -> zone servers; also admin registration
  NS_DEL {name}                    NS_LIST {prefix} -> {names}               LDAP-style search under a prefix
  NS_CREATE {names, rf, sync, val, where}   create replicated objects, place them, register the names      (master)
  NS_MIGRATE {name, dest}          move the primary copy while clients keep running                      (master)
  NS_SUSPECT {name, node}          a client could not reach `node`: verify, and promote a backup if it is dead (master)
  NS_HELLO {node, boot}            a node (re)started: a new boot id means it lost its memory                  (master)
  NS_RESET                         forget everything (experiments)
Failure handling in the master: a monitor pings every holder; two missed pings, or a client's suspicion confirmed by a
ping, fail the node over: every object whose primary was the node gets its most up-to-date backup as the new primary
(epoch + 1); every object that had the node as a backup drops it. When the node is back (NS_HELLO) it is re-added as
a backup and copies the state from the current primary.
"""
import asyncio
import json
import os
import time
import zlib


class NamingService:
    def __init__(self, node):
        self.node = node
        self.lrs = node.cfg.get("lrs") or {}
        self.enabled = bool(self.lrs.get("enabled"))
        mgrs = [n["name"] for n in node.cfg["nodes"] if n["role"] == "manager"]
        self.storage = self.lrs.get("storage") or (mgrs + ["cloud", node.core_name])      # placement pool
        self.holders = self.storage + [h for h in self.lrs.get("extra", []) if h not in self.storage]
        self.zones = self.lrs.get("zones") or {}
        self.is_master = node.name == node.core_name
        self.records, self.evicted, self.boots = {}, {}, {}
        self.down, self.misses = set(), {}
        self.lock = asyncio.Lock()
        self.n = {"resolve": 0, "zone": 0, "put": 0, "list": 0, "create": 0, "migrate": 0, "promote": 0, "suspect": 0,
                  "rejoin": 0, "hello": 0, "refer": 0}
        self.last_migration_s = 0.0
        self.journal = None
        if self.enabled and self.is_master:
            self._load()

    # ---------------------------------------------------------------- journal (master)
    def _path(self):
        return os.path.join(self.node.cfg.get("log_dir", "logs"), f"{self.node.name}.ns.jsonl")

    def _load(self):
        p = self._path()
        if os.path.exists(p):
            with open(p) as f:
                for line in f:
                    try:
                        e = json.loads(line)
                    except ValueError:
                        continue
                    if e["op"] == "put":
                        self.records[e["rec"]["name"]] = e["rec"]
                    elif e["op"] == "del":
                        self.records.pop(e["name"], None)
                    elif e["op"] == "boot":
                        self.boots[e["node"]] = e["boot"]
                    elif e["op"] == "evict":
                        self.evicted[e["name"]] = e["nodes"]
        os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
        self.journal = open(p, "a", buffering=1)

    def _j(self, e):
        if self.journal:
            self.journal.write(json.dumps(e) + "\n")

    def start(self):
        if self.enabled and self.is_master and self.lrs.get("monitor_s", 0) > 0:
            asyncio.create_task(self._monitor())
        if self.enabled and not self.is_master and self.node.name in self.zones.values():
            pass                                              # a zone server fills up through NS_HELLO (see store._hello)

    # ---------------------------------------------------------------- helpers
    def zone_of(self, name):
        best = None
        for pre in self.zones:
            if name.startswith(pre) and (best is None or len(pre) > len(best)):
                best = pre
        return best

    async def ctl(self, dest, typ, payload, timeout=1.0):
        """Control-plane call to another node (no link emulation: this is orchestration, not application traffic)."""
        if dest == self.node.name:
            if typ.startswith("OBJ_"):
                r = await self.node.store.handle(self.node.msg(self.node.name, typ, payload))
                return r
            return None
        return await self.node.send(dest, self.node.msg(dest, typ, payload), wait_reply=True, emulate=False, timeout=timeout)

    def _put(self, rec):
        rec["ts"] = time.time()
        self.records[rec["name"]] = rec
        if self.is_master:
            self._j({"op": "put", "rec": rec})

    def _push(self, recs):
        """Master -> zone servers: keep the read-only zone copies up to date."""
        if not self.is_master or not self.zones:
            return
        per = {}
        for rec in recs:
            pre = self.zone_of(rec["name"])
            if pre and self.zones[pre] != self.node.name:
                per.setdefault(self.zones[pre], []).append(rec)
        for srv, rs in per.items():
            asyncio.create_task(self.ctl(srv, "NS_PUT", {"recs": rs, "replica": True}, timeout=1.0))

    def _place(self, name, rf):
        pool = [s for s in self.storage if s not in self.down] or self.storage
        i = zlib.crc32(name.encode()) % len(pool)
        return [pool[(i + k) % len(pool)] for k in range(min(rf, len(pool)))]

    async def alive(self, node, timeout=0.3):
        r = await self.ctl(node, "OBJ_PING", {}, timeout=timeout)
        return r is not None

    def _rep(self, m, **kw):
        return self.node.msg(m.sender, "NS_REPLY", kw)

    # ---------------------------------------------------------------- message handler
    async def handle(self, m):
        t, p = m.type, m.payload
        if t == "NS_RESOLVE":
            self.n["resolve"] += 1
            rec = self.records.get(p["name"])
            return self._rep(m, found=rec is not None, rec=rec)
        if t == "NS_ZONE":
            self.n["zone"] += 1
            pre = self.zone_of(p["name"])
            return self._rep(m, found=pre is not None, prefix=pre, server=self.zones.get(pre))
        if t == "NS_PUT":
            self.n["put"] += 1
            for rec in p["recs"]:
                self._put(rec)
            if self.is_master and not p.get("replica"):
                self._push(p["recs"])
            return self._rep(m, ok=True)
        if t == "NS_DEL":
            self.records.pop(p["name"], None)
            if self.is_master:
                self._j({"op": "del", "name": p["name"]})
                for srv in set(self.zones.values()) - {self.node.name}:
                    asyncio.create_task(self.ctl(srv, "NS_DEL", {"name": p["name"]}, timeout=1.0))
            return self._rep(m, ok=True)
        if t == "NS_LIST":
            self.n["list"] += 1
            return self._rep(m, names=sorted(k for k in self.records if k.startswith(p["prefix"])))
        if not self.is_master:
            if t == "NS_RESET":
                self.records.clear()
                return self._rep(m, ok=True)
            return None
        # ----- master only
        if t == "NS_CREATE":
            async with self.lock:
                return self._rep(m, **await self.create(p))
        if t == "NS_MIGRATE":
            async with self.lock:
                return self._rep(m, **await self.migrate(p["name"], p["dest"]))
        if t == "NS_SUSPECT":
            return self._rep(m, **await self.suspect(p["name"], p["node"]))
        if t == "NS_HELLO":
            asyncio.create_task(self.hello(p["node"], p["boot"]))
            return self._rep(m, ok=True)
        if t == "NS_RESET":
            async with self.lock:
                self.records.clear(), self.evicted.clear(), self.down.clear(), self.misses.clear()
                if self.journal:
                    self.journal.close()
                    self.journal = open(self._path(), "w", buffering=1)
                for h in self.holders:
                    await self.ctl(h, "OBJ_RESET", {})
                for srv in set(self.zones.values()) - {self.node.name}:
                    await self.ctl(srv, "NS_RESET", {})
            return self._rep(m, ok=True)
        return None

    # ---------------------------------------------------------------- create
    async def create(self, p):
        names, rf, sync, val, where = p["names"], p.get("rf", 2), p.get("sync", True), p.get("val"), p.get("where")
        self.n["create"] += len(names)
        per, recs = {}, []
        for i, name in enumerate(names):
            nodes = list(where[i] if where and isinstance(where[0], list) else where) if where else self._place(name, rf)
            rec = {"name": name, "kind": "obj", "primary": nodes[0], "backups": nodes[1:], "epoch": 1, "sync": sync}
            recs.append(rec)
            st = {"v": 1, "val": val} if val is not None else {"v": 0, "val": None}
            for k, nd in enumerate(nodes):
                per.setdefault(nd, []).append({"name": name, "role": "primary" if k == 0 else "backup", "epoch": 1,
                                               "backups": nodes[1:] if k == 0 else [], "sync": sync, "state": st})
        results = await asyncio.gather(*[self.ctl(nd, "OBJ_INSTALL_MANY", {"items": its}, timeout=5.0) for nd, its in per.items()])
        if any(r is None for r in results):
            return {"ok": False, "err": "INSTALL_FAILED"}
        for rec in recs:
            self._put(rec)
        self._push(recs)
        return {"ok": True, "recs": recs if (p.get("want_recs") or len(recs) <= 50) else None, "n": len(recs)}

    # ---------------------------------------------------------------- migration
    async def migrate(self, name, dest):
        rec = self.records.get(name)
        if rec is None or rec.get("kind") != "obj":
            return {"ok": False, "err": "NO_SUCH_OBJECT"}
        old = rec["primary"]
        if dest == old or dest in rec["backups"] or dest not in self.holders:
            return {"ok": False, "err": "BAD_DESTINATION"}
        self.n["migrate"] += 1
        t0 = time.time()
        fr = await self.ctl(old, "OBJ_FREEZE", {"name": name}, timeout=1.0)        # writes get RETRY from here on
        if fr is None or not fr.payload.get("ok"):
            return {"ok": False, "err": "SOURCE_UNREACHABLE"}
        epoch = rec["epoch"] + 1
        ok = await self.ctl(dest, "OBJ_INSTALL", {"name": name, "role": "primary", "epoch": epoch, "backups": rec["backups"],
                                                  "sync": rec.get("sync", True),
                                                  "state": {"v": fr.payload["v"], "val": fr.payload["val"]}}, timeout=1.0)
        if ok is None:                                                              # abort: unfreeze the old primary
            await self.ctl(old, "OBJ_INSTALL", {"name": name, "role": "primary", "epoch": rec["epoch"], "backups": rec["backups"],
                                                "sync": rec.get("sync", True)})
            return {"ok": False, "err": "DEST_UNREACHABLE"}
        for b in rec["backups"]:
            await self.ctl(b, "OBJ_INSTALL", {"name": name, "role": "backup", "epoch": epoch, "sync": rec.get("sync", True)})
        new = {**rec, "primary": dest, "epoch": epoch}
        self._put(new)
        self._push([new])
        await self.ctl(old, "OBJ_INSTALL", {"name": name, "role": "moved", "epoch": epoch, "hint": dest})
        self.last_migration_s = time.time() - t0
        self.node.log.event("ns_migrated", name=name, src=old, dest=dest, epoch=epoch, freeze_s=round(self.last_migration_s, 4))
        return {"ok": True, "rec": new, "freeze_s": self.last_migration_s}

    # ---------------------------------------------------------------- failure handling
    async def suspect(self, name, node):
        self.n["suspect"] += 1
        rec = self.records.get(name)
        if rec is None:
            return {"found": False}
        if rec["primary"] != node:
            return {"found": True, "rec": rec}                                      # already changed: stale suspicion
        if not await self.alive(node):
            async with self.lock:
                await self.fail_node(node)
        return {"found": True, "rec": self.records.get(name)}

    async def fail_node(self, node):
        """Promote the most up-to-date backup of every object the node was primary of; drop the node from every backup list."""
        if node in self.down:
            return
        self.down.add(node)
        t0 = time.time()
        changed = []
        for name, rec in list(self.records.items()):
            if rec.get("kind") != "obj":
                continue
            if rec["primary"] == node:
                if not rec["backups"]:
                    self._put({**rec, "unavailable": True})                         # no copy left: wait for the node
                    continue
                vers = []
                for b in rec["backups"]:
                    r = await self.ctl(b, "OBJ_VER", {"name": name}, timeout=0.3)
                    if r is not None and r.payload.get("ok"):
                        vers.append((r.payload["v"], b))
                if not vers:
                    self._put({**rec, "unavailable": True})
                    continue
                new = max(vers)[1]
                rest = [b for b in rec["backups"] if b != new]
                epoch = rec["epoch"] + 1
                await self.ctl(new, "OBJ_INSTALL", {"name": name, "role": "primary", "epoch": epoch, "backups": rest,
                                                    "sync": rec.get("sync", True), "keep": True})
                for b in rest:
                    await self.ctl(b, "OBJ_INSTALL", {"name": name, "role": "backup", "epoch": epoch, "keep": True})
                self.evicted.setdefault(name, []).append(node)
                self._j({"op": "evict", "name": name, "nodes": self.evicted[name]})
                nrec = {k: v for k, v in rec.items() if k != "unavailable"} | {"primary": new, "backups": rest, "epoch": epoch}
                self._put(nrec)
                changed.append(nrec)
                self.n["promote"] += 1
                self.node.log.event("ns_promoted", name=name, dead=node, new=new, epoch=epoch)
            elif node in rec["backups"]:
                rest = [b for b in rec["backups"] if b != node]
                self.evicted.setdefault(name, []).append(node)
                self._j({"op": "evict", "name": name, "nodes": self.evicted[name]})
                nrec = {**rec, "backups": rest}
                self._put(nrec)
                changed.append(nrec)
                await self.ctl(rec["primary"], "OBJ_INSTALL", {"name": name, "role": "primary", "epoch": rec["epoch"], "backups": rest,
                                                               "sync": rec.get("sync", True), "keep": True})
        self._push(changed)
        self.node.log.event("ns_node_failed", node=node, objects=len(changed), took_s=round(time.time() - t0, 4))

    async def hello(self, node, boot):
        async with self.lock:
            seen = self.boots.get(node)
            self.boots[node] = boot
            self._j({"op": "boot", "node": node, "boot": boot})
            self.n["hello"] += 1
            if node in self.zones.values() and node != self.node.name:
                self._push([r for r in self.records.values() if self.zone_of(r["name"]) and self.zones[self.zone_of(r["name"])] == node])
            if seen is not None and seen != boot:                                   # restarted: memory gone
                await self.fail_node(node)
            if node in self.down:
                await self.rejoin(node)

    async def rejoin(self, node):
        """The node is back: re-add it as a backup (state copied from the current primary) or let it resume as sole primary."""
        self.down.discard(node)
        self.n["rejoin"] += 1
        changed = []
        for name, rec in list(self.records.items()):
            if rec.get("kind") != "obj":
                continue
            if node in self.evicted.get(name, []):
                st = await self.ctl(rec["primary"], "OBJ_STATE", {"name": name}, timeout=1.0)
                if st is None or not st.payload.get("ok"):
                    continue
                backups = rec["backups"] + [node]
                await self.ctl(node, "OBJ_INSTALL", {"name": name, "role": "backup", "epoch": rec["epoch"], "sync": rec.get("sync", True),
                                                     "state": {"v": st.payload["v"], "val": st.payload["val"]}})
                await self.ctl(rec["primary"], "OBJ_INSTALL", {"name": name, "role": "primary", "epoch": rec["epoch"], "backups": backups,
                                                               "sync": rec.get("sync", True), "keep": True})
                self.evicted[name] = [x for x in self.evicted[name] if x != node]
                self._j({"op": "evict", "name": name, "nodes": self.evicted[name]})
                nrec = {**rec, "backups": backups}
                self._put(nrec)
                changed.append(nrec)
            elif rec["primary"] == node and rec.get("unavailable"):
                await self.ctl(node, "OBJ_INSTALL", {"name": name, "role": "primary", "epoch": rec["epoch"], "backups": rec["backups"],
                                                     "sync": rec.get("sync", True), "keep": True})
                nrec = {k: v for k, v in rec.items() if k != "unavailable"}
                self._put(nrec)
                changed.append(nrec)
        self._push(changed)
        self.node.log.event("ns_rejoined", node=node, objects=len(changed))

    async def _monitor(self):
        """Proactive failure detection: ping every holder (in parallel); two misses fail it over, an answer re-adds it."""
        iv = self.lrs.get("monitor_s", 0.5)
        while True:
            await asyncio.sleep(iv)
            targets = [h for h in self.holders if h != self.node.name]
            oks = await asyncio.gather(*[self.alive(h, timeout=min(0.3, iv)) for h in targets])
            for h, ok in zip(targets, oks):
                self.misses[h] = 0 if ok else self.misses.get(h, 0) + 1
                if self.misses[h] >= 2 and h not in self.down:
                    async with self.lock:
                        await self.fail_node(h)
                elif ok and h in self.down:                       # it answers again (restarted or resumed): re-add it
                    async with self.lock:
                        if h in self.down:
                            await self.rejoin(h)

    def stats(self):
        return {"ns_" + k: v for k, v in self.n.items()} | {"ns_records": len(self.records), "ns_down": sorted(self.down)}
