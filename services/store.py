"""Physical resource layer (Milestone 9): a replicated object store that runs on every storage node.

An APPLICATION never talks to this layer directly. It names a logical resource ("r1/site1/status") and the
transparency layer (services/transparency.py) finds, reaches and, if needed, recovers the physical copies.

Objects and roles
  object   name -> {v: version, val: any JSON value}. Every write bumps the version by one.
  role     this node's part for one object: primary | backup | moved
           primary  serves reads and writes, replicates every write to its backups
           backup   keeps a copy; serves reads only if the client asks for a possibly stale read ("any")
           moved    tombstone left behind by a migration: answers MOVED + the new location
  epoch    grows every time the PRIMARY changes (promotion, migration). A write carrying an older epoch is rejected
           (STALE), so a client with an outdated cache, or a primary that was replaced while it was cut off, can
           never write into the wrong copy (fencing).

Replication (per object): sync  = the primary acknowledges a write only after every reachable backup has it
                          async = the primary acknowledges at once and ships the newest values every async_flush_s
A backup that does not answer is skipped for 2 s (it is evicted by the name server meanwhile), so one dead
backup does not slow every write.

Storage backends (access transparency: the application cannot tell them apart)
  memory   a dict, lost when the process dies
  file     the same dict plus an append-only journal <log_dir>/<node>.store.jsonl, reloaded after a restart
After a restart a node's roles are UNVERIFIED: it refuses to serve until the name server confirms them with
OBJ_INSTALL, because another copy may have been promoted while it was down.

Messages (all answered with OBJ_REPLY {ok, err, v, val, epoch, node, role, hint})
  OBJ_READ {name, any}      OBJ_WRITE {name, val, epoch}      OBJ_REPL {name, v, val, epoch} (primary -> backup)
  OBJ_REPL_MANY {items}     OBJ_INSTALL {name, role, epoch, backups, sync, state, keep, hint}
  OBJ_INSTALL_MANY {items}  OBJ_FREEZE {name}   OBJ_STATE {name}   OBJ_VER {name}   OBJ_PING   OBJ_RESET
  NS_WHO {name}             (broadcast resolution: "do you hold the primary of this name?")
"""
import asyncio
import json
import os
import time
import uuid

from comms.message import Message


class ObjectStore:
    def __init__(self, node):
        self.node = node
        self.lrs = node.cfg.get("lrs") or {}
        self.enabled = bool(self.lrs.get("enabled"))
        default = "file" if node.role in ("core", "cloud") else "memory"
        self.backend = (self.lrs.get("backends") or {}).get(node.name, default)
        self.boot = uuid.uuid4().hex[:8]
        self.objs, self.roles, self.dead, self.dirty, self.locks = {}, {}, {}, {}, {}
        self.timeout = self.lrs.get("repl_timeout", 0.3)
        self.flush_s = self.lrs.get("async_flush_s", 0.05)
        self.n = {"read": 0, "write": 0, "repl": 0, "repl_items": 0, "who": 0, "moved": 0, "stale": 0, "retry": 0, "local": 0, "fenced": 0}
        self.journal = None
        if self.enabled and self.backend == "file":
            self._load()

    # ---------------------------------------------------------------- persistence (file backend)
    def _path(self):
        return os.path.join(self.node.cfg.get("log_dir", "logs"), f"{self.node.name}.store.jsonl")

    def _load(self):
        p = self._path()
        if os.path.exists(p):
            with open(p) as f:
                for line in f:
                    try:
                        e = json.loads(line)
                    except ValueError:
                        continue
                    if e["k"] == "obj":
                        self.objs[e["n"]] = {"v": e["v"], "val": e["val"]}
                    elif e["k"] == "role":
                        self.roles[e["n"]] = {**e["r"], "verified": False, "frozen": False}
                    elif e["k"] == "drop":
                        self.objs.pop(e["n"], None)
                        self.roles.pop(e["n"], None)
        os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
        self.journal = open(p, "a", buffering=1)

    def _log(self, rec):
        if self.journal:
            self.journal.write(json.dumps(rec) + "\n")

    def _save_obj(self, name):
        o = self.objs[name]
        self._log({"k": "obj", "n": name, "v": o["v"], "val": o["val"]})

    def _save_role(self, name):
        r = self.roles[name]
        self._log({"k": "role", "n": name, "r": {k: r[k] for k in ("role", "epoch", "backups", "sync", "hint")}})

    # ---------------------------------------------------------------- lifecycle
    def start(self):
        if not self.enabled:
            return
        asyncio.create_task(self._flush_loop())
        if self.node.name != self.node.core_name:
            asyncio.create_task(self._hello())

    async def _hello(self):
        """Tell the name server we (re)started. A new boot id means our memory is gone: it fails us over and re-adds us."""
        for _ in range(200):
            r = await self.node.send(self.node.core_name, self.node.msg(self.node.core_name, "NS_HELLO", {
                "node": self.node.name, "boot": self.boot}), wait_reply=True, emulate=False, timeout=1.0)
            if r is not None:
                return
            await asyncio.sleep(0.25)

    def _lock(self, name):
        return self.locks.setdefault(name, asyncio.Lock())

    def _reply(self, m, **kw):
        return self.node.msg(m.sender, "OBJ_REPLY", {"node": self.node.name, **kw})

    def _err(self, m, err, **kw):
        return self.node.msg(m.sender, "OBJ_REPLY", {"ok": False, "err": err, "node": self.node.name, **kw})

    # ---------------------------------------------------------------- local access (same node: no network)
    async def local(self, typ, payload):
        """The same operations, called in-process: used when the logical client runs on the node that holds the copy."""
        self.n["local"] += 1
        r = await self.handle(Message(self.node.name, self.node.name, typ, payload))
        return r.payload if r else None

    # ---------------------------------------------------------------- message handler
    async def handle(self, m):
        t, p = m.type, m.payload
        name = p.get("name")
        if t == "OBJ_READ":
            self.n["read"] += 1
            r = self.roles.get(name)
            if not r or not r["verified"]:
                return self._err(m, "NOT_HERE")
            if r["role"] == "moved":
                self.n["moved"] += 1
                return self._err(m, "MOVED", hint=r.get("hint"))
            if r["role"] == "backup" and not p.get("any"):
                return self._err(m, "NOT_PRIMARY")
            o = self.objs.get(name, {"v": 0, "val": None})
            return self._reply(m, ok=True, v=o["v"], val=o["val"], epoch=r["epoch"], role=r["role"])
        if t == "OBJ_WRITE":
            self.n["write"] += 1
            r = self.roles.get(name)
            if not r or not r["verified"]:
                return self._err(m, "NOT_HERE")
            if r["role"] == "moved":
                self.n["moved"] += 1
                return self._err(m, "MOVED", hint=r.get("hint"))
            if r["role"] != "primary":
                return self._err(m, "NOT_PRIMARY")
            if p.get("epoch") is not None and p["epoch"] != r["epoch"]:
                self.n["stale"] += 1
                return self._err(m, "STALE", epoch=r["epoch"])
            if r["frozen"]:
                self.n["retry"] += 1
                return self._err(m, "RETRY")
            async with self._lock(name):
                if r["role"] == "moved":                          # fenced or migrated while this request waited for the lock
                    self.n["moved"] += 1
                    return self._err(m, "MOVED", hint=r.get("hint"))
                if r["frozen"] or self.roles.get(name) is not r or r["role"] != "primary":
                    self.n["retry"] += 1
                    return self._err(m, "RETRY")
                o = self.objs.setdefault(name, {"v": 0, "val": None})
                o["v"] += 1
                o["val"] = p["val"]
                self._save_obj(name)
                if r["backups"]:
                    if r["sync"]:
                        if await self._replicate(name, o, r):          # a backup rejected our epoch: we were replaced
                            r["role"], r["hint"] = "moved", None       # fence ourselves; the write is NOT acknowledged
                            self.n["fenced"] += 1
                            return self._err(m, "MOVED", hint=None)
                    else:
                        self.dirty[name] = r
                return self._reply(m, ok=True, v=o["v"], epoch=r["epoch"], role="primary")
        if t == "OBJ_REPL":
            self.n["repl"] += 1                                   # messages received
            self.n["repl_items"] += 1                             # object updates received
            return self._apply_repl(m, p)
        if t == "OBJ_REPL_MANY":
            self.n["repl"] += 1
            self.n["repl_items"] += len(p["items"])
            ok = all(self._apply_repl(m, it).payload.get("ok") for it in p["items"])
            return self._reply(m, ok=ok)
        if t in ("OBJ_INSTALL", "OBJ_INSTALL_MANY"):
            for it in (p["items"] if t == "OBJ_INSTALL_MANY" else [p]):
                self._install(it)
            return self._reply(m, ok=True)
        if t == "OBJ_FREEZE":
            r = self.roles.get(name)
            if not r or r["role"] != "primary" or not r["verified"]:
                return self._err(m, "NOT_PRIMARY")
            r["frozen"] = True
            async with self._lock(name):                      # wait for the write in progress to finish
                o = self.objs.get(name, {"v": 0, "val": None})
                return self._reply(m, ok=True, v=o["v"], val=o["val"])
        if t == "OBJ_STATE":
            o = self.objs.get(name)
            return self._reply(m, ok=o is not None, v=o["v"] if o else 0, val=o["val"] if o else None)
        if t == "OBJ_VER":
            o = self.objs.get(name)
            return self._reply(m, ok=o is not None, v=o["v"] if o else -1)
        if t == "OBJ_PING":
            return self._reply(m, ok=True, boot=self.boot)
        if t == "OBJ_RESET":
            self.objs.clear(), self.roles.clear(), self.dead.clear(), self.dirty.clear()
            if self.journal:
                self.journal.close()
                self.journal = open(self._path(), "w", buffering=1)
            return self._reply(m, ok=True)
        if t == "NS_WHO":
            self.n["who"] += 1
            r = self.roles.get(name)
            if r and r["verified"] and r["role"] == "primary":
                return self.node.msg(m.sender, "NS_REPLY", {"found": True, "rec": {
                    "name": name, "kind": "obj", "primary": self.node.name, "backups": r["backups"], "epoch": r["epoch"]}})
            return self.node.msg(m.sender, "NS_REPLY", {"found": False})
        return None

    def who(self, name):
        r = self.roles.get(name)
        return bool(r and r["verified"] and r["role"] == "primary")

    # ---------------------------------------------------------------- replication
    def _apply_repl(self, m, p):
        name = p["name"]
        r = self.roles.get(name)
        if not r or not r["verified"]:
            return self._err(m, "NOT_HERE")
        if p["epoch"] < r["epoch"]:
            return self._err(m, "STALE", epoch=r["epoch"])
        o = self.objs.setdefault(name, {"v": 0, "val": None})
        if p["v"] > o["v"]:
            o["v"], o["val"] = p["v"], p["val"]
            self._save_obj(name)
        return self._reply(m, ok=True, v=o["v"])

    async def _send_repl(self, b, typ, payload):
        ack = await self.node.send(b, self.node.msg(b, typ, payload), wait_reply=True, timeout=self.timeout)
        if ack is None or (not ack.payload.get("ok") and ack.payload.get("err") != "STALE"):
            self.dead[b] = time.time() + 2.0                      # unreachable: skip it for 2 s (a STALE answer is not a failure: it is fencing)
        return ack

    async def _replicate(self, name, o, r):
        now = time.time()
        acks = await asyncio.gather(*[self._send_repl(b, "OBJ_REPL", {"name": name, "v": o["v"], "val": o["val"], "epoch": r["epoch"]})
                                      for b in r["backups"] if self.dead.get(b, 0) <= now])
        return any(a is not None and a.payload.get("err") == "STALE" for a in acks)

    async def _flush_loop(self):
        """Asynchronous replication: ship the newest value of every changed object, batched per backup."""
        while True:
            await asyncio.sleep(self.flush_s)
            if not self.dirty:
                continue
            dirty, self.dirty = self.dirty, {}
            per = {}
            now = time.time()
            for name, r in dirty.items():
                o = self.objs.get(name)
                for b in r["backups"]:
                    if o and self.dead.get(b, 0) <= now:
                        per.setdefault(b, []).append({"name": name, "v": o["v"], "val": o["val"], "epoch": r["epoch"]})
            for b, items in per.items():
                asyncio.create_task(self._send_repl(b, "OBJ_REPL_MANY", {"items": items}))

    # ---------------------------------------------------------------- roles
    def _install(self, it):
        name, role = it["name"], it["role"]
        if role == "drop":
            self.objs.pop(name, None)
            self.roles.pop(name, None)
            self._log({"k": "drop", "n": name})
            return
        st = it.get("state")
        if st is not None:
            self.objs[name] = {"v": st["v"], "val": st["val"]}
            self._save_obj(name)
        elif name not in self.objs:                           # keep=True but we hold nothing: data lost with our memory
            self.objs[name] = {"v": 0, "val": None}
            self.node.log.event("lrs_data_lost", name=name)
        self.roles[name] = {"role": role, "epoch": it["epoch"], "backups": list(it.get("backups", [])),
                            "sync": it.get("sync", True), "hint": it.get("hint"), "verified": True, "frozen": False}
        self._save_role(name)

    def stats(self):
        return {"lrs_" + k: v for k, v in self.n.items()} | {
            "lrs_objects": len(self.objs), "lrs_primary": sum(1 for r in self.roles.values() if r["role"] == "primary"),
            "lrs_backend": self.backend}
