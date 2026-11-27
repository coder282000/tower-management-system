"""Distributed process management (Milestone 10):   Create -> Locate -> Schedule -> Coordinate -> Terminate

A WORKFLOW is a small dependency graph of processes, e.g. the response to a tower fault:

        diagnose  ->  dispatch_team  ->  verify          (each task = one process: cpu, mem, burst, priority)
                  ->  order_fuel     ->

Roles
  EXECUTOR    every worker node (sites and managers). It creates the process with its local process manager and
              scheduler (kernel/), reports the end to the coordinator and answers queries and kills.
  COORDINATOR the Bully-elected LEADER manager (Milestone 4). It runs the workflows: creates the process names,
              chooses the node for each process, starts it, waits for its end, starts the processes that depended on
              it, and terminates what has to be terminated. Any other manager answers NOT_COORD + the leader's name.

The five steps
  Create      the workflow is stored as the replicated object  wf/<wid>  in the Milestone 9 store (durable state)
  Locate      every running process has a name  proc/<pid>  registered with the name server (location = its node);
              any client resolves it with the Milestone 10 naming schemes (resolver.resolve("proc/<pid>"))
  Schedule    placement policy picks the node (round_robin | random | registry | probe2), the node's own scheduler
              (round robin / priority) runs the process; a refused or silent node is skipped
  Coordinate  tasks start when their dependencies are done; state changes are written to wf/<wid>; if the
              coordinator dies, the new leader lists wf/ (a prefix search in the naming service), reads every
              unfinished workflow, re-attaches to the processes that still run, reschedules the lost ones and goes on
  Terminate   PM_TERMINATE kills every process of a workflow on its node, frees its resources, removes its names;
              a finished process is cleaned up the same way

Placement policies
  round_robin  the coordinator's own counter                      0 extra messages
  random       a random worker                                    0 extra messages
  registry     least loaded by the core registry (the Milestone 2 policy: reports are up to 0.5 s old),
               plus the tasks placed in the last 0.5 s            1 extra message
  probe2       ask two random workers for their CURRENT load and take the lighter ("power of two choices")  2 extra messages

Messages: PM_RUN {wf} PM_STATUS {wid} PM_TERMINATE {wid} PM_LOADS (core) | PROC_START PROC_DONE PROC_QUERY PROC_KILL PROC_LOAD
Cfg  cfg["pm"] = {"policy": "round_robin", "rf": 2, "durable": true, "check_s": 0.5}
"""
import asyncio
import random
import time

from services.transparency import AdminClient, LogicalClient, NameServerResolver

PM_TYPES = ("PM_CONFIG", "PM_RUN", "PM_STATUS", "PM_TERMINATE", "PM_LOADS", "PROC_START", "PROC_DONE", "PROC_QUERY", "PROC_KILL", "PROC_LOAD")
TERMINAL = ("DONE", "KILLED", "FAILED")


class ProcessManagement:
    def __init__(self, node):
        self.node = node
        self.cfg = node.cfg.get("pm") or {}
        self.enabled = bool(self.cfg)
        self.policy = self.cfg.get("policy", "round_robin")
        self.durable = self.cfg.get("durable", True)
        self.check_s = self.cfg.get("check_s", 0.5)
        self.rng = random.Random(node.cfg["seed"] + node.spec["port"])
        self.workers = [n["name"] for n in node.cfg["nodes"] if n["role"] in ("site", "manager")]
        self.rr = -1
        self.running, self.finished = {}, {}              # executor: pid -> Process / pid -> status
        self.wfs = {}                                     # coordinator: wid -> workflow dict
        self.events = {}                                  # wid -> asyncio.Event (a task finished)
        self.active = False                               # coordinator: am I the leader and recovered?
        self.recent = {}                                  # registry policy: node -> times of recent placements
        self.seq = 0
        self.n = {"started": 0, "done": 0, "killed": 0, "refused": 0, "probes": 0, "persist": 0, "rescheduled": 0,
                  "recovered": 0, "workflows": 0, "queries": 0}
        self.lc = self.admin = None

    # ---------------------------------------------------------------- lifecycle
    def start(self):
        if not self.enabled or self.node.role != "manager":
            return
        asyncio.create_task(self._leader_loop())

    def _clients(self):
        if self.lc is None:
            self.lc = LogicalClient(self.node, NameServerResolver(self.node, ttl=1e9), seed=self.node.spec["port"])
            self.admin = AdminClient(self.node)

    # ---------------------------------------------------------------- message handler
    async def handle(self, m):
        t, p = m.type, m.payload
        if t == "PROC_START":
            return self._start(m, p)
        if t == "PROC_QUERY":
            self.n["queries"] += 1
            pid = p["pid"]
            if pid in self.running:
                if p.get("coord"):
                    self.running[pid].meta["coord"] = p["coord"]       # a new coordinator adopts the process
                return self.node.msg(m.sender, "PROC_REPLY", {"state": "RUNNING", "node": self.node.name,
                                                              "remaining": round(self.running[pid].remaining, 3)})
            return self.node.msg(m.sender, "PROC_REPLY", {"state": self.finished.get(pid, "UNKNOWN"), "node": self.node.name})
        if t == "PROC_KILL":
            pr = self.running.get(p["pid"])
            if pr is None:
                return self.node.msg(m.sender, "PROC_REPLY", {"ok": False})
            pr.meta["killed"] = True
            pr.remaining = 0.0                                          # the scheduler finishes it in its next slice
            return self.node.msg(m.sender, "PROC_REPLY", {"ok": True})
        if t == "PROC_LOAD":
            return self.node.msg(m.sender, "PROC_REPLY", {"load": self.node.sched.load(), "free": self.node.res.free()["cpu"]})
        if t == "PM_LOADS":
            return self.node.msg(m.sender, "PM_REPLY", {"loads": {k: v.get("load", 0) for k, v in self.node.registry.items()
                                                                  if v.get("role") in ("site", "manager")}})
        if t == "PM_CONFIG":                                            # experiments: switch policy / durability, forget old state
            self.policy = p.get("policy", self.policy)
            self.durable = p.get("durable", self.durable)
            if p.get("reset"):
                self.wfs.clear(), self.events.clear(), self.finished.clear(), self.recent.clear()
            return self.node.msg(m.sender, "PM_REPLY", {"ok": True})
        if t == "PROC_DONE":
            self._on_done(p)
            return self.node.msg(m.sender, "PM_REPLY", {"ok": True})
        if self.node.role != "manager":
            return None
        if t == "PM_RUN":
            leader = self.node.election.leader if self.node.election else None
            if leader != self.node.name or not self.active:
                return self.node.msg(m.sender, "PM_REPLY", {"ok": False, "err": "NOT_COORD", "hint": leader})
            wf = await self._create(p["wf"])
            asyncio.create_task(self._run(wf))
            return self.node.msg(m.sender, "PM_REPLY", {"ok": True, "wid": wf["wid"], "create_s": wf["create_s"]})
        if t == "PM_STATUS":
            wf = self.wfs.get(p["wid"])
            return self.node.msg(m.sender, "PM_REPLY", {"found": wf is not None, "wf": self._summary(wf) if wf else None})
        if t == "PM_TERMINATE":
            return self.node.msg(m.sender, "PM_REPLY", await self.terminate(p["wid"]))
        return None

    # ---------------------------------------------------------------- executor
    def _start(self, m, p):
        pid, sp = p["pid"], p["spec"]
        if pid in self.running or pid in self.finished:                  # a re-sent start (recovered coordinator)
            return self.node.msg(m.sender, "PROC_REPLY", {"ok": True, "node": self.node.name, "dup": True})
        pr = self.node.pm.create_job("proc", sp.get("cpu", 10), sp.get("mem", 32), sp["burst"], sp.get("prio", 5),
                                     {"pid": pid, "coord": p["coord"]})
        if pr.state == "refused":
            self.n["refused"] += 1
            return self.node.msg(m.sender, "PROC_REPLY", {"ok": False, "err": "REFUSED", "node": self.node.name})
        self.running[pid] = pr
        self.n["started"] += 1
        self.node.log.event("pm_exec_start", pid=pid, wid=pid.split(".")[0])
        return self.node.msg(m.sender, "PROC_REPLY", {"ok": True, "node": self.node.name})

    async def on_proc_done(self, pr):
        """Called by Node.on_job_done when a process created by PROC_START ends."""
        pid = pr.meta["pid"]
        self.running.pop(pid, None)
        status = "KILLED" if pr.meta.get("killed") else "DONE"
        self.finished[pid] = status
        if len(self.finished) > 5000:
            self.finished.pop(next(iter(self.finished)))
        self.n["killed" if status == "KILLED" else "done"] += 1
        self.node.log.event("pm_exec_end", pid=pid, status=status)
        for _ in range(3):
            r = await self.node.send(pr.meta["coord"], self.node.msg(pr.meta["coord"], "PROC_DONE", {
                "pid": pid, "node": self.node.name, "status": status}), wait_reply=True, timeout=0.5)
            if r is not None:
                return

    def who(self, name):
        """Broadcast resolution of a process name: the node that runs it answers."""
        return name.startswith("proc/") and name[5:] in self.running

    # ---------------------------------------------------------------- coordinator: create
    async def _create(self, spec):
        self._clients()
        t0 = time.time()
        self.seq += 1
        wid = spec.get("wid") or f"{self.node.name}-{self.node.store.boot[:4]}-w{self.seq}"     # unique across coordinator restarts
        tasks = {}
        for tk in spec["tasks"]:
            tasks[tk["id"]] = {"id": tk["id"], "pid": f"{wid}.{tk['id']}", "after": tk.get("after", []), "state": "PENDING",
                               "node": None, "attempt": 0, "excluded": [],
                               "spec": {k: tk.get(k, d) for k, d in (("cpu", 10), ("mem", 32), ("burst", 0.2), ("prio", 5))}}
        wf = {"wid": wid, "state": "RUNNING", "coord": self.node.name, "tasks": tasks, "t_submit": t0, "create_s": 0.0}
        self.wfs[wid] = wf
        self.events[wid] = asyncio.Event()
        self.n["workflows"] += 1
        if self.durable:
            await self.admin.create([f"wf/{wid}"], rf=self.cfg.get("rf", 2), val=self._durable(wf))
        wf["create_s"] = time.time() - t0
        self.node.log.event("pm_wf_created", wid=wid, tasks=len(tasks), create_s=round(wf["create_s"], 4))
        return wf

    def _durable(self, wf):
        return {k: wf[k] for k in ("wid", "state", "coord", "tasks", "t_submit")}

    async def _persist(self, wf):
        if self.durable:
            self.n["persist"] += 1
            await self.lc.write(f"wf/{wf['wid']}", self._durable(wf))

    # ---------------------------------------------------------------- coordinator: run / schedule / coordinate
    async def _run(self, wf):
        wid, tasks, ev = wf["wid"], wf["tasks"], self.events[wf["wid"]]
        last_check = time.time()
        while wf["state"] == "RUNNING":
            ready = [t for t in tasks.values() if t["state"] == "PENDING" and all(tasks[a]["state"] == "DONE" for a in t["after"])]
            if ready:
                now = time.time()
                for t in ready:
                    t["state"] = "SCHEDULING"
                    t.setdefault("ready_ts", now)
                await asyncio.gather(*[self._pick(t) for t in ready])
                await self._persist(wf)                                          # write-ahead: the chosen node is durable BEFORE the start command
                await asyncio.gather(*[self._schedule(wf, t) for t in ready])    # (a new coordinator then knows where to look: no process runs twice)
                await self._persist(wf)
            if all(t["state"] in TERMINAL for t in tasks.values()):
                break
            ev.clear()
            try:
                await asyncio.wait_for(ev.wait(), self.check_s)
            except asyncio.TimeoutError:
                pass
            if time.time() - last_check >= self.check_s:
                last_check = time.time()
                await self._watch(wf)
        if wf["state"] == "RUNNING":
            wf["state"] = "DONE" if all(t["state"] == "DONE" for t in tasks.values()) else "FAILED"
            wf["t_done"] = time.time()
            await self._persist(wf)
            self.node.log.event("pm_wf_done", wid=wid, state=wf["state"], makespan_s=round(wf["t_done"] - wf["t_submit"], 4))

    async def _choose(self, t, exclude):
        workers = [w for w in self.workers if w not in exclude] or self.workers
        pol = self.policy
        if pol == "random":
            return self.rng.choice(workers)
        if pol == "registry":
            r = await self.node.send(self.node.core_name, self.node.msg(self.node.core_name, "PM_LOADS"), wait_reply=True, timeout=0.5)
            loads = r.payload["loads"] if r else {}
            now = time.time()
            for k in self.recent:
                self.recent[k] = [x for x in self.recent[k] if now - x < 0.5]
            return min(workers, key=lambda w: (loads.get(w, 0) + len(self.recent.get(w, [])), w))
        if pol == "probe2":
            a, b = self.rng.sample(workers, 2) if len(workers) > 1 else (workers[0], workers[0])
            self.n["probes"] += 2
            ra, rb = await asyncio.gather(*[self.node.send(w, self.node.msg(w, "PROC_LOAD"), wait_reply=True, timeout=0.5) for w in (a, b)])
            la = ra.payload["load"] if ra else 1e9
            lb = rb.payload["load"] if rb else 1e9
            return a if la <= lb else b
        self.rr += 1
        return workers[self.rr % len(workers)]

    async def _pick(self, t):
        t["_t0"] = time.time()
        t["node"] = await self._choose(t, t["excluded"])
        t["_place"] = time.time() - t["_t0"]

    async def _schedule(self, wf, t):
        t0 = t.get("_t0", time.time())
        t_place = t0 + t.get("_place", 0.0)
        for attempt in range(len(self.workers) + 2):
            if attempt:                                                          # the first node refused or was silent: choose again
                t["node"] = await self._choose(t, t["excluded"])
                t_place = time.time()
            node = t["node"]
            r = await self.node.send(node, self.node.msg(node, "PROC_START", {"pid": t["pid"], "spec": t["spec"], "coord": self.node.name}),
                                     wait_reply=True, timeout=1.0)
            if r is not None and r.payload.get("ok"):
                t_start = time.time()
                self.recent.setdefault(node, []).append(t_start)
                await self.node.send(self.node.core_name, self.node.msg(self.node.core_name, "NS_PUT", {"recs": [{
                    "name": f"proc/{t['pid']}", "kind": "proc", "primary": node, "backups": [], "epoch": 1}]}), wait_reply=True, timeout=1.0)
                t.update(state="RUNNING", started=time.time(), attempt=t["attempt"] + 1)
                self.node.log.event("pm_task_scheduled", wid=wf["wid"], tid=t["id"], node=node, policy=self.policy,
                                    wait_s=round(t0 - t.get("ready_ts", t0), 4), place_s=round(t_place - t0, 4),
                                    start_s=round(t_start - t_place, 4), register_s=round(time.time() - t_start, 4),
                                    schedule_s=round(time.time() - t0, 4))
                return
            t["excluded"].append(node)
        t["state"] = "FAILED"

    def _on_done(self, p):
        pid, status = p["pid"], p["status"]
        for wf in self.wfs.values():
            for t in wf["tasks"].values():
                if t["pid"] == pid and t["state"] == "RUNNING":
                    t["state"] = status if status != "KILLED" else "KILLED"
                    t["ended"] = time.time()
                    self.node.log.event("pm_task_done", wid=wf["wid"], tid=t["id"], node=t["node"], status=status,
                                        run_s=round(t["ended"] - t["started"], 4))
                    asyncio.create_task(self._cleanup_name(t["pid"]))
                    ev = self.events.get(wf["wid"])
                    if ev:
                        ev.set()
                    return

    async def _cleanup_name(self, pid):
        await self.node.send(self.node.core_name, self.node.msg(self.node.core_name, "NS_DEL", {"name": f"proc/{pid}"}),
                             wait_reply=True, timeout=1.0)

    async def _watch(self, wf):
        """Overdue processes: ask their node; finished -> record, silent -> the node is dead: reschedule elsewhere."""
        now = time.time()
        for t in wf["tasks"].values():
            if t["state"] != "RUNNING" or now - t["started"] < t["spec"]["burst"] * 2 + 1.0:
                continue
            r = await self.node.send(t["node"], self.node.msg(t["node"], "PROC_QUERY", {"pid": t["pid"], "coord": self.node.name}),
                                     wait_reply=True, timeout=0.5)
            if r is None or r.payload.get("state") == "UNKNOWN":
                self.n["rescheduled"] += 1
                self.node.log.event("pm_task_rescheduled", wid=wf["wid"], tid=t["id"], dead=t["node"])
                t["excluded"].append(t["node"])
                t["state"] = "PENDING"
                t.pop("ready_ts", None)
            elif r.payload["state"] in TERMINAL:
                self._on_done({"pid": t["pid"], "status": r.payload["state"]})
            else:
                t["started"] = now - t["spec"]["burst"] * 2                  # still running: look again in a second

    # ---------------------------------------------------------------- coordinator: terminate
    async def terminate(self, wid):
        t0 = time.time()
        wf = self.wfs.get(wid)
        if wf is None:
            return {"ok": False, "err": "UNKNOWN_WORKFLOW"}
        wf["state"] = "TERMINATED"
        killed = 0
        for t in wf["tasks"].values():
            if t["state"] == "RUNNING":
                await self.node.send(t["node"], self.node.msg(t["node"], "PROC_KILL", {"pid": t["pid"]}), wait_reply=True, timeout=0.5)
                killed += 1
            if t["state"] not in TERMINAL:
                t["state"] = "KILLED"
            await self._cleanup_name(t["pid"])
        await self._persist(wf)
        self.node.log.event("pm_wf_terminated", wid=wid, killed=killed, terminate_s=round(time.time() - t0, 4))
        return {"ok": True, "killed": killed, "terminate_s": time.time() - t0}

    # ---------------------------------------------------------------- coordinator: leadership and recovery
    async def _leader_loop(self):
        while True:
            await asyncio.sleep(0.25)
            leader = self.node.election.leader if self.node.election else None
            if leader == self.node.name and not self.active:
                await self._recover()
                self.active = True
            elif leader != self.node.name:
                self.active = False

    async def _recover(self):
        """New leader: find the unfinished workflows of the previous coordinator and carry them on."""
        self._clients()
        names = await self.admin.list("wf/") or []
        adopted = 0
        for name in names:
            wid = name[3:]
            if wid in self.wfs:
                continue
            res = await self.lc.read(name)
            wf = res.get("val") if res["ok"] else None
            if not wf or wf["state"] != "RUNNING":
                continue
            wf["coord"], wf["create_s"] = self.node.name, 0.0
            for t in wf["tasks"].values():
                if t["state"] in ("RUNNING", "SCHEDULING"):                  # re-attach to the process or reschedule it
                    r = await self.node.send(t["node"], self.node.msg(t["node"], "PROC_QUERY", {"pid": t["pid"], "coord": self.node.name}),
                                             wait_reply=True, timeout=0.5) if t["node"] else None
                    if r is not None and r.payload.get("state") == "RUNNING":
                        t["state"], t["started"] = "RUNNING", time.time()
                    elif r is not None and r.payload.get("state") in TERMINAL:
                        t["state"] = r.payload["state"]
                    else:                                                  # unknown to the node (start never arrived) or the node is dead
                        if r is None and t["node"]:
                            t["excluded"] = t["excluded"] + [t["node"]]
                        t["state"] = "PENDING"
                        self.n["rescheduled"] += 1
            self.wfs[wid] = wf
            self.events[wid] = asyncio.Event()
            asyncio.create_task(self._run(wf))
            adopted += 1
        self.n["recovered"] += adopted
        self.node.log.event("pm_recovered", workflows=adopted)

    def _summary(self, wf):
        return {"wid": wf["wid"], "state": wf["state"], "coord": wf["coord"], "t_submit": wf["t_submit"], "t_done": wf.get("t_done"),
                "tasks": {k: {"state": v["state"], "node": v["node"], "attempt": v["attempt"]} for k, v in wf["tasks"].items()}}

    def stats(self):
        return {"pm_" + k: v for k, v in self.n.items()} | {"pm_active": self.active, "pm_running": len(self.running), "pm_wfs": len(self.wfs)}


class ProcessClient:
    """Application side: submit workflows, locate processes by name, terminate. Knows no node except the managers' names."""

    def __init__(self, node, resolver, managers):
        self.node, self.resolver, self.managers = node, resolver, list(managers)
        self.coord = self.managers[-1]

    async def _pm(self, typ, payload, dest=None, timeout=5.0):
        d = dest or self.coord
        r = await self.node.send(d, self.node.msg(d, typ, payload), wait_reply=True, emulate=False, timeout=timeout)
        return r.payload if r is not None else None

    async def run(self, wf, deadline=15.0):
        t0 = time.time()
        tried = 0
        while time.time() - t0 < deadline:
            r = await self._pm("PM_RUN", {"wf": wf}, timeout=8.0)
            if r is not None and r.get("ok"):
                return r
            if r is not None and r.get("err") == "NOT_COORD" and r.get("hint"):
                self.coord = r["hint"]
            else:
                tried += 1
                self.coord = self.managers[tried % len(self.managers)]
            await asyncio.sleep(0.2)
        return None

    async def status(self, wid):
        r = await self._pm("PM_STATUS", {"wid": wid})
        return r["wf"] if r and r.get("found") else None

    async def terminate(self, wid):
        return await self._pm("PM_TERMINATE", {"wid": wid})

    async def locate(self, pid):
        t0 = time.time()
        rec = await self.resolver.resolve(f"proc/{pid}")
        return (rec["primary"] if rec else None), time.time() - t0
