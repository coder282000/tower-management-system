"""Fault-tolerance service (Milestone 7).

Enabled by an "ft" block in the cluster config (absent = every earlier milestone behaves exactly as before):

    "ft": {"mode": "baseline" | "proposed", "miss_limit": 3, "detect_timeout": 1.5, "sync_interval": 0.5}

Modes (switchable at run time with an FT_CONFIG message, or at start-up with the FT_MODE environment variable so a
relaunched node comes back in the same mode as the rest of the cluster):

  baseline   the original system. Sites send to their own manager and never react; managers and sites send
             escalations once, fire-and-forget. The service still MEASURES health (acknowledgement age) so
             availability can be compared, but it takes no corrective action.
  proposed   1. failure detection   a site that gets no HEALTHY acknowledgement miss_limit times in a row suspects
                                    its manager; the core suspects a manager silent for detect_timeout seconds
             2. failover            the site switches to the backup manager (next manager in the ring)
             3. replication         every sync_interval a manager copies its open incidents to its backup
                                    (FT_SYNC), so the backup continues the incident instead of starting over
             4. store-and-forward   escalations are kept in an outbox and re-sent until the core acknowledges
                                    them; the core drops duplicates by escalation id (eid)
             5. soft-state          every node re-registers with the core periodically, so a restarted core
                re-registration  learns the cluster again without help

  6. site-down detection   (proposed) sites re-register with the core every rereg_interval; the core suspects a site
                           silent for site_timeout (default 3 x rereg_interval) and logs ft_site_down / ft_site_up

Fault injection (FT_INJECT, sent by the experiment driver):
  {"kind": "omission", "loss": 0.7, "seconds": 8}        outgoing emulated messages are dropped with probability loss
                                                          (loss 1.0 = the node is cut off from the network: partition)
  {"kind": "byzantine", "seconds": 8}                     the manager fabricates incidents for its healthy sites
                                                          (ground-truth label "bogus" is only used by the measurement)
  {"kind": "service_stop" | "service_start", "service": "correlation"}
                                                          the node stays alive but the service is down and it
                                                          acknowledges "unhealthy"
Node crashes are injected by the driver (SIGKILL + relaunch), not by this module.

Events written to the log: ft_escalate (manager/site, one per escalation), ft_receive (core, first copy only, or again
after a core restart: that is how duplicates are counted), ft_failover (site), ft_suspect / ft_recovered (core, managers),
ft_site_down / ft_site_up (core, sites), ft_adopt (backup manager).
"""
import asyncio
import os
import random
import time
from collections import OrderedDict


def _num(name):
    return int("".join(ch for ch in name if ch.isdigit()) or 0)


class FaultTolerance:
    def __init__(self, node):
        self.node = node
        cfg = node.cfg.get("ft")
        c = cfg or {}
        self.enabled = cfg is not None
        self.mode = os.environ.get("FT_MODE") or c.get("mode", "baseline")
        self.miss_limit = c.get("miss_limit", 3)
        self.detect_timeout = c.get("detect_timeout", 1.5)
        self.sync_interval = c.get("sync_interval", 0.5)
        self.rereg_interval = c.get("rereg_interval", 3.0)
        self.ack_timeout = c.get("ack_timeout", 0.4)
        self.site_timeout = c.get("site_timeout", 3 * self.rereg_interval)
        self.managers = sorted((n["name"] for n in node.cfg["nodes"] if n["role"] == "manager"), key=_num)
        self.sites = [n["name"] for n in node.cfg["nodes"] if n["role"] == "site"]
        self.primary = node.spec.get("manager")
        self.rng = random.Random(node.cfg["seed"] + node.port + 7)
        self.lock = asyncio.Lock()
        self.dropped = 0
        self.dirty = False
        self.reset()

    @property
    def proposed(self):
        return self.enabled and self.mode == "proposed"

    def reset(self):
        self.outbox = OrderedDict()      # eid -> (type, payload)
        self.seen = set()                # core: eids already accepted
        self.replica = {}                # manager: primary manager -> its open incidents
        self.suspects = set()            # core: managers it currently suspects
        self.last_seen = {}              # core: manager -> time of its last message
        self.current = self.primary      # site: manager it talks to
        self.misses = 0
        self.last_ok = time.time()
        self.services = {"correlation": True}
        self.loss, self.loss_until = 0.0, 0.0
        self.byz_until = 0.0              # manager: fabricating incidents until this time (Byzantine injection)
        self.site_down = set()            # core: sites it currently suspects
        self.bogus_accepted = 0           # core: fabricated incidents it accepted
        self.n_esc = 0
        self.failovers = 0

    def backup_of(self, name):
        if len(self.managers) < 2 or name not in self.managers:
            return None
        return self.managers[(self.managers.index(name) + 1) % len(self.managers)]

    # ---------- lifecycle ----------
    def start(self):
        if not self.enabled:
            return
        role = self.node.role
        if role == "manager":
            asyncio.create_task(self._sync_loop())
            asyncio.create_task(self._outbox_loop())
            asyncio.create_task(self._byz_loop())
        if role == "site" and self.node.cfg.get("correlation_tier") in ("site", "integrated"):
            asyncio.create_task(self._outbox_loop())          # workstation sites escalate straight to the core
        if role == "core":
            asyncio.create_task(self._detector_loop())
        if role in ("manager", "site", "cloud"):
            asyncio.create_task(self._rereg_loop())

    # ---------- control messages ----------
    def configure(self, payload):
        if payload.get("mode"):
            self.mode = payload["mode"]
        for k in ("miss_limit", "detect_timeout", "sync_interval", "ack_timeout", "rereg_interval", "site_timeout"):
            if k in payload:                                   # tunables, used by the Week 7 parameter sweep
                setattr(self, k, type(getattr(self, k))(payload[k]))
        if payload.get("reset"):
            self.reset()
        self.node.log.event("ft_config", mode=self.mode, reset=bool(payload.get("reset")), miss_limit=self.miss_limit,
                            detect_timeout=self.detect_timeout, sync_interval=self.sync_interval)

    def inject(self, payload):
        kind = payload.get("kind")
        if kind == "omission":
            self.loss = float(payload.get("loss", 0.5))
            self.loss_until = time.time() + float(payload.get("seconds", 1e9))
        elif kind == "service_stop":
            self.services[payload.get("service", "correlation")] = False
        elif kind == "service_start":
            self.services[payload.get("service", "correlation")] = True
        elif kind == "byzantine":
            self.byz_until = time.time() + float(payload.get("seconds", 1e9))
        elif kind == "clear":
            self.loss, self.services["correlation"], self.byz_until = 0.0, True, 0.0
        self.node.log.event("ft_inject", what=kind, **{k: v for k, v in payload.items() if k != "kind"})

    def drop(self):
        """Omission failure: called by Node.send for every emulated (data-plane) message."""
        if self.loss > 0 and time.time() < self.loss_until and self.rng.random() < self.loss:
            self.dropped += 1
            return True
        return False

    def service_up(self, name="correlation"):
        return self.services.get(name, True)

    def on_message(self, m):
        """Core: every message from a manager counts as a heartbeat for the failure detector."""
        if self.node.role == "core" and (m.sender in self.managers or m.sender in self.sites):
            self.last_seen[m.sender] = time.time()

    # ---------- site side: health monitoring and failover ----------
    def manager(self):
        return self.current

    def age(self):
        return time.time() - self.last_ok

    def on_ack(self, ack):
        """Every acknowledgement (or its absence) from the manager lands here, in both modes."""
        if ack is not None and ack.payload.get("healthy", True):
            self.last_ok, self.misses = time.time(), 0
            return
        self.misses += 1
        if self.proposed and self.misses >= self.miss_limit:
            self.failover()

    def failover(self):
        new = self.backup_of(self.current)
        if new and new != self.current:
            self.node.log.event("ft_failover", site=self.node.name, frm=self.current, to=new, misses=self.misses)
            self.current = new
            self.failovers += 1
        self.misses = 0

    # ---------- manager side: replication and store-and-forward ----------
    def adopt(self, site):
        """A site failed over to me: continue its open incident from the replica instead of re-opening it."""
        node = self.node
        if not self.proposed or site in node.incidents:
            return
        owner = next((n["manager"] for n in node.cfg["nodes"] if n["name"] == site), None)
        inc = (self.replica.get(owner) or {}).get(site)
        if owner and owner != node.name and inc:
            node.incidents[site] = inc
            node.log.event("ft_adopt", site=site, owner=owner)

    def on_sync(self, m):
        self.replica[m.sender] = m.payload.get("incidents", {})

    async def _sync_loop(self):
        node = self.node
        while True:
            await asyncio.sleep(self.sync_interval)
            if not self.proposed:
                continue
            backup = self.backup_of(node.name)
            if backup:
                await node.send(backup, node.msg(backup, "FT_SYNC", {"incidents": dict(node.incidents)}))

    def escalate(self, typ, payload):
        """Send an incident (typ = CORRELATED_INCIDENT / INCIDENT_RESOLVED) to the core."""
        node = self.node
        core = node.core_name
        if not self.enabled:                                   # every earlier milestone: unchanged
            asyncio.create_task(node.send(core, node.msg(core, typ, payload)))
            return
        self.n_esc += 1
        eid = f"{node.name}-{self.n_esc}"
        payload = {**payload, "eid": eid, "gen_ts": time.time()}
        node.log.event("ft_escalate", eid=eid, msg_type=typ, site=payload.get("site"), bogus=bool(payload.get("bogus")))   # not "kind": EventLog.event uses that name
        if self.proposed:
            self.outbox[eid] = (typ, payload)
            asyncio.create_task(self._flush())
        else:                                                  # baseline: one shot, no acknowledgement
            asyncio.create_task(node.send(core, node.msg(core, typ, payload)))

    async def _flush(self):
        node = self.node
        self.dirty = True
        if self.lock.locked():                                 # the running flush will pick the new entry up
            return
        async with self.lock:
            while self.dirty:
                self.dirty = False
                for eid, (typ, payload) in list(self.outbox.items()):
                    r = await node.send(node.core_name, node.msg(node.core_name, typ, payload),
                                        wait_reply=True, timeout=0.5)
                    if r is not None and r.type == "ACK":
                        self.outbox.pop(eid, None)
                    else:
                        break                                  # core unreachable: keep order, retry later

    async def _outbox_loop(self):
        while True:
            await asyncio.sleep(self.sync_interval)
            if self.proposed and self.outbox:
                await self._flush()

    # ---------- core side ----------
    def accept(self, m):
        """True the first time an escalation id is seen. Duplicates are acknowledged but not processed again."""
        eid = m.payload.get("eid")
        if eid is None:
            return True
        if eid in self.seen:
            return False
        self.seen.add(eid)
        bogus = bool(m.payload.get("bogus"))
        self.bogus_accepted += bogus
        self.node.log.event("ft_receive", eid=eid, delay=time.time() - m.payload.get("gen_ts", time.time()),
                            site=m.payload.get("site"), bogus=bogus)
        return True

    async def _detector_loop(self):
        node = self.node
        while True:
            await asyncio.sleep(0.25)
            if not self.proposed:
                continue
            now = time.time()
            for mgr in self.managers:
                ls = self.last_seen.get(mgr)
                if ls is None:
                    continue
                silent = now - ls
                if silent > self.detect_timeout and mgr not in self.suspects:
                    self.suspects.add(mgr)
                    node.log.event("ft_suspect", target=mgr, silent=round(silent, 2))
                elif silent <= self.detect_timeout and mgr in self.suspects:
                    self.suspects.discard(mgr)
                    node.log.event("ft_recovered", target=mgr)
            for site in self.sites:                       # a dead tower is itself a fault: nobody else would notice it
                ls = self.last_seen.get(site)
                if ls is None:
                    continue
                silent = now - ls
                if silent > self.site_timeout and site not in self.site_down:
                    self.site_down.add(site)
                    node.log.event("ft_site_down", target=site, silent=round(silent, 2))
                elif silent <= self.site_timeout and site in self.site_down:
                    self.site_down.discard(site)
                    node.log.event("ft_site_up", target=site)

    async def _byz_loop(self):
        """Byzantine behaviour (fault injection only): a faulty manager fabricates incidents for healthy sites.
        The 'bogus' flag is the experiment's ground truth; nothing in the system reads it."""
        node = self.node
        while True:
            await asyncio.sleep(0.5)
            if time.time() >= self.byz_until:
                continue
            for n in node.cfg["nodes"]:
                if n["role"] == "site" and n.get("manager") == node.name:
                    self.escalate("CORRELATED_INCIDENT", {"site": n["name"], "cause": "grid", "opened": time.time(),
                                                          "battery": 50.0, "bogus": True})

    async def _rereg_loop(self):
        node = self.node
        payload = {"role": node.role, "cpu": node.res.cpu_total, "mem": node.res.mem_total}
        while True:
            await asyncio.sleep(self.rereg_interval)
            if self.proposed:
                await node.send(node.core_name, node.msg(node.core_name, "REGISTER", payload),
                                wait_reply=True, emulate=False, timeout=1.0)

    def stats(self):
        if not self.enabled:
            return {}
        return {"ft_mode": self.mode, "ft_ok_age": round(self.age(), 3), "ft_manager": self.current,
                "ft_failovers": self.failovers, "ft_outbox": len(self.outbox), "ft_dropped": self.dropped,
                "ft_suspects": sorted(self.suspects), "ft_site_down": sorted(self.site_down),
                "ft_bogus_accepted": self.bogus_accepted}
