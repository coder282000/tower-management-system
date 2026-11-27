"""Shared test bench for the Milestone 9 and 10 experiments.

  Lab            a real cluster (one OS process per node) with the lrs / pm blocks switched on, plus the driver's own
                 node ("client"), which also holds objects when an experiment needs a copy next to the application
  Lab.kill / restart / reset       crash a node (SIGKILL), relaunch it, return every node and the name server to a clean state
  drive()        open-loop load: operations start at fixed times whether or not earlier ones have finished
  pct()/summ()   percentiles, mean and sd for the summaries
"""
import asyncio
import json
import os
import shutil
import signal
import statistics
import subprocess
import sys
import time

from cluster import launch, shutdown, wait_registered
from kernel.topology import build
from nodes.node import Node, load_config
from services.transparency import AdminClient, default_lrs


class Lab:
    def __init__(self, tag, managers=3, sites=0, base_port=30000, lrs=None, pm=None, cfg_extra=None, extra=("client",)):
        self.tag = tag
        self.cfg = build(load_config("config/cluster.json"), sites, managers, base_port=base_port)
        self.log_dir = f"logs/{tag}"
        self.tmp = f"config/_{tag}_tmp.json"
        self.cfg.update({"log_dir": self.log_dir, "log_append": True, "sensor_interval": 60.0})   # no sensor traffic: clean measurements
        self.cfg.update(cfg_extra or {})
        default_lrs(self.cfg, extra=list(extra), **(lrs or {}))
        if pm is not None:
            self.cfg["pm"] = pm
        self.names = [n["name"] for n in self.cfg["nodes"] if n["role"] != "client"]
        self.managers = [n["name"] for n in self.cfg["nodes"] if n["role"] == "manager"]
        self.sites = [n["name"] for n in self.cfg["nodes"] if n["role"] == "site"]
        self.workers = self.managers + self.sites

    async def __aenter__(self):
        shutil.rmtree(self.log_dir, ignore_errors=True)
        os.makedirs(self.log_dir, exist_ok=True)
        with open(self.tmp, "w") as f:
            json.dump(self.cfg, f)
        self.procs = launch(self.tmp, self.names)
        self.client = Node(self.cfg, "client")
        await self.client.start()
        await wait_registered(self.client, [n for n in self.names if n != "core"], timeout=60)
        await self.wait_ready()
        self.admin = AdminClient(self.client)
        return self

    async def wait_ready(self):
        """Every node answers, a leader is elected and (with process management) it has taken over."""
        t0 = time.time()
        top = self.managers[-1]
        while time.time() - t0 < 60:
            sts = [await self.stats(m) for m in self.managers]
            if all(sts) and all(s.get("leader") == top for s in sts) and (not self.cfg.get("pm") or sts[-1].get("pm_active")):
                await asyncio.sleep(0.5)
                return
            await asyncio.sleep(0.3)
        raise RuntimeError("cluster not ready (no leader)")

    async def __aexit__(self, *exc):
        await shutdown(self.client, self.names)
        self.client.tp.server.close()
        await self.client.tp.server.wait_closed()
        for p in self.procs:
            try:
                p.wait(timeout=2)
            except Exception:
                p.kill()
                try:
                    p.wait(timeout=2)
                except Exception:
                    pass
        # force-kill any grandchildren/orphans whose cmdline mentions this lab's config
        try:
            import subprocess as _sp
            _sp.run(["pkill", "-9", "-f", self.tmp], capture_output=True)
        except Exception:
            pass
        await asyncio.sleep(1.0)

    # ---------------------------------------------------------------- faults
    def kill(self, name):
        p = self.procs[self.names.index(name)]
        p.kill()
        p.wait()
        return time.time()

    def stop(self, name):                                      # a hung node: alive, silent (SIGSTOP)
        self.procs[self.names.index(name)].send_signal(signal.SIGSTOP)
        return time.time()

    def cont(self, name):
        self.procs[self.names.index(name)].send_signal(signal.SIGCONT)
        return time.time()

    def restart(self, name):
        self.procs[self.names.index(name)] = subprocess.Popen([sys.executable, "-m", "nodes.run", "--config", self.tmp, "--name", name])
        return time.time()

    def dead(self):
        return [n for n, p in zip(self.names, self.procs) if p.poll() is not None]

    async def stats(self, name, timeout=1.0):
        r = await self.client.send(name, self.client.msg(name, "STATS"), wait_reply=True, emulate=False, timeout=timeout)
        return r.payload if r is not None else None

    async def reset(self):
        """Back to a clean state: every node alive, no objects, no names, no workflows."""
        for p in self.procs:
            if p.poll() is None:
                p.send_signal(signal.SIGCONT)                  # never leave a node hung from the previous run
        for n in self.dead():
            self.restart(n)
        t0 = time.time()
        while time.time() - t0 < 30:
            if all([await self.stats(n) for n in self.names]):
                break
            await asyncio.sleep(0.3)
        await asyncio.sleep(1.0)                              # let the restarted nodes say hello and be re-added
        await self.admin.reset()
        await asyncio.sleep(0.3)
        if self.cfg.get("pm"):                                # forget old workflows, and let a restarted coordinator win the election again
            for n in self.managers + self.sites:
                await self.client.send(n, self.client.msg(n, "PM_CONFIG", {"reset": True}), wait_reply=True, emulate=False, timeout=1.0)
            await self.wait_ready()

    def events(self, node, kinds):
        out = []
        try:
            with open(os.path.join(self.log_dir, f"{node}.jsonl")) as f:
                for line in f:
                    try:
                        e = json.loads(line)
                    except ValueError:
                        continue
                    if e.get("event") in kinds:
                        out.append(e)
        except OSError:
            pass
        return out


async def drive(rate, duration, make_op, t0=None, count=None):
    """Open-loop load: start make_op(i, t_rel) at i/rate seconds, collect every result."""
    t0 = t0 or time.time()
    n = count or int(rate * duration)
    out, tasks = [], []

    async def one(i):
        ts = time.time()
        r = await make_op(i, ts - t0)
        r["t"] = ts - t0
        out.append(r)

    for i in range(n):
        await asyncio.sleep(max(0.0, t0 + i / rate - time.time()))
        tasks.append(asyncio.create_task(one(i)))
    await asyncio.gather(*tasks)
    return sorted(out, key=lambda r: r["t"])


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * (len(xs) - 1) + 0.5))] if xs else None


def ms(x):
    return None if x is None else round(1000 * x, 2)


def msd(xs):
    xs = [x for x in xs if x not in ("", None)]
    if not xs:
        return None, None
    return statistics.mean(xs), (statistics.stdev(xs) if len(xs) > 1 else 0.0)
