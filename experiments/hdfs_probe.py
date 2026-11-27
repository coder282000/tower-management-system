"""HDFS failure-transparency probe (Milestone 9 evidence, Milestone 10 detection timescales).

Question: when a DataNode dies, does a reader notice? The earlier demo read the file AFTER the NameNode had declared the node dead
(30 s later). This probe reads CONTINUOUSLY from before the failure until after the rejoin, so it covers the window in which the NameNode
still believes the node is alive.

Two read paths, because they are expected to behave differently (the probe measures it, it does not assume it):
  native    `hdfs dfs -cat`: the HDFS client library asks the NameNode for the block locations and, if a DataNode does not answer,
            tries the next replica itself (client-side failover: the same idea as the Milestone 9 client that suspects a node)
  webhdfs   HTTP GET /webhdfs/v1/<path>?op=OPEN: the NameNode picks ONE DataNode and redirects the client to it; the NameNode's
            view is stale for the heartbeat-detection time (about 30 s with the tuned settings), so this path relies on the master's view
Timeline (default): reads for 70 s; the DataNode is stopped at 10 s and started again at 45 s. The NameNode's own view (NumLiveDataNodes,
from its JMX page: no JVM start) gives the detection time (stop -> the NameNode counts one DataNode less) and the rejoin time.

    python experiments/hdfs_probe.py                         # both paths, one after the other (about 3 minutes)
    python experiments/hdfs_probe.py --mode webhdfs --dn 2
Run it ALONE: HDFS is JVM-heavy and shares the one CPU. Writes results/hdfs_probe.csv (every read) and results/hdfs_probe_summary.csv.
Needs: HDFS running, ~/hdfs/dn.sh <n> start|stop (your DataNode control script), the NameNode web port 9870.
"""
import argparse
import csv
import json
import os
import statistics
import subprocess
import threading
import time
import urllib.request

PAYLOAD = ("tower fault management probe line\n" * 20).encode()


def live_datanodes(jmx_url):
    """The NameNode's own count of live DataNodes (None if the page cannot be read)."""
    try:
        with urllib.request.urlopen(jmx_url, timeout=2) as r:
            for b in json.load(r).get("beans", []):
                if "NumLiveDataNodes" in b:
                    return int(b["NumLiveDataNodes"])
    except Exception:
        return None
    return None


def read_native(a):
    t0 = time.time()
    try:
        r = subprocess.run([a.hdfs, "dfs", "-cat", a.path], capture_output=True, timeout=30)
        ok = r.returncode == 0 and r.stdout == PAYLOAD
        return ok, time.time() - t0, "" if ok else (r.stderr.decode(errors="replace").strip().splitlines() or ["bad content"])[-1][:80]
    except subprocess.TimeoutExpired:
        return False, time.time() - t0, "timeout"


def read_webhdfs(a):
    t0 = time.time()
    try:
        with urllib.request.urlopen(f"{a.webhdfs}/webhdfs/v1{a.path}?op=OPEN", timeout=4) as r:   # follows the NameNode's redirect to one DataNode
            body = r.read()
        ok = body == PAYLOAD
        return ok, time.time() - t0, "" if ok else "bad content"
    except Exception as e:
        return False, time.time() - t0, type(e).__name__ + ": " + str(e)[:60]


def dn(a, action):
    subprocess.run(["bash", os.path.expanduser(a.dn_script), str(a.dn), action], capture_output=True, timeout=120)
    return time.time()


def preflight(a):
    n = live_datanodes(a.jmx)
    if n is None or n < 2:
        raise SystemExit(f"HDFS not reachable or fewer than 2 live DataNodes (JMX {a.jmx} says {n}). Start it first (docs/HDFS_SPARK.md).")
    if subprocess.run([a.hdfs, "dfs", "-test", "-e", a.path], capture_output=True).returncode != 0:
        subprocess.run([a.hdfs, "dfs", "-put", "-f", "-", a.path], input=PAYLOAD, capture_output=True, check=True)
        print(f"created {a.path}")
    return n


def run(a, mode, base):
    reader = read_native if mode == "native" else read_webhdfs
    ev, rows = {}, []
    t0 = time.time()

    def control():
        time.sleep(a.stop_at)
        ev["stop"] = dn(a, "stop")
        while time.time() - ev["stop"] < 120 and live_datanodes(a.jmx) != base - 1:
            time.sleep(0.5)
        ev["detected"] = time.time()
        time.sleep(max(0, t0 + a.start_at - time.time()))
        ev["start"] = dn(a, "start")
        while time.time() - ev["start"] < 120 and live_datanodes(a.jmx) != base:
            time.sleep(0.5)
        ev["rejoined"] = time.time()

    th = threading.Thread(target=control)
    th.start()
    while time.time() - t0 < a.duration:
        ts = time.time()
        ok, lat, err = reader(a)
        rows.append({"mode": mode, "t": round(ts - t0, 3), "ok": int(ok), "lat_ms": round(1000 * lat, 1), "err": err})
        time.sleep(max(0, a.interval - lat))
    th.join()
    ev = {k: v - t0 for k, v in ev.items()}
    return rows, ev


def summarise(rows, ev, mode):
    stop, det = ev.get("stop"), ev.get("detected")
    inwin = [r for r in rows if stop is not None and det is not None and stop <= r["t"] < det]
    before = [r for r in rows if stop is None or r["t"] < stop]
    after = [r for r in rows if det is not None and r["t"] >= det]
    pct = lambda rs: round(100 * sum(r["ok"] for r in rs) / len(rs), 1) if rs else None
    gaps = [b["t"] - a["t"] for a, b in zip([r for r in rows if r["ok"]], [r for r in rows if r["ok"]][1:])]
    return {"mode": mode, "reads": len(rows), "ok_pct": pct(rows), "ok_pct_before": pct(before), "ok_pct_in_detection_window": pct(inwin),
            "reads_in_window": len(inwin), "ok_pct_after_detection": pct(after), "errors": sum(1 - r["ok"] for r in rows),
            "max_latency_ms": max(r["lat_ms"] for r in rows), "longest_gap_between_successes_s": round(max(gaps), 2) if gaps else None,
            "detection_s": round(det - stop, 1) if stop is not None and det is not None else None,
            "rejoin_s": round(ev["rejoined"] - ev["start"], 1) if "rejoined" in ev and "start" in ev else None}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=("native", "webhdfs", "both"), default="both")
    ap.add_argument("--dn", type=int, default=2)
    ap.add_argument("--dn-script", default="~/hdfs/dn.sh")
    ap.add_argument("--hdfs", default="hdfs")
    ap.add_argument("--path", default="/tower/probe.txt")
    ap.add_argument("--jmx", default="http://localhost:9870/jmx")
    ap.add_argument("--webhdfs", default="http://localhost:9870")
    ap.add_argument("--duration", type=float, default=70.0)
    ap.add_argument("--stop-at", type=float, default=10.0)
    ap.add_argument("--start-at", type=float, default=45.0)
    ap.add_argument("--interval", type=float, default=0.5)
    a = ap.parse_args()
    base = preflight(a)
    print(f"live DataNodes: {base}; stopping DataNode {a.dn} at {a.stop_at:.0f} s, restarting at {a.start_at:.0f} s, reading for {a.duration:.0f} s")
    all_rows, sums = [], []
    for mode in (["native", "webhdfs"] if a.mode == "both" else [a.mode]):
        print(f"\n== {mode}", flush=True)
        rows, ev = run(a, mode, base)
        s = summarise(rows, ev, mode)
        all_rows += rows
        sums.append(s)
        for k, v in s.items():
            print(f"   {k:34} {v}")
        time.sleep(5)
    os.makedirs("results", exist_ok=True)
    for path, data in (("results/hdfs_probe.csv", all_rows), ("results/hdfs_probe_summary.csv", sums)):
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(data[0]))
            w.writeheader()
            w.writerows(data)
        print("saved", path)


if __name__ == "__main__":
    main()
