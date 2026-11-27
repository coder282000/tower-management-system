"""Helpers shared by the Milestone 7 and 8 experiments.

  acquire_lock()   one experiment at a time. A second run exits at once instead of colliding on the fixed ports and
                   the temporary config file (this is the fix for the Week 2 double-run failure). The lock is released by
                   the OS when the process ends, even after a crash or Ctrl+C, so it can never get stuck.
  write_env()      writes results/environment.txt: hardware, OS, Python, psutil, seed, config. A second researcher needs it.
  mean_sd()        mean and sample standard deviation of a list (sd = 0 for one value).
  ci95()           half-width of an approximate 95% confidence interval of the mean (t-distribution, small n).
"""
import fcntl
import json
import os
import platform
import statistics
import sys
import time

LOCK_PATH = ".experiment.lock"
_T95 = {1: 12.71, 2: 4.30, 3: 3.18, 4: 2.78, 5: 2.57, 6: 2.45, 7: 2.36, 8: 2.31, 9: 2.26, 10: 2.23,
        12: 2.18, 15: 2.14, 20: 2.09, 30: 2.05}
_lock_file = None


def acquire_lock():
    global _lock_file
    _lock_file = open(LOCK_PATH, "w")
    try:
        fcntl.flock(_lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        sys.exit("Another experiment is already running (lock file %s). Wait for it to finish, or run:\n"
                 "    pkill -f 'experiments[.]'; pkill -f 'nodes[.]run'" % LOCK_PATH)
    _lock_file.write(str(os.getpid()))
    _lock_file.flush()


def mean_sd(xs):
    xs = [x for x in xs if x not in ("", None)]
    if not xs:
        return None, None
    return statistics.mean(xs), (statistics.stdev(xs) if len(xs) > 1 else 0.0)


def ci95(xs):
    xs = [x for x in xs if x not in ("", None)]
    if len(xs) < 2:
        return None
    df = len(xs) - 1
    t = _T95.get(df) or _T95[min(_T95, key=lambda k: abs(k - df))]
    return t * statistics.stdev(xs) / len(xs) ** 0.5


def _sh(cmd):
    try:
        import subprocess
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=5).stdout.strip()
    except Exception:
        return "?"


def write_env(extra=None, path="results/environment.txt"):
    import psutil
    os.makedirs("results", exist_ok=True)
    try:
        cfg = json.load(open("config/cluster.json"))
    except Exception:
        cfg = {}
    lines = {
        "date": time.strftime("%Y-%m-%d %H:%M:%S"),
        "cpu": _sh("grep -m1 'model name' /proc/cpuinfo | cut -d: -f2"),
        "vcpus (os.cpu_count)": os.cpu_count(),
        "ram_total_mb": round(psutil.virtual_memory().total / 1e6),
        "os": _sh("grep PRETTY_NAME /etc/os-release | cut -d= -f2"),
        "kernel": platform.release(),
        "python": platform.python_version(),
        "psutil": psutil.__version__,
        "seed": cfg.get("seed"),
        "link": cfg.get("link"),
        "topology": "all nodes are OS processes on one machine, TCP over localhost",
        "command": " ".join(sys.argv),
    }
    lines.update(extra or {})
    with open(path, "w") as f:
        for k, v in lines.items():
            f.write(f"{k}: {v}\n")
    return lines
