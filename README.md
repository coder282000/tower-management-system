# Tower-Site Fault Management: Milestones 1 and 2 starter

Python 3.10+ (tested on 3.12), one dependency (psutil). Runs on Windows via WSL2 Ubuntu, or natively.

## Setup (WSL2 Ubuntu)
    sudo apt update && sudo apt install -y python3 python3-venv git
    cd ~ && unzip tower_fm_starter.zip && cd tower_fm
    python3 -m venv .venv && source .venv/bin/activate
    pip install -r requirements.txt

## Milestone 1 demo
    python cluster.py

Shows: nodes registering with the core, site services (sensor, heartbeat) started as processes,
jobs placed and scheduled, a resource refusal, and message flow. Per-node event logs are in logs/.

## Milestone 2 experiment
    python -m experiments.week2_load

Runs 3 placement policies x 3 request rates, writes results/week2_results.csv
(throughput, mean and p95 latency, jitter, packet loss, CPU, memory, jobs per node).

## Layout
    comms/        message format, TCP transport, seeded link emulator (delay, jitter, loss)
    kernel/       resource manager, process manager, scheduler, placement rule
    nodes/        node implementation for core, manager, site and client roles
    metrics/      JSON-lines event logger
    config/       cluster.json (nodes, capacities, seed, link settings, policies)
    experiments/  week2_load.py
    docs/         templates for the weekly log and experiments

## Metric definitions (Week 2)
- Throughput = completed jobs / (time of last completion - time of first submission)
- Latency = T_response - T_request per job (client clock)
- Jitter = standard deviation of latency
- Packet loss = dropped messages / messages sent through the emulated link
- CPU / memory = psutil samples of each worker node process

## Known limitations (state these in your report)
- Job "CPU work" is simulated with timed sleeps, so measured process CPU% is low. Report the
  simulated resource accounting (ResourceManager) alongside it, or add real CPU work later.
- Each node models a single CPU (one job slice at a time).
- All nodes run on one machine, so network conditions come from the emulated link, not a real network.
- Nodes know each other's addresses from config.json (static naming). Real naming comes in Week 10.

---
# Milestones 5 and 6 (added)

New files
    services/transactions.py        M5  2PC and 3PC, write-ahead logs, locks, cooperative termination, live fault injection
    services/concurrency.py         M6  distributed locks, global deadlock detection by the elected leader, prevention
    experiments/week5_transactions.py   M5  failure-free cost + coordinator/participant failures (real crashes)
    experiments/week6_deadlock.py       M6  race condition, deadlock strategies, detection-interval sweep
    docs/RUN_GUIDE.md               step-by-step: install, run, collect results, push to GitHub
Changed files (small)
    nodes/node.py                   creates self.tx and self.concurrency, routes TX_* and lock messages, adds their stats to STATS

Run
    python3 -m experiments.week5_transactions --demo     # ~1 min narrated
    python3 -m experiments.week5_transactions            # full, writes results/week5_*.csv
    python3 -m experiments.week6_deadlock --demo         # ~1 min narrated
    python3 -m experiments.week6_deadlock                # full, writes results/week6_*.csv

---
# Milestones 7 and 8 (added)

New / changed files
    services/faulttolerance.py          M7  failure detection, backup-manager failover, replication, outbox, soft state,
                                            site-down detection, fault injection (crash, omission, service, Byzantine)
    experiments/week7_faulttolerance.py M7  7 fault scenarios x baseline/proposed, availability, MTTR, coverage, sweep
    experiments/week8_models.py         M8  5 system models + 2 controls: latency, throughput, utilisation, scalability,
                                            reliability, cost, stress, decision matrix with sensitivity analysis
    experiments/_common.py              run lock (one experiment at a time), environment snapshot, mean/sd/CI
    docs/RUN_M78.md, docs/run_m78.ps1   step-by-step run guide and PowerShell runner
Changed by one line: nodes/node.py, metrics/logger.py (optional append-mode logs, so a relaunched node keeps its history).

Run
    python3 -m experiments.week7_faulttolerance --demo      # ~1.5 min narrated
    python3 -m experiments.week7_faulttolerance             # ~25 min, results/week7_*.csv
    python3 -m experiments.week8_models --demo              # ~3 min table
    python3 -m experiments.week8_models                     # ~50 min, results/week8_*.csv

---
# Milestones 9 and 10 (added)
    services/store.py           M9   physical object store: primary-backup replication (sync/async), epochs and fencing, memory/file backends
    services/naming.py          M9/10 name server (master with journal) and read-only zone servers; promotion, migration, rejoin, monitor
    services/transparency.py    M9/10 transparent client (access, location, replication, migration, failure), 6 resolution schemes, baseline client
    services/procmgmt.py        M10  workflows: Create -> Locate -> Schedule -> Coordinate -> Terminate, durable state, leader as coordinator
    experiments/_lab.py         shared test bench;  week9_transparency.py, week10_naming_procs.py
    docs/RUN_M910.md, run_m910.ps1, weekly_log_M910.md
Run:  python3 -m experiments.week9_transparency --demo      python3 -m experiments.week10_naming_procs --demo

---
# HDFS + Spark integration (added with Milestones 9 and 10)
    scripts/hdfs_up.sh, hdfs_down.sh      start HDFS and wait until 3 DataNodes are live / stop it before timing runs
    scripts/archive_to_hdfs.sh            results -> /tower/<weekN>/ and /tower/archive/<stamp>/, then fsck
    analysis/spark_analysis.py            Spark (reads HDFS) or pandas: mean/sd per group for every result file, plus the metric that
                                          discriminates each Week 7 scenario
    experiments/hdfs_probe.py             reads during a DataNode loss: native client vs WebHDFS (run alone)
    docs/HDFS_SPARK.md                    how it fits Milestones 9 and 10, what you can claim, limits
