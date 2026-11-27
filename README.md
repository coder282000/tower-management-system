# Tower-Site Fault Management System

A distributed telecom fault-management system built across **Milestones 1–10** of ICS 2403 Distributed Computing.

**Repository:** https://github.com/coder282000/tower-management-system

---

## Project Overview

The Tower-Site Fault Management System monitors, detects, and responds to faults across distributed telecom tower sites. It evolves from a single-process prototype (M1–M2) to a full distributed system: tiered architecture (M3), distributed algorithms (M4), atomic transactions (M5), concurrency control (M6), fault tolerance (M7), architectural models (M8), transparency (M9), and naming + process management (M10).

## Milestone Summary

| # | Theme | Key Result |
|---|---|---|
| M1–M2 | Distributed OS foundation | Base cluster, load experiments |
| M3 | Distributed architecture | Multi-tier edge/core/cloud, 2× edge-vs-core latency vs 6× core load reduction |
| M4 | Distributed algorithms | Lamport 100% correct vs 31% wall-clock; Bully 12/12 converged |
| M5 | Transactions | 2PC ~12 msg, 3PC ~18 msg; 3PC self-resolves in 1.4 s vs 2PC 5 s |
| M6 | Concurrency | Locked counter 0 lost updates vs 70/120 unsafe; deadlock detected + broken |
| M7 | Fault tolerance | manager_crash: 8.09 s → 0.25 s MTTR (−97%) |
| M8 | Architectural models | Only integrated + server_based_ft survive core kill |
| M9 | Transparency & reliability | Transparent + rf2: 100% availability, 0 lost writes |
| M10 | Naming & process mgmt | prefix maintains 100% under core failure; coordinator durability 12/12 vs 0/12 |

## Deployment

- **Host:** VirtualBox VM, Debian 13 (trixie), kernel 6.12
- **Access:** SSH via port-forwarded 2222 → VM 22
- **Runtime:** Python 3.13.5, asyncio-based
- **Storage/analysis:** Apache Hadoop 3.5.0 (HDFS 1 NameNode + 3 DataNodes, replication 3), PySpark 4.3.0
- **JDK:** Eclipse Temurin 17.0.21.1

---

## Milestone 1–2: Distributed OS Foundation

Base cluster with `site`, `mgr`, `core`, `cloud` node types. Sensor readings, heartbeat, message transport, and early load experiments (`experiments/week2_load.py`).

## Milestone 3: Distributed Architecture

**What it tests:** Distributed architecture design, scalability, modularity, edge-core-cloud reasoning.

**Delivered:**
- Multi-tier architecture: **Edge** (sites) → **Core** (managers + core) → **Cloud** (storage/analysis)
- Service decomposition: `services/correlation.py`, `services/archive.py`, `services/feedback.py`
- Two-way **Edge ↔ Core ↔ Cloud** feedback loop (archive up, policy down)
- Architectural styles evaluated: client-server (rejected, single point of failure), multi-tier (adopted for physical layout), service-oriented (adopted for internal structure), microservices (partially adopted, honestly limited)

**Key results:**

| Experiment | Result |
|---|---|
| Scalability (3→9 sites, 2→4 managers) | Throughput 34.6–37.3 jobs/s across all sizes; mean latency 83.8→105.9 ms; coordination overhead grows faster than node count (6.88 → 10.00 msgs/node) |
| Edge vs Core correlation | Edge: 19.70 ms detection, 1.60 core msg/s. Core: 10.70 ms detection, 9.73 core msg/s — 2× faster but 6–12× more core load |
| Two-way loop | 38 archived, 15 policy updates, 13/38 incidents reprioritized HIGH (21% msg overhead) |

**Trade-off stated:** Edge-tier correlation doubles detection latency but reduces core message load by ~6×. For a fault-management system specifically, edge-tier correlation is the right default — the small latency cost is worth the large reduction in central load.

## Milestone 4: Distributed Algorithms and Coordination

**What it tests:** Distributed algorithms, coordination, synchronization, event ordering.

**Delivered:**
- **Lamport logical clocks** (`kernel/clock.py`) — per-node counter with send-increment and receive-max rules; 12 extra bytes per message, 0 extra messages
- **Bully leader election** (`kernel/election.py`) — highest-ranked live manager wins; heartbeat-based failure detection; ELECTION → OK → COORDINATOR protocol with re-election on late challenges

**Key results:**

| Experiment | Result |
|---|---|
| Lamport vs wall-clock ordering | Wall-clock correct 31% (5/16); Lamport correct **100%** (16/16 and 17/17 in two runs). 12 extra bytes, 0 extra messages |
| Bully election (2,4,6,8 managers × 3 trials) | **12/12 trials converged correctly**; detection delay 1.31–1.41 s (dominated by `leader_timeout`, not node count); election phase 0.01→0.23 s; 60.3 messages at 8 managers (near-quadratic, matching theory + re-election overhead) |

**Interpretation:** Wall-clock ordering fails most of the time under <1s clock skew; Lamport ordering is free (no extra messages) and correct on all causally related events. Bully election is cheap at small manager counts (1 msg for 2 managers) but the dominant cost at 8 managers (60+ msgs) — informs the manager-count decision for larger deployments.

## Milestone 5: Transactions (2PC and 3PC)

**What it tests:** Atomicity of dispatch assignment across managers and sites.

**Delivered:** `services/transactions.py` — 2PC and 3PC with write-ahead logs; coordinator = Bully-elected leader from M4.

**Key results:**

| Protocol | Messages/dispatch | Coordinator killed after votes | Recovery |
|---|---|---|---|
| 2PC | ~12 | Participants block in doubt | ~5 s (wait for coordinator restart) |
| 3PC | ~18 | Participants self-resolve to commit | ~1.4 s |

Participant killed after YES vote: both protocols recover to consistent outcome after restart. All runs ended with `on all nodes: True` (atomicity verified).

## Milestone 6: Concurrency and Deadlock

**What it tests:** Concurrent dispatch requests contending for shared field-team resources.

**Delivered:** `services/concurrency.py` — 3 strategies: `none`, `detect_priority`, `prevent`.

**Key results:**

| Measurement | Unsafe | Locked |
|---|---|---|
| Counter lost updates (of 120) | ~70 | **0** |
| Deadlock under `none` | yes — RAG cycle printed | — |
| Deadlock under `detect_priority` | — | routine aborted, both finish |
| Deadlock under `prevent` | — | finishes, no abort |

Resource-allocation-graph file `results/week6_rag_deadlock.dot` visualises the deadlock cycle (solid = held, dashed = waiting, red = cycle).

## Milestone 7: Fault Tolerance

**What it tests:** Recovery from 7 fault classes — manager_crash, core_crash, site_crash, omission, partition, service_failure, byzantine.

**Delivered:** `services/faulttolerance.py` — backup manager, replicated registry, durable coverage outbox, suspicion-based detection.

**Key results (baseline → proposed, n=3):**

| Scenario | Metric | Baseline | Proposed | Δ |
|---|---|---|---|---|
| manager_crash | availability | 0.665 | **0.988** | +0.323 |
| manager_crash | MTTR | 8.09 s | **0.25 s** | −97% |
| service_failure | availability | 0.681 | **0.990** | +0.309 |
| service_failure | MTTR | 7.70 s | **0.33 s** | −96% |
| core_crash | coverage | 0.000 | **1.000** | +1.000 |
| core_crash | registry_restore_s | never | 1.24 s | new |
| site_crash | detect_s | never | **2.99 s** | new |
| omission | delivery_ratio | 0.408 | **1.000** | +0.592 |
| partition | delivery_ratio | 0.000 | **1.000** | +1.000 |
| byzantine | bogus_accepted | 32 | 32 | **0 (gap)** |

**Key insight:** `availability` is not the discriminating metric in 4 of 7 scenarios — coverage, detect_s, and delivery_ratio are. Reporting only availability would understate the result by an entire fault class. Byzantine is a genuine gap (needs 3f+1 managers for BFT) — stated openly as future work.

## Milestone 8: Distributed System Models

**What it tests:** 5 architectural models + 2 controls under the same workload.

**Delivered:** `experiments/week8_models.py` — host_based, server_based, workstation, processor_pool, integrated, integrated_noft (control), server_based_ft (control).

**Key results:**

| Model | Core survival (detect_ratio) | Detection @ 9 sites | Core load @ 9 sites | Stress (0.05s interval) |
|---|---|---|---|---|
| host_based | 0.00 | **11.00 ms** | 50.15 msg/s | 100% detection |
| integrated | **1.00** (5.21 s) | 27.65 ms | **29.45 msg/s** | 100% detection |
| integrated_noft | 0.00 | — | — | — (control) |
| server_based | 0.00 | — | — | 94% detection |
| server_based_ft | **1.00** (5.53 s) | — | — | — |
| processor_pool | 0.00 | 24.10 ms | 50.10 msg/s | **18% detection** |

**Conclusion:** Only `integrated` and `server_based_ft` survive a core failure — because they carry the M7 fault-tolerance service. `integrated_noft` confirms this: same architecture, no FT, 0.00 core survival. **Model choice is a fault-tolerance contract decision, not just a latency decision.** `host_based` wins under stress (fastest, highest throughput); `integrated` wins under core failure. No model wins on every criterion.

## Milestone 9: Transparency and Reliability

**What it tests:** Access, location, replication, migration, failure transparency — the user interacts with a *logical* resource, not a physical one.

**Delivered:** `services/transparency.py`, `services/store.py`.

**Key results:**

| Transparency | Result |
|---|---|
| **Access** | local 0.03 ms; warm 9.27 ms (2 msgs); cold 18.19 ms (4 msgs) — 2× one-time cost for name resolution |
| **Location / migration** | transparent: **0 errors / 400 ops**; direct & static: **250 errors / 400 ops** (62.5%) |
| **Replication** | rf1 sync 9.12 ms; rf2 sync **19.02 ms**; rf3 sync **21.02 ms** — 2× write cost per replica; async hides cost but produces 46–61% stale reads from non-primary |
| **Failure** | transparent_rf2_async: **100% ok, 0 lost writes**; direct_file: 57.94% ok; direct_mem: **192 lost writes** |
| **Detection** | kill: 0.02 s promotion, 0 errors; hang: 0.82 s promotion, 0 errors |
| **Fencing** | zombie_acked = 0; write_survived = 1 |
| **Reliability model** | Closed-form prediction matches measurement to **±0.01 in all 8 cells** (single/double × direct/rf1/rf2/rf3) |

**Key insight:** `transparent + rf1` is worse than no transparency (3046 ms p99) — transparency alone moves the failure rather than absorbing it. The right design is **transparent naming + rf2**; rf3 is required for double-fault survivability.

## Milestone 10: Naming and Process Management

**What it tests:** Naming schemes, name resolution, process lifecycle, distributed coordination.

**Delivered:** `services/naming.py`, `services/procmgmt.py`.

**Naming schemes — resolution cost and failure behaviour:**

| Scheme | warm mean (ms) | msgs/res | hit_rate | under core failure |
|---|---|---|---|---|
| static | 0.00 | 0 | 0% | 100% (permanently stale) |
| broadcast | 16.54 | **10** | 0% | 86.5% |
| nameserver | 8.45 | 2 | 0% | **0.0%** |
| **nameserver+cache** | **3.48** | **0.83** | 58% | 100% warm / 58.9% cold |
| prefix | 8.30 | 2 | 0% | **100%** |
| prefix+cache | 4.01 | 0.86 | 57% | 100% |

**Staleness — the cost of caching:**

| Scheme | wrong right after move | converge_s |
|---|---|---|
| static | 100% | **never** |
| nameserver, prefix, broadcast | 0% | 0 |
| nameserver+cache | 100% | **1.23 s** |
| prefix+cache | 100% | **~2.09 s** |

**Scalability:** nameserver scales 5.5× (1 → 32 resolvers, p95 10 → 63 ms); prefix saturates at 8 (463 → 517 res/s); broadcast collapses (67 → 110 res/s, p95 20 → 328 ms). Broadcast message cost O(h): 8 → 12 → 20 as holders grow 4 → 6 → 10.

**Process management (5-phase lifecycle):**
- Create workflow: **5.3 ms**; Locate: **10.3 ms**; full lifecycle: **4.27 s**; Terminate: 6 procs in **154 ms**
- **Coordinator failure: 12/12 workflows complete** with replicated state vs **0/12** with in-memory state
- Fencing at process level: 0 duplicate processes in both modes

**Key insight:** nameserver is centralised and **fails completely (0%)** when the core is down — prefix maintains **100%** because prefix routing bypasses the central coordinator. The naming choice trades resolution cost against core-dependency, and the failure table makes the choice unambiguous.

---

## Results Archive

All results are archived with replication 3 to HDFS at `/tower/week7` … `/tower/week10`, and aggregated by PySpark into `/tower/analysis/`. The substrate itself survives a DataNode kill with **31 s detection** and **100% read success** from the native Hadoop client.

## Running the Experiments

Start HDFS
scripts/hdfs_up.sh

Demos (fast, good for live presentation)
python3 -m experiments.week7_faulttolerance --demo
python3 -m experiments.week8_models --demo

Full runs
python3 -m experiments.week7_faulttolerance
python3 -m experiments.week8_models
python3 -m experiments.week9_transparency
python3 -m experiments.week10_naming_schemes
python3 -m experiments.week10_process_management

Archive results to HDFS
scripts/archive_to_hdfs.sh

Spark analysis
source ~/venv-spark/bin/activate
python analysis/spark_analysis.py

## Repository Structure
tower_fm/
├── comms/ message transport (SO_REUSEADDR patched)
├── config/ cluster.json (seed=42)
├── experiments/ week-by-week experiment scripts
├── kernel/ topology, clock, election
├── metrics/ logger
├── nodes/ node.py, run.py
├── results/ CSV output (all milestones)
├── scripts/ hdfs_up.sh, hdfs_down.sh, archive_to_hdfs.sh
├── services/ correlation, archive, feedback, transactions,
│ concurrency, faulttolerance, transparency,
│ store, naming, procmgmt
├── analysis/ spark_analysis.py
└── docs/ runbooks, weekly logs

## Technologies

Python 3.13 · asyncio · Hadoop 3.5.0 (HDFS) · PySpark 4.3.0 · Eclipse Temurin JDK 17 · Debian 13 · VirtualBox

## Authors

- Bernadette Weloba
- Bathshua Miheso
- Tebby Manyala
- **Victor Kirimi** (group leader)
- Kelvin Oluchina

## License

Academic project — ICS 2403, 2026.
