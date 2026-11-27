# HDFS and Spark in the tower fault-management project (Milestones 9 and 10)

Your Hadoop 3.5.0 / Spark 4.3.0 setup (1 NameNode + 3 co-resident DataNodes, replication 3, PySpark in `~/venv-spark`) is the
**archive and analysis layer**. It is deliberately NOT in the timing path: the JVMs take the one CPU, so the experiments run with HDFS
stopped, and HDFS is started afterwards.

## 1. Operating sequence
    bash scripts/hdfs_down.sh                      # before any timing run: only Jps must be left
    python3 -m experiments.week9_transparency      # ... Week 7 to 10 experiments, one at a time
    bash scripts/hdfs_up.sh                        # starts NameNode + 3 DataNodes and WAITS until 3 are live
    bash scripts/archive_to_hdfs.sh                # /tower/<weekN>/ (latest) and /tower/archive/<stamp>/ (snapshot), then fsck
    source ~/venv-spark/bin/activate
    python analysis/spark_analysis.py              # Spark reads hdfs://localhost:9000/tower, writes /tower/analysis/
    python analysis/spark_analysis.py --engine pandas --source results     # same tables without Spark/HDFS (to check numbers)
    python experiments/hdfs_probe.py               # optional, run ALONE: do reads survive a DataNode loss? (about 3 min)
    bash scripts/hdfs_down.sh
`hdfs_up.sh` exists because your log shows `hdfs dfsadmin -report` failing with "Connection refused" after a fixed `sleep 10`: the NameNode
only answers on port 9000 once its namespace is loaded. The script polls instead of sleeping.

## 2. Correction to section 6 of your summary (Milestone 7 table)
Your Spark job averages `availability` and `mttr_s` only. For three kinds of scenario that is, by construction, not the metric that can move:

| Scenario | What `availability` measures | The metric that discriminates |
|---|---|---|
| core_crash | the core PROCESS: down until relaunched in both modes | coverage, delivery_ratio, registry_restore_s |
| site_crash | the site PROCESS: down until relaunched in both modes | detect_s (never vs about 3 s) |
| omission, partition | the sites' acknowledgements: unaffected by mgr1's outgoing loss, so 1.0 by construction | delivery_ratio, coverage |
| byzantine | unaffected | bogus_accepted (the core accepts all fabricated incidents in both modes) |

So "1.0 / 0.0 in both modes" and "core_crash identical" do NOT show that the fault was not injected; they show that availability is the wrong
column for those rows. Your CSV already has the right columns (the header line in your log contains coverage, delivery_ratio, detect_s,
bogus_accepted). `analysis/spark_analysis.py` prints "the metric that discriminates each scenario" for every scenario in your file, so check
the numbers there rather than trusting my reading of rows I have not seen. Use `--trials 10` (not 3) before quoting Group B differences such
as the 0.003 availability gap in site_crash.

## 3. Milestone 9: HDFS as a second instance of the five transparencies
| Transparency | HDFS | Our replicated store (Week 9) | Evidence in this project |
|---|---|---|---|
| Access | one `hdfs://` path and one FileSystem API for any DataNode | one `read/write(name)` for memory, file and local copies | both: you read `/tower/week7_faults.csv` without naming a node |
| Location | the NameNode maps path -> blocks -> DataNodes | the name server maps name -> primary + backups | your fsck lists the 3 DataNodes the application never named |
| Replication | replication 3 is invisible to the reader | rf copies, one logical object | your fsck: `Live_repl=3`, `Missing replicas 0` |
| Migration | the Balancer / decommissioning move replicas without changing the path | `migrate()` moves the primary, clients are redirected | ours measured (0 errors, 4 ms freeze); HDFS: mechanism exists, NOT demonstrated on your cluster |
| Failure | the client library tries another replica; the NameNode declares the node dead after 2 x recheck + 10 x heartbeat = 30 s | client suspects, name server promotes a backup (about 0.03 s outage) | yours: a read after the 30 s wait works and the node rejoins. NOT yet shown: reads DURING the 30 s |

Expectations the probe can confirm or refute (do not state them as results before you run it): the native client should keep reading during the
window (client-side failover, like our client), while WebHDFS reads may fail for the share of requests the NameNode redirects to the dead node
(its view is stale for 30 s). That would be the same lesson as H9.6: the party that actually talks to the node notices first. Also check, during an
outage, `hdfs fsck /tower | grep -i under`: with 3 DataNodes and replication 3 the NameNode has no fourth node to re-replicate to, so I expect the block to
stay under-replicated until the node returns.

## 4. Milestone 10: naming and process management
* NameNode = name server. The namespace (path -> inode -> block list) is held by ONE master, which is our `nameserver` scheme (one request,
  one master, a single point of failure; your cluster has no standby NameNode, say so). Block LOCATIONS are not stored on disk: DataNodes report
  them, so after a NameNode restart they are rebuilt. That is the same soft-state idea as our core re-learning the cluster (registry_restore_s).
* Prefix tables. HDFS Federation with ViewFs mount tables maps path prefixes to different NameNodes on the client side: our `prefix` scheme.
  Not deployed here; it is a design comparison, not a measurement.
* Spark as process management: driver ~ coordinator, executors ~ workers, a DAG of stages ~ our workflow, locality preferences ~ placement, task
  retry ~ reschedule. In `local[1]` there are no separate executors and no distributed scheduling: the precise claim is "HDFS provides distributed
  storage; Spark provides the analysis; Spark itself is not distributed in this setup".

## 5. What you can claim
| Claim | Backed by |
|---|---|
| 3 live DataNodes, every block on 3 of them, no missing replicas | your `dfsadmin -report` and `fsck` |
| Losing one DataNode is detected in about 30 s (defaults: 10.5 min) and the file stays readable afterwards; the node rejoins | your fault-injection log (the formula 2 x 10 + 10 x 1 = 30 s) |
| Spark reads the CSV from HDFS and aggregates it | your Spark output |
| Reads succeed throughout the detection window | NOT yet: run `experiments/hdfs_probe.py` |
| A three-node cluster / physical redundancy | NO: three processes on one VM |
| Spark is distributed | NO: `local[1]` |
Small wording fix for your log: `hdfs dfsadmin -report | grep -E "Live datanodes|Name:"` also matches the entry of the node under "Dead datanodes", which
is why three `Name:` lines appeared under "Live datanodes (2)". Use `grep -E "Live datanodes|Dead datanodes"`.

## 6. Limits to add
The Week 9/10 replicated store is our own (primary-backup, epochs, fencing), not HDFS-backed; HDFS archives and analyses its results. `hdfs_probe.py` and
`archive_to_hdfs.sh` were tested only against stand-ins for the NameNode, WebHDFS and the `hdfs` command (control flow, not real HDFS behaviour); the Spark
engine of `spark_analysis.py` was not run here (no Spark in my environment), only its pandas twin on your real header and rows. Check the first real
run's output.
