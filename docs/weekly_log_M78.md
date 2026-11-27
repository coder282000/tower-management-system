# Failure-Driven Engineering Log: Weeks 7 and 8 (candidate entries)

These are things that genuinely went wrong in the Week 7 and 8 measurement code during review and testing of this snapshot.
Use the ones that really happened in YOUR runs, and add your own (every bug you hit counts). Do not claim a failure you did not see.

## Week 7
| Question | Response |
|---|---|
| What changed? | Added a fault-injection framework (crash, omission, partition, service, Byzantine), failure detection, backup-manager failover, replication, a store-and-forward outbox, soft-state re-registration and site-down detection. Baseline vs proposed on 7 scenarios, alternating run order, mean ± sd. |
| What failed? | (1) The availability probe read the acknowledgement from the site's manager, so a core crash showed availability 1.0 in both modes. (2) With "up" meaning an acknowledgement younger than 1 s, a 0.47 s failover was invisible (MTTR 0.0). (3) The duplicate counter read the whole log, so eids reused by earlier runs looked like 180 duplicates. (4) A killed and relaunched node lost its earlier log because the logger opened the file in write mode. (5) A crashed tower site went unnoticed: nothing watched the sites. |
| Why did it fail? | (1) The probe measured the manager path, not the core path. (2) The threshold was larger than the recovery time being measured. (3) Escalation ids restart at every run and the filter was not limited to the current run. (4) `EventLog` truncates on start. (5) Detection only existed for managers. |
| How was it fixed? | (1) A direct probe of the core plus a ground-truth `coverage` metric (fault episodes at the sites vs incidents that reached the core). (2) Threshold 0.5 s and the worst gap reported next to it. (3) Counting restricted to the current run. (4) `log_append` option for Week 7/8 clusters. (5) The core suspects a site that stops re-registering (`ft_site_down`). |
| What alternative was considered? | (1) Probing through the manager but adding an end-to-end heartbeat. (5) Letting each manager flag silent sites; rejected because a site that failed over would look dead to its old manager. |
| What was learned? | A metric must be checked against a fault it is supposed to catch before it is trusted. Ground truth (the fault id travelling with the reading) beats proxies. The in-memory outbox and duplicate filter are lost in a crash: say so as a limitation. |

## Week 8
| Question | Response |
|---|---|
| What changed? | Throughput and a saturation (stress) test, CPU and memory per tier, two control models, repeated reliability trials, a decision matrix with weights/cost/size sensitivity, and an automatic SUPPORTED / NOT SUPPORTED verdict per hypothesis. |
| What failed? | (1) "Messages into the core" included constant background traffic (resource updates, registration), which hid the difference between models. (2) The integrated model was the only one with fault tolerance on, so its reliability advantage could not be attributed. (3) Uniformly scaling the unit costs cannot change a min-max normalised ranking. |
| Why did it fail? | (1) The counter used every received message. (2) Two things changed at once. (3) Min-max normalisation is scale-invariant. |
| How was it fixed? | (1) A separate fault-management (data-plane) message count is used for the core-load criterion. (2) Controls `integrated_noft` and `server_based_ft`. (3) Cost sensitivity changes individual unit costs (workstation, manager, bandwidth, core) instead. |
| What alternative was considered? | Weighting by an expert survey instead of four fixed profiles; rejected as out of scope, noted as future work. |
| What was learned? | Change one variable at a time; a comparison is only as good as its controls. A ranking should be reported with what it depends on. |
