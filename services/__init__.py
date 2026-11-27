"""Service modules (Milestone 3: service decomposition).

Each service owns one responsibility and is created by the Node that hosts it:

  correlation.CorrelationService   manager tier   raw sensor readings -> fault incidents
  archive.ArchiveService           core -> cloud  periodic snapshots, cloud-side store
  feedback.FeedbackService         core <-> cloud incident history up, watch-list policy down
  faulttolerance.FaultTolerance    Milestone 7: failure detection, failover, replication, outbox, fault injection

Placement, election, scheduling and resource management live in kernel/.
Services read and write shared node state (incidents, logical clock, log) through the
node they are attached to, so message handling stays in Node.handle.
"""
