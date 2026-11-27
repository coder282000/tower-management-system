#!/usr/bin/env bash
# Start HDFS (NameNode + 3 DataNodes) and WAIT until it is really usable. A fixed "sleep 10" is not enough: the NameNode
# answers on port 9000 only after it has loaded its namespace, so an early `hdfs dfsadmin -report` says "Connection refused".
#   bash scripts/hdfs_up.sh          run it AFTER the timing experiments, never during them (the JVMs take the one CPU)
command -v hdfs >/dev/null || { echo "hdfs is not on the PATH: run 'source ~/.bashrc' first"; exit 1; }
hdfs --daemon start namenode
for i in 1 2 3; do bash ~/hdfs/dn.sh $i start; done
for t in $(seq 1 60); do
  live=$(hdfs dfsadmin -report 2>/dev/null | grep -m1 "Live datanodes" || true)
  case "$live" in *"(3)"*) echo "$live  (after about $((t * 3)) s)"; hdfs dfsadmin -safemode get 2>/dev/null; exit 0;; esac
  sleep 3
done
echo "HDFS did not reach 3 live DataNodes within 3 minutes: check 'jps' and the logs in ~/hadoop*/logs"; exit 1
