#!/usr/bin/env bash
# Archive the experiment results into HDFS (replication 3) and verify them.
#   bash scripts/archive_to_hdfs.sh            run it AFTER the experiments, with HDFS started (never during a timing run)
# Layout:  /tower/<weekN>/<file>            the latest results (what analysis/spark_analysis.py reads)
#          /tower/archive/<stamp>/<weekN>/  an untouched snapshot of this archive run
set -euo pipefail
cd "$(dirname "$0")/.."
command -v hdfs >/dev/null || { echo "hdfs is not on the PATH: run 'source ~/.bashrc' first"; exit 1; }
live=""
for t in 1 2 3 4 5 6 7 8 9 10; do                   # the NameNode may still be loading: wait up to 30 s instead of failing at once
  live=$(hdfs dfsadmin -report 2>/dev/null | grep -m1 "Live datanodes" || true)
  [ -n "$live" ] && break
  sleep 3
done
[ -n "$live" ] || { echo "HDFS is not running: bash scripts/hdfs_up.sh first"; exit 1; }
echo "$live"
stamp=${1:-$(date +%Y%m%d_%H%M)}
n=0
for wk in week7 week8 week9 week10; do
  files=$(ls results/${wk}_*.csv results/${wk}_*.txt 2>/dev/null || true)
  if [ -z "$files" ]; then echo "no results for $wk: skipped"; continue; fi
  hdfs dfs -mkdir -p "/tower/$wk" "/tower/archive/$stamp/$wk"
  hdfs dfs -put -f $files "/tower/$wk/"
  hdfs dfs -put -f $files "/tower/archive/$stamp/$wk/"
  n=$((n + $(echo "$files" | wc -w)))
  echo "$wk: $(echo "$files" | wc -w) files"
done
[ -f results/environment.txt ] && hdfs dfs -put -f results/environment.txt /tower/environment.txt
[ "$n" -gt 0 ] || { echo "nothing to archive: run the experiments first"; exit 1; }
echo; hdfs dfs -ls -R /tower | grep -v archive | head -50
echo; echo "== replication check"
hdfs fsck /tower -files -blocks | grep -E "Status|Number of data-nodes|Missing|Under-replicated|Total files" || true
echo; echo "archived $n files; snapshot /tower/archive/$stamp"
