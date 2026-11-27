#!/usr/bin/env bash
# Stop HDFS completely: do this BEFORE any Week 7 to 10 timing run.
for i in 1 2 3; do bash ~/hdfs/dn.sh $i stop; done
hdfs --daemon stop namenode
sleep 2; jps
echo "(only Jps should be left)"
