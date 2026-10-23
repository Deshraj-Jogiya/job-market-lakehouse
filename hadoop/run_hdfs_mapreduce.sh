#!/usr/bin/env bash
# Single-node HDFS plus a Hadoop Streaming MapReduce job over the raw postings, then a cross-check against Spark.
# Expects HADOOP_HOME to point at an unpacked Hadoop release and JAVA_HOME to be set.
set -euo pipefail
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
CONF="$(pwd)/hadoop/conf"
export HADOOP_CONF_DIR="$CONF"
DATA_DIR="${RUNNER_TEMP:-/tmp}/hdfs-data"
mkdir -p "$DATA_DIR"
sed "s|__DATA_DIR__|$DATA_DIR|g" hadoop/conf/hdfs-site.template.xml > "$CONF/hdfs-site.xml"

hdfs namenode -format -force -nonInteractive
hdfs --daemon start namenode
hdfs --daemon start datanode
hdfs dfsadmin -safemode wait

hdfs dfs -mkdir -p /lakehouse/raw
hdfs dfs -put -f data/raw_postings.csv /lakehouse/raw/
hdfs dfs -ls /lakehouse/raw

STREAMING_JAR=$(ls "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar)
hdfs dfs -rm -r -f /lakehouse/monthly_by_source
hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.reduces=2 \
  -files hadoop/mapper.py,hadoop/reducer.py \
  -mapper "python3 mapper.py" -reducer "python3 reducer.py" \
  -input /lakehouse/raw/raw_postings.csv -output /lakehouse/monthly_by_source

hdfs dfs -ls /lakehouse/monthly_by_source
hdfs dfs -cat '/lakehouse/monthly_by_source/part-*' > data/output/mapreduce_monthly_by_source.tsv
python3 hadoop/check_against_spark.py hdfs://localhost:9000/lakehouse/raw/raw_postings.csv data/output/mapreduce_monthly_by_source.tsv

hdfs --daemon stop datanode
hdfs --daemon stop namenode
