"""Reads the raw postings back out of HDFS with PySpark and checks that the MapReduce counts match exactly."""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

hdfs_csv, mapreduce_tsv = sys.argv[1], sys.argv[2]
spark = SparkSession.builder.appName("hdfs-check").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
df = spark.read.csv(hdfs_csv, header=True, inferSchema=True)
spark_counts = {f"{r['m']}|{r['source']}": r["n"] for r in
                df.groupBy(F.date_format("posted_date", "yyyy-MM").alias("m"), "source").agg(F.count("*").alias("n")).collect()}
mr_counts = {}
for line in open(mapreduce_tsv):
    key, value = line.rstrip("\n").split("\t")
    mr_counts[key] = int(value)
print(f"rows read from HDFS by Spark: {df.count()}; MapReduce keys: {len(mr_counts)}; total counted: {sum(mr_counts.values())}")
assert mr_counts == spark_counts, "MapReduce and Spark disagree"
assert sum(mr_counts.values()) == df.count()
print("MapReduce output matches Spark exactly.")
spark.stop()
