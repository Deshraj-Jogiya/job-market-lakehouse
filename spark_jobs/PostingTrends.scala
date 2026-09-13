// Real Scala Spark job (Spark SQL DataFrame API), run non-interactively
// via `spark-shell -i` -- top-level statements, REPL-style, rather than
// an object/App wrapper, since spark-shell already provides `spark`.
//
// Complements transform.py's per-industry salary aggregation with a
// posting-volume-over-time view: monthly posting counts per source.

import org.apache.spark.sql.functions._

val postings = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data/raw_postings.csv")

val monthlyTrends = postings
  .withColumn("posted_month", date_format(col("posted_date"), "yyyy-MM"))
  .groupBy("posted_month", "source")
  .agg(count("*").alias("posting_count"))
  .orderBy("posted_month", "source")

println("Monthly posting counts by source:")
monthlyTrends.show(100, false)

monthlyTrends.write.mode("overwrite").parquet("data/output/monthly_posting_trends_by_source")

val totalMonths = monthlyTrends.select("posted_month").distinct().count()
println(s"Wrote monthly trend rows across $totalMonths distinct months.")

System.exit(0)
