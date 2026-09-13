"""Real PySpark job: joins postings to companies, aggregates salary
stats per industry/seniority with the DataFrame API (Spark SQL under
the hood), and ranks postings within each industry with a window
function. Runs in local mode -- no cluster required, but the same
DataFrame/Spark-SQL code runs unchanged against a real cluster."""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def run(spark, postings_path, companies_path, output_dir):
    postings = spark.read.csv(postings_path, header=True, inferSchema=True)
    companies = spark.read.csv(companies_path, header=True, inferSchema=True)

    joined = postings.join(companies, on="company_id", how="inner")

    # Spark SQL aggregation via the DataFrame API.
    salary_stats = (
        joined.groupBy("industry", "seniority")
        .agg(
            F.count("*").alias("posting_count"),
            F.round(F.avg("salary_usd"), 2).alias("avg_salary"),
            F.min("salary_usd").alias("min_salary"),
            F.max("salary_usd").alias("max_salary"),
        )
        .orderBy("industry", "seniority")
    )

    # Window function: rank postings within each industry by salary.
    industry_window = Window.partitionBy("industry").orderBy(F.desc("salary_usd"))
    top_postings = (
        joined.withColumn("salary_rank_in_industry", F.dense_rank().over(industry_window))
        .filter(F.col("salary_rank_in_industry") <= 5)
        .select("posting_id", "industry", "seniority", "salary_usd", "salary_rank_in_industry")
        .orderBy("industry", "salary_rank_in_industry")
    )

    salary_stats.write.mode("overwrite").parquet(f"{output_dir}/salary_stats_by_industry_seniority")
    top_postings.write.mode("overwrite").parquet(f"{output_dir}/top_postings_by_industry")

    return salary_stats, top_postings


def main():
    postings_path = sys.argv[1] if len(sys.argv) > 1 else "data/raw_postings.csv"
    companies_path = sys.argv[2] if len(sys.argv) > 2 else "data/raw_companies.csv"
    output_dir = sys.argv[3] if len(sys.argv) > 3 else "data/output"

    spark = SparkSession.builder.appName("job-market-lakehouse").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    salary_stats, top_postings = run(spark, postings_path, companies_path, output_dir)

    print("Salary stats by industry/seniority:")
    salary_stats.show(50, truncate=False)
    print("Top 5 highest-paying postings per industry:")
    top_postings.show(50, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
