"""Hive tables over the raw postings, written in HiveQL and run through Spark's Hive support.

Spark is started with enableHiveSupport(), so tables live in a Hive metastore (embedded Derby, kept under
data/hive/) and every statement below is HiveQL: an EXTERNAL text table over the raw CSV, a managed Parquet table
PARTITIONED BY month and filled with a dynamic-partition INSERT OVERWRITE, MSCK REPAIR TABLE to register a partition
written straight to storage, and a query whose plan prunes partitions instead of scanning the whole table.
"""

import os
import shutil
import sys

from pyspark.sql import SparkSession

HIVE_ROOT = os.path.abspath("data/hive")


def session(hive_root=HIVE_ROOT):
    os.makedirs(hive_root, exist_ok=True)
    return (
        SparkSession.builder.appName("job-market-hive")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", os.path.join(hive_root, "warehouse"))
        .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={os.path.join(hive_root, 'metastore_db')};create=true")
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )


def run(spark, postings_csv, hive_root=HIVE_ROOT):
    raw_dir = os.path.join(hive_root, "external", "raw_postings")
    shutil.rmtree(raw_dir, ignore_errors=True)
    os.makedirs(raw_dir)
    shutil.copy(postings_csv, os.path.join(raw_dir, "postings.csv"))

    spark.sql("CREATE DATABASE IF NOT EXISTS job_market")
    spark.sql("USE job_market")

    # External table: Hive owns only the schema; dropping it leaves the CSV where it is.
    spark.sql("DROP TABLE IF EXISTS raw_postings")
    spark.sql(f"""
        CREATE EXTERNAL TABLE raw_postings (
            posting_id INT, company_id INT, seniority STRING, source STRING, posted_date STRING, salary_usd INT)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{raw_dir.replace(os.sep, "/")}'
    """)

    # Managed Parquet table partitioned by month, filled with a dynamic-partition insert.
    spark.sql("DROP TABLE IF EXISTS postings_by_month")
    spark.sql("""
        CREATE TABLE postings_by_month (
            posting_id INT, company_id INT, seniority STRING, source STRING, salary_usd INT)
        PARTITIONED BY (posted_month STRING)
        STORED AS PARQUET
    """)
    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    # The header line arrives as a row whose posting_id cannot be cast to INT, so it reads as NULL and is dropped here.
    spark.sql("""
        INSERT OVERWRITE TABLE postings_by_month PARTITION (posted_month)
        SELECT posting_id, company_id, seniority, source, salary_usd, substr(posted_date, 1, 7) AS posted_month
        FROM raw_postings
        WHERE posting_id IS NOT NULL
    """)

    # A partition written straight to storage is invisible until MSCK REPAIR TABLE registers it.
    table_dir = spark.sql("DESCRIBE FORMATTED postings_by_month").filter("col_name = 'Location'").first()["data_type"]
    extra = spark.sql("SELECT posting_id, company_id, seniority, source, salary_usd FROM postings_by_month "
                      "WHERE posted_month = (SELECT max(posted_month) FROM postings_by_month) LIMIT 3")
    extra.write.mode("overwrite").parquet(f"{table_dir}/posted_month=2099-01")
    before_repair = spark.sql("SHOW PARTITIONS postings_by_month").count()
    spark.sql("MSCK REPAIR TABLE postings_by_month")
    after_repair = spark.sql("SHOW PARTITIONS postings_by_month").count()
    spark.sql("ALTER TABLE postings_by_month DROP PARTITION (posted_month='2099-01')")

    monthly = spark.sql("""
        SELECT posted_month, source, count(*) AS posting_count, round(avg(salary_usd), 2) AS avg_salary
        FROM postings_by_month
        GROUP BY posted_month, source
        ORDER BY posted_month, source
    """)
    pruned_query = "SELECT count(*) AS n FROM postings_by_month WHERE posted_month = '2025-06'"
    plan = spark.sql(pruned_query)._jdf.queryExecution().executedPlan().toString()

    return {
        "raw_rows": spark.sql("SELECT count(*) FROM raw_postings WHERE posting_id IS NOT NULL").first()[0],
        "partitioned_rows": spark.sql("SELECT count(*) FROM postings_by_month").first()[0],
        "partitions": spark.sql("SHOW PARTITIONS postings_by_month").count(),
        "partitions_before_repair": before_repair,
        "partitions_after_repair": after_repair,
        "june_2025_rows": spark.sql(pruned_query).first()[0],
        "plan_prunes_partitions": "PartitionFilters: [isnotnull(posted_month" in plan and "2025-06" in plan,
        "monthly": monthly,
    }


def main():
    postings_csv = sys.argv[1] if len(sys.argv) > 1 else "data/raw_postings.csv"
    spark = session()
    spark.sparkContext.setLogLevel("WARN")
    result = run(spark, postings_csv)
    result["monthly"].show(20, truncate=False)
    result["monthly"].write.mode("overwrite").parquet("data/output/hive_monthly_by_source")
    for key, value in result.items():
        if key != "monthly":
            print(f"{key}: {value}")
    assert result["raw_rows"] == result["partitioned_rows"], "every raw row must land in exactly one partition"
    assert result["partitions_after_repair"] == result["partitions_before_repair"] + 1, "MSCK REPAIR must register the new partition"
    assert result["plan_prunes_partitions"], "the month filter must prune partitions, not scan the table"
    spark.stop()


if __name__ == "__main__":
    main()
