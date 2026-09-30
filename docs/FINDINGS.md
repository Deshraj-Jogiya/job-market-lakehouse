# Findings

What the pipeline's checks found, with the numbers CI prints on every run. The input is 60,000 synthetic job postings generated with a
fixed seed (`random.seed(42)`), so every number below comes out the same on each run.

## 1. Does the month partition let a monthly query skip the rest of the table?

**Why it matters.** A query for one month should not scan 20 months of files.

**Test.** Load the 60,000 rows into a Parquet Hive table `PARTITIONED BY` month with a dynamic-partition `INSERT OVERWRITE`, then run
`SELECT count(*) ... WHERE posted_month = '2025-06'` and read Spark's executed plan.

**Results.**

| | Value |
|---|---|
| Rows loaded | 60,000 of 60,000 |
| Partitions | 20 (one per month) |
| Rows in June 2025 | 3,015 (5.0% of the table) |
| Partitions the June query reads | 1 of 20 (the plan carries a partition filter) |

**Decision.** Month stays the partition key: the monthly reports read one partition each.

## 2. Is a partition written straight to storage visible to queries?

**Test.** Write a test partition (3 rows, `posted_month=2099-01`) as Parquet files directly into the table's storage folder, count
`SHOW PARTITIONS`, run `MSCK REPAIR TABLE`, count again, then drop the test partition.

**Results.** 20 partitions before the repair, 21 after. Until the repair, the new partition's rows exist on disk but no query sees
them.

**Decision.** Any load that writes files outside `INSERT` runs `MSCK REPAIR TABLE` before the data is used.

## 3. Does "top 5 per industry" return five postings?

**Test.** Rank postings by salary within each of the 7 industries with `dense_rank()` and keep ranks 1 to 5.

**Results.** 37 rows, not 35. In Retail two postings tie at $204,956 (both rank 2), and in Software two tie at $204,928 (both rank 3),
so those industries return 6 rows each. `dense_rank()` gives tied values the same rank and skips no numbers, so "rank <= 5" means the
five highest salary levels, not five postings.

| Industry | Rows returned for ranks 1 to 5 |
|---|---|
| Software | 6 |
| Retail | 6 |
| Finance | 5 |
| Healthcare | 5 |
| Manufacturing | 5 |
| Logistics | 5 |
| Energy | 5 |

**Decision.** Kept, because the question is "which postings pay the top five salaries"; a report that needs exactly five rows would use
`row_number()` with a tie-breaker.

## 4. Do Hadoop MapReduce and Spark agree on the same file in HDFS?

**Test.** Load the raw CSV into a single-node HDFS, count postings per month and source with a Hadoop Streaming job (Python mapper and
reducer, 2 reducers), then read the same HDFS file with PySpark and compare.

**Results.** The mapper read 60,001 lines (the header plus 60,000 rows) and skipped the header. The two reducers wrote 49 and 51 keys,
100 in all (20 months x 5 sources). The 100 counts sum to 60,000, and Spark read 60,000 rows from the same file; every count matches.

## 5. Does the data-quality gate stop a bad run?

**Results.** 11 of 11 Great Expectations checks pass on the run's output (row counts, value sets, not-null, ranges, uniqueness, and a
cross-column `max_salary >= min_salary` rule). A test feeds the dashboard builder a failed expectation and checks that it exits non-zero,
so a failed check fails CI instead of only showing in a report.

## Reproduce

The CI workflow (`.github/workflows/ci.yml`) runs every step above; `python spark_jobs/hive_tables.py` prints the partition numbers and
`bash hadoop/run_hdfs_mapreduce.sh` the MapReduce comparison.
