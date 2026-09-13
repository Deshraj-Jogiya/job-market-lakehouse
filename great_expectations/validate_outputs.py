"""Real Great Expectations data-quality checks on the PySpark job's
output. Reads the written Parquet (via pandas, so this runs the same
way whether the job ran on a real cluster or locally) and validates it
with real expectation suites -- not just a "does the file exist" smoke
check."""

import sys

import great_expectations as ge
import pandas as pd

INDUSTRIES = {"Software", "Finance", "Healthcare", "Manufacturing", "Retail", "Logistics", "Energy"}
SENIORITIES = {"Junior", "Mid", "Senior", "Staff"}


def validate_salary_stats(output_dir):
    df = pd.read_parquet(f"{output_dir}/salary_stats_by_industry_seniority")
    dataset = ge.from_pandas(df)

    results = [
        dataset.expect_table_row_count_to_be_between(min_value=1),
        dataset.expect_column_values_to_be_in_set("industry", list(INDUSTRIES)),
        dataset.expect_column_values_to_be_in_set("seniority", list(SENIORITIES)),
        dataset.expect_column_values_to_not_be_null("avg_salary"),
        dataset.expect_column_values_to_be_between("avg_salary", min_value=1, max_value=500000),
        dataset.expect_column_values_to_be_between("posting_count", min_value=1),
        dataset.expect_column_pair_values_A_to_be_greater_than_B("max_salary", "min_salary", or_equal=True),
    ]
    return results


def validate_top_postings(output_dir):
    df = pd.read_parquet(f"{output_dir}/top_postings_by_industry")
    dataset = ge.from_pandas(df)

    results = [
        dataset.expect_table_row_count_to_be_between(min_value=1),
        dataset.expect_column_values_to_be_between("salary_rank_in_industry", min_value=1, max_value=5),
        dataset.expect_column_values_to_be_unique("posting_id"),
        dataset.expect_column_values_to_not_be_null("salary_usd"),
    ]
    return results


def main():
    output_dir = sys.argv[1] if len(sys.argv) > 1 else "data/output"

    all_results = validate_salary_stats(output_dir) + validate_top_postings(output_dir)
    failures = [r for r in all_results if not r["success"]]

    for r in all_results:
        expectation = r["expectation_config"]["expectation_type"]
        status = "PASS" if r["success"] else "FAIL"
        print(f"[{status}] {expectation}")

    if failures:
        print(f"\n{len(failures)} of {len(all_results)} expectations FAILED.")
        sys.exit(1)

    print(f"\nAll {len(all_results)} expectations passed.")


if __name__ == "__main__":
    main()
