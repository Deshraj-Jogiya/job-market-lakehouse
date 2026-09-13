import os

import pytest
from pyspark.sql import Row, SparkSession

from spark_jobs.transform import run


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("test").master("local[2]").getOrCreate()
    yield session
    session.stop()


def test_salary_stats_and_window_rank_are_computed_correctly(spark, tmp_path):
    postings_path = tmp_path / "postings.csv"
    companies_path = tmp_path / "companies.csv"
    output_dir = str(tmp_path / "output")

    postings_path.write_text(
        "posting_id,company_id,seniority,source,posted_date,salary_usd\n"
        "1,10,Mid,greenhouse,2026-01-01,100000\n"
        "2,10,Mid,greenhouse,2026-01-02,120000\n"
        "3,20,Mid,lever,2026-01-03,110000\n"
        "4,20,Senior,lever,2026-01-04,160000\n"
    )
    companies_path.write_text(
        "company_id,industry,size_band\n"
        "10,Software,Mid\n"
        "20,Software,Large\n"
    )

    salary_stats, top_postings = run(spark, str(postings_path), str(companies_path), output_dir)

    stats_rows = {r["seniority"]: r for r in salary_stats.collect()}
    assert stats_rows["Mid"]["posting_count"] == 3
    assert stats_rows["Mid"]["avg_salary"] == pytest.approx(110000.0)
    assert stats_rows["Mid"]["min_salary"] == 100000
    assert stats_rows["Mid"]["max_salary"] == 120000
    assert stats_rows["Senior"]["posting_count"] == 1

    # All 4 postings share one industry (Software), so the window function
    # should rank all of them, highest salary first.
    ranked = [r["posting_id"] for r in top_postings.orderBy("salary_rank_in_industry").collect()]
    assert ranked == [4, 2, 3, 1]

    assert os.path.isdir(f"{output_dir}/salary_stats_by_industry_seniority")
    assert os.path.isdir(f"{output_dir}/top_postings_by_industry")
