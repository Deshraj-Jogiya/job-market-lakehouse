import subprocess
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from spark_jobs.transform import run

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("test-dashboard").master("local[2]").getOrCreate()
    yield session
    session.stop()


def test_dashboard_reflects_the_real_pipeline_output(spark, tmp_path):
    postings_path = tmp_path / "postings.csv"
    companies_path = tmp_path / "companies.csv"
    output_dir = tmp_path / "output"

    postings_path.write_text(
        "posting_id,company_id,seniority,source,posted_date,salary_usd\n"
        "1,10,Mid,greenhouse,2026-01-01,100000\n"
        "2,10,Mid,greenhouse,2026-01-02,120000\n"
        "3,20,Senior,lever,2026-01-03,160000\n"
    )
    companies_path.write_text(
        "company_id,industry,size_band\n"
        "10,Software,Mid\n"
        "20,Finance,Large\n"
    )

    run(spark, str(postings_path), str(companies_path), str(output_dir))

    dashboard_path = tmp_path / "dashboard.html"
    result = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "great_expectations" / "build_dashboard.py"),
            str(output_dir),
            str(dashboard_path),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert dashboard_path.exists()

    html = dashboard_path.read_text(encoding="utf-8")
    assert "Job Market Lakehouse" in html
    assert "Software" in html
    assert "Finance" in html
    assert "PASS" in html
    assert "Expectations passed" in html


def test_dashboard_exits_non_zero_when_an_expectation_fails(spark, tmp_path, monkeypatch):
    postings_path = tmp_path / "postings.csv"
    companies_path = tmp_path / "companies.csv"
    output_dir = tmp_path / "output"

    # A salary of -50 will fail the real "avg_salary between 1 and 500000"
    # expectation -- a real, deliberately broken pipeline run.
    postings_path.write_text(
        "posting_id,company_id,seniority,source,posted_date,salary_usd\n"
        "1,10,Mid,greenhouse,2026-01-01,-50\n"
    )
    companies_path.write_text(
        "company_id,industry,size_band\n"
        "10,Software,Mid\n"
    )

    run(spark, str(postings_path), str(companies_path), str(output_dir))

    dashboard_path = tmp_path / "dashboard.html"
    result = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "great_expectations" / "build_dashboard.py"),
            str(output_dir),
            str(dashboard_path),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    html = dashboard_path.read_text(encoding="utf-8")
    assert "FAIL" in html
