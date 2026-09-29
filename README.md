# Job Market Lakehouse

A small local-mode data lakehouse pipeline over synthetic job-market data (60k postings,
400 companies -- large enough that a real DataFrame-API pipeline is a reasonable choice,
not theater): PySpark for transformation, Great Expectations for data-quality gating,
Airflow for orchestration, and a Scala Spark job doing a complementary aggregation.

## Data

`data/generate_raw_data.py` writes `raw_companies.csv` and `raw_postings.csv` -- clearly
synthetic (seeded, same disclosure as every other seed dataset in this author's other
repos), not a real company's data.

## Pipeline

1. **PySpark transform** (`spark_jobs/transform.py`) -- joins postings to companies,
   aggregates salary stats per industry/seniority (`groupBy`/`agg`, real Spark SQL under
   the DataFrame API), and ranks postings within each industry by salary using a
   `dense_rank()` window function. Writes two Parquet outputs.
2. **Great Expectations validation** (`great_expectations/validate_outputs.py`) -- real
   expectation suites against the PySpark output: row counts, categorical value sets,
   non-null/uniqueness constraints, a cross-column comparison (max >= min). Exits non-zero
   on any failed expectation.
3. **Data-quality dashboard** (`great_expectations/build_dashboard.py`) -- builds a real,
   static HTML report (`data/output/dashboard.html`) from the same expectation results
   plus the PySpark aggregate output: pass/fail counts, segment counts, and an average-
   salary-by-industry bar chart rendered as plain inline SVG (no JS charting library).
   Exits non-zero on the same conditions `validate_outputs.py` does, so a failing pipeline
   run fails the dashboard step too rather than silently publishing a green-looking report.
   CI uploads it as a downloadable artifact on every run.
4. **Airflow DAG** (`dags/job_market_pipeline.py`) -- `generate_raw_data >>
   pyspark_transform >> great_expectations_validate`, each a `BashOperator` running the
   exact same scripts a human would run by hand.
5. **Scala Spark job** (`spark_jobs/PostingTrends.scala`) -- a complementary monthly
   posting-volume-by-source aggregation, run via `spark-shell -i` rather than a full sbt
   project (no cluster or build tool needed, same Spark SQL DataFrame API in Scala).

## Running it

```bash
pip install -r requirements.txt   # needs a JDK on PATH for Spark (Java 17 used in CI)

python data/generate_raw_data.py
python spark_jobs/transform.py
python great_expectations/validate_outputs.py
python great_expectations/build_dashboard.py   # writes data/output/dashboard.html
python -m pytest tests/ -v
```

Scala job (needs a Spark distribution with `spark-shell` on PATH):

```bash
spark-shell -i spark_jobs/PostingTrends.scala
```

Airflow DAG (real, run via the CLI test mode rather than a persistent scheduler):

```bash
pip install "apache-airflow==2.10.2" --constraint <official-constraints-url-for-your-python-version>
export AIRFLOW_HOME=/tmp/airflow_home
mkdir -p "$AIRFLOW_HOME/dags" && cp dags/job_market_pipeline.py "$AIRFLOW_HOME/dags/"
airflow db migrate
airflow dags test job_market_pipeline 2026-01-01
```

## CI

`.github/workflows/ci.yml` runs the PySpark job, the Great Expectations validation, and
the pytest suite on every push, plus a second job that installs Airflow and runs the real
DAG end-to-end via `airflow dags test`. The Scala job is verified the same way Docker and
Jenkins were verified elsewhere in this author's work -- run for real against a Spark
distribution, not wired into this repo's CI (installing a full Spark distribution just to
run one Scala script in CI adds a lot of weight for a single job that's already proven).
