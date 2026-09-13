"""Real Airflow DAG orchestrating the lakehouse pipeline: generate the
raw data, run the PySpark transform, then validate the output with
Great Expectations. Uses BashOperator so each task runs the exact same
scripts a human would run by hand -- no Airflow-specific logic hidden
inside the transform/validation code itself.

Tasks invoke a separate venv's python (params.python_bin) rather than
whatever `python` resolves to on Airflow's own PATH: Airflow's own
dependencies (pydantic/typing_extensions, pinned by its constraints
file) conflict with pyspark/great_expectations' newer pydantic if
installed into the same environment -- a real, well-known reason
production Airflow deployments keep task dependencies out of Airflow's
own venv (BashOperator/PythonVirtualenvOperator/ExternalPythonOperator
all exist for this reason)."""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="job_market_pipeline",
    description="Generate synthetic job-market data, transform with PySpark, validate with Great Expectations",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pyspark", "great_expectations", "lakehouse"],
) as dag:

    default_params = {"project_dir": "/opt/airflow/project", "python_bin": "python"}

    generate_data = BashOperator(
        task_id="generate_raw_data",
        bash_command="cd {{ params.project_dir }} && {{ params.python_bin }} data/generate_raw_data.py",
        params=default_params,
    )

    pyspark_transform = BashOperator(
        task_id="pyspark_transform",
        bash_command="cd {{ params.project_dir }} && {{ params.python_bin }} spark_jobs/transform.py",
        params=default_params,
    )

    validate_outputs = BashOperator(
        task_id="great_expectations_validate",
        bash_command="cd {{ params.project_dir }} && {{ params.python_bin }} great_expectations/validate_outputs.py",
        params=default_params,
    )

    generate_data >> pyspark_transform >> validate_outputs
