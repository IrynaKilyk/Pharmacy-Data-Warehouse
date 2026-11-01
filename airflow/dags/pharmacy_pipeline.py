import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# Add the scripts folder (next to this file) to the Python path
# so that Airflow can import our modules
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "scripts"))
)

from data_generator import load_data_to_dwh  # noqa: E402

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,  # Number of retries on failure
    "retry_delay": timedelta(minutes=5),
}

# Project root: airflow/dags/pharmacy_pipeline.py -> dags -> airflow -> project root
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)

# Path to the dbt project folder (computed dynamically, works on Linux/WSL)
DBT_PROJECT_DIR = os.path.join(PROJECT_ROOT, "pharmacy_dbt")

# Path to the folder containing profiles.yml
DBT_PROFILES_DIR = DBT_PROJECT_DIR

# DAG definition
with DAG(
    dag_id="pharmacy_elt_pipeline",
    default_args=default_args,
    description="ELT pipeline: load raw data, transform it with dbt, then test it",
    schedule="@daily",  # Run every day at midnight
    catchup=False,  # Do not backfill past runs on the first start
) as dag:

    # Task 1: Extract and load raw data (Lab #1)
    extract_load_task = PythonOperator(
        task_id="extract_and_load_raw_data",
        python_callable=load_data_to_dwh,
    )

    # Task 2: Transform data with dbt (Lab #2)
    # BashOperator runs the dbt CLI command in the terminal
    dbt_run_task = BashOperator(
        task_id="dbt_run_transformations",
        bash_command=(
            f"dbt run --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # Task 3: Run dbt tests (data quality gate)
    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # Task dependencies (execution order):
    # transformation starts ONLY after the load succeeds,
    # tests run ONLY after the transformation succeeds
    extract_load_task >> dbt_run_task >> dbt_test_task