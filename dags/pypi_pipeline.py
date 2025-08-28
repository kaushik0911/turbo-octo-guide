import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.collect.pypi_collector import fetch_pypi_downloads
from src.clean.pypi_cleaner import pypi_clean_data
from src.validate.pypi_validation import pypi_validate_and_stage
from src.load.load_to_snowflake import load_parquet_to_snowflake

RAW_DIR = "/opt/airflow/data/raw"
CLEAN_DIR = "/opt/airflow/data/cleaned"
STAGING_DIR = "/opt/airflow/data/staging"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(CLEAN_DIR, exist_ok=True)
os.makedirs(STAGING_DIR, exist_ok=True)

with DAG(
    "pypi_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["data-engineering-project"],
) as dag:
    
    task_pypi_data_collect = PythonOperator(
        task_id="fetch_pypi_downloads",
        python_callable=fetch_pypi_downloads,
        provide_context=True
    )

    task_pypi_cleaned = PythonOperator(
        task_id="pipy_clean_data",
        python_callable=pypi_clean_data,
        provide_context=True,
    )

    task_pypi_validate_and_stage = PythonOperator(
        task_id="pypi_validate_and_stage",
        python_callable=pypi_validate_and_stage,
        provide_context=True,
    )

    task_load_to_snowflake = PythonOperator(
        task_id="load_github_to_snowflake",
        python_callable=load_parquet_to_snowflake,
        op_kwargs={
            "table_name": "pypi_downloads",
            "task_id": "pypi_validate_and_stage"
        }
    )

    task_pypi_data_collect >> task_pypi_cleaned >> task_pypi_validate_and_stage >> task_load_to_snowflake
