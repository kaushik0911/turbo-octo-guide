import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.collect.pypi import fetch_pypi_downloads
from src.clean_and_validate.pypi import clean_and_validate_pipy_data
from src.load.to_snowflake import load_parquet_to_snowflake


with DAG(
    "pypi_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["data-engineering-project"],
) as dag:
    
    task_fetch_pypi_downloads = PythonOperator(
        task_id="fetch_pypi_downloads",
        python_callable=fetch_pypi_downloads,
        provide_context=True
    )

    task_clean_and_validate_pipy_data = PythonOperator(
        task_id="clean_and_validate_pipy_data",
        python_callable=clean_and_validate_pipy_data,
        provide_context=True,
    )

    task_load_parquet_to_snowflake = PythonOperator(
        task_id="load_parquet_to_snowflake",
        python_callable=load_parquet_to_snowflake,
        op_kwargs={
            "source_name": "pypi",
            "task_id": "clean_and_validate_pipy_data"
        }
    )

    task_fetch_pypi_downloads >> \
        task_clean_and_validate_pipy_data >> \
            task_load_parquet_to_snowflake
