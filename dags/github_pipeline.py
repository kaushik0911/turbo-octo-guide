import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from src.collect.github_collector import fetch_github_metrics
from src.clean.github_cleaner import github_clean_data
from src.validate.github_validation import github_validate_and_stage
from src.load.load_to_snowflake import load_parquet_to_snowflake


with DAG(
    "github_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["data-engineering-project"],
) as dag:

    task_github_data_collect = PythonOperator(
        task_id="fetch_github_metrics",
        python_callable=fetch_github_metrics,
        provide_context=True
    )

    task_github_cleaned = PythonOperator(
        task_id="github_clean_data",
        python_callable=github_clean_data,
        provide_context=True,
    )

    task_github_validate_and_stage = PythonOperator(
        task_id="github_validate_and_stage",
        python_callable=github_validate_and_stage,
        provide_context=True,
    )

    task_load_to_snowflake = PythonOperator(
        task_id="load_github_to_snowflake",
        python_callable=load_parquet_to_snowflake,
        op_kwargs={
            "table_name": "github_metrics",
            "task_id": "github_validate_and_stage"
        }
    )

    task_github_data_collect >> task_github_cleaned >> task_github_validate_and_stage >> task_load_to_snowflake
