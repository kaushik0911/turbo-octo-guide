import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from src.collect.github import fetch_github_metrics
from src.clean_and_validate.github import clean_and_validate_github_data
from src.load.to_snowflake import load_parquet_to_snowflake


with DAG(
    "github_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
        # "@once", "@hourly", "@daily", "@weekly", "@monthly", "0 6 * * *" 
        # from datetime import timedelta
        # timedelta(hours=6)
        # schedule_interval=None, You must trigger it manually
    catchup=False,
    tags=["data-engineering-project"],
) as dag:

    task_fetch_github_metrics = PythonOperator(
        task_id="fetch_github_metrics",
        python_callable=fetch_github_metrics,
        provide_context=True
    )

    task_clean_and_validate_github_data = PythonOperator(
        task_id="clean_and_validate_github_data",
        python_callable=clean_and_validate_github_data,
        provide_context=True,
    )

    task_load_parquet_to_snowflake = PythonOperator(
        task_id="load_parquet_to_snowflake",
        python_callable=load_parquet_to_snowflake,
        op_kwargs={
            "source_name": "github",
            "task_id": "clean_and_validate_github_data"
        }
    )

    task_fetch_github_metrics >> \
        task_clean_and_validate_github_data >> \
            task_load_parquet_to_snowflake
