import os
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from src import STAGING_DIR

def load_parquet_to_snowflake(table_name, task_id, **context) -> None:
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    task_instance = context["ti"]

    try:
        conn = hook.get_conn()
        cursor = conn.cursor()

        snowflake_table = get_snowflake_table(table_name, task_instance)

        snowflake_stage_name = f"{snowflake_table}_STAGE"
        cursor.execute(f"CREATE STAGE IF NOT EXISTS {snowflake_stage_name}")

        # upload parquet file
        parquet_file = task_instance.xcom_pull(task_ids=task_id, key="parquet_file")
        parquet_file_path = os.path.join(STAGING_DIR, parquet_file)

        cursor.execute(f"PUT file://{parquet_file_path} @{snowflake_stage_name} OVERWRITE = TRUE")

        # copy into target table
        copy_sql = f"""
        COPY INTO {snowflake_table}
        FROM @{snowflake_stage_name}
        FILE_FORMAT = (TYPE = PARQUET)
        PURGE = TRUE
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
        """
        cursor.execute(copy_sql)

    except Exception as e:
        task_instance.log.error(f"Error loading data into Snowflake: {e}")
        raise
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

def get_snowflake_table(table_name, task_instance) -> str:
    if table_name == "github_metrics":
        return "GITHUB_DATA"
    elif table_name == "pypi_downloads":
        return "PYPI_DATA"
    else:
        task_instance.log.error(f"Unknown table name: {table_name}")
        raise
