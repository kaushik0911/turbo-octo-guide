import os
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from src.load import STAGING_DIR


def load_parquet_to_snowflake(source_name, task_id, **context) -> None:
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    task_instance = context["ti"]

    try:
        task_instance.log.info(f"Connecting to Snowflake...")
        conn = hook.get_conn()
        cursor = conn.cursor()
        task_instance.log.info(f"Connected to Snowflake...")

        snowflake_table = get_snowflake_table(source_name, task_instance)

        snowflake_stage_name = f"{snowflake_table}_STAGE"
        cursor.execute(f"CREATE STAGE IF NOT EXISTS {snowflake_stage_name}")

        # --- Upload parquet file ---
        task_instance.log.info(f"Start upload parquet file to snowflake stage.")

        parquet_file_key = f"{source_name}_parquet_file"

        parquet_file = task_instance.xcom_pull(task_ids=task_id, key=parquet_file_key)
        parquet_file_path = os.path.join(STAGING_DIR, parquet_file)

        cursor.execute(f"PUT file://{parquet_file_path} @{snowflake_stage_name} OVERWRITE = TRUE")

        task_instance.log.info(f"Insert file to {snowflake_table} table.")
        # copy into target table
        copy_sql = f"""
        COPY INTO {snowflake_table}
        FROM @{snowflake_stage_name}
        FILE_FORMAT = (TYPE = PARQUET)
        PURGE = TRUE
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
        """
        cursor.execute(copy_sql)
        task_instance.log.info(f"Insertion complete.")

    except Exception as e:
        task_instance.log.error(f"Error loading data into Snowflake: {e}")
        raise
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

def get_snowflake_table(source_name, task_instance) -> str:
    if source_name == "github":
        return "GITHUB_DATA"
    elif source_name == "pypi":
        return "PYPI_DATA"
    else:
        task_instance.log.error(f"Unknown table name: {source_name}")
        raise
