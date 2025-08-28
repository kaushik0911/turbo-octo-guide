import os
import tempfile
import pytest
from unittest.mock import MagicMock, patch

from plugins.src.load.load_to_snowflake import load_parquet_to_snowflake, get_snowflake_table

def test_get_snowflake_table_valid():
    ti = MagicMock()
    assert get_snowflake_table("github_metrics", ti) == "GITHUB_DATA"
    assert get_snowflake_table("pypi_downloads", ti) == "PYPI_DATA"

@patch("plugins.src.load.load_to_snowflake.SnowflakeHook")
def test_load_parquet_to_snowflake_success(mock_hook, monkeypatch):
    tmp_dir = tempfile.mkdtemp()
    monkeypatch.setattr("plugins.src.load.load_to_snowflake.STAGING_DIR", tmp_dir)

    # create dummy parquet file name (simulate xcom)
    parquet_file = "dummy.parquet"
    parquet_path = os.path.join(tmp_dir, parquet_file)
    with open(parquet_path, "wb") as f:
        f.write(b"dummy-parquet-bytes")

    ti = MagicMock()
    ti.xcom_pull = lambda task_ids, key: parquet_file
    ti.log = MagicMock()

    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_hook.return_value.get_conn.return_value = mock_conn

    load_parquet_to_snowflake("github_metrics", task_id="stage_task", ti=ti)

    mock_cursor.execute.assert_any_call("CREATE STAGE IF NOT EXISTS GITHUB_DATA_stage")

    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()
