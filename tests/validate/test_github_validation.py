import os
import json
import tempfile
import pandas as pd
from unittest.mock import MagicMock
import pytest

from plugins.src.validate.github_validation import github_validate_and_stage

@pytest.fixture
def valid_github_data():
    return [
        {
            "technology": "airflow",
            "stars": 30000,
            "forks": 12000,
            "open_issues": 500,
            "watchers": 2000,
            "last_updated": "2025-08-25T10:00:00Z",
        }
    ]


def test_github_validate_and_stage_success(monkeypatch, valid_github_data):
    tmp_dir = tempfile.mkdtemp()
    monkeypatch.setattr("plugins.src.validate.github_validation.STAGING_DIR", tmp_dir)

    # write fake cleaned json
    cleaned_path = os.path.join(tmp_dir, "github_cleaned.json")
    with open(cleaned_path, "w") as f:
        json.dump(valid_github_data, f)

    # fake TI
    ti = MagicMock()
    ti.xcom_pull = lambda key: cleaned_path
    ti.xcom_push = MagicMock()
    ti.log = MagicMock()

    github_validate_and_stage(ti=ti)

    # parquet file should be created
    files = [f for f in os.listdir(tmp_dir) if f.endswith(".parquet")]
    assert len(files) == 1

    df = pd.read_parquet(os.path.join(tmp_dir, files[0]))
    assert "technology" in df.columns
    assert df.iloc[0]["technology"] == "airflow"
    assert df.iloc[0]["stars"] == 30000

    ti.xcom_push.assert_called_once()
