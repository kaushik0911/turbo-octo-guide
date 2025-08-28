import os
import json
import tempfile
import pandas as pd
from unittest.mock import MagicMock
import pytest

from plugins.src.validate.pypi_validation import pypi_validate_and_stage

@pytest.fixture
def valid_pypi_data():
    return [
        {"source": "pypi", "package": "pandas", "last_day": 10, "last_week": 70, "last_month": 300}
    ]

def test_pypi_validate_and_stage_success(monkeypatch, valid_pypi_data):
    tmp_dir = tempfile.mkdtemp()
    monkeypatch.setattr("plugins.src.validate.pypi_validation.STAGING_DIR", tmp_dir)

    # write fake cleaned json
    cleaned_path = os.path.join(tmp_dir, "pypi_cleaned.json")
    with open(cleaned_path, "w") as f:
        json.dump(valid_pypi_data, f)

    # fake TI
    ti = MagicMock()
    ti.xcom_pull = lambda key: cleaned_path
    ti.xcom_push = MagicMock()
    ti.log = MagicMock()

    pypi_validate_and_stage(ti=ti)

    files = [f for f in os.listdir(tmp_dir) if f.endswith(".parquet")]
    assert len(files) == 1

    df = pd.read_parquet(os.path.join(tmp_dir, files[0]))
    assert "package" in df.columns
    assert df.iloc[0]["package"] == "pandas"

    ti.xcom_push.assert_called_once()

def test_pypi_validate_and_stage_missing_columns(monkeypatch):
    tmp_dir = tempfile.mkdtemp()
    monkeypatch.setattr("plugins.src.validate.pypi_validation.STAGING_DIR", tmp_dir)

    bad_data = [{"foo": 1, "bar": 2}]
    bad_path = os.path.join(tmp_dir, "bad.json")
    with open(bad_path, "w") as f:
        json.dump(bad_data, f)

    ti = MagicMock()
    ti.xcom_pull = lambda key: bad_path
    ti.log = MagicMock()

    with pytest.raises(Exception):
        pypi_validate_and_stage(ti=ti)
