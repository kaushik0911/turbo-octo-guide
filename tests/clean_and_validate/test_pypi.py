import os
import json
import tempfile
from unittest.mock import MagicMock
import pytest

from plugins.src.clean.pypi_cleaner import pypi_clean_data

@pytest.fixture
def sample_raw_data():
    return [
        {
            "source": "PyPI",
            "package": "requests",
            "last_day": "10",
            "last_week": "50",
            "last_month": "200"
        },
        {
            "source": None,  # should be skipped
            "package": "numpy",
            "last_day": "5",
            "last_week": "20",
            "last_month": "100"
        },
        {
            "source": "PyPI",
            "package": None,  # should also be skipped
            "last_day": "1",
            "last_week": "3",
            "last_month": "10"
        }
    ]


def test_pypi_clean_data(sample_raw_data, monkeypatch):
    tmp_dir = tempfile.mkdtemp()
    # patch CLEAN_DIR inside your operator
    monkeypatch.setattr("plugins.src.clean.pypi_cleaner.CLEAN_DIR", tmp_dir)

    # create fake raw JSON file
    raw_file = os.path.join(tmp_dir, "pypi_raw.json")
    with open(raw_file, "w") as f:
        json.dump(sample_raw_data, f)

    # mock Airflow TaskInstance context
    ti = MagicMock()
    ti.xcom_pull.return_value = raw_file
    ti.xcom_push = MagicMock()
    context = {"ti": ti}

    # run the function
    pypi_clean_data(**context)

    # check cleaned file exists
    cleaned_files = [f for f in os.listdir(tmp_dir) if f.startswith("pypi_cleaned_")]
    assert len(cleaned_files) == 1

    # load cleaned JSON
    with open(os.path.join(tmp_dir, cleaned_files[0])) as f:
        cleaned = json.load(f)

    # only 1 valid record should remain (requests)
    assert len(cleaned) == 1
    assert cleaned[0]["package"] == "requests"
    assert cleaned[0]["source"] == "pypi"
    assert cleaned[0]["last_day"] == 10
    assert cleaned[0]["last_week"] == 50
    assert cleaned[0]["last_month"] == 200

    # ensure xcom_push was called with cleaned path
    ti.xcom_push.assert_called_once()
