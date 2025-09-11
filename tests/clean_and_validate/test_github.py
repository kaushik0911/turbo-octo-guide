import os
import json
import tempfile
from unittest.mock import MagicMock
import pytest

from plugins.src.clean_and_validate.github import clean_and_validate_github_data

@pytest.fixture
def sample_raw_data():
    return [
        {
            "technology": "Airflow",
            "stars": "100",
            "forks": "50",
            "watchers": "20",
            "open_issues": "5",
            "last_updated": "2025-08-20"
        },
        {
            "technology": None,  # should be skipped
            "stars": "10",
            "forks": "3",
            "watchers": "2",
            "open_issues": "1",
            "last_updated": "2025-08-18"
        }
    ]


def test_clean_and_validate_github_data(sample_raw_data, monkeypatch):
    tmp_dir = tempfile.mkdtemp()
    monkeypatch.setattr("plugins.src.clean_and_validate.STAGING_DIR", tmp_dir)

    # create fake raw file
    raw_file = os.path.join(tmp_dir, "github_raw.json")
    with open(raw_file, "w") as f:
        json.dump(sample_raw_data, f)

    # mock airflow context
    ti = MagicMock()
    ti.xcom_pull.return_value = raw_file
    ti.xcom_push = MagicMock()
    context = {"ti": ti}

    github_clean_data(**context)

    # check cleaned file
    cleaned_files = [f for f in os.listdir(tmp_dir) if f.startswith("github_cleaned_")]
    assert len(cleaned_files) == 1

    with open(os.path.join(tmp_dir, cleaned_files[0])) as f:
        cleaned = json.load(f)

    assert len(cleaned) == 1
    assert cleaned[0]["technology"] == "airflow"
    ti.xcom_push.assert_called_once()
