import os
import json
import tempfile
from unittest.mock import MagicMock
import pytest

from plugins.src.collect.github import fetch_github_metrics


@pytest.fixture
def fake_repos():
    return {
        "airflow": "apache/airflow",
        "dbt": "dbt-labs/dbt-core"
    }


@pytest.fixture
def fake_github_response():
    return [
        {
            "technology": "airflow",
            "stars": 42267,
            "forks": 15560,
            "open_issues": 1564,
            "watchers": 764,
            "last_updated": "2025-09-10T15:29:28Z"
        },
        {
            "technology": "dbt",
            "stars": 11366,
            "forks": 1797,
            "open_issues": 783,
            "watchers": 140,
            "last_updated": "2025-09-10T14:42:27Z"
        }
    ]


def test_fetch_github_metrics(monkeypatch, fake_repos, fake_github_response):
    tmp_dir = tempfile.mkdtemp()
    # patch RAW_DIR so we don’t touch real data/
    monkeypatch.setattr("plugins.src.collect.github.RAW_DIR", tmp_dir)

    # mock Airflow Variable.get
    monkeypatch.setattr(
        "plugins.src.collect.github.Variable.get",
        lambda key, default_var="{}", deserialize_json=True: fake_repos,
    )

    # mock requests.get
    class FakeResponse:
        def json(self):
            return fake_github_response

    monkeypatch.setattr("plugins.src.collect.github.requests.get", lambda *a, **k: FakeResponse())

    # mock TaskInstance
    ti = MagicMock()
    ti.xcom_push = MagicMock()
    ti.log = MagicMock()
    context = {"ti": ti}

    # run operator
    fetch_github_metrics(**context)

    # check cleaned file exists
    files = [f for f in os.listdir(tmp_dir) if f.startswith("github_")]
    assert len(files) == 1

    # get first file from `files` list
    with open(os.path.join(tmp_dir, files[0])) as f:
        data = json.load(f)

    # should have entries for both repos
    assert len(data) == 2
    assert data[0]["technology"] == "airflow"
    assert data[0]["stars"] == 42267
    assert data[0]["forks"] == 15560
    assert data[0]["watchers"] == 764
    assert data[0]["open_issues"] == 1564
    assert data[0]["last_updated"] == "2025-09-10T14:42:27Z"

    # ensure xcom_push was called
    ti.xcom_push.assert_called_once()
