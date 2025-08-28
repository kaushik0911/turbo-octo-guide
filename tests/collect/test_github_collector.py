import os
import json
import tempfile
from unittest.mock import MagicMock
import pytest

from plugins.src.collect.github_collector import fetch_github_metrics

@pytest.fixture
def fake_repos():
    return {"python": "psf/requests", "data": "pandas-dev/pandas"}


@pytest.fixture
def fake_github_response():
    return {
        "stargazers_count": 123,
        "forks_count": 45,
        "open_issues_count": 7,
        "subscribers_count": 9,
        "updated_at": "2025-08-26T12:34:56Z",
    }


def test_fetch_github_metrics(monkeypatch, fake_repos, fake_github_response):
    tmp_dir = tempfile.mkdtemp()
    # patch RAW_DIR so we don’t touch real data/
    monkeypatch.setattr("plugins.src.collect.github_collector.RAW_DIR", tmp_dir)

    # mock Airflow Variable.get
    monkeypatch.setattr(
        "plugins.src.collect.github_collector.Variable.get",
        lambda key, default_var=None, deserialize_json=False: fake_repos,
    )

    # mock requests.get
    class FakeResponse:
        def json(self):
            return fake_github_response

    monkeypatch.setattr("plugins.src.collect.github_collector.requests.get", lambda *a, **k: FakeResponse())

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

    with open(os.path.join(tmp_dir, files[0])) as f:
        data = json.load(f)

    # should have entries for both repos
    assert len(data) == 2
    assert data[0]["technology"] == "python"
    assert data[0]["stars"] == 123
    assert data[0]["forks"] == 45
    assert data[0]["watchers"] == 9
    assert data[0]["open_issues"] == 7
    assert data[0]["last_updated"] == "2025-08-26T12:34:56Z"

    # ensure xcom_push was called
    ti.xcom_push.assert_called_once()
