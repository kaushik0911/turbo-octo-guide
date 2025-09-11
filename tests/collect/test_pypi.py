import os
import json
import tempfile
from unittest.mock import MagicMock
import pytest

from plugins.src.collect.pypi import fetch_pypi_downloads

@pytest.fixture
def fake_packages():
    return ["apache-airflow", "dbt-core"]


@pytest.fixture
def fake_pypi_response():
    return [
        {
            "source": "pypi",
            "package": "apache-airflow",
            "last_day": 488960,
            "last_week": 3329820,
            "last_month": 13852932
        },
        {
            "source": "pypi",
            "package": "dbt-core",
            "last_day": 1354969,
            "last_week": 8868442,
            "last_month": 36931825
        }
    ]

def test_fetch_pypi_downloads(monkeypatch, fake_packages, fake_pypi_response):
    tmp_dir = tempfile.mkdtemp()
    monkeypatch.setattr("plugins.src.collect.pypi.RAW_DIR", tmp_dir)

    # mock airflow Variable.get
    monkeypatch.setattr(
        "plugins.src.collect.pypi.Variable.get",
        lambda key, default_var="[]", deserialize_json=True: fake_packages,
    )

    # fake requests.get
    class FakeResponse:
        def json(self): return fake_pypi_response

    monkeypatch.setattr("plugins.src.collect.pypi.requests.get", lambda *a, **k: FakeResponse())

    # disable sleep
    monkeypatch.setattr("plugins.src.collect.pypi.sleep", lambda x: None)

    # mock TaskInstance
    ti = MagicMock()
    ti.xcom_push = MagicMock()
    ti.log = MagicMock()

    fetch_pypi_downloads(ti=ti)

    # check file created
    files = [f for f in os.listdir(tmp_dir) if f.startswith("pypi_")]
    assert len(files) == 1

    # get first file from `files` list
    with open(os.path.join(tmp_dir, files[0])) as f:
        data = json.load(f)

    assert data[0]["source"] == "pypi"
    assert data[0]["package"] == "apache-airflow"
    assert data[0]["last_day"] == 488960
    assert data[0]["last_week"] == 3329820
    assert data[0]["last_month"] == 13852932

    ti.xcom_push.assert_called_once()
