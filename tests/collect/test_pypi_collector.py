import os
import json
import tempfile
from unittest.mock import MagicMock
import pytest

from plugins.src.collect.pypi_collector import fetch_pypi_downloads

@pytest.fixture
def fake_packages():
    return ["pandas", "numpy"]


@pytest.fixture
def fake_pypi_response():
    return {
        "data": {
            "last_day": 10,
            "last_week": 70,
            "last_month": 300
        }
    }

def test_fetch_pypi_downloads_success(monkeypatch, fake_packages, fake_pypi_response):
    tmp_dir = tempfile.mkdtemp()
    monkeypatch.setattr("plugins.src.collect.pypi_collector.RAW_DIR", tmp_dir)

    # mock airflow Variable.get
    monkeypatch.setattr(
        "plugins.src.collect.pypi_collector.Variable.get",
        lambda key, default_var=None, deserialize_json=False: fake_packages,
    )

    # fake requests.get
    class FakeResponse:
        def json(self): return fake_pypi_response
    monkeypatch.setattr("plugins.src.collect.pypi_collector.requests.get", lambda *a, **k: FakeResponse())

    # disable sleep
    monkeypatch.setattr("plugins.src.collect.pypi_collector.sleep", lambda x: None)

    # mock TaskInstance
    ti = MagicMock()
    ti.xcom_push = MagicMock()
    ti.log = MagicMock()

    fetch_pypi_downloads(ti=ti)

    # check file created
    files = [f for f in os.listdir(tmp_dir) if f.startswith("pypi_")]
    assert len(files) == 1

    with open(os.path.join(tmp_dir, files[0])) as f:
        data = json.load(f)

    assert data[0]["source"] == "pypi"
    assert data[0]["package"] == "pandas"
    assert data[0]["last_day"] == 10
    assert data[1]["package"] == "numpy"

    ti.xcom_push.assert_called_once()
