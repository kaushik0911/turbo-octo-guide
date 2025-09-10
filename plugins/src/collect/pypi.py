import requests
import os
import json
from time import sleep
from datetime import datetime
from airflow.models import Variable
from src.collect import RAW_DIR


def fetch_pypi_downloads(**context) -> None:
    results = []
    task_instance = context['ti']

    packages = Variable.get("python_packages", default_var="[]", deserialize_json=True)
    
    packages = [pkg.strip() for pkg in packages if pkg.strip()]

    for pkg in packages:
        url = f"https://pypistats.org/api/packages/{pkg}/recent"
        try:
            r = requests.get(url)
            data = r.json().get("data", {})
            results.append({
                "source": "pypi",
                "package": pkg,
                "last_day": data.get("last_day"),
                "last_week": data.get("last_week"),
                "last_month": data.get("last_month")
            })

        except Exception as e:
            task_instance.log.error(f"Error fetching data for {pkg}: {e}")
            results.append({
                "source": "pypi",
                "package": pkg,
                "last_day": 0,
                "last_week": 0,
                "last_month": 0
            })

        sleep(1)  # to avoid hitting rate limits

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    pypi_path = os.path.join(RAW_DIR, f"pypi_{ts}.json")

    with open(pypi_path, "w") as f:
        json.dump(results, f, indent=2)
        f.close()

    task_instance.xcom_push(key="pypi_raw", value=pypi_path)
