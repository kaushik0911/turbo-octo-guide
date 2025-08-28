import os
import json
from datetime import datetime

from src import CLEAN_DIR

def pypi_clean_data(**context) -> None:
    task_instance = context['ti']
    pypi_raw_path = task_instance.xcom_pull(key="pypi_raw")

    with open(pypi_raw_path, "r") as f:
        pypi_data = json.load(f)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    pypi_cleaned_path = os.path.join(CLEAN_DIR, f"pypi_cleaned_{ts}.json")

    pypi_cleaned = []
    for pkg in pypi_data:
        if not pkg.get("package") or not pkg.get("source"):
            continue
        pypi_cleaned.append({
            "source": pkg.get("source", "").lower(),
            "package": pkg.get("package", "").lower(),
            "last_day": int(pkg.get("last_day", 0)),
            "last_week": int(pkg.get("last_week", 0)),
            "last_month": int(pkg.get("last_month", 0)),
            "fetched_at": ts
        })

    with open(pypi_cleaned_path, "w") as f:
        json.dump(pypi_cleaned, f, indent=2)
        f.close()

    task_instance.xcom_push(key="pypi_cleaned", value=pypi_cleaned_path)
