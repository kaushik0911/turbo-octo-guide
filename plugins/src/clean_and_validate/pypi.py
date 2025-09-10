import os
import json
from datetime import datetime
import pandas as pd

from src.clean_and_validate import STAGING_DIR


def clean_and_validate_pipy_data(**context) -> None:
    task_instance = context["ti"]
    now = datetime.now()

    try:
        # --- Load raw file ---
        pypi_raw_path = task_instance.xcom_pull(key="pypi_raw")

        with open(pypi_raw_path, "r") as f:
            pypi_data = json.load(f)

        ts = now.strftime("%Y%m%d_%H%M%S")

        pypi_cleaned = []
        task_instance.log.info(f"Start cleaning and validating data")
        for pkg in pypi_data:
            if not pkg.get("package") or not pkg.get("source"):
                # skip entries without a package or source field
                continue

            cleaned_pkg = {
                "source": pkg.get("source", "").lower(),
                "package": pkg.get("package", "").lower(),
                # --- Ensure non-negative --
                "last_day": max(int(pkg.get("last_day", 0)), 0),
                "last_week": max(int(pkg.get("last_week", 0)), 0),
                "last_month": max(int(pkg.get("last_month", 0)), 0),
                # --- Ensure non-negative --
                "fetched_at": ts
            }

            pypi_cleaned.append(cleaned_pkg)

        # --- Write cleaned data, save parquet ---
        parquet_file = f"pypi_{ts}.parquet"
        pypi_stage_path = os.path.join(STAGING_DIR, parquet_file)

        task_instance.log.info(f"Start writing cleaned data written to {pypi_stage_path}")

        df_pypi = pd.DataFrame(pypi_data)

        required_cols = ["source", "package", "last_day", "last_week", "last_month"]
        missing = [col for col in required_cols if col not in df_pypi.columns]

        if missing:
            task_instance.log.error(f"PyPI data missing required cols: {missing}")
            raise

        df_pypi.to_parquet(pypi_stage_path, index=False)

        # --- Push to XCom ---
        task_instance.xcom_push(key="pypi_parquet_file", value=parquet_file)
        task_instance.log.info(f"PyPI cleaned data written to {pypi_stage_path}")

    except Exception as e:
        task_instance.log.error(f"Error in plugins/src/clean_and_validate/pypi.py: {e}", exc_info=True)
        raise
