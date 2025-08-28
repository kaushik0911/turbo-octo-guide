import os
import json
from datetime import datetime
import pandas as pd
from src import STAGING_DIR

def pypi_validate_and_stage(**context) -> None:
	try:
		task_instance = context["ti"]
		pypi_path = task_instance.xcom_pull(key="pypi_cleaned")

		with open(pypi_path, "r") as f:
			pypi_data = json.load(f)

		df_pypi = pd.DataFrame(pypi_data)

		required_cols = ["source", "package", "last_day", "last_week", "last_month"]
		missing = [col for col in required_cols if col not in df_pypi.columns]
		if missing:
			task_instance.log.error(f"PyPI data missing required cols: {missing}")
			raise

		ts = datetime.now().strftime("%Y%m%d_%H%M%S")
		parquet_file = f"pypi_{ts}.parquet"
		pypi_stage_path = os.path.join(STAGING_DIR, parquet_file)
		df_pypi.to_parquet(pypi_stage_path, index=False)

		task_instance.xcom_push(key="parquet_file", value=parquet_file)

	except Exception as e:
		task_instance.log.error(f"Error in PyPI validation/staging: {e}")
		raise
