from datetime import datetime
import os
import json
import pandas as pd
from src import STAGING_DIR

def github_validate_and_stage(**context) -> None:
    try:
        task_instance = context["ti"]
        github_cleaned_path = task_instance.xcom_pull(key="github_cleaned")

        with open(github_cleaned_path, "r") as f:
            github_data = json.load(f)

        df_github = pd.DataFrame(github_data)

        # Basic validation: ensure required columns
        required_cols = ["technology", "stars", "forks", "open_issues", "watchers", "last_updated"]
        missing = [col for col in required_cols if col not in df_github.columns]
        if missing:
            task_instance.log.error(f"GitHub data missing required cols: {missing}")
            raise

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        parquet_file = f"github_{ts}.parquet"
        github_stage_path = os.path.join(STAGING_DIR, parquet_file)
        df_github.to_parquet(github_stage_path, index=False)

        task_instance.xcom_push(key="parquet_file", value=parquet_file)
    except Exception as e:
        task_instance.log.error(f"Error in GitHub validation/staging: {e}")
        raise
