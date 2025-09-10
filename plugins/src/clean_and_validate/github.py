import os
import json
from datetime import datetime
from dateutil import parser
import pandas as pd

from src.clean_and_validate import STAGING_DIR


def clean_and_validate_github_data(**context) -> None:
    task_instance = context["ti"]
    now = datetime.now()

    try:
        # --- Load raw file ---
        github_raw_path = task_instance.xcom_pull(key="github_raw")

        with open(github_raw_path, "r") as f:
            github_data = json.load(f)

        ts = now.strftime("%Y%m%d_%H%M%S")

        github_cleaned = []
        task_instance.log.info(f"Start cleaning and validating data")
        for repo in github_data:
            if not repo.get("technology"):
                # skip entries without a technology field
                continue

            # --- Validation of last_updated ---
            try:
                last_updated_str = repo.get("last_updated")
                last_updated = datetime.strptime(last_updated_str, "%Y-%m-%dT%H:%M:%SZ") if last_updated_str else None

                # reject repos with last_updated in the future
                if last_updated and last_updated > now:
                    task_instance.log.warning(
                        f"Skipping repo {repo.get('technology')} - last_updated is in the future: {last_updated_str}"
                    )
                    continue

            except Exception:
                # if last_updated is not a valid ISO date, skip
                task_instance.log.warning(
                    f"Skipping repo {repo.get('technology')} - invalid last_updated format: {repo.get('last_updated')}"
                )
                continue

            # --- Build cleaned record ---

            cleaned_repo = {
                "technology": repo.get("technology", "").lower(),
                # --- Ensure non-negative --
                "stars": max(int(repo.get("stars", 0)), 0),   
                "forks": max(int(repo.get("forks", 0)), 0),
                "watchers": max(int(repo.get("watchers", 0)), 0),
                "open_issues": max(int(repo.get("open_issues", 0)), 0),
                # --- Ensure non-negative --
                "last_updated": last_updated_str,
                "fetched_at": ts
            }

            github_cleaned.append(cleaned_repo)

        # --- Write cleaned data, save parquet ---
        parquet_file = f"github_{ts}.parquet"
        github_stage_path = os.path.join(STAGING_DIR, parquet_file)

        task_instance.log.info(f"Start writing cleaned data written to {github_stage_path}")

        df_github = pd.DataFrame(github_cleaned)



        required_cols = ["technology", "stars", "forks", "open_issues", "watchers", "last_updated"]
        missing = [col for col in required_cols if col not in df_github.columns]
        if missing:
            task_instance.log.error(f"GitHub data missing required cols: {missing}")
            raise

        df_github.to_parquet(github_stage_path, index=False)

        # --- Push to XCom ---
        task_instance.xcom_push(key="github_parquet_file", value=parquet_file)
        task_instance.log.info(f"GitHub cleaned data written to {github_stage_path}")

    except Exception as e:
        task_instance.log.error(f"Error in plugins/src/clean_and_validate/github.py: {e}", exc_info=True)
        raise
