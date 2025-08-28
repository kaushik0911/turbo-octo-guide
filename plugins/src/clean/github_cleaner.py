import os
import json
from datetime import datetime

from src import CLEAN_DIR

def github_clean_data(**context) -> None:
    task_instance = context['ti']
    github_raw_path = task_instance.xcom_pull(key="github_raw")

    with open(github_raw_path, "r") as f:
        github_data = json.load(f)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    github_cleaned_path = os.path.join(CLEAN_DIR, f"github_cleaned_{ts}.json")

    github_cleaned = []
    for repo in github_data:
        if not repo.get("technology"):
            continue

        github_cleaned.append({
            "technology": repo.get("technology", "").lower(),
            "stars": int(repo.get("stars", 0)),
            "forks": int(repo.get("forks", 0)),
            "watchers": int(repo.get("watchers", 0)),
            "open_issues": int(repo.get("open_issues", 0)),
            "last_updated": repo.get("last_updated"),
            "fetched_at": ts
        })

    with open(github_cleaned_path, "w") as f:
        json.dump(github_cleaned, f, indent=2)
        f.close()

    task_instance.xcom_push(key="github_cleaned", value=github_cleaned_path)
