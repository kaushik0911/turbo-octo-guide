import os
import json
import requests
from datetime import datetime
from airflow.models import Variable
from src.collect import RAW_DIR

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")


def fetch_github_metrics(**context) -> None:
    results = []
    task_instance = context['ti']
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}

    repos = Variable.get("github_repos", default_var="{}", deserialize_json=True)

    for tech, repo in repos.items():
        url = f"https://api.github.com/repos/{repo}"

        try:
            r = requests.get(url, headers=headers)
            data = r.json()
            results.append({
                "technology": tech,
                "stars": data.get("stargazers_count"),
                "forks": data.get("forks_count"),
                "open_issues": data.get("open_issues_count"),
                "watchers": data.get("subscribers_count"),
                "last_updated": data.get("updated_at")
            })
        except Exception as e:
            task_instance.log.error(f"Error fetching data for {repo}: {e}")
            results.append({
                "technology": tech,
                "stars": 0,
                "forks": 0,
                "open_issues": 0,
                "watchers": 0,
                "last_updated": None
            })

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    github_path = os.path.join(RAW_DIR, f"github_{ts}.json")

    with open(github_path, "w") as f:
        json.dump(results, f, indent=2)
        f.close()

    task_instance.xcom_push(key="github_raw", value=github_path)
