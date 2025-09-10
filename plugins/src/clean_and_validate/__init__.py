import os

STAGING_DIR = "/opt/airflow/data/staging"

try:
    os.makedirs(STAGING_DIR, exist_ok=True)
except Exception as e:
    print(f"Error creating directories: {e}")