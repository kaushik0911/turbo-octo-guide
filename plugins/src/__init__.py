import os

RAW_DIR = "/opt/airflow/data/raw"
CLEAN_DIR = "/opt/airflow/data/cleaned"
STAGING_DIR = "/opt/airflow/data/staging"

try:
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(CLEAN_DIR, exist_ok=True)
    os.makedirs(STAGING_DIR, exist_ok=True)
except Exception as e:
    print(f"Error creating directories: {e}")