import os

RAW_DIR = "/opt/airflow/data/raw"

try:
    os.makedirs(RAW_DIR, exist_ok=True)
except Exception as e:
    print(f"Error creating directories: {e}")