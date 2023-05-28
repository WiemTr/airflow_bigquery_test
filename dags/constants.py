import os
from datetime import datetime
from pathlib import Path

# Project
PROJECT_ROOT_DIR = Path(os.path.abspath(__file__)).parents[0]
GOOGLE_CLOUD_PROJECT="oceanic-hangout-388114"


# Airflow
DEFAULT_DAG_ARGS = {
    "owner": "Airflow",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "start_date": datetime(2022, 1, 1),
}
