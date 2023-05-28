from datetime import datetime

DEFAULT_DAG_ARGS = {
    "owner": "Airflow",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "start_date": datetime(2022, 1, 1),
}
