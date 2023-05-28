from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
)
from dags.constants import DEFAULT_DAG_ARGS


with DAG(
    dag_id="sales_management_dag",
    default_args=DEFAULT_DAG_ARGS,
    schedule_interval=None,
    catchup=False,
) as dag:
    create_sales_management_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id="create_sales_management_dataset",
        dataset_id="sales_management_dataset",
        location="eu",
    )
