import functools

from airflow import DAG
from airflow.operators.python_operator import (  # pylint: disable=import-error, no-name-in-module
    PythonOperator,
)
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

from dags.constants import (
    DEFAULT_DAG_ARGS,
    GOOGLE_CLOUD_PROJECT,
    PROJECT_ROOT_DIR,
    QUALITY_TESTS_DAG,
    SALES_MANAGEMENT_DATASET,
)
from dags.dags_utils import create_table_from_csv, query_renderer

with DAG(
    dag_id="sales_management_dag",
    default_args=DEFAULT_DAG_ARGS,
    schedule_interval=None,
    catchup=False,
) as dag:
    create_sales_management_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id="create_sales_management_dataset",
        dataset_id=SALES_MANAGEMENT_DATASET,
        location="eu",
    )
    create_customers_table_task = GCSToBigQueryOperator(
        task_id="create_customers_table_from_csv",
        bucket="sales-management-bucket",
        source_objects=["customers.csv"],
        destination_project_dataset_table=f"{SALES_MANAGEMENT_DATASET}.customers",
        schema_fields=[
            {"name": "customer_id", "type": "STRING"},
            {"name": "customer_name", "type": "STRING"},
            {"name": "customer_email", "type": "STRING"},
            {"name": "customer_phone", "type": "INTEGER"},
        ],
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=1,
    )

    create_products_table_task = GCSToBigQueryOperator(
        task_id="create_products_table_from_csv",
        bucket="sales-management-bucket",
        source_objects=["products.csv"],
        destination_project_dataset_table=f"{SALES_MANAGEMENT_DATASET}.products",
        schema_fields=[
            {"name": "product_id", "type": "STRING"},
            {"name": "product_name", "type": "STRING"},
            {"name": "price", "type": "FLOAT"},
            {"name": "brand_type", "type": "STRING"},
        ],
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=1,
    )

    create_purchases_table_task = PythonOperator(
        task_id="create_purchases_table_from_csv",
        python_callable=functools.partial(
            create_table_from_csv,
            filepath=PROJECT_ROOT_DIR / "data" / "purchases.csv",
            columns_types={
                "id": str,
                "customer_id": str,
                "product_id": str,
                "quantity": int,
                "date": str,
            },
            date_columns=["date"],
            table_id=f"{GOOGLE_CLOUD_PROJECT}.{SALES_MANAGEMENT_DATASET}.purchases",
        ),
    )

    create_sales_per_customer_table_task = BigQueryInsertJobOperator(
        task_id="create_sales_per_customer_table",
        configuration={
            "query": {
                "query": query_renderer(
                    template_search_path=PROJECT_ROOT_DIR / "queries",
                    query_name="generate_sales_per_customer.sql",
                    params={
                        "google_cloud_project_id": GOOGLE_CLOUD_PROJECT,
                        "brand_types_list": ("MDD", "MN"),
                        "project_dataset": SALES_MANAGEMENT_DATASET,
                    },
                ),
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GOOGLE_CLOUD_PROJECT,
                    "datasetId": SALES_MANAGEMENT_DATASET,
                    "tableId": "sales_per_customer",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    trigger_quality_tests_dag_task = TriggerDagRunOperator(
        task_id="trigger_quality_tests_dag",
        trigger_dag_id=QUALITY_TESTS_DAG,
        wait_for_completion=True,
    )

# pylint: disable=pointless-statement
(
    create_sales_management_dataset_task
    >> create_customers_table_task
    >> create_sales_per_customer_table_task
    >> trigger_quality_tests_dag_task
)
(
    create_sales_management_dataset_task
    >> create_products_table_task
    >> create_sales_per_customer_table_task
    >> trigger_quality_tests_dag_task
)
(
    create_sales_management_dataset_task
    >> create_purchases_table_task
    >> create_sales_per_customer_table_task
    >> trigger_quality_tests_dag_task
)
