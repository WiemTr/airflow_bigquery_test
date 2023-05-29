import functools

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

from dags.constants import DEFAULT_DAG_ARGS, GOOGLE_CLOUD_PROJECT, PROJECT_ROOT_DIR
from dags.dags_utils import create_table_from_csv, query_renderer

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
    create_customers_table_task = GCSToBigQueryOperator(
        task_id="create_customers_table_from_csv",
        bucket="sales-management-bucket",
        source_objects=["customers.csv"],
        destination_project_dataset_table="sales_management_dataset.customers",
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
        destination_project_dataset_table="sales_management_dataset.products",
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
            table_id=f"{GOOGLE_CLOUD_PROJECT}.sales_management_dataset.purchases",
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
                    },
                ),
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GOOGLE_CLOUD_PROJECT,
                    "datasetId": "sales_management_dataset",
                    "tableId": "sales_per_customer",
                },
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )
# pylint: disable=pointless-statement
(
    create_sales_management_dataset_task
    >> create_customers_table_task
    >> create_sales_per_customer_table_task
)
(
    create_sales_management_dataset_task
    >> create_products_table_task
    >> create_sales_per_customer_table_task
)
(
    create_sales_management_dataset_task
    >> create_purchases_table_task
    >> create_sales_per_customer_table_task
)
