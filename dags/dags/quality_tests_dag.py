from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (  # pylint: disable = no-name-in-module
    BigQueryColumnCheckOperator,
)

from dags.constants import DEFAULT_DAG_ARGS, QUALITY_TESTS_DAG, SALES_MANAGEMENT_DATASET

with DAG(
    dag_id=QUALITY_TESTS_DAG,
    default_args=DEFAULT_DAG_ARGS,
    schedule_interval=None,
    catchup=False,
) as dag:
    customers_table_check_task = BigQueryColumnCheckOperator(
        task_id="customers_table_check",
        table=f"{SALES_MANAGEMENT_DATASET}.customers",
        column_mapping={"customer_id": {"null_check": {"equal_to": 0}}},
        use_legacy_sql=False,
    )
    products_table_check_task = BigQueryColumnCheckOperator(
        task_id="products_table_check",
        table=f"{SALES_MANAGEMENT_DATASET}.products",
        column_mapping={
            "product_id": {"null_check": {"equal_to": 0}},
            "price": {
                "null_check": {"equal_to": 0},
                "min": {"greater_than": 0},
                "max": {"less_than": 100},
            },
        },
        use_legacy_sql=False,
    )
    purchases_table_check_task = BigQueryColumnCheckOperator(
        task_id="purchases_table_check",
        table=f"{SALES_MANAGEMENT_DATASET}.purchases",
        column_mapping={
            "id": {"null_check": {"equal_to": 0}},
            "product_id": {"null_check": {"equal_to": 0}},
            "customer_id": {"null_check": {"equal_to": 0}},
        },
        use_legacy_sql=False,
    )
    sales_per_customer_table_check_task = BigQueryColumnCheckOperator(
        task_id="sales_per_customer_table_check",
        table=f"{SALES_MANAGEMENT_DATASET}.sales_per_customer",
        column_mapping={
            "customer_id": {"null_check": {"equal_to": 0}},
        },
        use_legacy_sql=False,
    )
