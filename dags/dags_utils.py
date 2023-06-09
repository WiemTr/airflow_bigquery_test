from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from jinja2 import Environment, FileSystemLoader

from dags.constants import GOOGLE_CLOUD_PROJECT


def create_table_from_csv(
    filepath: Path, columns_types: dict, table_id: str, date_columns: list = None
) -> None:
    """
    create bigquery table from csv file

    :param filepath: the csv file path
    :param columns_types: the table columns types
    :param table_id: the bigquery table to create
    :param date_columns: the columns with date type, defaults to None
    """
    dataframe = pd.read_csv(filepath, dtype=columns_types, parse_dates=date_columns)
    load_dataframe_into_bq_table(
        dataframe,
        table_id=table_id,
    )


def load_dataframe_into_bq_table(dataframe: pd.DataFrame, table_id: str) -> None:
    """
    Load a dataframe into a biquery table
    :param dataframe: dataframe containing data to load
    :param table_id: id of the bigquery table where data will be loaded
    """
    client = bigquery.Client(project=GOOGLE_CLOUD_PROJECT)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
    job.result()


def query_renderer(
    template_search_path: Path, query_name: str, params: dict = {}
) -> str:
    """
    Fetch query and fill parameters.

    :param template_search_path: the path to the sql template file
    :param query_name: the sql template filename
    :param params: the parameters to use to fill the query template, defaults to {}
    :return: the query filled with parameters
    """

    env = Environment(loader=FileSystemLoader(template_search_path))
    template = env.get_template(name=query_name)
    return template.render(params)
