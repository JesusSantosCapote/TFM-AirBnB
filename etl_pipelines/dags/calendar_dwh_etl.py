from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from pathlib import Path
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.url import make_url
from utils import create_listing_calendar_table
from loader_factory import LoaderFactory


def stg_calendar_to_dwh_listing_calendar():
    target_db_connection_string = os.getenv("DWH_CONN_STRING")
    target_schema = os.getenv("DWH_SCHEMA")
    target_table = os.getenv("CALENDAR_DWH_TABLE_NAME")

    engine = create_engine(target_db_connection_string)

    inspector = inspect(engine)

    if target_table not in inspector.get_table_names(schema=target_schema):
        create_listing_calendar_table(target_db_connection_string, target_schema, target_table)

    query = f"SELECT * FROM {target_schema}.{target_table};"

    df = pd.read_sql(query, engine)

    df = df.drop(columns=['adjusted_price_dollar'])

    df = df.dropna(subset=['minimum_nights', 'maximum_nights'])


    #Load to target
    dwh_db_type = os.getenv("DWH_DB_TYPE")
    data_loader = LoaderFactory.get_loader(dwh_db_type)
    url = make_url(target_db_connection_string)
    data_loader.load_data(
        df,
        target_schema,
        target_table,
        url.database,
        url.username,
        url.password,
        url.host,
        url.port
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
}

with DAG(
    "dwh_etl_airbnb_reviews",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    extract_transform_stg_reviews_to_dwh_reviews = PythonOperator(task_id="extract_transform_stg_reviews_to_dwh_reviews", python_callable=stg_reviews_to_dwh_reviews)

    extract_transform_stg_reviews_to_dwh_reviews