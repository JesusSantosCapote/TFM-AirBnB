from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from pathlib import Path
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.url import make_url
from utils import create_calendar_table
from loader_factory import LoaderFactory


def extract_transform_calendar():
    continent_path = Path(os.getenv("ORIGIN_DATA_PATH"))
    target_db_connection_string = os.getenv("DWH_CONN_STRING")
    target_schema = os.getenv("STG_SCHEMA")
    target_table = os.getenv("CALENDAR_STG_TABLE_NAME")

    engine = create_engine(target_db_connection_string)

    inspector = inspect(engine)

    if target_table not in inspector.get_table_names(schema=target_schema):
        create_calendar_table(target_db_connection_string, target_schema, target_table)

    for continent in [d.name for d in continent_path.iterdir() if d.is_dir()]:
        countries_path = continent_path.joinpath(continent)
        countries = [d.name for d in countries_path.iterdir() if d.is_dir()]

        for country in countries:
            province_path = countries_path.joinpath(country)
            provinces = [d.name for d in province_path.iterdir() if d.is_dir()]

            for province in provinces:
                cities_path = province_path.joinpath(province)
                cities = [d.name for d in cities_path.iterdir() if d.is_dir()]

                for city in cities:
                    calendar_path = cities_path.joinpath(city, "calendar.csv.gz")

                    df = pd.read_csv(calendar_path, compression='gzip', quotechar='"')
                    
                    df["date"] = df["date"].astype(str)

                    df["available"] = df["available"].map({"t": True, "f": False})

                    df["price"] = df["price"].astype(str).str.replace(r"[$,]", "", regex=True).astype(float)
                    df = df.rename(columns={"price": "price_dollar"})

                    df["adjusted_price"] = df["adjusted_price"].astype(str).str.replace(r"[$,]", "", regex=True).astype(float)
                    df = df.rename(columns={"adjusted_price": "adjusted_price_dollar"})

                    #Load to target
                    dwh_db_type = os.getenv("DWH_DB_TYPE")
                    data_loader = LoaderFactory.get_loader(dwh_db_type)
                    url = make_url(target_db_connection_string)
                    data_loader.load_data(
                        df,  # Pass the current chunk
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
    "stg_etl_airbnb_calendar",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    extract_transform_calendar = PythonOperator(task_id="stg_calendar_extract_transform", python_callable=extract_transform_calendar)
    # load_data_calendar = PythonOperator(task_id="stg_calendar_load", python_callable=load_data)

    extract_transform_calendar