from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from pathlib import Path
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.url import make_url
from utils import create_listings_table
from loader_factory import LoaderFactory

def extract_transform_listings():
    continent_path = Path(os.getenv("ORIGIN_DATA_PATH"))
    target_db_connection_string = os.getenv("DWH_CONN_STRING")
    target_schema = os.getenv("STG_SCHEMA")
    target_table = os.getenv("LISTINGS_STG_TABLE_NAME")

    stg_clean_listing_data_path = os.getenv("STG_CLEANSED_DATA_PATH")
    listing_file_path = os.path.join(stg_clean_listing_data_path, f"{target_table}.csv")
    if os.path.exists(listing_file_path):
        os.remove(listing_file_path)

    engine = create_engine(target_db_connection_string)

    inspector = inspect(engine)

    if target_table not in inspector.get_table_names(schema=target_schema):
        create_listings_table(target_db_connection_string, target_schema, target_table)


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
                    listing_path = cities_path.joinpath(city, "summary_listings.csv")

                    df = pd.read_csv(listing_path, compression=None, quotechar='"')

                    #Drop duplicates by id
                    df.drop_duplicates(subset=["id"], inplace=True)

                    #Lower categorical variables
                    df["room_type"] = df["room_type"].fillna("unknown").astype(str).str.lower()
                    df["license"] = df["license"].fillna("unknown").astype(str).str.lower()
                    df["neighbourhood_group"] = df["neighbourhood_group"].fillna("unknown").astype(str).str.lower()
                    
                    df["last_review"] = df["last_review"].astype(str)  # Convertir a string para evitar NaN como float
                    df["last_review"] = df["last_review"].replace("nan", None)  # Reemplazar "nan" string con None

                    #Add new gegrafic columns
                    df["city"] = city
                    df["province"] = province
                    df["country"] = country
                    df["continent"] = continent

                    #Add load date
                    df["etl_loaded_at"] = datetime.now()

                    #Drop private info of the host
                    df = df.drop(columns=["host_name"])

                    #Load to target
                    file_exists = os.path.exists(listing_file_path)
                    df.to_csv(listing_file_path, mode='a', index=False, header=not file_exists)


def load_data():
    target_db_connection_string = os.getenv("DWH_CONN_STRING")
    target_table = os.getenv("LISTINGS_STG_TABLE_NAME")
    csv_path = os.getenv("STG_CLEANSED_DATA_PATH")
    csv_path = os.path.join(csv_path, f"{target_table}.csv")
    target_schema = os.getenv("STG_SCHEMA")
    dwh_db_type = os.getenv("DWH_DB_TYPE")

    url = make_url(target_db_connection_string)

    data_loader = LoaderFactory.get_loader(dwh_db_type)

    df = pd.read_csv(csv_path, quotechar='"')

    data_loader.load_data(df, target_schema, target_table, url.database, url.username, url.password, url.host, url.port)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    "stg_etl_airbnb_listings",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    extract_transform_listings = PythonOperator(task_id="stg_listing_extract_transform", python_callable=extract_transform_listings)
    load_data_listings = PythonOperator(task_id="stg_listing_load", python_callable=load_data)

    extract_transform_listings >> load_data_listings