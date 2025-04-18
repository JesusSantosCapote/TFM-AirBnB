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
import numpy as np

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
    log = {}
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
                    listing_path = cities_path.joinpath(city, "listings.csv.gz")
                    df = pd.read_csv(listing_path, compression="gzip", quotechar='"')

                    # Drop duplicates by id
                    df.drop_duplicates(subset=["id"], inplace=True)

                    # Deleting $
                    df["price"] = df["price"].astype(str).str.replace(r"[$,]", "", regex=True).astype(float)
                    df.rename(columns={"price": "price_dollar"}, inplace=True)

                    # Transform to bool
                    df["instant_bookable"] = df["instant_bookable"].map({"t": 1, "f": 0})
                    df["host_is_superhost"] = df["host_is_superhost"].map({"t": 1, "f": 0})
                    df["has_availability"] = df["has_availability"].map({"t": 1, "f": 0})
                    df["host_has_profile_pic"] = df["host_has_profile_pic"].map({"t": 1, "f": 0})
                    df["host_identity_verified"] = df["host_identity_verified"].map({"t": 1, "f": 0})

                    # Deleting %
                    df["host_response_rate"] = (
                        df["host_response_rate"]
                        .astype(str)
                        .str.replace(r"%", "", regex=True)
                        .apply(lambda x: int(x) if x.isdigit() else None)
                    )

                    df["host_acceptance_rate"] = (
                        df["host_acceptance_rate"]
                        .astype(str)
                        .str.replace(r"%", "", regex=True)
                        .apply(lambda x: int(x) if x.isdigit() else None)
                    )
                    df.rename(columns={"host_response_rate": "host_response_rate_percentage",
                                       "host_acceptance_rate": "host_acceptance_rate_percentage"}, inplace=True)

                    # Lower categorical variables
                    try:
                        df["source"] = df["source"].astype(str).str.lower()
                    except:
                        print(country, province, city)

                    df["host_response_time"] = df["host_response_time"].astype(str).str.lower()
                    df["property_type"] = df["property_type"].astype(str).str.lower()
                    df["room_type"] = df["room_type"].astype(str).str.lower()
                    df["license"] = df["license"].astype(str).str.lower()

                    # Add new gegrafic columns
                    df["city"] = city
                    df["province"] = province
                    df["country"] = country
                    df["continent"] = continent

                    # Add load date
                    df["etl_loaded_at"] = datetime.now()

                    # Drop private info of the host
                    df = df.drop(
                        columns=["host_name", "host_location", "host_about", "host_thumbnail_url", "host_picture_url",
                                 "host_neighbourhood"])

                    df = df.drop(
                        columns=[
                            "minimum_minimum_nights",
                            "maximum_minimum_nights",
                            "minimum_maximum_nights",
                            "maximum_maximum_nights",
                            "minimum_nights_avg_ntm",
                            "maximum_nights_avg_ntm"]
                    )


                    log[(country, province, city)] = []
                    try:
                        df["availability_eoy"]
                        log[(country, province, city)].append("availability_eoy")
                    except:
                        pass

                    try:
                        df["number_of_reviews_ly"]
                        log[(country, province, city)].append("number_of_reviews_ly")
                    except:
                        pass

                    try:
                        df["estimated_occupancy_l365d"]
                        log[(country, province, city)].append("estimated_occupancy_l365d")
                    except:
                        pass

                    try:
                        df["estimated_revenue_l365d"]
                        log[(country, province, city)].append("estimated_revenue_l365d")
                    except:
                        pass

                    try:
                        df = df.drop( columns =["availability_eoy"])
                        df = df.drop( columns =["number_of_reviews_ly"])
                        df = df.drop( columns =["estimated_occupancy_l365d"])
                        df = df.drop( columns =["estimated_revenue_l365d"])
                    except:
                        pass

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

    print([i[0] for i in log.items() if len(i[1]) == 4])


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
}

with DAG(
    "stg_etl_airbnb_listings",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    extract_transform_listings = PythonOperator(task_id="stg_listing_extract_transform", python_callable=extract_transform_listings)
    # load_data_listings = PythonOperator(task_id="stg_listing_load", python_callable=load_data)

    extract_transform_listings