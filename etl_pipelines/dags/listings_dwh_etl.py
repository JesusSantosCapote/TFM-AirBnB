from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.url import make_url
from utils import create_listing_dwh_table
from loader_factory import LoaderFactory
import re

def extract_transform_load_listings():
    target_db_connection_string = os.getenv("DWH_CONN_STRING")
    target_schema = os.getenv("DWH_SCHEMA")
    target_table = os.getenv("LISTINGS_DWH_TABLE_NAME")

    engine = create_engine(target_db_connection_string)

    inspector = inspect(engine)

    if target_table not in inspector.get_table_names(schema=target_schema):
        create_listing_dwh_table(target_db_connection_string, target_schema, target_table)

    
    query = f"SELECT * FROM {target_schema}.{target_table};"

    df = pd.read_sql(query, engine)

    df = df.drop(columns=["calculated_host_listings_count", "calculated_host_listings_count_entire_homes", "calculated_host_listings_count_private_rooms", "calculated_host_listings_count_shared_rooms", "description", "neighborhood_overview", "picture_url", "host_url", "host_response_time", "host_response_rate_percentage", "host_acceptance_rate_percentage", "host_is_superhost", "host_listings_count", "host_total_listings_count", "host_verifications", "host_has_profile_pic", "host_identity_verified", "neighbourhood", "neighbourhood_group_cleansed", "calendar_updated"])

    # Impute baths
    for i in range(df.shape[0]):
        if pd.isna(df.loc[i, "bathrooms"]):
            if not pd.isna(df.loc[i, "bathrooms_text"]):
                half_bath_regex = r"\b(half[-\s]?bath(room)?|medio\s+bañ[oe])\b"
                text = df.loc[i, "bathrooms_text"].lower()

                if re.search(half_bath_regex, text):
                    df.loc[i, "bathrooms"] = 0.5
                    continue

                text_list = text.split(" ")

                if text_list[0].isdigit():
                    df.loc[i, "bathrooms"] = float(text_list[0])
                    continue

            df.loc[i, "bathrooms"] = 1

    df["bathrooms_text"] = df["bathrooms_text"].fillna("unknown")

    # Impute beds and bedrooms
    group_stats = df.groupby('accommodates').agg({
        'beds': 'median',
        'bedrooms': 'median'
    })

    def impute_by_group(row, col):
        if pd.isna(row[col]):
            return group_stats.loc[row['accommodates'], col]
        return row[col]

    df['beds'] = df.apply(lambda r: impute_by_group(r, 'beds'), axis=1)
    df['bedrooms'] = df.apply(lambda r: impute_by_group(r, 'bedrooms'), axis=1)

    med_beds = df['beds'].median()
    med_bedrooms = df['bedrooms'].median()
    df['beds'].fillna(med_beds, inplace=True)
    df['bedrooms'].fillna(med_bedrooms, inplace=True)

    df['has_availability'] = df['has_availability'].fillna(
        df['availability_365'].gt(0).astype(int)
    )

    df = df.dropna()

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
    "dwh_etl_airbnb_listings",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    extract_transform_load = PythonOperator(task_id="dwh_listing_extract_transform", python_callable=extract_transform_load_listings)

    extract_transform_load