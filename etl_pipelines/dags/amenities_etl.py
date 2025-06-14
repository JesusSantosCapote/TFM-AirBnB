from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from sqlalchemy.engine.url import make_url
from utils import create_amenities_table
from loader_factory import LoaderFactory
import json
import psycopg2


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
}

with DAG(
    "dwh_etl_airbnb_amenities",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    def create_dwh_amenitie_table(**context):
        target_db_connection_string = os.getenv("DWH_CONN_STRING")
        target_schema = os.getenv("DWH_SCHEMA")

        try:
            create_amenities_table(target_db_connection_string, target_schema)
        except Exception as e:
            print(f"Unable to create amenitie table: {e}")

        print("Amenitie table succefully created")

    
    def load_batch_to_dwh(df_batch, connection_string, schema, table):
        dwh_db_type = os.getenv("DWH_DB_TYPE")
        data_loader = LoaderFactory.get_loader(dwh_db_type)
        url = make_url(connection_string)
        
        data_loader.load_data(
            df_batch,
            schema,
            table,
            url.database,
            url.username,
            url.password,
            url.host,
            url.port
        )
        
        print(f"Batch cargado exitosamente: {len(df_batch)} registros")


    def transform_and_load_data(**context):
        target_db_connection_string = os.getenv("DWH_CONN_STRING")
        target_schema = os.getenv("DWH_SCHEMA")
        target_table = os.getenv("AMENITIE_TABLE_NAME")
        
        amenitie_table_name = os.getenv("AMENITIE_TABLE_NAME")

        url = make_url(target_db_connection_string)
    
        # Crear conexión directa con psycopg2
        conn = psycopg2.connect(
            host=url.host,
            port=url.port,
            database=url.database,
            user=url.username,
            password=url.password
        )
        
        amenities_regex_dict = {}
        amenities_regex_path = os.path.join(os.getenv("PROCESSED_DATA_PATH"), "amenities.json")
        with open(amenities_regex_path, 'r', encoding='utf-8') as f:
            amenities_regex_dict = json.load(f)

        amenities_list = []    
        for amenitie_name, _ in amenities_regex_dict.items():
            amenities_list.append({'name': amenitie_name})
        
        amenitie_df = pd.DataFrame(amenities_list)

        load_batch_to_dwh(amenitie_df, target_db_connection_string, target_schema, target_table)


    create_dwh_amenitie_listing_table_task = PythonOperator(
        task_id='create_dwh_amenitie_listing_table',
        python_callable=create_dwh_amenitie_table,
        dag=dag
    )

    transform_and_load_data_task = PythonOperator(
        task_id='transform_and_load_data_amenitie',
        python_callable=transform_and_load_data,
        dag=dag
    )

    create_dwh_amenitie_listing_table_task >> transform_and_load_data_task