from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from pathlib import Path
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.url import make_url
from utils import create_amentie_listing_table
from loader_factory import LoaderFactory
import json
import ast
import psycopg2
import re
import gc


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
}

with DAG(
    "dwh_etl_airbnb_amenities_listing",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    def create_dwh_amenitie_listing_table(**context):
        target_db_connection_string = os.getenv("DWH_CONN_STRING")
        target_schema = os.getenv("DWH_SCHEMA")

        try:
            create_amentie_listing_table(target_db_connection_string, target_schema)
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


    def get_listing_amenities(amenitie_list_exp, amenitie_regex_dict):
        listing_amenities = []

        for amenitie in ast.literal_eval(amenitie_list_exp):
            for amenitie_class, regex in amenitie_regex_dict.items():
                if re.search(regex, amenitie):
                    listing_amenities.append(amenitie_class)
                    break

        return listing_amenities
    

    def transform_and_load_data(**context):
        target_db_connection_string = os.getenv("DWH_CONN_STRING")
        target_schema = os.getenv("DWH_SCHEMA")
        target_table = os.getenv("AMENITIE_LISTING_TABLE_NAME")
        
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
        
        batch_size = os.getenv("BATCH_SIZE")

        extract_schema = os.getenv("STG_SCHEMA")
        extract_table = os.getenv("LISTINGS_STG_TABLE_NAME")

        count_query = f"SELECT COUNT(*) FROM {extract_schema}.{extract_table};"
        total_records = pd.read_sql_query(count_query, conn).iloc[0, 0]

        print(f"Total de registros a procesar: {total_records}")
        print(f"Tamaño de batch: {batch_size}")

        amenitie_query = f"SELECT amenitie_id, name FROM {target_schema}.{amenitie_table_name}"

        amenitie_df = pd.read_sql_query(amenitie_query, conn)
        amenitie_mapper = amenitie_df.set_index('name')['amenitie_id'].to_dict()

        amenities_regex_dict = {}
        amenities_regex_path = os.path.join(os.getenv("PROCESSED_DATA_PATH"), "amenities.json")
        with open(amenities_regex_path, 'r', encoding='utf-8') as f:
            amenities_regex_dict = json.load(f)

        # Procesar en batches
        processed_records = 0
        batch_number = 0

        while processed_records < 30000: #TODO:Cambia por total records
            batch_number += 1
            offset = processed_records
            
            print(f"Procesando batch {batch_number} - Registros {offset} a {offset + batch_size}")
            
            # Query con LIMIT y OFFSET para batch
            batch_query = f"""
                SELECT id, amenities FROM {extract_schema}.{extract_table} 
                ORDER BY id 
                LIMIT {batch_size} OFFSET {offset};
            """
            
            # Leer batch actual
            df_batch = pd.read_sql_query(batch_query, conn)
            
            if df_batch.empty:
                break
            
            listing_amenitie_list = []
            for _, row in df_batch[['id', 'amenities']].iterrows():
                row_amenities = get_listing_amenities(row['amenities'], amenities_regex_dict)
                for amenitie in row_amenities:
                    amenitie_id = amenitie_mapper[amenitie]
                    listing_amenitie_list.append({'amenitie_id': amenitie_id, 'listing_id': row['id']})

            listing_amenitie_df = pd.DataFrame(listing_amenitie_list)
            
            load_batch_to_dwh(listing_amenitie_df, target_db_connection_string, target_schema, target_table)

            processed_records += len(df_batch)
            print(f"Batch {batch_number} completado. Procesados: {processed_records}/{total_records}")
            
            # Liberar memoria
            del df_batch, listing_amenitie_df
            gc.collect()
        
        print(f"Proceso completado. Total de registros procesados: {processed_records}")


    create_dwh_amenitie_listing_table_task = PythonOperator(
        task_id='create_dwh_amenitie_listing_table',
        python_callable=create_dwh_amenitie_listing_table,
        dag=dag
    )

    transform_and_load_data_task = PythonOperator(
        task_id='transform_and_load_data',
        python_callable=transform_and_load_data,
        dag=dag
    )

    create_dwh_amenitie_listing_table_task >> transform_and_load_data_task