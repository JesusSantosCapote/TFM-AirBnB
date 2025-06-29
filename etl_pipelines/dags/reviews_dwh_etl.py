from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.url import make_url
from utils import create_reviews_table
from loader_factory import LoaderFactory
import psycopg2
import gc
from airflow.sensors.external_task import ExternalTaskSensor


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
    
    def create_dwh_reviews_table(**context):
        target_db_connection_string = os.getenv("DWH_CONN_STRING")
        target_schema = os.getenv("DWH_SCHEMA")
        target_table = os.getenv("REVIEWS_DWH_TABLE_NAME")

        engine = create_engine(target_db_connection_string)

        inspector = inspect(engine)

        if target_table not in inspector.get_table_names(schema=target_schema):
            create_reviews_table(target_db_connection_string, target_schema, target_table)

    
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


    def transform_and_load(**context):
        target_db_connection_string = os.getenv("DWH_CONN_STRING")
        target_schema = os.getenv("DWH_SCHEMA")
        target_table = os.getenv("REVIEWS_DWH_TABLE_NAME")

        url = make_url(target_db_connection_string)

        # Crear conexión directa con psycopg2
        conn = psycopg2.connect(
            host=url.host,
            port=url.port,
            database=url.database,
            user=url.username,
            password=url.password
        )

        extract_schema = os.getenv("STG_SCHEMA")
        extract_table = os.getenv("REVIEWS_DWH_TABLE_NAME")

        batch_size = 1000000

        count_query = f"SELECT COUNT(*) FROM {extract_schema}.{target_table};"
        total_records = pd.read_sql_query(count_query, conn).iloc[0, 0]

        print(f"Total de registros a procesar: {total_records}")
        print(f"Tamaño de batch: {batch_size}")

        # Procesar en batches
        processed_records = 0
        batch_number = 0

        while processed_records < total_records: #TODO:Cambia por total records
            batch_number += 1
            offset = processed_records
            
            print(f"Procesando batch {batch_number} - Registros {offset} a {offset + batch_size}")
            
            # Query con LIMIT y OFFSET para batch
            batch_query = f"""
                SELECT * FROM {extract_schema}.{extract_table} 
                LIMIT {batch_size} OFFSET {offset};
            """
            
            # Leer batch actual
            df_batch = pd.read_sql_query(batch_query, conn)
            
            if df_batch.empty:
                break

            df_batch = df_batch.dropna(subset=['listing_id', 'date'])

            df_batch['comments'] = df_batch['comments'].fillna('unknown')

            load_batch_to_dwh(df_batch, target_db_connection_string, target_schema, target_table)

            processed_records += len(df_batch)
            print(f"Batch {batch_number} completado. Procesados: {processed_records}/{total_records}")
            
            # Liberar memoria
            del df_batch
            gc.collect()
        
        print(f"Proceso completado. Total de registros procesados: {processed_records}")


    wait_for_stg_reviews_dag = ExternalTaskSensor(
        task_id='wait_for_stg_reviews_dag',
        external_dag_id='stg_etl_airbnb_reviews',
        external_task_id=None,
        timeout=600,
        poke_interval=60,
        mode='poke'
    )

    create_dwh_review_table_task = PythonOperator(
        task_id='create_dwh_review_table',
        python_callable=create_dwh_reviews_table,
        dag=dag
    )

    transform_and_load_data_task = PythonOperator(
        task_id='transform_and_load_data',
        python_callable=transform_and_load,
        dag=dag
    )