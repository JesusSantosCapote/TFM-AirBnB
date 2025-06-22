from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from sqlalchemy.engine.url import make_url
from utils import create_stg_surface_table
from loader_factory import LoaderFactory


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
}

with DAG(
    "stg_etl_surface",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    def create_surface_table(**context):
        target_db_connection_string = os.getenv("DWH_CONN_STRING")
        target_schema = os.getenv("STG_SCHEMA")
        target_table = os.getenv("CITY_SURFACE_TABLE_NAME")

        try:
            create_stg_surface_table(target_db_connection_string, target_schema, target_table)
        except Exception as e:
            print(f"Unable to create city surface table: {e}")

        print("City surface table succefully created")

    
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
        target_schema = os.getenv("STG_SCHEMA")
        target_table = os.getenv("CITY_SURFACE_TABLE_NAME")
        
        surface_data_path = os.path.join(
            os.getenv("EXTERNAL_DATA_PATH"),
            "superficie_ciudad_km2.csv"
        )

        surface_df = pd.read_csv(surface_data_path)

        # Clean the 'km' column - remove commas and convert to numeric
        if 'km' in surface_df.columns:
            surface_df['km'] = surface_df['km'].astype(str).str.replace(',', '').str.replace(' ', '')
            # Convert to numeric, handling any non-numeric values
            surface_df['km'] = pd.to_numeric(surface_df['km'], errors='coerce')

        surface_df["country"] = surface_df["country"].apply(lambda x: str(x).title())
        
        # Add ETL timestamp
        surface_df['etl_loaded_at'] = datetime.utcnow()
        
        print(f"DataFrame columns after transformation: {list(surface_df.columns)}")
        print(f"DataFrame shape: {surface_df.shape}")

        load_batch_to_dwh(surface_df, target_db_connection_string, target_schema, target_table)


    create_stg_surface_table_task = PythonOperator(
        task_id='create_stg_surface_table',
        python_callable=create_surface_table,
        dag=dag
    )

    transform_and_load_data_task = PythonOperator(
        task_id='transform_and_load_data_surface',
        python_callable=transform_and_load_data,
        dag=dag
    )

    create_stg_surface_table_task >> transform_and_load_data_task