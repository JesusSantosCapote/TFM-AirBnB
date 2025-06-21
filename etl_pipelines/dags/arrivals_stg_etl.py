from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from sqlalchemy.engine.url import make_url
from utils import create_stg_arrivals_table
from loader_factory import LoaderFactory
import psycopg2


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
}

with DAG(
    "stg_etl_arrivals",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    def create_arrivals_table(**context):
        target_db_connection_string = os.getenv("DWH_CONN_STRING")
        target_schema = os.getenv("STG_SCHEMA")
        target_table = os.getenv("ARRIVALS_STG_TABLE_NAME")

        try:
            create_stg_arrivals_table(target_db_connection_string, target_schema, target_table)
        except Exception as e:
            print(f"Unable to create arrivals table: {e}")

        print("Arrivals table succefully created")

    
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
        
        print(f"Batch succefully loaded: {len(df_batch)} records")


    def transform_and_load_data(**context):
        target_db_connection_string = os.getenv("DWH_CONN_STRING")
        target_schema = os.getenv("STG_SCHEMA")
        target_table = os.getenv("ARRIVALS_STG_TABLE_NAME")
        
        url = make_url(target_db_connection_string)
    
        # Crear conexión directa con psycopg2
        conn = psycopg2.connect(
            host=url.host,
            port=url.port,
            database=url.database,
            user=url.username,
            password=url.password
        )
        
        country_economics_data_path = os.path.join(
            os.getenv("EXTERNAL_DATA_PATH"),
            "international_arrivals.csv"
        )

        arrivals_df = pd.read_csv(country_economics_data_path)

        columns_to_drop = ["year", "source"]

        arrivals_df = arrivals_df.drop(columns=columns_to_drop)

        arrivals_df["country"] = arrivals_df["country"].apply(lambda x: str(x).title())

        arrivals_df["etl_loaded_at"] = datetime.now()

        load_batch_to_dwh(arrivals_df, target_db_connection_string, target_schema, target_table)


    create_arrivals_table_task = PythonOperator(
        task_id='create_arrivals_table',
        python_callable=create_arrivals_table,
        dag=dag
    )

    transform_and_load_data_task = PythonOperator(
        task_id='transform_and_load_data',
        python_callable=transform_and_load_data,
        dag=dag
    )

    create_arrivals_table_task >> transform_and_load_data_task