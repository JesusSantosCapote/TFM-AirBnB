from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from sqlalchemy.engine.url import make_url
from utils import create_stg_crime_index_table
from loader_factory import LoaderFactory


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
}

with DAG(
    "stg_etl_crime_index",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    def create_crime_index_table(**context):
        target_db_connection_string = os.getenv("DWH_CONN_STRING")
        target_schema = os.getenv("STG_SCHEMA")
        target_table = os.getenv("CRIME_STG_TABLE_NAME")

        try:
            create_stg_crime_index_table(target_db_connection_string, target_schema, target_table)
        except Exception as e:
            print(f"Unable to create crime index table: {e}")

        print("Crime index table succefully created")

    
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
        target_table = os.getenv("CRIME_STG_TABLE_NAME")
        
        crime_index_data_path = os.path.join(
            os.getenv("EXTERNAL_DATA_PATH"),
            "crime_index.csv"
        )

        crime_index_df = pd.read_csv(crime_index_data_path)

        # Map original column names to database-friendly names
        column_mapping = {
            "Numbeo_link": "numbeo_link",
        }
        
        # Rename columns to match database schema
        crime_index_df = crime_index_df.rename(columns=column_mapping)

        numeric_columns = [
        "crime_index", 
        "safety_index"
        ]
    
        for col in numeric_columns:
            if col in crime_index_df.columns:
                crime_index_df[col] = crime_index_df[col].replace("no data", pd.NA)
                # Convert to numeric, coercing any remaining non-numeric values to NaN
                crime_index_df[col] = pd.to_numeric(crime_index_df[col], errors='coerce')

        crime_index_df["country"] = crime_index_df["country"].apply(lambda x: str(x).title())
        
        # Add ETL timestamp
        crime_index_df['etl_loaded_at'] = datetime.utcnow()
        
        print(f"DataFrame columns after transformation: {list(crime_index_df.columns)}")
        print(f"DataFrame shape: {crime_index_df.shape}")

        load_batch_to_dwh(crime_index_df, target_db_connection_string, target_schema, target_table)


    create_stg_crime_index_table_task = PythonOperator(
        task_id='create_stg_crime_index_table',
        python_callable=create_crime_index_table,
        dag=dag
    )

    transform_and_load_data_task = PythonOperator(
        task_id='transform_and_load_data_crime_index',
        python_callable=transform_and_load_data,
        dag=dag
    )

    create_stg_crime_index_table_task >> transform_and_load_data_task
