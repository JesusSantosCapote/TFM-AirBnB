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

        # Read CSV and handle trailing comma issue
        arrivals_df = pd.read_csv(country_economics_data_path)
        
        # Debug: Print column names to understand the structure
        print(f"Available columns: {list(arrivals_df.columns)}")
        print(f"DataFrame shape: {arrivals_df.shape}")
        
        # Clean up column names and remove unnamed columns
        arrivals_df.columns = arrivals_df.columns.str.strip()  # Remove whitespace
        
        # Remove columns that are unnamed or contain only NaN values
        columns_to_remove = []
        for col in arrivals_df.columns:
            if col.startswith('Unnamed:') or arrivals_df[col].isna().all():
                columns_to_remove.append(col)
        
        if columns_to_remove:
            print(f"Removing columns: {columns_to_remove}")
            arrivals_df = arrivals_df.drop(columns=columns_to_remove)
        
        # Remove rows that are completely empty
        arrivals_df = arrivals_df.dropna(how='all')
        
        # Define columns to drop (if they exist)
        columns_to_drop = ["year", "source"]
        existing_columns_to_drop = [col for col in columns_to_drop if col in arrivals_df.columns]
        
        if existing_columns_to_drop:
            print(f"Dropping columns: {existing_columns_to_drop}")
            arrivals_df = arrivals_df.drop(columns=existing_columns_to_drop)

        # Transform country column
        if "country" in arrivals_df.columns:
            arrivals_df["country"] = arrivals_df["country"].apply(lambda x: str(x).title())
        else:
            print(f"Warning: 'country' column not found. Available columns: {list(arrivals_df.columns)}")
            raise ValueError("Country column not found in the dataset")

        # Add ETL timestamp
        arrivals_df["etl_loaded_at"] = datetime.now()
        
        # Clean column names to be SQL-safe (remove special characters, spaces, etc.)
        arrivals_df.columns = [col.replace(' ', '_').replace(':', '_').replace('-', '_').lower() 
                              for col in arrivals_df.columns]
        
        print(f"Final columns: {list(arrivals_df.columns)}")
        print(f"Final DataFrame shape: {arrivals_df.shape}")

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