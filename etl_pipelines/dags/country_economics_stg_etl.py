from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from sqlalchemy.engine.url import make_url
from utils import create_stg_country_economics_table
from loader_factory import LoaderFactory
import psycopg2


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
}

with DAG(
    "stg_etl_country_economics",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    def create_country_economics_table(**context):
        target_db_connection_string = os.getenv("DWH_CONN_STRING")
        target_schema = os.getenv("STG_SCHEMA")
        target_table = os.getenv("COUNTRY_ECONOMICS_STG_TABLE_NAME")

        try:
            create_stg_country_economics_table(target_db_connection_string, target_schema, target_table)
        except Exception as e:
            print(f"Unable to create country economics table: {e}")

        print("Country Economics table succefully created")

    
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
        target_table = os.getenv("COUNTRY_ECONOMICS_STG_TABLE_NAME")
        
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
            "PIB_Banco_Mundial.csv"
        )

        economics_df = pd.read_csv(country_economics_data_path)

        # Map original column names to database-friendly names
        column_mapping = {
            "Country Name": "country_name",
            "Country Code": "country_code", 
            "Series Name": "series_name",
            "Series Code": "series_code",
            "2014 [YR2014]": "yr_2014",
            "2015 [YR2015]": "yr_2015",
            "2016 [YR2016]": "yr_2016",
            "2017 [YR2017]": "yr_2017",
            "2018 [YR2018]": "yr_2018",
            "2019 [YR2019]": "yr_2019",
            "2020 [YR2020]": "yr_2020",
            "2021 [YR2021]": "yr_2021",
            "2022 [YR2022]": "yr_2022",
            "2023 [YR2023]": "yr_2023"
        }
        
        # Rename columns to match database schema
        economics_df = economics_df.rename(columns=column_mapping)

        numeric_columns = [
        "yr_2014", "yr_2015", "yr_2016", "yr_2017", "yr_2018", 
        "yr_2019", "yr_2020", "yr_2021", "yr_2022", "yr_2023"
    ]
    
        # Replace ".." with NaN in numeric columns
        for col in numeric_columns:
            if col in economics_df.columns:
                economics_df[col] = economics_df[col].replace("..", pd.NA)
                # Convert to numeric, coercing any remaining non-numeric values to NaN
                economics_df[col] = pd.to_numeric(economics_df[col], errors='coerce')
        
        # Add ETL timestamp
        economics_df['etl_loaded_at'] = datetime.utcnow()
        
        print(f"DataFrame columns after transformation: {list(economics_df.columns)}")
        print(f"DataFrame shape: {economics_df.shape}")

        load_batch_to_dwh(economics_df, target_db_connection_string, target_schema, target_table)


    create_stg_country_economics_table_task = PythonOperator(
        task_id='create_stg_country_economics_table',
        python_callable=create_country_economics_table,
        dag=dag
    )

    transform_and_load_data_task = PythonOperator(
        task_id='transform_and_load_data_country_economics',
        python_callable=transform_and_load_data,
        dag=dag
    )

    create_stg_country_economics_table_task >> transform_and_load_data_task