from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from sqlalchemy.engine.url import make_url
from utils import create_date_master_table
from loader_factory import LoaderFactory
import psycopg2
from airflow.sensors.external_task import ExternalTaskSensor


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
}

with DAG(
    "dwh_etl_date_master",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    def create_dwh_date_calendar_table(**context):
        target_db_connection_string = os.getenv("DWH_CONN_STRING")
        target_schema = os.getenv("DWH_SCHEMA")
        target_table = os.getenv("DATE_MASTER_TABLE")

        try:
            create_date_master_table(target_db_connection_string, target_schema, target_table)
        except Exception as e:
            print(f"Unable to create date calendar table: {e}")

        print("Date calendar table succefully created")

    
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

    
    def get_date_master_df(start_date, end_date):
        # Create date range
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # Create DataFrame with all fields in English
        df_master = pd.DataFrame({
            'date': dates,
            'date_str': dates.strftime('%Y-%m-%d'),
            'year': dates.year,
            'month': dates.month,
            'day': dates.day,
            'day_of_week': dates.dayofweek + 1,  # 1=Monday, 7=Sunday
            'day_name': dates.day_name(),
            'month_name': dates.month_name(),
            'quarter': dates.quarter,
            'week_of_year': dates.isocalendar().week,
            'day_of_year': dates.dayofyear,
            'is_weekend': (dates.dayofweek >= 5),  # Saturday=5, Sunday=6
            'is_month_start': dates.to_series().dt.is_month_start.values,
            'is_month_end': dates.to_series().dt.is_month_end.values,
            'is_leap_year': dates.to_series().dt.is_leap_year.values,
            'is_high_season': dates.month.isin([6, 7, 8, 12]),  # Summer and Christmas
            'is_holiday': False,  # Default False, can be updated later
            'etl_loaded_at': datetime.utcnow()
        })

        # Convert numpy dtypes to native Python types to avoid psycopg2 adaptation issues
        df_master = df_master.astype({
            'year': 'int32',
            'month': 'int32', 
            'day': 'int32',
            'day_of_week': 'int32',
            'quarter': 'int32',
            'week_of_year': 'int32',
            'day_of_year': 'int32',
            'is_weekend': 'bool',
            'is_month_start': 'bool',
            'is_month_end': 'bool',
            'is_leap_year': 'bool',
            'is_high_season': 'bool',
            'is_holiday': 'bool'
        })
        
        # Convert int32 columns to native Python int to ensure compatibility
        int_columns = ['year', 'month', 'day', 'day_of_week', 'quarter', 'week_of_year', 'day_of_year']
        for col in int_columns:
            df_master[col] = df_master[col].astype(int)
        
        return df_master

    def transform_and_load_data(**context):
        target_db_connection_string = os.getenv("DWH_CONN_STRING")
        target_schema = os.getenv("DWH_SCHEMA")
        target_table = os.getenv("DATE_MASTER_TABLE")
        extract_table = os.getenv("LISTINGS_DWH_TABLE_NAME")
        
        url = make_url(target_db_connection_string)

        # Create direct connection with psycopg2
        conn = psycopg2.connect(
            host=url.host,
            port=url.port,
            database=url.database,
            user=url.username,
            password=url.password
        )
        
        try:
            cursor = conn.cursor()
           
            query_complete_date_range = f"""
            SELECT 
                -- Minimum date among multiple columns
                LEAST(
                    COALESCE(MIN(first_review), '9999-12-31'::date),
                    COALESCE(MIN(host_since), '9999-12-31'::date),
                    COALESCE(MIN(last_review), '9999-12-31'::date)
                ) as global_minimum_date,
                
                -- Maximum date from specific columns
                GREATEST(
                    COALESCE(MAX(last_scraped), '1900-01-01'::date),
                    COALESCE(MAX(calendar_last_scraped), '1900-01-01'::date),
                    COALESCE(MAX(last_review), '1900-01-01'::date)
                ) as global_maximum_date,
                
                -- Individual dates for reference
                MIN(first_review) as min_first_review,
                MAX(last_review) as max_last_review,
                MIN(host_since) as min_host_since,
                MAX(last_scraped) as max_last_scraped,
                MAX(calendar_last_scraped) as max_calendar_scraped,
                
                -- Counts for validation
                COUNT(*) as total_records,
                COUNT(first_review) as records_with_first_review,
                COUNT(last_review) as records_with_last_review,
                COUNT(host_since) as records_with_host_since,
                COUNT(last_scraped) as records_with_last_scraped
                
            FROM {target_schema}.{extract_table};
            """
            
            cursor.execute(query_complete_date_range)
            complete_result = cursor.fetchone()
            
            if complete_result:
                global_min_date = complete_result[0]
                global_max_date = complete_result[1]
                
                print(f"=== DATE SUMMARY ===")
                print(f"Global minimum date: {global_min_date}")
                print(f"Global maximum date: {global_max_date}")
                print(f"Min first_review: {complete_result[2]}")
                print(f"Max last_review: {complete_result[3]}")
                print(f"Min host_since: {complete_result[4]}")
                print(f"Max last_scraped: {complete_result[5]}")
                print(f"Max calendar_scraped: {complete_result[6]}")
                print(f"Total records: {complete_result[7]}")
                print(f"Records with first_review: {complete_result[8]}")
                print(f"Records with last_review: {complete_result[9]}")
                print(f"Records with host_since: {complete_result[10]}")
                print(f"Records with last_scraped: {complete_result[11]}")
                
                # Add buffer days if needed
                from datetime import timedelta
                if global_min_date:
                    master_start_date = global_min_date - timedelta(days=365)  # 1 year before
                else:
                    master_start_date = datetime(2020, 1, 1).date()
                    
                if global_max_date:
                    master_end_date = global_max_date + timedelta(days=365)  # 1 year after
                else:
                    master_end_date = datetime(2025, 12, 31).date()
                
                print(f"Date master range: {master_start_date} to {master_end_date}")
                
                df = get_date_master_df(master_start_date, master_end_date)

                load_batch_to_dwh(
                    df,
                    target_db_connection_string,
                    target_schema,
                    target_table
                )

        except Exception as e:
            print(f"Error in transform_and_load_data: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()


    wait_for_dwh_listing_dag = ExternalTaskSensor(
        task_id='wait_for_listings_dag',
        external_dag_id='dwh_etl_airbnb_listings',
        external_task_id=None,
        timeout=600,
        poke_interval=60,
        mode='poke'
    )

    create_dwh_date_calendar_table_task = PythonOperator(
        task_id='create_dwh_date_calendar_table',
        python_callable=create_dwh_date_calendar_table,
        dag=dag
    )

    transform_and_load_data_task = PythonOperator(
        task_id='transform_and_load_data_amenitie',
        python_callable=transform_and_load_data,
        dag=dag
    )

    wait_for_dwh_listing_dag >> create_dwh_date_calendar_table_task >> transform_and_load_data_task