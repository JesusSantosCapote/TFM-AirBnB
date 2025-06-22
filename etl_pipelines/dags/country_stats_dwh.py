from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import psycopg2
from sqlalchemy.engine.url import make_url
from airflow.sensors.external_task import ExternalTaskSensor


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
}

with DAG(
    'dwh_update_country_metrics',
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    def add_country_metrics_columns(connection_string, schema_name, country_table_name):
        url = make_url(connection_string)

        country_table_name = os.getenv("COUNTRY_TABLE_NAME")
        arrivals_table_name = os.getenv("ARRIVALS_STG_TABLE_NAME")
        extract_schema = os.getenv("STG_SCHEMA")
        
        # Crear conexión con psycopg2
        conn = psycopg2.connect(
            host=url.host,
            port=url.port,
            database=url.database,
            user=url.username,
            password=url.password
        )
        
        cursor = conn.cursor()
        
        try:
            # 1. Agregar las nuevas columnas
            new_columns = {
                "international_arrivals": "NUMERIC(12,2)"
            }
            
            for column_name, column_type in new_columns.items():
                try:
                    alter_query = f"ALTER TABLE {schema_name}.{country_table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
                    cursor.execute(alter_query)
                    conn.commit()
                    print(f"Columna '{column_name}' agregada exitosamente")
                except Exception as e:
                    print(f"Error agregando columna '{column_name}': {e}")
                    conn.rollback()
            
            extract_query = f"SELECT * FROM {extract_schema}.{arrivals_table_name}"
            arrivals_df = pd.read_sql_query(extract_query, conn)
            
            country_query = f"""
                SELECT c.country_id, c.country_name, c.continent_id, c.etl_loaded_at 
                FROM {schema_name}.{country_table_name} c
            """
            cursor.execute(country_query)
            city_rows = cursor.fetchall()
            
            country_data = []
            for row in city_rows:
                country_data.append({
                    'country_id': row[0],
                    'country_name': row[1],
                    'continent_id': row[2],
                    'etl_loaded_at': row[3]
                })
            
            print("Updating arrivals data...")
            arrivals_updates = 0
            
            for _, arrivals_row in arrivals_df.iterrows():
                arrivals_country = arrivals_row['country']
                
                for country_row in country_data:
                    db_country = country_row['country_name']
                    
                    if arrivals_country == db_country:
                        try:
                            arrivals_float = float(arrivals_row['international_arrivals'])
                            
                            update_query = f"""
                                UPDATE {schema_name}.{country_table_name} 
                                SET 
                                    international_arrivals = %s
                                WHERE country_id = %s
                            """
                            
                            cursor.execute(update_query, (
                                arrivals_float,
                                country_row['country_id']
                            ))
                            
                            arrivals_updates += 1
                            print(f"Arrival updated: {country_row['country_name']} -> {arrivals_float}")
                            break
                            
                        except (ValueError, TypeError) as e:
                            print(f"Error processing arrivals for {arrivals_row['country']}: {e}")
            
            conn.commit()
            print(f"Total updates of arrivals: {arrivals_updates}")
            
            stats_query = f"""
                SELECT 
                    COUNT(*) as total_countries,
                    COUNT(international_arrivals) as countries_with_arrivals
                FROM {schema_name}.{country_table_name}
            """
            
            cursor.execute(stats_query)
            stats = cursor.fetchone()
            
            print("\n=== Final Stats ===")
            print(f"Total countries: {stats[0]}")
            print(f"Countries with arrivals: {stats[1]}")
            
            print("✅ Country metrics updates finished")
            
        except Exception as e:
            print(f"❌ Error on updates: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()


    def update_country_metrics():
        connection_string = os.getenv('DWH_CONN_STRING')
        schema_name = os.getenv('DWH_SCHEMA')
        country_table_name = os.getenv('COUNTRY_TABLE_NAME')
        
        try:
            add_country_metrics_columns(connection_string, schema_name, country_table_name)
            print("✅ City metrics updated successfully")
        except Exception as e:
            print(f"❌ Error updating metrics: {e}")
            raise
    
    
    wait_for_stg_arrivals_dag = ExternalTaskSensor(
        task_id='wait_for_stg_arrivals',
        external_dag_id='stg_etl_arrivals',
        external_task_id=None,
        timeout=600,
        poke_interval=60,
        mode='poke'
    )

    wait_for_dwh_listing_dag = ExternalTaskSensor(
        task_id='wait_for_listings_dag',
        external_dag_id='dwh_etl_airbnb_listings',
        external_task_id=None,
        timeout=600,
        poke_interval=60,
        mode='poke'
    )

    update_country_metrics_task = PythonOperator(
        task_id='update_country_metrics',
        python_callable=update_country_metrics,
        dag=dag
    )

    wait_for_stg_arrivals_dag >> wait_for_dwh_listing_dag >> update_country_metrics_task