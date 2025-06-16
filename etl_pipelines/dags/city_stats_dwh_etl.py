from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
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
    'dwh_update_city_metrics',
    default_args=default_args,
    schedule_interval=None,  # Ejecutar manualmente
    catchup=False,
    tags=['dwh', 'city', 'metrics']
) as dag:
    
    def add_city_metrics_columns(connection_string, schema_name, city_table_name):
        url = make_url(connection_string)

        country_table_name = os.getenv("COUNTRY_TABLE_NAME")
        province_table_name = os.getenv("PROVINCE_TABLE_NAME")
        
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
                'area_km2': 'NUMERIC(12,2)',
                'crime_index': 'NUMERIC(5,2)',
                'safety_index': 'NUMERIC(5,2)', 
                'numbeo_crime_level': 'VARCHAR(50)',
            }
            
            for column_name, column_type in new_columns.items():
                try:
                    alter_query = f"ALTER TABLE {schema_name}.{city_table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
                    cursor.execute(alter_query)
                    conn.commit()
                    print(f"Columna '{column_name}' agregada exitosamente")
                except Exception as e:
                    print(f"Error agregando columna '{column_name}': {e}")
                    conn.rollback()
            
            base_path = os.getenv("EXTERNAL_DATA_PATH")
            
            superficie_path = os.path.join(base_path, 'superficie_ciudad_km2_clean.csv')
            superficie_df = pd.read_csv(superficie_path)
            
            crime_path = os.path.join(base_path, 'crime_index_clean.csv')
            crime_df = pd.read_csv(crime_path)
            
            city_query = f"""
                SELECT c.city_id, c.city_name, co.country_name 
                FROM {schema_name}.{city_table_name} c
                LEFT JOIN {schema_name}.{province_table_name} p ON c.province_id = p.province_id
                LEFT JOIN {schema_name}.{country_table_name} co ON p.country_id = co.country_id
            """
            cursor.execute(city_query)
            city_rows = cursor.fetchall()
            
            # Convertir a lista de diccionarios para facilitar el manejo
            city_data = []
            for row in city_rows:
                city_data.append({
                    'city_id': row[0],
                    'city_name': row[1],
                    'country_name': row[2]
                })
            
            print("Actualizando datos de superficie...")
            superficie_updates = 0
            
            for _, superficie_row in superficie_df.iterrows():
                # Normalizar nombres para matching
                csv_city = superficie_row['city']
                csv_country = superficie_row['country']
                
                # Buscar coincidencias en la tabla city
                for city_row in city_data:
                    db_city = city_row['city_name']
                    db_country = city_row['country_name'].lower()
                    
                    # Matching por ciudad y país
                    if csv_city == db_city and csv_country == db_country:
                        try:
                            # Limpiar el valor de km (remover comas y convertir)
                            km_value = str(superficie_row['km']).replace(',', '').replace('"', '')
                            km_float = float(km_value)
                            
                            update_query = f"""
                                UPDATE {schema_name}.{city_table_name} 
                                SET 
                                    area_km2 = %s
                                WHERE city_id = %s
                            """
                            
                            cursor.execute(update_query, (
                                km_float,
                                city_row['city_id']
                            ))
                            
                            superficie_updates += 1
                            print(f"Superficie actualizada: {city_row['city_name']} -> {km_float} km²")
                            break
                            
                        except (ValueError, TypeError) as e:
                            print(f"Error procesando superficie para {superficie_row['city']}: {e}")
            
            conn.commit()
            print(f"Total actualizaciones de superficie: {superficie_updates}")
            
            # 6. Actualizar datos de criminalidad
            print("Actualizando datos de criminalidad...")
            crime_updates = 0
            
            for _, crime_row in crime_df.iterrows():
                # Normalizar nombres para matching
                csv_city = crime_row['city']
                csv_country = crime_row['country']
                
                # Buscar coincidencias en la tabla city
                for city_row in city_data:
                    db_city = city_row['city_name']
                    db_country = city_row['country_name'].lower()
                    
                    # Matching por ciudad y país
                    if csv_city == db_city and csv_country == db_country:
                        try:
                            # Procesar valores de criminalidad
                            crime_index = None
                            safety_index = None
                            
                            if pd.notna(crime_row['crime_index']) and str(crime_row['crime_index']).lower() != 'no data':
                                try:
                                    crime_index = float(crime_row['crime_index'])
                                except (ValueError, TypeError):
                                    crime_index = None
                            
                            if pd.notna(crime_row['safety_index']) and str(crime_row['safety_index']) != '�':
                                try:
                                    safety_index = float(crime_row['safety_index'])
                                except (ValueError, TypeError):
                                    safety_index = None
                            
                            crime_level = None
                            if pd.notna(crime_row['numbeo_crime_level']) and str(crime_row['numbeo_crime_level']) not in ['�', ' ', '']:
                                crime_level = str(crime_row['numbeo_crime_level'])
                            
                            update_query = f"""
                                UPDATE {schema_name}.{city_table_name} 
                                SET 
                                    crime_index = %s,
                                    safety_index = %s,
                                    numbeo_crime_level = %s
                                WHERE city_id = %s
                            """
                            
                            cursor.execute(update_query, (
                                crime_index,
                                safety_index,
                                crime_level,
                                city_row['city_id']
                            ))
                            
                            crime_updates += 1
                            print(f"Criminalidad actualizada: {city_row['city_name']} -> Crime: {crime_index}, Safety: {safety_index}")
                            break
                            
                        except Exception as e:
                            print(f"Error procesando criminalidad para {crime_row['city']}: {e}")
            
            conn.commit()
            print(f"Total actualizaciones de criminalidad: {crime_updates}")
            
            # 7. Mostrar estadísticas finales
            stats_query = f"""
                SELECT 
                    COUNT(*) as total_cities,
                    COUNT(area_km2) as cities_with_area,
                    COUNT(crime_index) as cities_with_crime_data,
                    COUNT(safety_index) as cities_with_safety_data
                FROM {schema_name}.{city_table_name}
            """
            
            cursor.execute(stats_query)
            stats = cursor.fetchone()
            
            print("\n=== Estadísticas finales ===")
            print(f"Total ciudades: {stats[0]}")
            print(f"Ciudades con área: {stats[1]}")
            print(f"Ciudades con datos de criminalidad: {stats[2]}")
            print(f"Ciudades con datos de seguridad: {stats[3]}")
            
            print("✅ Actualización de métricas de ciudad completada")
            
        except Exception as e:
            print(f"❌ Error durante la actualización: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()


    def update_city_metrics():
        connection_string = os.getenv('DWH_CONN_STRING')
        schema_name = os.getenv('DWH_SCHEMA')
        city_table_name = os.getenv('CITY_TABLE_NAME')
        
        try:
            add_city_metrics_columns(connection_string, schema_name, city_table_name)
            print("✅ Métricas de ciudad actualizadas exitosamente")
        except Exception as e:
            print(f"❌ Error actualizando métricas: {e}")
            raise
    

    wait_for_dwh_listing_dag = ExternalTaskSensor(
        task_id='wait_for_listings_dag',
        external_dag_id='dwh_etl_airbnb_listings',
        external_task_id=None,
        timeout=600,
        poke_interval=60,
        mode='poke'
    )

    update_city_metrics_task = PythonOperator(
        task_id='update_city_metrics',
        python_callable=update_city_metrics,
        dag=dag
    )

    wait_for_dwh_listing_dag >> update_city_metrics_task