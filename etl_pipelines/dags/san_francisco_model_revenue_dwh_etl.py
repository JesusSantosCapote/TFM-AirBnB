from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from utils import create_san_francisco_model_table
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.url import make_url
from loader_factory import LoaderFactory
import psycopg2
import gc
from airflow.sensors.external_task import ExternalTaskSensor
import warnings

# Suprimir warnings de pandas
warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)
warnings.filterwarnings('ignore', category=FutureWarning, module='pandas')
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

# O más simple, suprimir todos los warnings de pandas
pd.options.mode.chained_assignment = None  # default='warn'


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
}

with DAG(
    "dwh_etl_san_francisco_model",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    def create_dwh_san_francisco_table(**context):
        target_db_connection_string = os.getenv("DWH_CONN_STRING")
        target_schema = os.getenv("DWH_SCHEMA")
        target_table = os.getenv("SAN_FRANCISCO_MODEL_TABLE_NAME")

        engine = create_engine(target_db_connection_string)

        inspector = inspect(engine)

        if target_table not in inspector.get_table_names(schema=target_schema):
            create_san_francisco_model_table(target_db_connection_string, target_schema, target_table)
            print("San Francisco model table succefully created")


    def calculate_san_francisco_model_batch(df_batch, df_reviews):
        dwh_connection_string = os.getenv("DWH_CONN_STRING")
        
        url = make_url(dwh_connection_string)
        
        conn = psycopg2.connect(
            host=url.host,
            port=url.port,
            database=url.database,
            user=url.username,
            password=url.password
        )
        
        df_batch['last_review'] = pd.to_datetime(df_batch['last_review'])
        
        MAX_OCCUPANCY_RATE = 0.70
        MAX_OCCUPIED_NIGHTS = 365 * MAX_OCCUPANCY_RATE
        
        print(f"Calculando métricas del modelo San Francisco con ocupación máxima del {MAX_OCCUPANCY_RATE*100}%...")
        
        # Calcular métricas por listing
        model_results = []
        
        for _, listing in df_batch.iterrows():
            listing_id = listing['id']

            listing_reviews = df_reviews[df_reviews["listing_id"] == listing_id]

            listing_reviews['date'] = pd.to_datetime(listing_reviews['date'])

            
            if len(listing_reviews) == 0:
                continue
                
            # Calcular métricas por año y mes
            listing_reviews['year'] = listing_reviews['date'].dt.year
            listing_reviews['month'] = listing_reviews['date'].dt.month
            listing_reviews['year_month'] = listing_reviews['date'].dt.to_period('M')
            
            # Agrupar por año-mes para calcular ocupación
            monthly_stats = listing_reviews.groupby('year_month').agg({
                'id': 'count'
            }).reset_index()
            
            monthly_stats.columns = ['year_month', 'reviews_count']
            monthly_stats['year'] = monthly_stats['year_month'].dt.year
            monthly_stats['month'] = monthly_stats['year_month'].dt.month
            
            # Calcular estadísticas anuales
            yearly_stats = monthly_stats.groupby('year').agg({
                'reviews_count': ['sum']
            }).reset_index()
            
            yearly_stats.columns = ['year', 'total_reviews']
            
            # Modelo San Francisco: 
            # - Asume que cada review = 1 estancia
            # - Factor de corrección típico: no todos los huéspedes dejan review (estimado 50-70%)
            # - Estancia promedio: usar minimum_nights o asumir 2-3 noches
            
            review_rate = 0.5
            avg_stay_nights = max(listing['minimum_nights'], 2) if pd.notna(listing['minimum_nights']) else 2.5
            
            for _, year_stat in yearly_stats.iterrows():
                year = int(year_stat['year'])
                
                # Calcular ocupación estimada SIN LÍMITES (para tracking)
                estimated_bookings_raw = year_stat['total_reviews'] / review_rate
                estimated_occupied_nights_raw = estimated_bookings_raw * avg_stay_nights
                
                # APLICAR LÍMITES DE OCUPACIÓN
                # Capar las noches ocupadas al máximo permitido
                estimated_occupied_nights_capped = min(estimated_occupied_nights_raw, MAX_OCCUPIED_NIGHTS)
                
                # Recalcular métricas con el límite aplicado
                occupancy_rate_capped = estimated_occupied_nights_capped / 365
                
                # Indicador si se aplicó el límite
                is_capped = estimated_occupied_nights_raw > MAX_OCCUPIED_NIGHTS
                
                # Calcular revenue con valores capados
                estimated_annual_revenue = estimated_occupied_nights_capped * listing['price_dollar']
                
                model_result = {
                    'listing_id': listing_id,
                    'year': year,
                    'calculation_date': datetime.now(),
                    
                    # Métricas de reviews
                    'total_reviews_year': int(year_stat['total_reviews']),
                    
                    # Modelo San Francisco - Ocupación (VALORES CAPADOS)
                    'review_rate_assumed': review_rate,
                    'avg_stay_nights_assumed': avg_stay_nights,
                    'estimated_bookings': round(estimated_bookings_raw, 1),
                    'estimated_occupied_nights': round(estimated_occupied_nights_capped, 1),
                    'occupancy_rate': round(occupancy_rate_capped, 3),
                    'is_occupancy_capped': is_capped,
                    'max_occupancy_rate_applied': MAX_OCCUPANCY_RATE,
                    
                    # Modelo San Francisco - Revenue (con valores capados)
                    'estimated_annual_revenue': round(estimated_annual_revenue, 2),
                    'revenue_per_review': round(estimated_annual_revenue / year_stat['total_reviews'], 2),
                    
                    'etl_loaded_at': datetime.now()
                }
                
                model_results.append(model_result)
        
        # Crear DataFrame con resultados
        model_df = pd.DataFrame(model_results)
        
        # Estadísticas de capado
        if len(model_df) > 0:
            capped_count = model_df['is_occupancy_capped'].sum()
            total_count = len(model_df)
            print(f"Calculadas métricas para {total_count} registros listing-año")
            print(f"Aplicado límite de ocupación a {capped_count} registros ({capped_count/total_count*100:.1f}%)")
            print(f"Ocupación promedio: {model_df['occupancy_rate'].mean():.1%}")
            print(f"Ocupación máxima: {model_df['occupancy_rate'].max():.1%}")
        
        return model_df


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
        target_schema = os.getenv("DWH_SCHEMA")
        target_table = os.getenv("SAN_FRANCISCO_MODEL_TABLE_NAME")
        reviews_table = os.getenv("REVIEWS_DWH_TABLE_NAME")

        url = make_url(target_db_connection_string)
    
        # Crear conexión directa con psycopg2
        conn = psycopg2.connect(
            host=url.host,
            port=url.port,
            database=url.database,
            user=url.username,
            password=url.password
        )

        batch_size = int(os.getenv("BATCH_SIZE"))

        extract_schema = os.getenv("DWH_SCHEMA")
        extract_table = os.getenv("LISTINGS_DWH_TABLE_NAME")

        reviews_query = f"SELECT * FROM {extract_schema}.{reviews_table}"
        reviews_df = pd.read_sql_query(reviews_query, conn)

        count_query = f"SELECT COUNT(*) FROM {extract_schema}.{extract_table};"
        total_records = pd.read_sql_query(count_query, conn).iloc[0, 0]

        print(f"Total de registros a procesar: {total_records}")
        print(f"Tamaño de batch: {batch_size}")

        # Procesar en batches
        processed_records = 0
        batch_number = 0

        while processed_records < total_records:
            batch_number += 1
            offset = processed_records
            
            print(f"Procesando batch {batch_number} - Registros {offset} a {offset + batch_size}")
            
            # Query con LIMIT y OFFSET para batch
            batch_query = f"""
                SELECT * FROM {extract_schema}.{extract_table} 
                ORDER BY id 
                LIMIT {batch_size} OFFSET {offset};
            """
            
            # Leer batch actual
            df_batch = pd.read_sql_query(batch_query, conn)
            
            if df_batch.empty:
                break
            
            san_francisco_df = calculate_san_francisco_model_batch(df_batch, reviews_df)

            load_batch_to_dwh(san_francisco_df, target_db_connection_string, target_schema, target_table)

            processed_records += len(df_batch)
            print(f"Batch {batch_number} completado. Procesados: {processed_records}/{total_records}")
            
            # Liberar memoria
            del df_batch, san_francisco_df
            gc.collect()
        
        print(f"Proceso completado. Total de registros procesados: {processed_records}")

    
    def add_foreign_key_constraint():
        target_db_connection_string = os.getenv("DWH_CONN_STRING")
        target_schema = os.getenv("DWH_SCHEMA")
        target_table = os.getenv("SAN_FRANCISCO_MODEL_TABLE_NAME")
        referenced_table = os.getenv("LISTINGS_DWH_TABLE_NAME")

        url = make_url(target_db_connection_string)

        # Crear conexión directa con psycopg2
        conn = psycopg2.connect(
            host=url.host,
            port=url.port,
            database=url.database,
            user=url.username,
            password=url.password
        )

        try:
            cursor = conn.cursor()
            
            # Use raw SQL to add foreign key constraint
            constraint_name = f"fk_{target_table}_listing_id"
            sql = f"""
            ALTER TABLE {target_schema}.{target_table} 
            ADD CONSTRAINT {constraint_name} 
            FOREIGN KEY (listing_id) 
            REFERENCES {target_schema}.{referenced_table}(id)
            """
            
            cursor.execute(sql)
            conn.commit()
            print(f"Foreign key constraint {constraint_name} added successfully")
            
        except Exception as e:
            conn.rollback()
            print(f"Error adding foreign key constraint: {e}")
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

    create_san_francisco_table_task = PythonOperator(
        task_id='create_san_francisco_table',
        python_callable=create_dwh_san_francisco_table,
        dag=dag
    )

    transform_and_load_data_task = PythonOperator(
        task_id='transform_and_load_data',
        python_callable=transform_and_load_data,
        dag=dag
    )

    add_fk_task = PythonOperator(
        task_id='add_fk',
        python_callable=add_foreign_key_constraint,
        dag=dag
    )

    wait_for_dwh_listing_dag >> create_san_francisco_table_task >> transform_and_load_data_task >> add_fk_task