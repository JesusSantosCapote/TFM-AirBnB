from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.url import make_url
from utils import create_listing_dwh_table, create_geography_tables
from loader_factory import LoaderFactory
import re
import psycopg2
import gc


def extract_geography_hierarchy(df_listings):
    geography_df = df_listings[['city', 'province', 'country', 'continent']].drop_duplicates()
    
    # Limpiar valores nulos
    geography_df = geography_df.fillna('Unknown')
    
    return geography_df


def load_continents(geography_df):
    target_db_connection_string = os.getenv("DWH_CONN_STRING")
    target_schema = os.getenv("DWH_SCHEMA")
    target_table = os.getenv("CONTINENT_TABLE_NAME")

    url = make_url(target_db_connection_string)

    # Obtener continentes únicos
    continents = geography_df['continent'].unique()

    # Crear conexión directa con psycopg2
    conn = psycopg2.connect(
        host=url.host,
        port=url.port,
        database=url.database,
        user=url.username,
        password=url.password
    )

    query = f"SELECT continent_id, continent_name FROM {target_schema}.{target_table}"
    existing_continent_df = pd.read_sql_query(query, conn)

    unrecord_continents = set(continents).difference(existing_continent_df['continent_name'])

    continents_data = [{'continent_name': cont, 'etl_loaded_at': datetime.now()} for cont in unrecord_continents]
    continents_df = pd.DataFrame(continents_data)
    
    dwh_db_type = os.getenv("DWH_DB_TYPE")
    data_loader = LoaderFactory.get_loader(dwh_db_type)
    data_loader.load_data(
        continents_df,
        target_schema,
        target_table,
        url.database,
        url.username,
        url.password,
        url.host,
        url.port
    )
    
    # Obtener mapping de continent_name -> continent_id
    continent_mapping = pd.read_sql_query(query, conn).set_index('continent_name')['continent_id'].to_dict()
    
    return continent_mapping


def load_countries(geography_df, continent_mapping):
    target_db_connection_string = os.getenv("DWH_CONN_STRING")
    target_schema = os.getenv("DWH_SCHEMA")
    target_table = os.getenv("COUNTRY_TABLE_NAME")

    url = make_url(target_db_connection_string)

    # Crear conexión directa con psycopg2
    conn = psycopg2.connect(
        host=url.host,
        port=url.port,
        database=url.database,
        user=url.username,
        password=url.password
    )

    # Preparar datos de países con continent_id
    batch_countries = geography_df['country'].unique()

    query = f"SELECT country_id, country_name FROM {target_schema}.{target_table}"

    existing_countries_df = pd.read_sql_query(query, conn)

    unrecord_countries = set(batch_countries).difference(existing_countries_df['country_name'])

    countries_data = []

    for _, row in geography_df[['country', 'continent']].drop_duplicates().iterrows():
        if row['country'] in unrecord_countries:
            countries_data.append({
                'country_name': row['country'],
                'continent_id': continent_mapping[row['continent']],
                'etl_loaded_at': datetime.now()
            })
    
    countries_df = pd.DataFrame(countries_data)

    dwh_db_type = os.getenv("DWH_DB_TYPE")
    data_loader = LoaderFactory.get_loader(dwh_db_type)
    url = make_url(target_db_connection_string)
    data_loader.load_data(
        countries_df,
        target_schema,
        target_table,
        url.database,
        url.username,
        url.password,
        url.host,
        url.port
    )
    
    # Obtener mapping
    country_mapping = pd.read_sql(query, conn).set_index('country_name')['country_id'].to_dict()
    
    return country_mapping


def load_provinces(geography_df, country_mapping):
    target_db_connection_string = os.getenv("DWH_CONN_STRING")
    target_schema = os.getenv("DWH_SCHEMA")
    target_table = os.getenv("PROVINCE_TABLE_NAME")

    url = make_url(target_db_connection_string)

    # Crear conexión directa con psycopg2
    conn = psycopg2.connect(
        host=url.host,
        port=url.port,
        database=url.database,
        user=url.username,
        password=url.password
    )

    batch_provinces = geography_df['province'].unique()

    query = f"SELECT province_id, province_name FROM {target_schema}.{target_table}"

    existing_provinces_df = pd.read_sql_query(query, conn)

    unrecord_provinces = set(batch_provinces).difference(existing_provinces_df['province_name'])

    # Preparar datos de provincias
    provinces_data = []
    for _, row in geography_df[['province', 'country']].drop_duplicates().iterrows():
        if row['province'] in unrecord_provinces:
            provinces_data.append({
                'province_name': row['province'],
                'country_id': country_mapping[row['country']],
                'etl_loaded_at': datetime.now()
            })
    
    provinces_df = pd.DataFrame(provinces_data)

    dwh_db_type = os.getenv("DWH_DB_TYPE")
    data_loader = LoaderFactory.get_loader(dwh_db_type)
    data_loader.load_data(
        provinces_df,
        target_schema,
        target_table,
        url.database,
        url.username,
        url.password,
        url.host,
        url.port
    )

    # Obtener mapping
    province_mapping = pd.read_sql_query(query, conn).set_index('province_name')['province_id'].to_dict()
    
    return province_mapping


def load_cities(geography_df, province_mapping):
    target_db_connection_string = os.getenv("DWH_CONN_STRING")
    target_schema = os.getenv("DWH_SCHEMA")
    target_table = os.getenv("CITY_TABLE_NAME")
    
    url = make_url(target_db_connection_string)

    # Crear conexión directa con psycopg2
    conn = psycopg2.connect(
        host=url.host,
        port=url.port,
        database=url.database,
        user=url.username,
        password=url.password
    )

    batch_cities = geography_df['city'].unique()

    query = f"SELECT city_id, city_name FROM {target_schema}.{target_table}"

    existing_cities_df = pd.read_sql_query(query, conn)

    unrecord_cities = set(batch_cities).difference(existing_cities_df['city_name'])

    # Preparar datos de ciudades
    cities_data = []
    for _, row in geography_df[['city', 'province']].drop_duplicates().iterrows():
        if row['city'] in unrecord_cities:
            cities_data.append({
                'city_name': row['city'],
                'province_id': province_mapping[row['province']],
                'etl_loaded_at': datetime.now()
            })
    
    cities_df = pd.DataFrame(cities_data)

    dwh_db_type = os.getenv("DWH_DB_TYPE")
    data_loader = LoaderFactory.get_loader(dwh_db_type)
    data_loader.load_data(
        cities_df,
        target_schema,
        target_table,
        url.database,
        url.username,
        url.password,
        url.host,
        url.port
    )
    
    # Obtener mapping
    city_mapping = pd.read_sql_query(query, conn).set_index('city_name')['city_id'].to_dict()
    
    return city_mapping


def transform_listings_with_city_fk(df_listings, city_mapping):
    """Transformar listings agregando city_id y eliminando columnas geográficas"""
    
    # Crear copia del DataFrame
    df_transformed = df_listings.copy()
    
    # Llenar valores nulos en city
    df_transformed['city'] = df_transformed['city'].fillna('Unknown')
    
    # Agregar city_id
    df_transformed['city_id'] = df_transformed['city'].map(city_mapping)
    
    # Eliminar columnas geográficas originales
    columns_to_drop = ['city', 'province', 'country', 'continent']
    df_transformed = df_transformed.drop(columns=columns_to_drop)
    
    return df_transformed


def normalize_geography_etl(db_url, schema_name, df_listings):
    """ETL completo para normalizar geografía"""
    
    try:
        # 1. Extraer jerarquía geográfica
        geography_df = extract_geography_hierarchy(df_listings)
        
        # 2. Cargar tablas en orden jerárquico
        continent_mapping = load_continents(geography_df)
        country_mapping = load_countries(geography_df, continent_mapping)
        province_mapping = load_provinces(geography_df, country_mapping)
        city_mapping = load_cities(geography_df, province_mapping)
        
        # 3. Transformar listings con city_id
        df_normalized_listings = transform_listings_with_city_fk(df_listings, city_mapping)
        
        return df_normalized_listings, city_mapping
        
    except Exception as e:
        print(f"Error en normalización geográfica: {str(e)}")
        raise


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
}

with DAG(
    "dwh_etl_airbnb_listings",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    
    def create_geography_tables_task(**context):
        target_db_connection_string = os.getenv("DWH_CONN_STRING")
        target_schema = os.getenv("DWH_SCHEMA")
        target_table = os.getenv("LISTINGS_DWH_TABLE_NAME")

        engine = create_engine(target_db_connection_string)

        inspector = inspect(engine)
        create_geography_tables(target_db_connection_string, target_schema)
        print("Tablas geográficas creadas exitosamente")

    
    def create_dwh_listing_table(**context):
        target_db_connection_string = os.getenv("DWH_CONN_STRING")
        target_schema = os.getenv("DWH_SCHEMA")
        target_table = os.getenv("LISTINGS_DWH_TABLE_NAME")

        engine = create_engine(target_db_connection_string)

        inspector = inspect(engine)

        if target_table not in inspector.get_table_names(schema=target_schema):
            create_listing_dwh_table(target_db_connection_string, target_schema, target_table)
            print("Listings table succefully created")

    
    def add_foreign_key_constraint():
        target_db_connection_string = os.getenv("DWH_CONN_STRING")
        target_schema = os.getenv("DWH_SCHEMA")
        target_table = os.getenv("LISTINGS_DWH_TABLE_NAME")
        referenced_table = os.getenv("CITY_TABLE_NAME")

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
            constraint_name = f"fk_{target_table}_city_id"
            sql = f"""
            ALTER TABLE {target_schema}.{target_table} 
            ADD CONSTRAINT {constraint_name} 
            FOREIGN KEY (city_id) 
            REFERENCES {target_schema}.{referenced_table}(city_id)
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


    def transform_batch(df_batch, exchange_df):
        df_batch = df_batch.drop(columns=["calculated_host_listings_count", "calculated_host_listings_count_entire_homes", "calculated_host_listings_count_private_rooms", "calculated_host_listings_count_shared_rooms", "description", "neighborhood_overview", "picture_url", "host_url", "host_response_time", "host_response_rate_percentage", "host_acceptance_rate_percentage", "host_is_superhost", "host_listings_count", "host_total_listings_count", "host_verifications", "host_has_profile_pic", "host_identity_verified", "neighbourhood", "neighbourhood_group_cleansed", "calendar_updated"])

        # Impute baths
        for i in range(df_batch.shape[0]):
            if pd.isna(df_batch.loc[i, "bathrooms"]):
                if not pd.isna(df_batch.loc[i, "bathrooms_text"]):
                    half_bath_regex = r"\b(half[-\s]?bath(room)?|medio\s+bañ[oe])\b"
                    text = df_batch.loc[i, "bathrooms_text"].lower()

                    if re.search(half_bath_regex, text):
                        df_batch.loc[i, "bathrooms"] = 0.5
                        continue

                    text_list = text.split(" ")

                    if text_list[0].isdigit():
                        df_batch.loc[i, "bathrooms"] = float(text_list[0])
                        continue

                df_batch.loc[i, "bathrooms"] = 1

        df_batch["bathrooms_text"] = df_batch["bathrooms_text"].fillna("unknown")

        df_batch = df_batch.dropna()

        df_batch = df_batch.reset_index(drop=True)

        for idx, row in df_batch.iterrows():
            country = row["country"]
            try:
                df_batch.loc[idx, "price_dollar"] = df_batch.loc[idx, "price_dollar"] / exchange_df.loc[country, 'usd_exchange_rate']
            except Exception as e:
                print(f"Error in change price column: country:{country}, exchange: {exchange_df.loc[country, 'usd_exchange_rate']}")

        return df_batch


    def normalize_and_load_data(**context):
        target_db_connection_string = os.getenv("DWH_CONN_STRING")
        target_schema = os.getenv("DWH_SCHEMA")
        target_table = os.getenv("LISTINGS_DWH_TABLE_NAME")

        engine = create_engine(target_db_connection_string)
        url = make_url(target_db_connection_string)
    
        # Crear conexión directa con psycopg2
        conn = psycopg2.connect(
            host=url.host,
            port=url.port,
            database=url.database,
            user=url.username,
            password=url.password
        )

        batch_size = 100000

        extract_schema = os.getenv("STG_SCHEMA")

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
                SELECT * FROM {extract_schema}.{target_table} 
                ORDER BY id 
                LIMIT {batch_size} OFFSET {offset};
            """
            
            # Leer batch actual
            df_batch = pd.read_sql_query(batch_query, conn)
            
            if df_batch.empty:
                break

            # Ejecutar normalización
            exchange_data_path = os.path.join(os.getenv("EXTERNAL_DATA_PATH"), "usd_exchange.csv")
            exchange_df = pd.read_csv(exchange_data_path, sep=";")
            exchange_df.drop(columns=['country_name.1'], inplace=True)
            exchange_df.set_index('country_name', inplace=True)

            df_transformed = transform_batch(df_batch, exchange_df)

            df_batch_normalized, city_mapping = normalize_geography_etl(target_db_connection_string, target_schema, df_transformed)

            load_batch_to_dwh(df_batch_normalized, target_db_connection_string, target_schema, target_table)

            processed_records += len(df_batch)
            print(f"Batch {batch_number} completado. Procesados: {processed_records}/{total_records}")
            
            # Liberar memoria
            del df_batch, df_transformed
            gc.collect()
        
        print(f"Proceso completado. Total de registros procesados: {processed_records}")


    create_geography_tables_task = PythonOperator(
        task_id='create_geography_tables',
        python_callable=create_geography_tables_task,
        dag=dag
    )

    create_listings_table_task = PythonOperator(
        task_id='create_listings_table',
        python_callable=create_dwh_listing_table,
        dag=dag
    )

    normalize_task = PythonOperator(
        task_id='normalize_and_load',
        python_callable=normalize_and_load_data,
        dag=dag
    )

    add_fk_task = PythonOperator(
        task_id='add_fk',
        python_callable=add_foreign_key_constraint,
        dag=dag
    )

    create_geography_tables_task >> create_listings_table_task >> normalize_task >> add_fk_task