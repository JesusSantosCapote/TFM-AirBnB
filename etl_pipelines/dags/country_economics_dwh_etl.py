from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from sqlalchemy.engine.url import make_url
from utils import create_country_economics_table
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
    "dwh_etl_country_economics",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    def create_dwh_country_economics_table(**context):
        target_db_connection_string = os.getenv("DWH_CONN_STRING")
        target_schema = os.getenv("DWH_SCHEMA")
        target_table = os.getenv("COUNTRY_ECONOMICS_DWH_TABLE_NAME")

        try:
            create_country_economics_table(target_db_connection_string, target_schema, target_table)
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
        target_schema = os.getenv("DWH_SCHEMA")
        target_table = os.getenv("COUNTRY_ECONOMICS_DWH_TABLE_NAME")
        
        url = make_url(target_db_connection_string)

        # Crear conexión directa con psycopg2
        conn = psycopg2.connect(
            host=url.host,
            port=url.port,
            database=url.database,
            user=url.username,
            password=url.password
        )
        
        indicator_mapping = {
            "Current account balance (% of GDP)": 'current_account_balance_pct_gdp',
            "Current health expenditure (% of GDP)": 'health_expenditure_pct_gdp',
            "Domestic credit provided by financial sector (% of GDP)": 'domestic_credit_financial_sector_pct_gdp',
            "Domestic credit to private sector (% of GDP)": 'domestic_credit_private_sector_pct_gdp',
            "Domestic credit to private sector by banks (% of GDP)": 'bank_credit_private_sector_pct_gdp',
            "Expense (% of GDP)": 'government_expense_pct_gdp',
            "Exports of goods and services (% of GDP)": 'exports_goods_services_pct_gdp',
            "External balance on goods and services (% of GDP)": 'trade_balance_pct_gdp',
            "Final consumption expenditure (% of GDP)": 'final_consumption_pct_gdp',
            "Foreign direct investment, net inflows (% of GDP)": 'fdi_inflows_pct_gdp',
            "Foreign direct investment, net outflows (% of GDP)": 'fdi_outflows_pct_gdp',
            "GDP (current US$)": 'gdp_current_usd',
            "GDP growth (annual %)": 'gdp_growth_annual_pct',
            "GDP per capita (constant 2015 US$)": 'gdp_per_capita_constant_2015_usd',
            "GDP per capita growth (annual %)": 'gdp_per_capita_growth_annual_pct',
            "GDP per capita, PPP (constant 2021 international $)": 'gdp_per_capita_ppp_2021_intl_usd',
            "GDP per person employed (constant 2021 PPP $)": 'gdp_per_employed_person_ppp_2021_usd',
            "Gross capital formation (% of GDP)": 'gross_capital_formation_pct_gdp',
            "Gross domestic savings (% of GDP)": 'gross_domestic_savings_pct_gdp',
            "Imports of goods and services (% of GDP)": 'imports_goods_services_pct_gdp',
            "Inflation, GDP deflator (annual %)": 'inflation_gdp_deflator_annual_pct',
            "Industry (including construction), value added (% of GDP)": 'industry_value_added_pct_gdp',
            "Tax revenue (% of GDP)": 'tax_revenue_pct_gdp',
            "Trade (% of GDP)": 'total_trade_pct_gdp',
            "Water productivity, total (constant 2015 US$ GDP per cubic meter of total freshwater withdrawal)": 'water_productivity_gdp_per_m3_2015_usd'
        }

        country_table_name = os.getenv("COUNTRY_TABLE_NAME")
        country_query = f"SELECT country_id, country_name FROM {target_schema}.{country_table_name}"

        country_df = pd.read_sql_query(country_query, conn)
        country_mapper = country_df.set_index('country_name')['country_id'].to_dict()

        extract_schema = os.getenv("STG_SCHEMA")
        extract_table = os.getenv("COUNTRY_ECONOMICS_STG_TABLE_NAME")

        query = f"SELECT * FROM {extract_schema}.{extract_table}"
        economics_df = pd.read_sql_query(query, conn)

        # Diccionario principal para almacenar datos por país
        economics_dict = {}
        
        # Procesar cada país
        for country_name, country_group in economics_df.groupby('country_name'):
            country_id = country_mapper.get(country_name)
            
            if not country_id:
                print(f"País no encontrado: {country_name}")
                continue
            
            # Inicializar estructura para el país
            economics_dict[country_name] = {
                'country_id': country_id,
                'years': {}
            }
            
            # Procesar cada año (2014-2023)
            for year in range(2014, 2024):
                year_col = f'yr_{year}'
                year_data = {}
                
                # Para cada fila del país (cada series_name)
                for _, row in country_group.iterrows():
                    series_name = row['series_name']
                    
                    # Verificar si el año existe en los datos y no es nulo
                    if year_col in row and pd.notna(row[year_col]) and row[year_col] != '..':
                        try:
                            # Convertir a float y almacenar
                            value = float(row[year_col])
                            year_data[series_name] = value
                        except (ValueError, TypeError):
                            # Si no se puede convertir, almacenar None
                            year_data[series_name] = None
                    else:
                        # Si no hay dato, almacenar None
                        year_data[series_name] = None
                
                # Solo agregar el año si tiene al menos algunos datos
                if any(v is not None for v in year_data.values()):
                    economics_dict[country_name]['years'][year] = year_data
        
        # Ejemplo de cómo acceder a los datos:
        print("Estructura de economics_dict:")
        for country, data in list(economics_dict.items())[:2]:  # Solo primeros 2 países para ejemplo
            print(f"\nPaís: {country}")
            print(f"Country ID: {data['country_id']}")
            print("Años disponibles:", list(data['years'].keys()))
            
            # Mostrar datos de un año específico
            if 2023 in data['years']:
                print(f"Datos 2023 para {country}:")
                for series, value in list(data['years'][2023].items())[:3]:  # Solo primeros 3 indicadores
                    print(f"  {series}: {value}")
        
        # Convertir a formato para inserción en base de datos
        records_to_insert = []
        
        for country_name, country_data in economics_dict.items():
            country_id = country_data['country_id']
            
            for year, year_data in country_data['years'].items():
                # Crear registro base
                record = {
                    'country_id': country_id,
                    'year': year,
                    'etl_loaded_at': datetime.now()
                }
                
                # Agregar cada indicador usando el mapping pythónico
                for series_name, value in year_data.items():
                    if series_name in indicator_mapping:
                        pythonic_name = indicator_mapping[series_name]
                        record[pythonic_name] = value
                
                # Solo agregar si tiene al menos un indicador con valor
                if any(k != 'country_id' and k != 'year' and k != 'etl_loaded_at' and v is not None 
                    for k, v in record.items()):
                    records_to_insert.append(record)
        
        final_df = pd.DataFrame(records_to_insert)

        load_batch_to_dwh(final_df, target_db_connection_string, target_schema, target_table)


        conn.close()


    wait_for_dwh_listing_dag = ExternalTaskSensor(
        task_id='wait_for_listings_dag',
        external_dag_id='dwh_etl_airbnb_listings',
        external_task_id=None,
        timeout=600,
        poke_interval=60,
        mode='poke'
    )

    wait_for_stg_country_economics_dag = ExternalTaskSensor(
        task_id='wait_for_stg_country_economics_dag',
        external_dag_id='stg_etl_country_economics',
        external_task_id=None,
        timeout=600,
        poke_interval=60,
        mode='poke'
    )

    create_dwh_country_economics_table_task = PythonOperator(
        task_id='create_dwh_country_economics_table',
        python_callable=create_dwh_country_economics_table,
        dag=dag
    )

    transform_and_load_data_task = PythonOperator(
        task_id='transform_and_load_data_country_economics',
        python_callable=transform_and_load_data,
        dag=dag
    )

    wait_for_dwh_listing_dag >> wait_for_stg_country_economics_dag >> create_dwh_country_economics_table_task >> transform_and_load_data_task