from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, BigInteger, String, Text, Date, Numeric, Boolean, DateTime, ForeignKey, inspect
)
import psycopg2
from datetime import datetime
import os

def create_summary_listings_table(db_url, schema_name, table_name):
    engine = create_engine(db_url)
    metadata = MetaData(schema=schema_name)  # Especificar el esquema

    listings_table = Table(
        table_name, metadata,
        Column("id", BigInteger, primary_key=True),
        Column("name", String(255)),
        Column("host_id", BigInteger),
        Column("neighbourhood_group", String(255), nullable=True),
        Column("neighbourhood", String(255)),
        Column("latitude", Numeric(9, 6)),
        Column("longitude", Numeric(9, 6)),
        Column("room_type", String(255)),
        Column("price", Numeric(10, 2)),
        Column("minimum_nights", Integer),
        Column("number_of_reviews", Integer),
        Column("last_review", String(255), nullable=True),
        Column("reviews_per_month", Numeric(5, 2), nullable=True),
        Column("calculated_host_listings_count", Integer),
        Column("availability_365", Integer),
        Column("number_of_reviews_ltm", Integer),
        Column("license", String(255), nullable=True),
        Column("city", String(255)),
        Column("province", String(255)),
        Column("country", String(255)),
        Column("continent", String(255)),
        Column("etl_loaded_at", DateTime(255))
    )

    # Crear la tabla en el esquema especificado
    metadata.create_all(engine)


def create_listings_table(db_url, schema_name, table_name):
    engine = create_engine(db_url)
    metadata = MetaData(schema=schema_name)

    listings_table = Table(
        table_name, metadata,
        Column("id", BigInteger, primary_key=True),
        Column("listing_url", String(2083)),
        Column("scrape_id", BigInteger),
        Column("last_scraped", Date),
        Column("source", String(2083)),
        Column("name", String(2083)),
        Column("description", Text),
        Column("neighborhood_overview", Text),
        Column("picture_url", String(2083)),
        Column("host_id", BigInteger),
        Column("host_url", String(2083)),
        Column("host_since", Date),
        Column("host_response_time", String(50)),
        Column("host_response_rate_percentage", String(10)),
        Column("host_acceptance_rate_percentage", String(10)),
        Column("host_is_superhost", String(50), nullable=True),
        Column("host_listings_count", Integer),
        Column("host_total_listings_count", Integer),
        Column("host_verifications", Text),
        Column("host_has_profile_pic", Integer, nullable=True),
        Column("host_identity_verified", Integer, nullable=True),
        Column("neighbourhood", String(2083)),
        Column("neighbourhood_cleansed", String(2083)),
        Column("neighbourhood_group_cleansed", String(2083)),
        Column("latitude", Numeric),
        Column("longitude", Numeric),
        Column("property_type", String(2083)),
        Column("room_type", String(2083)),
        Column("accommodates", Integer),
        Column("bathrooms", Numeric),
        Column("bathrooms_text", String(2083)),
        Column("bedrooms", Integer),
        Column("beds", Numeric),
        Column("amenities", Text),
        Column("price_dollar", Numeric),
        Column("minimum_nights", Integer),
        Column("maximum_nights", Integer),
        Column("calendar_updated", Text),
        Column("has_availability", Integer, nullable=True),
        Column("availability_30", Integer),
        Column("availability_60", Integer),
        Column("availability_90", Integer),
        Column("availability_365", Integer),
        Column("calendar_last_scraped", Date),
        Column("number_of_reviews", Integer),
        Column("number_of_reviews_ltm", Integer),
        Column("number_of_reviews_l30d", Integer),
        Column("first_review", Date),
        Column("last_review", Date),
        Column("review_scores_rating", Numeric),
        Column("review_scores_accuracy", Numeric),
        Column("review_scores_cleanliness", Numeric),
        Column("review_scores_checkin", Numeric),
        Column("review_scores_communication", Numeric),
        Column("review_scores_location", Numeric),
        Column("review_scores_value", Numeric),
        Column("license", String(2083)),
        Column("instant_bookable", Integer, nullable=True),
        Column("calculated_host_listings_count", Integer),
        Column("calculated_host_listings_count_entire_homes", Integer),
        Column("calculated_host_listings_count_private_rooms", Integer),
        Column("calculated_host_listings_count_shared_rooms", Integer),
        Column("reviews_per_month", Numeric),
        Column("city", String(255)),
        Column("province", String(255)),
        Column("country", String(255)),
        Column("continent", String(255)),
        Column("etl_loaded_at", DateTime(255))
    )

    # Crear la tabla en el esquema especificado
    metadata.create_all(engine)


def create_reviews_table(db_url, schema_name, table_name):
    engine = create_engine(db_url)
    metadata = MetaData(schema=schema_name)  # Especificar el esquema

    reviews_table = Table(
        table_name, metadata,
        Column("listing_id", BigInteger, primary_key=True),
        Column("id", BigInteger, nullable=True),
        Column("date", String(255), nullable=True),
        Column("reviewer_id", BigInteger, nullable=True),
        Column("comments", Text, nullable=True)
    )

    # Crear la tabla en el esquema especificado
    metadata.create_all(engine)


def create_calendar_table(db_url, schema_name, table_name):
    engine = create_engine(db_url)
    metadata = MetaData(schema=schema_name)  # Especificar el esquema

    calendar_table = Table(
        table_name, metadata,
        Column("listing_id", BigInteger, primary_key=True),
        Column("date", String(255), primary_key=True, nullable=True),
        Column("available", Boolean, nullable=True),
        Column("price_dollar", Numeric, nullable=True),
        Column("adjusted_price_dollar", Numeric, nullable=True),
        Column("minimum_nights", Numeric, nullable=True),
        Column("maximum_nights", Numeric, nullable=True)
    )

    # Crear la tabla en el esquema especificado
    metadata.create_all(engine)


def create_listing_calendar_table(db_url, schema_name, table_name):
    engine = create_engine(db_url)
    metadata = MetaData(schema=schema_name)  # Especificar el esquema

    listing_calendar_table = Table(
        table_name, metadata,
        Column("listing_id", BigInteger, primary_key=True),
        Column("date", String(255), nullable=True),
        Column("available", Boolean, nullable=True),
        Column("price_dollar", Numeric, nullable=True),
        Column("minimum_nights", Numeric, nullable=True),
        Column("maximum_nights", Numeric, nullable=True)
    )

    # Crear la tabla en el esquema especificado
    metadata.create_all(engine)


def create_listing_dwh_table(db_url, schema_name, table_name):
    engine = create_engine(db_url)
    metadata = MetaData(schema=schema_name)

    city_table_name = os.getenv("CITY_TABLE_NAME")

    listng_table = Table(
        table_name, metadata,
        Column("id", BigInteger, primary_key=True),
        Column("listing_url", String(2083)),
        Column("scrape_id", BigInteger),
        Column("last_scraped", Date),
        Column("source", String(2083)),
        Column("name", String(2083)),
        Column("host_id", BigInteger),
        Column("host_since", Date),
        Column("neighbourhood_cleansed", String(2083)),
        Column("latitude", Numeric),
        Column("longitude", Numeric),
        Column("property_type", String(2083)),
        Column("room_type", String(2083)),
        Column("accommodates", Integer),
        Column("bathrooms", Numeric),
        Column("bathrooms_text", String(2083)),
        Column("bedrooms", Integer),
        Column("beds", Numeric),
        Column("amenities", Text),
        Column("price_dollar", Numeric),
        Column("minimum_nights", Integer),
        Column("maximum_nights", Integer),
        Column("has_availability", Integer, nullable=True),
        Column("availability_30", Integer),
        Column("availability_60", Integer),
        Column("availability_90", Integer),
        Column("availability_365", Integer),
        Column("calendar_last_scraped", Date),
        Column("number_of_reviews", Integer),
        Column("number_of_reviews_ltm", Integer),
        Column("number_of_reviews_l30d", Integer),
        Column("first_review", Date),
        Column("last_review", Date),
        Column("review_scores_rating", Numeric),
        Column("review_scores_accuracy", Numeric),
        Column("review_scores_cleanliness", Numeric),
        Column("review_scores_checkin", Numeric),
        Column("review_scores_communication", Numeric),
        Column("review_scores_location", Numeric),
        Column("review_scores_value", Numeric),
        Column("license", String(2083)),
        Column("instant_bookable", Integer, nullable=True),
        Column("reviews_per_month", Numeric),
        Column("city_id", Integer),
        Column("etl_loaded_at", DateTime(255))
    )

    metadata.create_all(engine)


def create_geography_tables(db_url, schema_name):
    """Crear las tablas de jerarquía geográfica"""
    engine = create_engine(db_url)
    metadata = MetaData(schema=schema_name)

    continent_table_name = os.getenv("CONTINENT_TABLE_NAME")
    country_table_name = os.getenv("COUNTRY_TABLE_NAME")
    province_table_name = os.getenv("PROVINCE_TABLE_NAME")
    city_table_name = os.getenv("CITY_TABLE_NAME")

    inspector = inspect(engine)

    if continent_table_name not in inspector.get_table_names(schema=schema_name):
        # Tabla continents
        continents_table = Table(
            continent_table_name, metadata,
            Column('continent_id', Integer, primary_key=True, autoincrement=True),
            Column('continent_name', String(255), unique=True, nullable=False),
            Column('etl_loaded_at', DateTime, default=datetime.utcnow)
        )

    if country_table_name not in inspector.get_table_names(schema=schema_name):
        # Tabla countries
        countries_table = Table(
            country_table_name, metadata,
            Column('country_id', Integer, primary_key=True, autoincrement=True),
            Column('country_name', String(255), nullable=False),
            Column('continent_id', Integer, ForeignKey(f'{schema_name}.{continent_table_name}.continent_id')),
            Column('etl_loaded_at', DateTime, default=datetime.utcnow)
        )

    if province_table_name not in inspector.get_table_names(schema=schema_name):
        provinces_table = Table(
            province_table_name, metadata,
            Column('province_id', Integer, primary_key=True, autoincrement=True),
            Column('province_name', String(255), nullable=False),
            Column('country_id', Integer, ForeignKey(f'{schema_name}.{country_table_name}.country_id')),
            Column('etl_loaded_at', DateTime, default=datetime.utcnow)
        )

    if city_table_name not in inspector.get_table_names(schema=schema_name):
        # Tabla cities
        cities_table = Table(
            city_table_name, metadata,
            Column('city_id', Integer, primary_key=True, autoincrement=True),
            Column('city_name', String(255), nullable=False),
            Column('province_id', Integer, ForeignKey(f'{schema_name}.{province_table_name}.province_id')),
            Column('etl_loaded_at', DateTime, default=datetime.utcnow)
        )

    # Crear todas las tablas
    metadata.create_all(engine)


def create_amenities_table(db_url, schema_name):
    engine = create_engine(db_url)
    metadata = MetaData(schema=schema_name)

    table_name = os.getenv("AMENITIE_TABLE_NAME")

    inspector = inspect(engine)

    if table_name not in inspector.get_table_names(schema=schema_name):
        amenities_table = Table(
            table_name, metadata,
            Column('amenitie_id', Integer, primary_key=True, autoincrement=True),
            Column('name', String(255), nullable=False)
        )

    metadata.create_all(engine)


def create_amentie_listing_table(db_url, schema_name):
    engine = create_engine(db_url)
    metadata = MetaData(schema=schema_name)

    listing_table_name = target_table = os.getenv("LISTINGS_DWH_TABLE_NAME")
    amenitie_table_name = os.getenv("AMENITIE_TABLE_NAME")
    amenitie_listing_table_name = os.getenv("AMENITIE_LISTING_TABLE_NAME")

    inspector = inspect(engine)

    if amenitie_listing_table_name not in inspector.get_table_names(schema=schema_name):
        amenitie_listing_table = Table(
            amenitie_listing_table_name, metadata,
            Column('amenitie_id', BigInteger, 
                   primary_key=True),
            
            Column('listing_id', BigInteger, 
                   primary_key=True)
        )

    metadata.create_all(engine)


def create_date_master_table(db_url, schema_name, table_name):
    """
    Creates the date master table in PostgreSQL
    """
    engine = create_engine(db_url)
    metadata = MetaData(schema=schema_name)

    inspector = inspect(engine)

    # Check if table already exists
    if table_name not in inspector.get_table_names(schema=schema_name):
        date_master_table = Table(
            table_name, metadata,
            Column("date", Date, primary_key=True, nullable=False),
            Column("date_str", String(10), nullable=False),
            Column("year", Integer, nullable=False),
            Column("month", Integer, nullable=False),
            Column("day", Integer, nullable=False),
            Column("day_of_week", Integer, nullable=False),  # 1=Monday, 7=Sunday
            Column("day_name", String(20), nullable=False),
            Column("month_name", String(20), nullable=False),
            Column("quarter", Integer, nullable=False),
            Column("week_of_year", Integer, nullable=False),
            Column("day_of_year", Integer, nullable=False),
            Column("is_weekend", Boolean, nullable=False, default=False),
            Column("is_month_start", Boolean, nullable=False, default=False),
            Column("is_month_end", Boolean, nullable=False, default=False),
            Column("is_leap_year", Boolean, nullable=False, default=False),
            Column("is_high_season", Boolean, nullable=True, default=False),
            Column("is_holiday", Boolean, nullable=True, default=False),
            Column("etl_loaded_at", DateTime, default=datetime.utcnow)
        )

    # Create the table in the specified schema
    metadata.create_all(engine)


def create_country_economics_table(db_url, schema_name, table_name):
    engine = create_engine(db_url)
    metadata = MetaData(schema=schema_name)

    inspector = inspect(engine)

    # Check if table already exists
    if table_name not in inspector.get_table_names(schema=schema_name):
        country_economics_table = Table(
            table_name, metadata,
            Column("country_id", Integer),
            Column("current_account_balance_pct_gdp", Numeric, nullable=True),
            Column("health_expenditure_pct_gdp", Numeric, nullable=True),
            Column("domestic_credit_financial_sector_pct_gdp", Numeric, nullable=True),
            Column("domestic_credit_private_sector_pct_gdp", Numeric, nullable=True),
            Column("bank_credit_private_sector_pct_gdp", Numeric, nullable=True),
            Column("government_expense_pct_gdp", Numeric, nullable=True),
            Column("exports_goods_services_pct_gdp", Numeric, nullable=True),
            Column("trade_balance_pct_gdp", Numeric, nullable=True),
            Column("final_consumption_pct_gdp", Numeric, nullable=True),
            Column("fdi_inflows_pct_gdp", Numeric, nullable=True),
            Column("fdi_outflows_pct_gdp", Numeric, nullable=True),
            Column("gdp_current_usd", Numeric, nullable=True),
            Column("gdp_growth_annual_pct", Numeric, nullable=True),
            Column("gdp_per_capita_constant_2015_usd", Numeric, nullable=True),
            Column("gdp_per_capita_growth_annual_pct", Numeric, nullable=True),
            Column("gdp_per_capita_ppp_2021_intl_usd", Numeric, nullable=True),
            Column("gdp_per_employed_person_ppp_2021_usd", Numeric, nullable=True),
            Column("gross_capital_formation_pct_gdp", Numeric, nullable=True),
            Column("gross_domestic_savings_pct_gdp", Numeric, nullable=True),
            Column("imports_goods_services_pct_gdp", Numeric, nullable=True),
            Column("inflation_gdp_deflator_annual_pct", Numeric, nullable=True),
            Column("industry_value_added_pct_gdp", Numeric, nullable=True),
            Column("tax_revenue_pct_gdp", Numeric, nullable=True),
            Column("total_trade_pct_gdp", Numeric, nullable=True),
            Column("water_productivity_gdp_per_m3_2015_usd", Numeric, nullable=True),
            Column("year", Integer, nullable=False),
            Column("etl_loaded_at", DateTime, default=datetime.utcnow)
        )

    # Create the table in the specified schema
    metadata.create_all(engine)


def create_stg_country_economics_table(db_url, schema_name, table_name):
    engine = create_engine(db_url)
    metadata = MetaData(schema=schema_name)

    inspector = inspect(engine)

    # Check if table already exists
    if table_name not in inspector.get_table_names(schema=schema_name):
        date_master_table = Table(
            table_name, metadata,
            Column("country_name", String(255)),
            Column("country_code", String(255), primary_key=True),
            Column("series_name", String(255)),
            Column("series_code", String(255), primary_key=True),
            Column("yr_2014", Numeric),
            Column("yr_2015", Numeric),
            Column("yr_2016", Numeric),
            Column("yr_2017", Numeric),
            Column("yr_2018", Numeric),
            Column("yr_2019", Numeric),
            Column("yr_2020", Numeric),
            Column("yr_2021", Numeric),
            Column("yr_2022", Numeric),
            Column("yr_2023", Numeric),
            Column("etl_loaded_at", DateTime, default=datetime.utcnow)
        )

    # Create the table in the specified schema
    metadata.create_all(engine)


def create_stg_arrivals_table(db_url, schema_name, table_name):
    engine = create_engine(db_url)
    metadata = MetaData(schema=schema_name)

    inspector = inspect(engine)

    # Check if table already exists
    if table_name not in inspector.get_table_names(schema=schema_name):
        date_master_table = Table(
            table_name, metadata,
            Column("country", String(255), primary_key=True),
            Column("international_arrivals", Numeric),
            Column("etl_loaded_at", DateTime, default=datetime.utcnow)
        )

    # Create the table in the specified schema
    metadata.create_all(engine)


def create_stg_crime_index_table(db_url, schema_name, table_name):
    engine = create_engine(db_url)
    metadata = MetaData(schema=schema_name)

    inspector = inspect(engine)

    # Check if table already exists
    if table_name not in inspector.get_table_names(schema=schema_name):
        date_master_table = Table(
            table_name, metadata,
            Column("country", String(255)),
            Column("city", String(255), primary_key=True),
            Column("crime_index", Numeric),
            Column("safety_index", Numeric),
            Column("numbeo_crime_level", String(255)),
            Column("numbeo_link", String(255)),
            Column("etl_loaded_at", DateTime, default=datetime.utcnow)
        )

    # Create the table in the specified schema
    metadata.create_all(engine)
