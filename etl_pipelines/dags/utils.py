from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, BigInteger, String, Text, Date, Numeric, Boolean, DateTime
)
import psycopg2

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
        Column("source", String(255)),
        Column("name", String(255)),
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
        Column("neighbourhood", String(255)),
        Column("neighbourhood_cleansed", String(255)),
        Column("neighbourhood_group_cleansed", String(255)),
        Column("latitude", Numeric(10, 6)),
        Column("longitude", Numeric(10, 6)),
        Column("property_type", String(255)),
        Column("room_type", String(255)),
        Column("accommodates", Integer),
        Column("bathrooms", Numeric(3, 1)),
        Column("bathrooms_text", String(255)),
        Column("bedrooms", Integer),
        Column("beds", Numeric(3, 1)),
        Column("amenities", Text),
        Column("price_dollar", Numeric(10, 2)),
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
        Column("review_scores_rating", Numeric(4, 2)),
        Column("review_scores_accuracy", Numeric(4, 2)),
        Column("review_scores_cleanliness", Numeric(4, 2)),
        Column("review_scores_checkin", Numeric(4, 2)),
        Column("review_scores_communication", Numeric(4, 2)),
        Column("review_scores_location", Numeric(4, 2)),
        Column("review_scores_value", Numeric(4, 2)),
        Column("license", String(255)),
        Column("instant_bookable", Integer, nullable=True),
        Column("calculated_host_listings_count", Integer),
        Column("calculated_host_listings_count_entire_homes", Integer),
        Column("calculated_host_listings_count_private_rooms", Integer),
        Column("calculated_host_listings_count_shared_rooms", Integer),
        Column("reviews_per_month", Numeric(5, 2)),
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
        Column("date", String(255), nullable=True),
        Column("available", Boolean, nullable=True),
        Column("price_dollar", Numeric, nullable=True),
        Column("adjusted_price_dollar", Numeric, nullable=True),
        Column("minimum_nights", Numeric, nullable=True),
        Column("maximum_nights", Numeric, nullable=True)
    )

    # Crear la tabla en el esquema especificado
    metadata.create_all(engine)


# def load_csv_mysql(db_url, table_name, csv_path):
#     # Parsear la URL manualmente (si es un string de conexión)
#     url = make_url(db_url)

#     # Conectar a la base de datos
#     conn = pymysql.connect(
#         host=url.host,
#         user=url.username,
#         password=url.password,
#         database=url.database,
#         port=url.port or 3306,  # Usar puerto 3306 por defecto si no está en la URL
#         local_infile=True  # Permitir LOAD DATA LOCAL INFILE
#     )

#     cursor = conn.cursor()

#     # Ejecutar el comando para cargar datos
#     sql = f"""
#         LOAD DATA LOCAL INFILE '{csv_path}' 
#         INTO TABLE {table_name} 
#         FIELDS TERMINATED BY ',' 
#         ENCLOSED BY '"'
#         LINES TERMINATED BY '\n'
#         IGNORE 1 LINES;
#         """
    
#     cursor.execute(sql)
#     conn.commit()
    
#     cursor.close()
#     conn.close()