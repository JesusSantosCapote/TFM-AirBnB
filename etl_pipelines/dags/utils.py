from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, BigInteger, String, Text, Date, Numeric, Boolean, DateTime
)
from sqlalchemy.engine.url import make_url

def create_listings_table(db_url, schema_name, table_name):
    """Crea la tabla 'listings' en un esquema específico de PostgreSQL si no existe."""
    
    # Crear motor y metadatos
    engine = create_engine(db_url)
    metadata = MetaData(schema=schema_name)  # Especificar el esquema

    # Definir la tabla
    listings_table = Table(
        table_name, metadata,
        Column("id", BigInteger, primary_key=True),
        Column("name", String(255)),
        Column("host_id", BigInteger),
        Column("host_name", String(255)),
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