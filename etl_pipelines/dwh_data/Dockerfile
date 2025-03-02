FROM postgres:13

# Variables de entorno para la configuración de PostgreSQL
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_DB=dwh_airbnb

# Copia el script de inicialización para crear los esquemas
COPY init.sql /docker-entrypoint-initdb.d/