FROM postgres:13

ENV POSTGRES_USER=postgre
ENV POSTGRES_PASSWORD=postgre
ENV POSTGRES_DB=airbnb_dwh

# Copia el script de inicialización para crear los esquemas
COPY init.sql /docker-entrypoint-initdb.d/