-- Conectar a la base de datos
\c airbnb_dwh;

-- Crear esquemas
CREATE SCHEMA stg;  -- Staging: Datos con transformaciones técnicas
CREATE SCHEMA wrk;  -- Work: Datos intermedios antes del DWH
CREATE SCHEMA dwh;  -- Data Warehouse: Modelo estrella

-- -- Opcional: Dar permisos al usuario por defecto
-- GRANT USAGE, CREATE ON SCHEMA stg, wrk, dwh TO postgre;
