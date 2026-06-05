-- =============================================================================
-- DDL: Crear base de datos y tablas externas en AWS Glue / Athena
-- Ejecutar en la consola de Athena (Query Editor) UNA sola vez, o en CI
-- usando: aws athena start-query-execution --query-string file://sql/create_tables.sql
--
-- Las tablas son EXTERNAL: Athena no mueve datos, solo registra dónde están.
-- Reemplaza <ACCOUNT_ID> con tu ID de cuenta AWS (en LocalStack: 000000000000)
-- =============================================================================

-- Base de datos
CREATE DATABASE IF NOT EXISTS gps_pipeline
COMMENT 'Pipeline GPS — Áncash, Perú';

-- ── Tabla: gps_eventos (silver) ──────────────────────────────────────────────
-- Parquet generado por validate_gps Lambda, particionado por año/mes/día/hora
CREATE EXTERNAL TABLE IF NOT EXISTS gps_pipeline.gps_eventos (
    equipo_id           STRING,
    latitude            DOUBLE,
    longitude           DOUBLE,
    timestamp           STRING,   -- ISO 8601; usar from_iso8601_timestamp() en queries
    speed_kmh           DOUBLE,
    heading             DOUBLE
)
PARTITIONED BY (
    year    INT,
    month   INT,
    day     INT,
    hour    INT
)
STORED AS PARQUET
LOCATION 's3://gps-silver/gps_eventos/'
TBLPROPERTIES (
    'parquet.compress'        = 'SNAPPY',
    'projection.enabled'      = 'true',
    'projection.year.type'    = 'integer',
    'projection.year.range'   = '2024,2030',
    'projection.month.type'   = 'integer',
    'projection.month.range'  = '1,12',
    'projection.day.type'     = 'integer',
    'projection.day.range'    = '1,31',
    'projection.hour.type'    = 'integer',
    'projection.hour.range'   = '0,23',
    'storage.location.template' = 's3://gps-silver/gps_eventos/year=${year}/month=${month}/day=${day}/hour=${hour}/'
);

-- ── Tabla: mantenimientos (silver) ───────────────────────────────────────────
-- Parquet generado por ingest_maintenance Lambda
CREATE EXTERNAL TABLE IF NOT EXISTS gps_pipeline.mantenimientos (
    equipo_id               STRING,
    fecha_mantenimiento     STRING,   -- 'YYYY-MM-DD'
    tipo_falla              STRING,   -- 'CRITICA' | 'MENOR'
    descripcion             STRING,
    tecnico                 STRING,
    estado                  STRING    -- 'RESUELTO' | 'PENDIENTE' | 'EN_PROCESO'
)
STORED AS PARQUET
LOCATION 's3://gps-silver/mantenimientos/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- ── Reparar particiones (correr después de cargar datos nuevos) ──────────────
-- MSCK REPAIR TABLE gps_pipeline.gps_eventos;
