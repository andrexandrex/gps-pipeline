-- =============================================================================
-- Query: Equipos con >3 fallas críticas + última señal GPS + estado
-- Dialecto: Athena / Trino (Presto)
-- Base de datos Glue: gps_pipeline
-- Tablas: gps_eventos (silver), mantenimientos (silver)
-- =============================================================================

WITH fallas_criticas AS (
    -- Contar fallas CRITICA por equipo; solo los que superan el umbral
    SELECT
        equipo_id,
        COUNT(*)                                              AS total_fallas_criticas,
        COUNT(CASE WHEN estado != 'RESUELTO' THEN 1 END)     AS fallas_pendientes,
        MAX(fecha_mantenimiento)                              AS ultimo_mantenimiento
    FROM "gps_pipeline"."mantenimientos"
    WHERE tipo_falla = 'CRITICA'
    GROUP BY equipo_id
    HAVING COUNT(*) > 3
),

ultima_gps AS (
    -- MAX timestamp por equipo; from_iso8601_timestamp convierte el string ISO a TIMESTAMP
    SELECT
        equipo_id,
        MAX(from_iso8601_timestamp(timestamp))  AS ultima_fecha_gps
    FROM "gps_pipeline"."gps_eventos"
    GROUP BY equipo_id
)

SELECT
    fc.equipo_id,
    fc.total_fallas_criticas,
    fc.fallas_pendientes,
    fc.ultimo_mantenimiento,
    g.ultima_fecha_gps,

    -- Estado GPS: OK si tuvo señal en los últimos 10 minutos, SIN_SEÑAL si no
    CASE
        WHEN g.ultima_fecha_gps IS NULL
            THEN 'SIN_SEÑAL'
        WHEN date_diff('minute', g.ultima_fecha_gps, now()) > 10
            THEN 'SIN_SEÑAL'
        ELSE 'OK'
    END                                         AS estado_gps,

    -- Minutos desde la última señal (útil para el dashboard)
    CASE
        WHEN g.ultima_fecha_gps IS NULL THEN NULL
        ELSE date_diff('minute', g.ultima_fecha_gps, now())
    END                                         AS minutos_sin_senal

FROM fallas_criticas fc
LEFT JOIN ultima_gps g
    ON fc.equipo_id = g.equipo_id

ORDER BY
    fc.total_fallas_criticas DESC,
    minutos_sin_senal         DESC NULLS LAST;
