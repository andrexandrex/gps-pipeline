-- =============================================================================
-- Query: Equipos con >3 fallas críticas + última señal GPS + estado
-- Dialecto: Athena / Trino (Presto)
-- Base de datos Glue: gps_pipeline
--
-- Nota de diseño: el CSV del PDF usa el campo 'criticidad' (ALTA/MEDIA/BAJA)
-- para indicar severidad. Se filtran registros con criticidad = 'ALTA'.
-- El campo 'tipo_falla' es descripción libre (e.g. "Falla Motor"), no categoría.
-- =============================================================================

WITH fallas_criticas AS (
    -- Contar fallas de criticidad ALTA por equipo; solo equipos que superan umbral
    SELECT
        equipo_id,
        COUNT(*)                                                    AS total_fallas_criticas,
        COUNT(CASE WHEN criticidad = 'ALTA' THEN 1 END)            AS fallas_alta,
        MAX(fecha_mantenimiento)                                    AS ultimo_mantenimiento,
        -- Fallas aún no resueltas son las más urgentes
        COUNT(CASE WHEN estado != 'RESUELTO' THEN 1 END)           AS fallas_pendientes
    FROM "gps_pipeline"."mantenimientos"
    WHERE criticidad = 'ALTA'
    GROUP BY equipo_id
    HAVING COUNT(*) > 3
),

ultima_gps AS (
    -- from_iso8601_timestamp: convierte el string ISO → TIMESTAMP para date_diff
    SELECT
        equipo_id,
        MAX(from_iso8601_timestamp(timestamp))  AS ultima_fecha_gps
    FROM "gps_pipeline"."gps_eventos"
    GROUP BY equipo_id
)

SELECT
    fc.equipo_id,
    fc.total_fallas_criticas,
    fc.fallas_alta,
    fc.fallas_pendientes,
    fc.ultimo_mantenimiento,
    g.ultima_fecha_gps,

    -- Estado GPS: OK si tuvo señal en los últimos 10 minutos
    CASE
        WHEN g.ultima_fecha_gps IS NULL
            THEN 'SIN_SEÑAL'
        WHEN date_diff('minute', g.ultima_fecha_gps, now()) > 10
            THEN 'SIN_SEÑAL'
        ELSE 'OK'
    END                                                             AS estado_gps,

    -- Minutos transcurridos (útil para ordenar por urgencia en dashboard)
    CASE
        WHEN g.ultima_fecha_gps IS NULL THEN NULL
        ELSE date_diff('minute', g.ultima_fecha_gps, now())
    END                                                             AS minutos_sin_senal

FROM fallas_criticas fc
LEFT JOIN ultima_gps g
    ON fc.equipo_id = g.equipo_id

ORDER BY
    fc.total_fallas_criticas DESC,
    minutos_sin_senal         DESC NULLS LAST;
