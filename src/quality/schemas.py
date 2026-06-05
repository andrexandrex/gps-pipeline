"""
Pandera schemas for GPS pipeline datasets.

Why Pandera over if/else checks:
  - Declarative: the schema IS the documentation
  - lazy=True collects ALL failures in one pass (vs stopping at first error)
  - failure_cases DataFrame feeds directly into quality metrics
  - Catches type coercion issues automatically (coerce=True)

Alternative: Great Expectations — more enterprise features (data docs, suites),
but heavier setup. Pandera is idiomatic Python and easier to version-control.
"""

import pandera as pa
from pandera import Check, Column, DataFrameSchema

# ── GPS eventos schema ────────────────────────────────────────────────────────
GPS_SCHEMA = DataFrameSchema(
    columns={
        "equipo_id": Column(
            str,
            nullable=False,
            checks=Check.str_length(min_value=1, max_value=20),
        ),
        "latitude": Column(
            float,
            nullable=False,
            checks=Check.in_range(-10.5, -7.8),
            # Áncash bounding box — out-of-range means GPS malfunction or wrong region
        ),
        "longitude": Column(
            float,
            nullable=False,
            checks=Check.in_range(-78.5, -76.5),
        ),
        "timestamp": Column(
            str,
            nullable=False,
            checks=Check.str_matches(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}"),
        ),
        "speed_kmh": Column(
            float,
            nullable=True,
            checks=Check.in_range(0, 200),  # equipment max ~120 km/h; 200 gives margin
        ),
    },
    coerce=True,
    strict=False,  # allow extra columns (heading, etc.)
)

# Deduplication key for GPS: same device at the same second = duplicate
GPS_DEDUP_KEYS = ["equipo_id", "timestamp"]

# ── Mantenimientos schema ────────────────────────────────────────────────────
MANT_SCHEMA = DataFrameSchema(
    columns={
        "equipo_id": Column(
            str,
            nullable=False,
            checks=Check.str_length(min_value=1),
        ),
        "fecha_mantenimiento": Column(
            str,
            nullable=False,
            checks=Check.str_matches(r"^\d{4}-\d{2}-\d{2}$"),
        ),
        "tipo_falla": Column(
            str,
            nullable=False,
            checks=Check.isin(["CRITICA", "MENOR"]),
        ),
        "estado": Column(
            str,
            nullable=True,
            checks=Check.isin(["RESUELTO", "PENDIENTE", "EN_PROCESO"]),
        ),
    },
    coerce=True,
    strict=False,
)

# Deduplication key: one maintenance record per equipment per event
MANT_DEDUP_KEYS = ["equipo_id", "fecha_mantenimiento", "tipo_falla"]

SCHEMAS = {
    "gps_eventos":    (GPS_SCHEMA,  GPS_DEDUP_KEYS),
    "mantenimientos": (MANT_SCHEMA, MANT_DEDUP_KEYS),
}
