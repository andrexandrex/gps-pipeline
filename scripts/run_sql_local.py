"""
Simulates the Athena query `sql/equipos_fallas_criticas.sql` using pandas
against the local S3 (LocalStack). No real Athena needed.

Usage:
    python3 scripts/run_sql_local.py          # reads from LocalStack
    AWS_ENDPOINT_URL="" python3 scripts/run_sql_local.py  # reads from real AWS
"""

import io
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd

# ── env defaults for LocalStack ───────────────────────────────────────────────
os.environ.setdefault("AWS_ACCESS_KEY_ID",     "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_DEFAULT_REGION",    "us-east-1")
os.environ.setdefault("AWS_ENDPOINT_URL",      "http://localhost:4566")
os.environ.setdefault("SILVER_BUCKET",         "gps-silver")

SILVER = os.environ["SILVER_BUCKET"]
EP     = os.environ.get("AWS_ENDPOINT_URL", "")

# ── boto3 ─────────────────────────────────────────────────────────────────────
kw = {"endpoint_url": EP} if EP else {}
s3 = boto3.client("s3", **kw)


def _read_parquets(bucket: str, prefix: str) -> pd.DataFrame:
    """Download all Parquet files under a prefix and concatenate them."""
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]
    if not keys:
        return pd.DataFrame()
    frames = []
    for key in keys:
        obj = s3.get_object(Bucket=bucket, Key=key)
        frames.append(pd.read_parquet(io.BytesIO(obj["Body"].read())))
    return pd.concat(frames, ignore_index=True)


# ── load data ─────────────────────────────────────────────────────────────────
print("Leyendo datos desde S3 (LocalStack)...")
mant_df = _read_parquets(SILVER, "mantenimientos/")
gps_df  = _read_parquets(SILVER, "gps_eventos/")

if mant_df.empty:
    print("\n⚠  Sin datos en silver/mantenimientos/")
    print("   Ejecuta:  make pipeline   (y asegúrate de correr con --batch o --all)")
    sys.exit(0)

print(f"  mantenimientos : {len(mant_df)} filas")
print(f"  gps_eventos    : {len(gps_df)} filas")

# ── CTE 1: fallas_criticas ────────────────────────────────────────────────────
# Equivale a:
#   SELECT equipo_id, COUNT(*) total, MAX(fecha_mantenimiento) ...
#   FROM mantenimientos WHERE criticidad = 'ALTA'
#   GROUP BY equipo_id HAVING COUNT(*) > 3
alta = mant_df[mant_df["criticidad"] == "ALTA"].copy()
fallas = (
    alta.groupby("equipo_id")
    .agg(
        total_fallas_criticas=("criticidad", "count"),
        fallas_alta=("criticidad", "count"),
        ultimo_mantenimiento=("fecha_mantenimiento", "max"),
    )
    .reset_index()
)
fallas = fallas[fallas["total_fallas_criticas"] > 3]

if fallas.empty:
    print("\nℹ  Ningún equipo supera 3 fallas ALTA todavía.")
    print("   (necesitas más datos — corre `make pipeline` varias veces)")
    # Show summary of what we DO have
    summary = (
        alta.groupby("equipo_id")["criticidad"]
        .count()
        .reset_index()
        .rename(columns={"criticidad": "fallas_ALTA"})
        .sort_values("fallas_ALTA", ascending=False)
    )
    print("\n  Fallas ALTA actuales por equipo:")
    print(summary.to_string(index=False))
    sys.exit(0)

# ── CTE 2: ultima_gps ─────────────────────────────────────────────────────────
# Equivale a:
#   SELECT equipo_id, MAX(from_iso8601_timestamp(timestamp)) AS ultima_fecha_gps
#   FROM gps_eventos GROUP BY equipo_id
if not gps_df.empty:
    gps_df["ts_parsed"] = pd.to_datetime(gps_df["timestamp"], utc=True, errors="coerce")
    ultima_gps = (
        gps_df.groupby("equipo_id")["ts_parsed"]
        .max()
        .reset_index()
        .rename(columns={"ts_parsed": "ultima_fecha_gps"})
    )
else:
    ultima_gps = pd.DataFrame(columns=["equipo_id", "ultima_fecha_gps"])

# ── JOIN + estado_gps ─────────────────────────────────────────────────────────
result = fallas.merge(ultima_gps, on="equipo_id", how="left")
now = datetime.now(timezone.utc)

def _estado(row):
    if pd.isna(row["ultima_fecha_gps"]):
        return "SIN_SEÑAL"
    minutes = (now - row["ultima_fecha_gps"]).total_seconds() / 60
    return "OK" if minutes <= 10 else "SIN_SEÑAL"

def _minutos(row):
    if pd.isna(row["ultima_fecha_gps"]):
        return None
    return int((now - row["ultima_fecha_gps"]).total_seconds() / 60)

result["estado_gps"]       = result.apply(_estado, axis=1)
result["minutos_sin_senal"] = result.apply(_minutos, axis=1)
result = result.sort_values(
    ["total_fallas_criticas", "minutos_sin_senal"],
    ascending=[False, False],
    na_position="last",
)

# ── print result ──────────────────────────────────────────────────────────────
print("\n" + "═"*72)
print("  RESULTADO — Equipos con >3 fallas ALTA (simula Athena SQL)")
print("═"*72)
print(result[[
    "equipo_id", "total_fallas_criticas", "ultimo_mantenimiento",
    "ultima_fecha_gps", "estado_gps", "minutos_sin_senal",
]].to_string(index=False))
print("═"*72)

ok  = len(result[result["estado_gps"] == "OK"])
sin = len(result[result["estado_gps"] == "SIN_SEÑAL"])
print(f"\n  Total equipos críticos : {len(result)}")
print(f"  Estado OK              : {ok}")
print(f"  Estado SIN_SEÑAL       : {sin}")
print(f"\n  SQL equivalente: sql/equipos_fallas_criticas.sql")
print(f"  En producción ejecutar en: AWS Athena (base: gps_pipeline)\n")
