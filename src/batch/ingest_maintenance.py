"""
Lambda / batch job: ingest_maintenance
Trigger: S3 PutObject on gps-bronze/mantenimientos/*.csv

Flow:
  1. Download CSV from bronze
  2. Normalize columns (uppercase, date parsing)
  3. Split valid / rejected rows
  4. Write valid → Parquet in gps-silver/mantenimientos/  (idempotent: same
     input key always maps to same output key, so re-runs overwrite safely)
  5. Write rejected → NDJSON in gps-bronze/bronze_rejected/mantenimientos/

Idempotency: output key is derived from input key, so uploading the same CSV
twice overwrites the same Parquet file instead of creating duplicates.
Alternative considered: add a hash of file content to the key — handles renamed
re-uploads, but adds complexity not needed here.
"""

import io
import json
import os
from datetime import datetime, timezone
from typing import Optional

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from common.logger import get_logger

logger = get_logger("ingest_maintenance")

VALID_FALLAS  = {"CRITICA", "MENOR"}
VALID_ESTADOS = {"RESUELTO", "PENDIENTE", "EN_PROCESO"}

_s3: Optional[boto3.client] = None


def _s3_client() -> boto3.client:
    global _s3
    if _s3 is None:
        kw = {"endpoint_url": ep} if (ep := os.getenv("AWS_ENDPOINT_URL")) else {}
        _s3 = boto3.client("s3", **kw)
    return _s3


# ── Normalization ─────────────────────────────────────────────────────────────

def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ("equipo_id", "tipo_falla", "estado"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()
    if "fecha_mantenimiento" in df.columns:
        df["fecha_mantenimiento"] = pd.to_datetime(
            df["fecha_mantenimiento"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
    if "descripcion" in df.columns:
        df["descripcion"] = df["descripcion"].astype(str).str.strip()
    if "tecnico" in df.columns:
        df["tecnico"] = df["tecnico"].astype(str).str.strip()
    return df


def _validate_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (valid_df, rejected_df). Adds rejection_reason column to rejected."""
    reasons = pd.Series([""] * len(df), index=df.index)

    # pandas converts None → "nan"/"None" depending on dtype; catch all variants
    _NULL = {"NAN", "NONE", "NULL", "NA", ""}
    missing_equipo = df["equipo_id"].isna() | df["equipo_id"].isin(_NULL)
    reasons[missing_equipo] += "missing_equipo_id;"

    invalid_falla = ~df["tipo_falla"].isin(VALID_FALLAS)
    reasons[invalid_falla] += "invalid_tipo_falla:" + df.loc[invalid_falla, "tipo_falla"].fillna("null") + ";"

    invalid_fecha = df["fecha_mantenimiento"].isna() | (df["fecha_mantenimiento"] == "NaT")
    reasons[invalid_fecha] += "invalid_fecha_mantenimiento;"

    invalid_mask = missing_equipo | invalid_falla | invalid_fecha

    valid_df = df[~invalid_mask].copy()
    rejected_df = df[invalid_mask].copy()
    rejected_df["rejection_reason"] = reasons[invalid_mask].str.rstrip(";")

    return valid_df, rejected_df


# ── S3 writes ─────────────────────────────────────────────────────────────────

def _silver_key(source_key: str) -> str:
    """
    Derive a deterministic output key from the input CSV key.
    e.g. mantenimientos/2024-01-15_mant.csv → mantenimientos/2024-01-15_mant.parquet
    """
    basename = os.path.basename(source_key).replace(".csv", "").replace(".CSV", "")
    return f"mantenimientos/{basename}.parquet"


def _write_parquet(df: pd.DataFrame, bucket: str, key: str) -> None:
    buf = io.BytesIO()
    pq.write_table(
        pa.Table.from_pandas(df, preserve_index=False), buf, compression="snappy"
    )
    buf.seek(0)
    _s3_client().put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    logger.info("Parquet written", extra={"bucket": bucket, "key": key, "rows": len(df)})


def _write_rejected(df: pd.DataFrame, bucket: str, source_key: str) -> None:
    if df.empty:
        return
    basename = os.path.basename(source_key).replace(".csv", "")
    now = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    key = f"bronze_rejected/mantenimientos/{basename}_{now}.json"
    body = "\n".join(json.dumps(r, default=str) for r in df.to_dict(orient="records"))
    _s3_client().put_object(Bucket=bucket, Key=key, Body=body.encode())
    logger.info("Rejected rows written", extra={"bucket": bucket, "key": key, "rows": len(df)})


# ── Handler ───────────────────────────────────────────────────────────────────

def handler(event: dict, context) -> dict:
    silver_bucket = os.getenv("SILVER_BUCKET", "gps-silver")
    bronze_bucket = os.getenv("BRONZE_BUCKET", "gps-bronze")

    results = []

    for record in event.get("Records", []):
        src_bucket = record["s3"]["bucket"]["name"]
        src_key    = record["s3"]["object"]["key"]

        logger.info("Processing CSV", extra={"bucket": src_bucket, "key": src_key})

        try:
            obj = _s3_client().get_object(Bucket=src_bucket, Key=src_key)
            raw = obj["Body"].read()
        except Exception as exc:
            logger.error("Failed to download CSV", extra={"key": src_key, "error": str(exc)})
            raise

        try:
            df = pd.read_csv(io.BytesIO(raw))
        except Exception as exc:
            logger.error("Failed to parse CSV", extra={"key": src_key, "error": str(exc)})
            raise

        required_cols = {"equipo_id", "fecha_mantenimiento", "tipo_falla"}
        missing_cols  = required_cols - set(df.columns)
        if missing_cols:
            logger.error("Missing required columns", extra={"key": src_key, "missing": list(missing_cols)})
            raise ValueError(f"CSV missing columns: {missing_cols}")

        df = _normalize(df)
        valid_df, rejected_df = _validate_rows(df)

        silver_key = _silver_key(src_key)
        if not valid_df.empty:
            _write_parquet(valid_df, silver_bucket, silver_key)
        else:
            logger.warning("No valid rows to write", extra={"key": src_key})

        _write_rejected(rejected_df, bronze_bucket, src_key)

        summary = {
            "source_key": src_key,
            "total": len(df),
            "valid": len(valid_df),
            "rejected": len(rejected_df),
            "silver_key": silver_key,
        }
        logger.info("Ingestion complete", extra=summary)
        results.append(summary)

    return {"processed": len(results), "files": results}
