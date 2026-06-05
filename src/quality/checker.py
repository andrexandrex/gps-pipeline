"""
QualityChecker: runs Pandera validation on a DataFrame and produces
structured quality metrics suitable for gold/ storage and QuickSight.

Can be called inline (from ingest_maintenance / validate_gps) or as a
standalone Lambda over full silver/ datasets.
"""

import io
import json
import os
from datetime import datetime, timezone
from typing import Optional

import boto3
import pandas as pd
import pandera as pa
import pyarrow as pa_arrow
import pyarrow.parquet as pq

from common.logger import get_logger
from quality.schemas import SCHEMAS

logger = get_logger("quality.checker")

_s3: Optional[boto3.client] = None


def _s3_client() -> boto3.client:
    global _s3
    if _s3 is None:
        kw = {"endpoint_url": ep} if (ep := os.getenv("AWS_ENDPOINT_URL")) else {}
        _s3 = boto3.client("s3", **kw)
    return _s3


# ── Core validation ───────────────────────────────────────────────────────────

def validate(df: pd.DataFrame, dataset: str) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Validate df against the registered schema for `dataset`.

    Returns:
        valid_df      — rows that pass all Pandera checks
        rejected_df   — rows that fail at least one check (with rejection_reason)
        metrics       — quality metrics dict ready to write to gold/
    """
    if dataset not in SCHEMAS:
        raise ValueError(f"No schema registered for dataset '{dataset}'. "
                         f"Available: {list(SCHEMAS)}")

    schema, dedup_keys = SCHEMAS[dataset]
    total = len(df)

    # ── 1. Pandera lazy validation — collects ALL failures ────────────────────
    failure_cases = pd.DataFrame()
    try:
        schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        failure_cases = exc.failure_cases
        logger.info("Pandera failures found", extra={
            "dataset": dataset, "failure_rows": len(failure_cases),
        })

    # ── 2. Split valid / rejected ─────────────────────────────────────────────
    if failure_cases.empty:
        valid_df   = df.copy()
        rejected_df = df.iloc[0:0].copy()  # empty with same columns
        rejected_df["rejection_reason"] = pd.Series(dtype=str)
    else:
        failed_idx = set(failure_cases["index"].dropna().astype(int))
        valid_df   = df[~df.index.isin(failed_idx)].copy()
        rejected_df = df[df.index.isin(failed_idx)].copy()
        # Build human-readable reason per row
        reasons = (
            failure_cases.dropna(subset=["index"])
            .assign(index=lambda x: x["index"].astype(int))
            .groupby("index")[["column", "check"]]
            .apply(lambda g: "; ".join(
                f"{row['column']}:{row['check']}" for _, row in g.iterrows()
            ), include_groups=False)
        )
        rejected_df["rejection_reason"] = rejected_df.index.map(reasons).fillna("unknown")

    # ── 3. Duplicates ─────────────────────────────────────────────────────────
    existing_dedup_keys = [k for k in dedup_keys if k in df.columns]
    dup_mask = df.duplicated(subset=existing_dedup_keys, keep="first") if existing_dedup_keys else pd.Series(False, index=df.index)
    dup_count = int(dup_mask.sum())

    # ── 4. Completeness per column ────────────────────────────────────────────
    completeness = {
        col: round(float(df[col].notna().mean() * 100), 2)
        for col in df.columns
        if col != "rejection_reason"
    }

    # ── 5. Out-of-range pct per column (from failure_cases) ──────────────────
    out_of_range: dict[str, float] = {}
    if not failure_cases.empty:
        range_fails = failure_cases[
            failure_cases["check"].str.contains("in_range", na=False)
        ]
        for col, grp in range_fails.groupby("column"):
            out_of_range[col] = round(len(grp) / total * 100, 2) if total > 0 else 0.0

    # ── 6. Assemble metrics dict ──────────────────────────────────────────────
    valid_count    = len(valid_df)
    rejected_count = len(rejected_df)
    metrics = {
        "dataset":           dataset,
        "run_timestamp":     datetime.now(timezone.utc).isoformat(),
        "total_rows":        total,
        "valid_rows":        valid_count,
        "rejected_rows":     rejected_count,
        "valid_pct":         round(valid_count / total * 100, 2) if total else 0.0,
        "duplicates_count":  dup_count,
        "duplicates_pct":    round(dup_count / total * 100, 2) if total else 0.0,
        "completeness_pct":  completeness,
        "out_of_range_pct":  out_of_range,
        "pandera_failures":  len(failure_cases),
    }

    logger.info("Quality check complete", extra={k: v for k, v in metrics.items()
                                                  if k not in ("completeness_pct", "out_of_range_pct")})
    return valid_df, rejected_df, metrics


# ── Gold/ write ───────────────────────────────────────────────────────────────

def write_metrics(metrics: dict, gold_bucket: str) -> str:
    """
    Persists quality metrics to two formats:
      - JSON  → gold/quality_metrics/<dataset>/run_<ts>.json  (human-readable)
      - Parquet → gold/quality_metrics/<dataset>/run_<ts>.parquet (Athena/QuickSight)

    Returns the S3 key of the JSON file.
    """
    dataset = metrics["dataset"]
    ts      = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    prefix  = f"quality_metrics/{dataset}/run_{ts}"

    # JSON — flatten nested dicts for readability
    flat = {**metrics}
    flat["completeness_pct"] = json.dumps(metrics.get("completeness_pct", {}))
    flat["out_of_range_pct"] = json.dumps(metrics.get("out_of_range_pct", {}))

    json_key = f"{prefix}.json"
    _s3_client().put_object(
        Bucket=gold_bucket,
        Key=json_key,
        Body=json.dumps(metrics, indent=2, default=str).encode(),
        ContentType="application/json",
    )

    # Parquet — one row, columnar for Athena
    df_row = pd.DataFrame([flat])
    buf = io.BytesIO()
    pq.write_table(pa_arrow.Table.from_pandas(df_row, preserve_index=False), buf, compression="snappy")
    buf.seek(0)
    pq.write_table(
        pa_arrow.Table.from_pandas(df_row, preserve_index=False),
        buf := io.BytesIO(),
        compression="snappy",
    )
    buf.seek(0)
    _s3_client().put_object(Bucket=gold_bucket, Key=f"{prefix}.parquet", Body=buf.getvalue())

    logger.info("Metrics written to gold", extra={"bucket": gold_bucket, "prefix": prefix})
    return json_key


# ── S3 reader (for standalone / scheduled Lambda) ────────────────────────────

def read_silver(bucket: str, prefix: str) -> pd.DataFrame:
    """Read all Parquet files under a given S3 prefix into one DataFrame."""
    paginator = _s3_client().get_paginator("list_objects_v2")
    frames: list[pd.DataFrame] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                body = _s3_client().get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
                frames.append(pd.read_parquet(io.BytesIO(body)))

    if not frames:
        logger.warning("No Parquet files found", extra={"bucket": bucket, "prefix": prefix})
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)
