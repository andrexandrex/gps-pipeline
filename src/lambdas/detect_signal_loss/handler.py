"""
Lambda: detect_signal_loss
Trigger: EventBridge Scheduler every 5 minutes

For each equipo whose last GPS event is older than SIGNAL_LOSS_THRESHOLD_MINUTES:
  1. Logs a warning
  2. Publishes a grouped SNS alert (one message for all affected devices)
  3. Auto-creates a maintenance record in silver/mantenimientos/ with
     tipo_falla="GPS DESCONECTADO" and criticidad="ALTA" — this feeds the
     Athena "fallas críticas" query automatically, no manual entry needed.

The auto-maintenance record uses AUTO_MAINTENANCE_THRESHOLD_MINUTES (default 30 min)
which is intentionally higher than the alert threshold (10 min) to avoid flooding
the maintenance table with transient signal dips.

Design note: separate thresholds for alert vs maintenance record is the key design
decision here. Alert at 10 min (operator should check), maintenance record at 30 min
(device is likely physically broken or stolen). Alternative: single threshold for both
— simpler but creates noisy maintenance records for brief connectivity issues.
"""

import io
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from common.logger import get_logger

logger = get_logger("detect_signal_loss")

_dynamo: Optional[boto3.client] = None
_sns:    Optional[boto3.client] = None
_s3:     Optional[boto3.client] = None


def _dynamo_client() -> boto3.client:
    global _dynamo
    if _dynamo is None:
        kw = {"endpoint_url": ep} if (ep := os.getenv("AWS_ENDPOINT_URL")) else {}
        _dynamo = boto3.client("dynamodb", **kw)
    return _dynamo


def _sns_client() -> boto3.client:
    global _sns
    if _sns is None:
        kw = {"endpoint_url": ep} if (ep := os.getenv("AWS_ENDPOINT_URL")) else {}
        _sns = boto3.client("sns", **kw)
    return _sns


def _s3_client() -> boto3.client:
    global _s3
    if _s3 is None:
        kw = {"endpoint_url": ep} if (ep := os.getenv("AWS_ENDPOINT_URL")) else {}
        _s3 = boto3.client("s3", **kw)
    return _s3


def _scan_all_devices(table_name: str) -> list[dict]:
    paginator = _dynamo_client().get_paginator("scan")
    items = []
    for page in paginator.paginate(TableName=table_name):
        items.extend(page.get("Items", []))
    return items


def _create_maintenance_records(equipos: list[dict], silver_bucket: str) -> str:
    """
    Writes a Parquet maintenance record for each device with extended signal loss.
    Key: auto_gps_loss_<timestamp>.parquet — unique per run, safe to re-process.
    """
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    records = [
        {
            "equipo_id":            e["equipo_id"],
            "fecha_mantenimiento":  now_str,
            "tipo_falla":           f"GPS DESCONECTADO ({e['minutes_silent']:.0f} min sin señal)",
            "criticidad":           "ALTA",
        }
        for e in equipos
    ]
    df = pd.DataFrame(records)
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), buf, compression="snappy")
    buf.seek(0)

    ts  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    key = f"mantenimientos/auto_gps_loss_{ts}.parquet"
    _s3_client().put_object(Bucket=silver_bucket, Key=key, Body=buf.getvalue())

    logger.info("Auto-maintenance records created", extra={
        "bucket": silver_bucket, "key": key, "count": len(records),
    })
    return key


def handler(event: dict, context) -> dict:
    table_name     = os.getenv("DYNAMO_TABLE_NAME",              "gps-last-seen")
    topic_arn      = os.getenv("SNS_TOPIC_ARN",                  "")
    silver_bucket  = os.getenv("SILVER_BUCKET",                  "gps-silver")
    threshold_min  = int(os.getenv("SIGNAL_LOSS_THRESHOLD_MINUTES",   "10"))
    maint_threshold = int(os.getenv("AUTO_MAINTENANCE_THRESHOLD_MINUTES", "30"))

    now    = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=threshold_min)
    maint_cutoff = now - timedelta(minutes=maint_threshold)

    logger.info("Signal-loss scan started", extra={
        "alert_threshold_min": threshold_min,
        "maintenance_threshold_min": maint_threshold,
    })

    items = _scan_all_devices(table_name)
    alert_lost = []    # silent > alert threshold  → SNS
    maint_lost = []    # silent > maint threshold  → auto maintenance record

    for item in items:
        equipo_id     = item.get("equipo_id", {}).get("S", "UNKNOWN")
        last_seen_str = item.get("last_seen",  {}).get("S")

        if not last_seen_str:
            logger.warning("No last_seen for device", extra={"equipo_id": equipo_id})
            continue

        try:
            last_seen_dt = datetime.fromisoformat(last_seen_str)
            if last_seen_dt.tzinfo is None:
                last_seen_dt = last_seen_dt.replace(tzinfo=timezone.utc)
        except ValueError:
            logger.error("Unparseable last_seen", extra={
                "equipo_id": equipo_id, "last_seen": last_seen_str,
            })
            continue

        minutes_silent = round((now - last_seen_dt).total_seconds() / 60, 1)

        if last_seen_dt < cutoff:
            entry = {"equipo_id": equipo_id, "last_seen": last_seen_str,
                     "minutes_silent": minutes_silent}
            alert_lost.append(entry)
            logger.warning("Signal loss — alert", extra={
                "equipo_id": equipo_id, "minutes_silent": minutes_silent,
            })

        if last_seen_dt < maint_cutoff:
            maint_lost.append({"equipo_id": equipo_id, "minutes_silent": minutes_silent})
            logger.warning("Signal loss — auto maintenance", extra={
                "equipo_id": equipo_id, "minutes_silent": minutes_silent,
            })

    logger.info("Scan complete", extra={
        "total_devices": len(items),
        "alert_lost": len(alert_lost),
        "maintenance_created": len(maint_lost),
    })

    # ── SNS alert ─────────────────────────────────────────────────────────────
    if alert_lost and topic_arn:
        payload = {
            "alert_type":        "SIGNAL_LOSS",
            "detected_at":       now.isoformat(),
            "alert_threshold_min": threshold_min,
            "maint_threshold_min": maint_threshold,
            "affected_count":    len(alert_lost),
            "affected_equipos":  alert_lost,
        }
        try:
            _sns_client().publish(
                TopicArn=topic_arn,
                Subject=f"[GPS ALERTA] {len(alert_lost)} equipo(s) sin señal >{threshold_min} min",
                Message=json.dumps(payload, indent=2),
                MessageAttributes={
                    "alert_type": {"DataType": "String", "StringValue": "SIGNAL_LOSS"},
                },
            )
            logger.info("SNS alert published", extra={"alert_lost": len(alert_lost)})
        except Exception as exc:
            logger.error("SNS publish failed", extra={"error": str(exc)})

    # ── Auto-maintenance record ────────────────────────────────────────────────
    maint_key = None
    if maint_lost:
        try:
            maint_key = _create_maintenance_records(maint_lost, silver_bucket)
        except Exception as exc:
            logger.error("Failed to create auto-maintenance records",
                         extra={"error": str(exc)})

    return {
        "alert_lost":   len(alert_lost),
        "maint_created": len(maint_lost),
        "maint_key":    maint_key,
        "equipos":      [e["equipo_id"] for e in alert_lost],
    }
