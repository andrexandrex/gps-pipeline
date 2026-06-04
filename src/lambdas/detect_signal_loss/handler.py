"""
Lambda: detect_signal_loss
Trigger: EventBridge Scheduler every 5 minutes

Scans gps-last-seen DynamoDB table.  For every equipo whose last_seen is
older than SIGNAL_LOSS_THRESHOLD_MINUTES, publishes one SNS message with
all affected devices grouped — avoids SNS fan-out storm for large fleets.

Idempotency: scanning + comparing timestamps is a read-only operation;
re-runs produce the same alert for the same silent window (acceptable;
downstream dedup on the alert consumer side if needed).

Alternative: for >10k devices, replace Scan with a GSI on last_seen
(sort key) and use Query with a KeyConditionExpression — O(results) vs O(table).
"""

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3

from common.logger import get_logger

logger = get_logger("detect_signal_loss")

_dynamo: Optional[boto3.client] = None
_sns: Optional[boto3.client] = None


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


def _scan_all_devices(table_name: str) -> list[dict]:
    """Full table scan with pagination — safe for fleets up to ~10k devices."""
    paginator = _dynamo_client().get_paginator("scan")
    items = []
    for page in paginator.paginate(TableName=table_name):
        items.extend(page.get("Items", []))
    return items


def handler(event: dict, context) -> dict:
    table_name = os.getenv("DYNAMO_TABLE_NAME", "gps-last-seen")
    topic_arn = os.getenv("SNS_TOPIC_ARN", "")
    threshold_min = int(os.getenv("SIGNAL_LOSS_THRESHOLD_MINUTES", "10"))

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=threshold_min)

    logger.info("Signal-loss scan started", extra={
        "threshold_minutes": threshold_min,
        "cutoff": cutoff.isoformat(),
    })

    items = _scan_all_devices(table_name)
    lost = []

    for item in items:
        equipo_id = item.get("equipo_id", {}).get("S", "UNKNOWN")
        last_seen_str = item.get("last_seen", {}).get("S")

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

        if last_seen_dt < cutoff:
            minutes_silent = round((now - last_seen_dt).total_seconds() / 60, 1)
            lost.append({
                "equipo_id": equipo_id,
                "last_seen": last_seen_str,
                "minutes_silent": minutes_silent,
            })
            logger.warning("Signal loss", extra={
                "equipo_id": equipo_id,
                "minutes_silent": minutes_silent,
            })

    logger.info("Scan complete", extra={"total_devices": len(items), "lost": len(lost)})

    if not lost:
        return {"lost": 0, "equipos": []}

    if topic_arn:
        payload = {
            "alert_type": "SIGNAL_LOSS",
            "detected_at": now.isoformat(),
            "threshold_minutes": threshold_min,
            "affected_count": len(lost),
            "affected_equipos": lost,
        }
        try:
            _sns_client().publish(
                TopicArn=topic_arn,
                Subject=f"[GPS ALERTA] {len(lost)} equipo(s) sin señal >{threshold_min} min",
                Message=json.dumps(payload, indent=2),
                MessageAttributes={
                    "alert_type": {"DataType": "String", "StringValue": "SIGNAL_LOSS"},
                },
            )
            logger.info("SNS alert published", extra={"lost": len(lost)})
        except Exception as exc:
            logger.error("SNS publish failed", extra={"error": str(exc)})
    else:
        logger.warning("SNS_TOPIC_ARN not set — alert not sent")

    return {"lost": len(lost), "equipos": [e["equipo_id"] for e in lost]}
