"""
Lambda: validate_gps
Trigger: SQS (event source mapping) — also accepts Kinesis format for backwards compat

Per record:
  1. Decode base64 payload → JSON
  2. Validate required fields, Áncash bbox, timestamp (not future, not >1h stale)
  3. Dedup via DynamoDB conditional write (equipo_id#timestamp → TTL 24h)
  4. Valid   → Parquet to s3://gps-silver/gps_eventos/year=.../
  5. Invalid → NDJSON to s3://gps-bronze/bronze_rejected/gps_eventos/year=.../
  6. Update gps-last-seen for signal-loss detection

Retry safety: DynamoDB conditional write is idempotent; Parquet files use
microsecond-precision keys so concurrent retries produce separate small files
(acceptable — Athena reads many small files fine).
"""

import base64
import io
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from common.logger import get_logger

logger = get_logger("validate_gps")

# ── Áncash bounding box ─────────────────────────────────────────────────────
LAT_MIN, LAT_MAX = -10.5, -7.8
LON_MIN, LON_MAX = -78.5, -76.5
MAX_AGE_H = 1

# ── Lazy boto3 clients (reused across warm Lambda invocations) ───────────────
_dynamo: Optional[boto3.client] = None
_s3: Optional[boto3.client] = None


def _dynamo_client() -> boto3.client:
    global _dynamo
    if _dynamo is None:
        kw = {"endpoint_url": ep} if (ep := os.getenv("AWS_ENDPOINT_URL")) else {}
        _dynamo = boto3.client("dynamodb", **kw)
    return _dynamo


def _s3_client() -> boto3.client:
    global _s3
    if _s3 is None:
        kw = {"endpoint_url": ep} if (ep := os.getenv("AWS_ENDPOINT_URL")) else {}
        _s3 = boto3.client("s3", **kw)
    return _s3


# ── Field normalization (PDF → internal canonical names) ─────────────────────
# The GPS device emits Spanish field names per the PDF spec.
# We normalize here at the ingestion boundary so all downstream code
# (validation, Pandera, Athena) uses consistent English names.
_FIELD_MAP = {
    "latitud":   "latitude",
    "longitud":  "longitude",
    "velocidad": "speed_kmh",
    # equipo_id and timestamp keep their names; estado is passed through
}


def _normalize_fields(record: dict) -> dict:
    return {_FIELD_MAP.get(k, k): v for k, v in record.items()}


# ── Validation ───────────────────────────────────────────────────────────────

def _decode(raw: dict) -> dict:
    """Decode SQS or Kinesis event record to a dict."""
    if "kinesis" in raw:
        return json.loads(base64.b64decode(raw["kinesis"]["data"]))
    return json.loads(raw["body"])


def _validate(rec: dict) -> tuple[bool, str]:
    required = {"equipo_id", "latitude", "longitude", "timestamp"}
    missing = required - rec.keys()
    if missing:
        return False, f"missing_fields:{','.join(sorted(missing))}"

    lat, lon = rec["latitude"], rec["longitude"]
    if not isinstance(lat, (int, float)) or not (LAT_MIN <= lat <= LAT_MAX):
        return False, f"lat_out_of_bbox:{lat}"
    if not isinstance(lon, (int, float)) or not (LON_MIN <= lon <= LON_MAX):
        return False, f"lon_out_of_bbox:{lon}"

    try:
        ts = datetime.fromisoformat(rec["timestamp"])
        ts = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return False, f"unparseable_timestamp:{rec.get('timestamp')}"

    now = datetime.now(timezone.utc)
    if ts > now + timedelta(seconds=30):   # 30s grace for clock skew
        return False, f"future_timestamp:{ts.isoformat()}"
    if ts < now - timedelta(hours=MAX_AGE_H):
        return False, f"stale_timestamp:{ts.isoformat()}"

    return True, "ok"


# ── Deduplication ────────────────────────────────────────────────────────────

def _is_duplicate(equipo_id: str, timestamp: str) -> bool:
    """
    Conditional PutItem on gps-dedup.  Returns True if already processed.
    TTL = 24 h so the table self-cleans; no manual purge needed.
    """
    table = os.getenv("DEDUP_TABLE_NAME", "gps-dedup")
    record_id = f"{equipo_id}#{timestamp}"
    ttl = int((datetime.now(timezone.utc) + timedelta(hours=24)).timestamp())
    try:
        _dynamo_client().put_item(
            TableName=table,
            Item={"record_id": {"S": record_id}, "ttl": {"N": str(ttl)}},
            ConditionExpression="attribute_not_exists(record_id)",
        )
        return False
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return True
        raise   # unexpected error → propagate so Lambda retries


# ── DynamoDB last-seen ────────────────────────────────────────────────────────

def _update_last_seen(equipo_id: str, timestamp: str) -> None:
    _dynamo_client().put_item(
        TableName=os.getenv("DYNAMO_TABLE_NAME", "gps-last-seen"),
        Item={
            "equipo_id": {"S": equipo_id},
            "last_seen": {"S": timestamp},
        },
    )


# ── S3 writes ────────────────────────────────────────────────────────────────

def _s3_key(prefix: str, now: datetime, ext: str) -> str:
    return (
        f"{prefix}/year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"{now.strftime('%Y%m%d_%H%M%S_%f')}.{ext}"
    )


def _write_valid(records: list[dict]) -> None:
    if not records:
        return
    bucket = os.getenv("SILVER_BUCKET", "gps-silver")
    df = pd.DataFrame(records)
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), buf, compression="snappy")
    buf.seek(0)
    key = _s3_key("gps_eventos", datetime.now(timezone.utc), "parquet")
    _s3_client().put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    logger.info("Valid records written", extra={"bucket": bucket, "key": key, "count": len(records)})


def _write_rejected(records: list[dict]) -> None:
    if not records:
        return
    bucket = os.getenv("BRONZE_BUCKET", "gps-bronze")
    key = _s3_key("bronze_rejected/gps_eventos", datetime.now(timezone.utc), "json")
    body = "\n".join(json.dumps(r, default=str) for r in records).encode()
    _s3_client().put_object(Bucket=bucket, Key=key, Body=body)
    logger.info("Rejected records written", extra={"bucket": bucket, "key": key, "count": len(records)})


# ── Handler ──────────────────────────────────────────────────────────────────

def handler(event: dict, context) -> dict:
    raw_records = event.get("Records", [])
    logger.info("Batch received", extra={"total": len(raw_records)})

    valid, rejected = [], []

    for raw in raw_records:
        try:
            rec = _normalize_fields(_decode(raw))
        except Exception as exc:
            logger.error("Decode error", extra={"error": str(exc)})
            rejected.append({"_raw": str(raw)[:500], "rejection_reason": f"decode_error:{exc}"})
            continue

        ok, reason = _validate(rec)
        if not ok:
            logger.warning("Validation failed", extra={
                "equipo_id": rec.get("equipo_id"), "reason": reason,
            })
            rec["rejection_reason"] = reason
            rejected.append(rec)
            continue

        equipo_id, timestamp = rec["equipo_id"], rec["timestamp"]

        try:
            if _is_duplicate(equipo_id, timestamp):
                logger.info("Duplicate skipped", extra={"equipo_id": equipo_id, "timestamp": timestamp})
                continue
        except Exception as exc:
            # Prefer accepting a potential duplicate over dropping a record
            logger.error("Dedup check failed, allowing through", extra={"error": str(exc)})

        try:
            _update_last_seen(equipo_id, timestamp)
        except Exception as exc:
            logger.error("last_seen update failed", extra={"equipo_id": equipo_id, "error": str(exc)})

        valid.append(rec)

    # Write results — if valid write fails, raise so Kinesis retries the full batch.
    # bisectBatchOnFunctionError will then split the batch to isolate the bad record.
    try:
        _write_valid(valid)
    except Exception as exc:
        logger.error("Failed to write valid batch to S3", extra={"error": str(exc)})
        raise

    try:
        _write_rejected(rejected)
    except Exception as exc:
        logger.error("Failed to write rejected batch to S3", extra={"error": str(exc)})
        # Don't re-raise; losing a rejected-record audit entry is less bad than
        # retrying the whole batch and duplicating valid records.

    summary = {"valid": len(valid), "rejected": len(rejected), "total": len(raw_records)}
    logger.info("Batch complete", extra=summary)
    return summary
