"""
GPS event simulator — publishes synthetic Kinesis records for Áncash, Perú.
Injects ~10% invalid records (bad coords / future ts) to exercise validation.

Run:
  python -m producer.simulator           # uses .env
  PRODUCER_INTERVAL_SECONDS=1 python -m producer.simulator
"""

import json
import os
import random
import time
from datetime import datetime, timedelta, timezone

import boto3
from dotenv import load_dotenv

from lambdas.common.logger import get_logger

load_dotenv()
logger = get_logger("producer")

# Áncash, Perú — approximate bounding box
ANCASH = {"lat": (-10.5, -7.8), "lon": (-78.5, -76.5)}

EQUIPOS = [f"EQ{str(i).zfill(3)}" for i in range(1, 11)]  # 10 devices


def _make_valid_event(equipo_id: str) -> dict:
    return {
        "equipo_id": equipo_id,
        "latitude": round(random.uniform(*ANCASH["lat"]), 6),
        "longitude": round(random.uniform(*ANCASH["lon"]), 6),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "speed_kmh": round(random.uniform(0, 120), 1),
        "heading": round(random.uniform(0, 360), 1),
    }


def _make_invalid_event(equipo_id: str) -> dict:
    """Returns a deliberately malformed event to test the DLQ path."""
    fault = random.choice(["bad_lat", "bad_lon", "future_ts", "missing_field"])
    event = _make_valid_event(equipo_id)
    if fault == "bad_lat":
        event["latitude"] = round(random.uniform(10, 90), 6)   # outside Perú
        event["_injected_fault"] = "bad_lat"
    elif fault == "bad_lon":
        event["longitude"] = round(random.uniform(-60, -40), 6)
        event["_injected_fault"] = "bad_lon"
    elif fault == "future_ts":
        event["timestamp"] = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
        event["_injected_fault"] = "future_ts"
    elif fault == "missing_field":
        del event["equipo_id"]
        event["_injected_fault"] = "missing_field"
    return event


def _kinesis_client() -> boto3.client:
    kwargs = {}
    if ep := os.getenv("AWS_ENDPOINT_URL"):
        kwargs["endpoint_url"] = ep
    return boto3.client("kinesis", **kwargs)


def main() -> None:
    kinesis = _kinesis_client()
    stream_name = os.getenv("KINESIS_STREAM_NAME", "gps-eventos")
    interval = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "5"))
    invalid_ratio = float(os.getenv("PRODUCER_INVALID_RATIO", "0.1"))

    logger.info("Producer started", extra={
        "stream": stream_name,
        "equipos": len(EQUIPOS),
        "interval_s": interval,
        "invalid_ratio": invalid_ratio,
    })

    while True:
        for equipo_id in EQUIPOS:
            event = (
                _make_invalid_event(equipo_id)
                if random.random() < invalid_ratio
                else _make_valid_event(equipo_id)
            )
            partition_key = event.get("equipo_id", equipo_id)  # fallback if field deleted
            try:
                resp = kinesis.put_record(
                    StreamName=stream_name,
                    Data=json.dumps(event).encode(),
                    PartitionKey=partition_key,
                )
                logger.info("Record published", extra={
                    "equipo_id": partition_key,
                    "shard": resp["ShardId"],
                    "fault": event.get("_injected_fault", "none"),
                })
            except Exception:
                logger.exception("Failed to publish", extra={"equipo_id": partition_key})
        time.sleep(interval)


if __name__ == "__main__":
    main()
