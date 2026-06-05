"""
GPS event simulator — publishes synthetic SQS messages for Áncash, Perú.
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

# Equipo IDs match the PDF format: CAM_001, CAM_002, ...
EQUIPOS = [f"CAM_{str(i).zfill(3)}" for i in range(1, 11)]
ESTADOS = ["ACTIVO", "EN_RUTA", "DETENIDO"]


def _make_valid_event(equipo_id: str) -> dict:
    # Field names match the PDF GPS event schema exactly:
    # latitud, longitud, velocidad, estado — normalized to English in the Lambda
    return {
        "equipo_id": equipo_id,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "latitud":    round(random.uniform(*ANCASH["lat"]), 6),
        "longitud":   round(random.uniform(*ANCASH["lon"]), 6),
        "velocidad":  round(random.uniform(0, 120), 1),
        "estado":     random.choice(ESTADOS),
    }


def _make_invalid_event(equipo_id: str) -> dict:
    """Returns a deliberately malformed event to test the DLQ path."""
    fault = random.choice(["bad_lat", "bad_lon", "future_ts", "missing_field"])
    event = _make_valid_event(equipo_id)
    if fault == "bad_lat":
        event["latitud"] = round(random.uniform(10, 90), 6)   # outside Perú
        event["_injected_fault"] = "bad_lat"
    elif fault == "bad_lon":
        event["longitud"] = round(random.uniform(-60, -40), 6)
        event["_injected_fault"] = "bad_lon"
    elif fault == "future_ts":
        event["timestamp"] = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
        event["_injected_fault"] = "future_ts"
    elif fault == "missing_field":
        del event["equipo_id"]
        event["_injected_fault"] = "missing_field"
    return event


def _sqs_client():
    kwargs = {}
    if ep := os.getenv("AWS_ENDPOINT_URL"):
        kwargs["endpoint_url"] = ep
    return boto3.client("sqs", **kwargs)


def _get_queue_url(sqs) -> str:
    url = os.getenv("SQS_GPS_QUEUE_URL", "")
    if url:
        return url
    queue_name = os.getenv("SQS_GPS_QUEUE_NAME", "gps-eventos")
    return sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]


def main() -> None:
    sqs = _sqs_client()
    queue_url = _get_queue_url(sqs)
    interval = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "5"))
    invalid_ratio = float(os.getenv("PRODUCER_INVALID_RATIO", "0.1"))

    logger.info("Producer started", extra={
        "queue": queue_url,
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
            try:
                resp = sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(event),
                )
                logger.info("Record published", extra={
                    "equipo_id": event.get("equipo_id", equipo_id),
                    "message_id": resp["MessageId"],
                    "fault": event.get("_injected_fault", "none"),
                })
            except Exception:
                logger.exception("Failed to publish", extra={"equipo_id": equipo_id})
        time.sleep(interval)


if __name__ == "__main__":
    main()
