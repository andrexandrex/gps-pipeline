"""
Integration tests against LocalStack.
Requires LocalStack running: docker compose up -d
Run: pytest tests/test_integration.py -v -s

Uses real boto3 calls with AWS_ENDPOINT_URL=http://localhost:4566.
No moto — we want to catch LocalStack-specific behaviour.
"""

import base64
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import boto3
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

EP = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
_kw = dict(endpoint_url=EP, region_name=REGION,
           aws_access_key_id="test", aws_secret_access_key="test")


# All equipo_ids created by integration tests — cleaned up after the session
_TEST_EQUIPO_IDS = [
    "CAM_IT_01", "CAM_IT_02", "CAM_IT_03",
    "CAM_LAST_SEEN", "CAM_ACTIVE_TEST", "CAM_SILENT_TEST", "CAM_MAINT_TEST",
]


@pytest.fixture(scope="session")
def dynamo():
    return boto3.client("dynamodb", **_kw)


@pytest.fixture(scope="session", autouse=True)
def cleanup_test_data(dynamo):
    """Remove all test equipo entries from DynamoDB after the session."""
    yield
    for eid in _TEST_EQUIPO_IDS:
        try:
            dynamo.delete_item(
                TableName="gps-last-seen",
                Key={"equipo_id": {"S": eid}},
            )
        except Exception:
            pass

@pytest.fixture(scope="session")
def s3():
    return boto3.client("s3", **_kw)


@pytest.fixture(scope="session")
def kinesis():
    return boto3.client("kinesis", **_kw)


# ── helpers ──────────────────────────────────────────────────────────────────

def _kinesis_event(records: list[dict]) -> dict:
    """Wrap records in a Kinesis Lambda event payload."""
    return {
        "Records": [
            {
                "kinesis": {
                    "data": base64.b64encode(json.dumps(r).encode()).decode(),
                    "partitionKey": r.get("equipo_id", "EQ000"),
                    "sequenceNumber": f"seq_{i}",
                },
                "eventSource": "aws:kinesis",
            }
            for i, r in enumerate(records)
        ]
    }


def _good_record(equipo_id: str = "CAM_001") -> dict:
    # PDF field names — handler normalizes latitud→latitude etc.
    return {
        "equipo_id": equipo_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "latitud":   -9.1,
        "longitud":  -77.5,
        "velocidad": 60.0,
        "estado":    "ACTIVO",
    }


# ── Tests ────────────────────────────────────────────────────────────────────

class TestValidateGps:
    def test_valid_record_lands_in_silver(self, s3):
        from lambdas.validate_gps.handler import handler

        os.environ.update({
            "AWS_ENDPOINT_URL": EP,
            "SILVER_BUCKET": "gps-silver",
            "BRONZE_BUCKET": "gps-bronze",
            "DYNAMO_TABLE_NAME": "gps-last-seen",
            "DEDUP_TABLE_NAME": "gps-dedup",
        })

        result = handler(_kinesis_event([_good_record("CAM_IT_01")]), None)
        assert result["valid"] == 1
        assert result["rejected"] == 0

        # Verify Parquet landed in silver
        objs = s3.list_objects_v2(Bucket="gps-silver", Prefix="gps_eventos/")
        assert objs.get("KeyCount", 0) >= 1

    def test_invalid_record_lands_in_bronze_rejected(self, s3):
        from lambdas.validate_gps import handler as mod
        # Reset lazy clients so env vars take effect
        mod._dynamo = None
        mod._s3 = None

        bad = _good_record("CAM_IT_02")
        bad["latitude"] = 50.0   # outside Áncash

        result = mod.handler(_kinesis_event([bad]), None)
        assert result["rejected"] == 1

        objs = s3.list_objects_v2(Bucket="gps-bronze", Prefix="bronze_rejected/")
        assert objs.get("KeyCount", 0) >= 1

    def test_duplicate_record_is_skipped(self):
        from lambdas.validate_gps import handler as mod
        mod._dynamo = None
        mod._s3 = None

        rec = _good_record("CAM_IT_03")
        event = _kinesis_event([rec])

        first = mod.handler(event, None)
        second = mod.handler(event, None)   # same event replayed

        # First run processes 1 valid; second run 0 (dedup kicks in)
        assert first["valid"] == 1
        assert second["valid"] == 0

    def test_last_seen_updated_in_dynamodb(self, dynamo):
        from lambdas.validate_gps import handler as mod
        mod._dynamo = None
        mod._s3 = None

        rec = _good_record("CAM_LAST_SEEN")
        mod.handler(_kinesis_event([rec]), None)

        item = dynamo.get_item(
            TableName="gps-last-seen",
            Key={"equipo_id": {"S": "CAM_LAST_SEEN"}},
        ).get("Item")
        assert item is not None
        assert "last_seen" in item


class TestDetectSignalLoss:
    @pytest.fixture(autouse=True)
    def reset_clients(self):
        from lambdas.detect_signal_loss import handler as mod
        mod._dynamo = mod._sns = mod._s3 = None
        yield
        mod._dynamo = mod._sns = mod._s3 = None

    def test_no_alert_when_devices_active(self, dynamo):
        from lambdas.detect_signal_loss.handler import handler
        dynamo.put_item(
            TableName="gps-last-seen",
            Item={
                "equipo_id": {"S": "CAM_ACTIVE_TEST"},
                "last_seen": {"S": datetime.now(timezone.utc).isoformat()},
            },
        )
        os.environ.update({
            "AWS_ENDPOINT_URL": EP,
            "DYNAMO_TABLE_NAME": "gps-last-seen",
            "SNS_TOPIC_ARN": f"arn:aws:sns:{REGION}:000000000000:gps-alertas",
            "SIGNAL_LOSS_THRESHOLD_MINUTES": "10",
            "AUTO_MAINTENANCE_THRESHOLD_MINUTES": "30",
        })
        result = handler({}, None)
        assert "CAM_ACTIVE_TEST" not in result.get("equipos", [])

    def test_alert_when_device_silent(self, dynamo):
        from lambdas.detect_signal_loss.handler import handler
        old_ts = (datetime.now(timezone.utc) - timedelta(minutes=20)).isoformat()
        dynamo.put_item(
            TableName="gps-last-seen",
            Item={
                "equipo_id": {"S": "CAM_SILENT_TEST"},
                "last_seen": {"S": old_ts},
            },
        )
        os.environ["SIGNAL_LOSS_THRESHOLD_MINUTES"] = "10"
        os.environ["AUTO_MAINTENANCE_THRESHOLD_MINUTES"] = "30"
        result = handler({}, None)
        assert "CAM_SILENT_TEST" in result.get("equipos", [])
        assert result["alert_lost"] >= 1

    def test_auto_maintenance_created_for_long_silence(self, dynamo, s3):
        from lambdas.detect_signal_loss.handler import handler
        # Silent for 40 minutes — exceeds both alert (10 min) and maint (30 min) thresholds
        old_ts = (datetime.now(timezone.utc) - timedelta(minutes=40)).isoformat()
        dynamo.put_item(
            TableName="gps-last-seen",
            Item={
                "equipo_id": {"S": "CAM_MAINT_TEST"},
                "last_seen": {"S": old_ts},
            },
        )
        os.environ.update({
            "SIGNAL_LOSS_THRESHOLD_MINUTES": "10",
            "AUTO_MAINTENANCE_THRESHOLD_MINUTES": "30",
            "SILVER_BUCKET": "gps-silver",
        })
        result = handler({}, None)
        assert result["maint_created"] >= 1
        assert result["maint_key"] is not None
        # Verify Parquet landed in silver/mantenimientos/
        objs = s3.list_objects_v2(Bucket="gps-silver", Prefix="mantenimientos/auto_gps_loss_")
        assert objs.get("KeyCount", 0) >= 1
