"""
Tests for ingest_maintenance — unit + integration.
Unit tests use no AWS; integration tests require LocalStack.

Run unit only:    pytest tests/test_batch.py -v -k "not Integration"
Run integration:  pytest tests/test_batch.py -v -k "Integration"
"""

import io
import json
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


# ── helpers ───────────────────────────────────────────────────────────────────

def _csv_bytes(rows: list[dict]) -> bytes:
    return pd.DataFrame(rows).to_csv(index=False).encode()


def _good_rows() -> list[dict]:
    # PDF format: equipo_id, fecha (not fecha_mantenimiento), tipo_falla, criticidad
    return [
        {"equipo_id": "CAM_001", "fecha": "2026-05-20",
         "tipo_falla": "Falla Motor", "criticidad": "ALTA"},
        {"equipo_id": "CAM_002", "fecha": "2026-04-10",
         "tipo_falla": "Cambio aceite", "criticidad": "BAJA"},
    ]


def _s3_event(bucket: str, key: str) -> dict:
    return {"Records": [{"s3": {"bucket": {"name": bucket}, "object": {"key": key}}}]}


# ── Unit tests (no AWS) ───────────────────────────────────────────────────────

class TestNormalize:
    def test_renames_fecha_to_fecha_mantenimiento(self):
        from batch.ingest_maintenance import _normalize
        df = pd.DataFrame([{
            "equipo_id": "CAM_001", "fecha": "2026-05-20",
            "tipo_falla": "Falla Motor", "criticidad": "alta",
        }])
        result = _normalize(df)
        assert "fecha_mantenimiento" in result.columns
        assert "fecha" not in result.columns

    def test_uppercase_criticidad(self):
        from batch.ingest_maintenance import _normalize
        df = pd.DataFrame([{
            "equipo_id": "cam_001", "fecha": "2026-05-20",
            "tipo_falla": "Falla Motor", "criticidad": "alta",
        }])
        result = _normalize(df)
        assert result.loc[0, "criticidad"] == "ALTA"
        assert result.loc[0, "equipo_id"] == "CAM_001"

    def test_date_normalization(self):
        from batch.ingest_maintenance import _normalize
        df = pd.DataFrame([{"equipo_id": "CAM_001", "fecha": "20/05/2026",
                             "tipo_falla": "Falla Motor", "criticidad": "ALTA"}])
        result = _normalize(df)
        assert result.loc[0, "fecha_mantenimiento"] == "2026-05-20"

    def test_strips_whitespace(self):
        from batch.ingest_maintenance import _normalize
        df = pd.DataFrame([{"equipo_id": "  CAM_001  ", "fecha": "2026-05-20",
                             "tipo_falla": "Falla Motor", "criticidad": "ALTA"}])
        result = _normalize(df)
        assert result.loc[0, "equipo_id"] == "CAM_001"


class TestValidateRows:
    def test_valid_rows_pass(self):
        from batch.ingest_maintenance import _normalize, _validate_rows
        df = _normalize(pd.DataFrame(_good_rows()))
        valid, rejected = _validate_rows(df)
        assert len(valid) == 2
        assert len(rejected) == 0

    def test_invalid_criticidad_rejected(self):
        from batch.ingest_maintenance import _normalize, _validate_rows
        rows = _good_rows()
        rows[0]["criticidad"] = "URGENTE"   # not in VALID_CRITICIDAD
        df = _normalize(pd.DataFrame(rows))
        valid, rejected = _validate_rows(df)
        assert len(valid) == 1
        assert len(rejected) == 1
        assert "invalid_criticidad" in rejected.iloc[0]["rejection_reason"]

    def test_missing_equipo_id_rejected(self):
        from batch.ingest_maintenance import _normalize, _validate_rows
        rows = _good_rows()
        rows[0]["equipo_id"] = None
        df = _normalize(pd.DataFrame(rows))
        valid, rejected = _validate_rows(df)
        assert len(rejected) == 1
        assert "missing_equipo_id" in rejected.iloc[0]["rejection_reason"]

    def test_invalid_date_rejected(self):
        from batch.ingest_maintenance import _normalize, _validate_rows
        rows = _good_rows()
        rows[0]["fecha"] = "not-a-date"
        df = _normalize(pd.DataFrame(rows))
        valid, rejected = _validate_rows(df)
        assert len(rejected) == 1


class TestSilverKey:
    def test_csv_extension_replaced(self):
        from batch.ingest_maintenance import _silver_key
        assert _silver_key("mantenimientos/2024-01-15_mant.csv") == \
               "mantenimientos/2024-01-15_mant.parquet"

    def test_nested_path_uses_basename(self):
        from batch.ingest_maintenance import _silver_key
        key = _silver_key("bronze/mantenimientos/some_file.csv")
        assert key == "mantenimientos/some_file.parquet"


# ── Integration tests (LocalStack) ───────────────────────────────────────────

class TestIntegrationBatch:
    @pytest.fixture(autouse=True)
    def reset_s3_client(self):
        from batch import ingest_maintenance as mod
        mod._s3 = None
        yield
        mod._s3 = None

    def _upload_csv(self, s3_client, rows: list[dict], key: str) -> None:
        s3_client.put_object(
            Bucket="gps-bronze", Key=key, Body=_csv_bytes(rows)
        )

    def test_valid_csv_lands_in_silver(self):
        import boto3
        from batch.ingest_maintenance import handler

        kw = dict(endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
                  region_name=os.getenv("AWS_DEFAULT_REGION"),
                  aws_access_key_id="test", aws_secret_access_key="test")
        s3 = boto3.client("s3", **kw)

        key = "mantenimientos/test_valid.csv"
        self._upload_csv(s3, _good_rows(), key)

        result = handler(_s3_event("gps-bronze", key), None)
        assert result["processed"] == 1
        assert result["files"][0]["valid"] == 2
        assert result["files"][0]["rejected"] == 0

        # Verify Parquet is in silver
        objs = s3.list_objects_v2(Bucket="gps-silver", Prefix="mantenimientos/")
        assert objs.get("KeyCount", 0) >= 1

    def test_rejected_rows_land_in_bronze_rejected(self):
        import boto3
        from batch.ingest_maintenance import handler

        kw = dict(endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
                  region_name=os.getenv("AWS_DEFAULT_REGION"),
                  aws_access_key_id="test", aws_secret_access_key="test")
        s3 = boto3.client("s3", **kw)

        rows = _good_rows()
        rows[0]["criticidad"] = "URGENTE"   # invalid — triggers rejection
        key = "mantenimientos/test_partial.csv"
        self._upload_csv(s3, rows, key)

        result = handler(_s3_event("gps-bronze", key), None)
        assert result["files"][0]["valid"] == 1
        assert result["files"][0]["rejected"] == 1

        objs = s3.list_objects_v2(Bucket="gps-bronze", Prefix="bronze_rejected/mantenimientos/")
        assert objs.get("KeyCount", 0) >= 1

    def test_idempotency_same_key_overwrites(self):
        import boto3
        from batch.ingest_maintenance import handler

        kw = dict(endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
                  region_name=os.getenv("AWS_DEFAULT_REGION"),
                  aws_access_key_id="test", aws_secret_access_key="test")
        s3 = boto3.client("s3", **kw)

        key = "mantenimientos/test_idem.csv"
        self._upload_csv(s3, _good_rows(), key)

        r1 = handler(_s3_event("gps-bronze", key), None)
        r2 = handler(_s3_event("gps-bronze", key), None)

        # Both runs produce the same valid count
        assert r1["files"][0]["silver_key"] == r2["files"][0]["silver_key"]

        # Only one Parquet file exists (second run overwrote)
        objs = s3.list_objects_v2(
            Bucket="gps-silver",
            Prefix="mantenimientos/test_idem"
        )
        assert objs.get("KeyCount", 0) == 1
