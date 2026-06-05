"""
Tests for quality module: schemas + checker.
Unit tests — no AWS.
Run: pytest tests/test_quality.py -v
"""

import os
import sys
from datetime import datetime, timezone

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "lambdas"))


# ── Helpers ───────────────────────────────────────────────────────────────────

def _gps_df(overrides: dict | None = None) -> pd.DataFrame:
    rows = [
        {"equipo_id": "EQ001", "latitude": -9.1,  "longitude": -77.5,
         "timestamp": datetime.now(timezone.utc).isoformat(), "speed_kmh": 60.0},
        {"equipo_id": "EQ002", "latitude": -9.5,  "longitude": -77.0,
         "timestamp": datetime.now(timezone.utc).isoformat(), "speed_kmh": 80.0},
        {"equipo_id": "EQ003", "latitude": -8.0,  "longitude": -76.6,
         "timestamp": datetime.now(timezone.utc).isoformat(), "speed_kmh": 30.0},
    ]
    df = pd.DataFrame(rows)
    if overrides:
        for col, val in overrides.items():
            df.loc[0, col] = val
    return df


def _mant_df(overrides: dict | None = None) -> pd.DataFrame:
    rows = [
        {"equipo_id": "EQ001", "fecha_mantenimiento": "2024-01-15",
         "tipo_falla": "CRITICA", "estado": "RESUELTO"},
        {"equipo_id": "EQ002", "fecha_mantenimiento": "2024-02-20",
         "tipo_falla": "MENOR",   "estado": "PENDIENTE"},
    ]
    df = pd.DataFrame(rows)
    if overrides:
        for col, val in overrides.items():
            df.loc[0, col] = val
    return df


# ── Schema tests ──────────────────────────────────────────────────────────────

class TestGpsSchema:
    def test_valid_df_passes(self):
        from quality.checker import validate
        valid_df, rejected_df, metrics = validate(_gps_df(), "gps_eventos")
        assert len(valid_df) == 3
        assert len(rejected_df) == 0
        assert metrics["valid_pct"] == 100.0

    def test_out_of_bbox_lat_rejected(self):
        from quality.checker import validate
        df = _gps_df({"latitude": 20.0})  # outside Áncash
        _, rejected_df, metrics = validate(df, "gps_eventos")
        assert len(rejected_df) == 1
        assert metrics["rejected_rows"] == 1
        assert "latitude" in metrics["out_of_range_pct"]

    def test_out_of_bbox_lon_rejected(self):
        from quality.checker import validate
        df = _gps_df({"longitude": -60.0})
        _, rejected_df, metrics = validate(df, "gps_eventos")
        assert len(rejected_df) == 1

    def test_null_equipo_id_rejected(self):
        from quality.checker import validate
        df = _gps_df({"equipo_id": None})
        _, rejected_df, metrics = validate(df, "gps_eventos")
        assert len(rejected_df) == 1

    def test_invalid_speed_rejected(self):
        from quality.checker import validate
        df = _gps_df({"speed_kmh": 999.0})  # > 200
        _, rejected_df, _ = validate(df, "gps_eventos")
        assert len(rejected_df) == 1

    def test_completeness_metric_computed(self):
        from quality.checker import validate
        _, _, metrics = validate(_gps_df(), "gps_eventos")
        assert "equipo_id" in metrics["completeness_pct"]
        assert metrics["completeness_pct"]["equipo_id"] == 100.0


class TestMantSchema:
    def test_valid_df_passes(self):
        from quality.checker import validate
        valid_df, rejected_df, metrics = validate(_mant_df(), "mantenimientos")
        assert metrics["valid_pct"] == 100.0

    def test_invalid_tipo_falla_rejected(self):
        from quality.checker import validate
        df = _mant_df({"tipo_falla": "URGENTE"})
        _, rejected_df, _ = validate(df, "mantenimientos")
        assert len(rejected_df) == 1

    def test_unknown_dataset_raises(self):
        from quality.checker import validate
        with pytest.raises(ValueError, match="No schema registered"):
            validate(pd.DataFrame(), "unknown_dataset")


# ── Deduplication tests ────────────────────────────────────────────────────────

class TestDuplicates:
    def test_exact_duplicate_counted(self):
        from quality.checker import validate
        df = _gps_df()
        df = pd.concat([df, df.iloc[[0]]], ignore_index=True)  # duplicate row 0
        _, _, metrics = validate(df, "gps_eventos")
        assert metrics["duplicates_count"] >= 1
        assert metrics["duplicates_pct"] > 0

    def test_no_duplicates_in_clean_data(self):
        from quality.checker import validate
        _, _, metrics = validate(_gps_df(), "gps_eventos")
        assert metrics["duplicates_count"] == 0


# ── Metrics structure tests ───────────────────────────────────────────────────

class TestMetricsStructure:
    def test_all_required_keys_present(self):
        from quality.checker import validate
        _, _, metrics = validate(_gps_df(), "gps_eventos")
        required = {"dataset", "run_timestamp", "total_rows", "valid_rows",
                    "rejected_rows", "valid_pct", "duplicates_count",
                    "duplicates_pct", "completeness_pct", "out_of_range_pct",
                    "pandera_failures"}
        assert required <= set(metrics.keys())

    def test_valid_plus_rejected_equals_total(self):
        from quality.checker import validate
        df = _gps_df({"latitude": 20.0})
        _, _, metrics = validate(df, "gps_eventos")
        assert metrics["valid_rows"] + metrics["rejected_rows"] == metrics["total_rows"]


# ── Integration: write_metrics to LocalStack ─────────────────────────────────

class TestWriteMetricsIntegration:
    def test_metrics_written_to_gold(self):
        from quality.checker import validate, write_metrics
        import boto3

        kw = dict(endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
                  region_name=os.getenv("AWS_DEFAULT_REGION"),
                  aws_access_key_id="test", aws_secret_access_key="test")
        s3 = boto3.client("s3", **kw)

        # reset lazy client
        import quality.checker as mod
        mod._s3 = None

        _, _, metrics = validate(_gps_df(), "gps_eventos")
        json_key = write_metrics(metrics, "gps-gold")

        assert json_key.startswith("quality_metrics/gps_eventos/")
        # Verify file is in S3
        obj = s3.get_object(Bucket="gps-gold", Key=json_key)
        data = json_key  # just check key exists
        assert obj["ContentLength"] > 0
