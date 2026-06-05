"""
Unit tests for validate_gps logic — no AWS calls needed.
Run: pytest tests/test_validate_gps.py -v
"""

import base64
import json
import sys
import os
from datetime import datetime, timedelta, timezone

import pytest

# Make src importable without installing the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from lambdas.validate_gps.handler import _validate, _decode

# ── _validate ────────────────────────────────────────────────────────────────

def _good() -> dict:
    # Post-normalization record (latitud→latitude, longitud→longitude done by handler)
    return {
        "equipo_id": "CAM_001",
        "latitude":  -9.1,
        "longitude": -77.5,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "speed_kmh": 60.0,
        "estado":    "ACTIVO",
    }


def test_valid_record_passes():
    ok, reason = _validate(_good())
    assert ok is True
    assert reason == "ok"


def test_missing_field_rejected():
    rec = _good()
    del rec["equipo_id"]
    ok, reason = _validate(rec)
    assert ok is False
    assert "missing_fields" in reason


def test_latitude_too_high_rejected():
    rec = _good()
    rec["latitude"] = 20.0   # outside Áncash
    ok, reason = _validate(rec)
    assert ok is False
    assert "lat_out_of_bbox" in reason


def test_latitude_too_low_rejected():
    rec = _good()
    rec["latitude"] = -11.0
    ok, reason = _validate(rec)
    assert ok is False


def test_longitude_out_of_range_rejected():
    rec = _good()
    rec["longitude"] = -60.0
    ok, reason = _validate(rec)
    assert ok is False
    assert "lon_out_of_bbox" in reason


def test_future_timestamp_rejected():
    rec = _good()
    rec["timestamp"] = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    ok, reason = _validate(rec)
    assert ok is False
    assert "future_timestamp" in reason


def test_stale_timestamp_rejected():
    rec = _good()
    rec["timestamp"] = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    ok, reason = _validate(rec)
    assert ok is False
    assert "stale_timestamp" in reason


def test_unparseable_timestamp_rejected():
    rec = _good()
    rec["timestamp"] = "not-a-date"
    ok, reason = _validate(rec)
    assert ok is False
    assert "unparseable_timestamp" in reason


def test_boundary_lat_min_accepted():
    rec = _good()
    rec["latitude"] = -10.5   # exactly on the southern boundary
    ok, _ = _validate(rec)
    assert ok is True


def test_boundary_lat_max_accepted():
    rec = _good()
    rec["latitude"] = -7.8
    ok, _ = _validate(rec)
    assert ok is True


# ── _decode ──────────────────────────────────────────────────────────────────

def test_decode_kinesis_record():
    # Decode should return raw PDF field names; normalization happens in handler
    payload = {"equipo_id": "CAM_001", "latitud": -9.0, "longitud": -77.0}
    encoded = base64.b64encode(json.dumps(payload).encode()).decode()
    raw = {"kinesis": {"data": encoded}}
    result = _decode(raw)
    assert result["equipo_id"] == "CAM_001"
    assert "latitud" in result   # raw, not yet normalized


def test_normalize_fields():
    from lambdas.validate_gps.handler import _normalize_fields
    raw = {"equipo_id": "CAM_001", "latitud": -9.0, "longitud": -77.5,
           "velocidad": 60.0, "estado": "ACTIVO", "timestamp": "2024-01-01T00:00:00Z"}
    normalized = _normalize_fields(raw)
    assert "latitude"  in normalized and "latitud"   not in normalized
    assert "longitude" in normalized and "longitud"  not in normalized
    assert "speed_kmh" in normalized and "velocidad" not in normalized
    assert normalized["estado"] == "ACTIVO"   # passthrough unchanged
