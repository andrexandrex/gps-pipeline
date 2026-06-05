import sys
import os

# ── Python path: replicate Lambda's flat package layout locally ──────────────
_LAMBDAS_DIR = os.path.join(os.path.dirname(__file__), "..", "src", "lambdas")
_SRC_DIR     = os.path.join(os.path.dirname(__file__), "..", "src")

for _p in (_SRC_DIR, _LAMBDAS_DIR):
    _p = os.path.abspath(_p)
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ── Default env vars for LocalStack ─────────────────────────────────────────
# setdefault: only sets if NOT already present — CI/CD can override via env.
# Without these, Lambda handlers create boto3 clients that try to reach real AWS.
_DEFAULTS = {
    "AWS_ACCESS_KEY_ID":             "test",
    "AWS_SECRET_ACCESS_KEY":         "test",
    "AWS_DEFAULT_REGION":            "us-east-1",
    "AWS_ENDPOINT_URL":              "http://localhost:4566",
    "DYNAMO_TABLE_NAME":             "gps-last-seen",
    "DEDUP_TABLE_NAME":              "gps-dedup",
    "SNS_TOPIC_ARN":                 "arn:aws:sns:us-east-1:000000000000:gps-alertas",
    "SILVER_BUCKET":                 "gps-silver",
    "BRONZE_BUCKET":                 "gps-bronze",
    "SIGNAL_LOSS_THRESHOLD_MINUTES":      "10",
    "AUTO_MAINTENANCE_THRESHOLD_MINUTES": "30",
}
for _k, _v in _DEFAULTS.items():
    os.environ.setdefault(_k, _v)
