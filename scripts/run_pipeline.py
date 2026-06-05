"""
Demo pipeline runner — simulates the full GPS + batch flow locally.

This script replaces the Lambda invocations for local demo:
  1. Reads events from Kinesis (or generates them if --generate)
  2. Runs validate_gps handler on them → writes to silver/
  3. Optionally ingests the sample CSV → silver/mantenimientos/
  4. Runs the quality checker → writes metrics to gold/
  5. Prints a summary of what happened

Run:
    python3 scripts/run_pipeline.py               # process what's in Kinesis
    python3 scripts/run_pipeline.py --generate    # generate + process 50 events
    python3 scripts/run_pipeline.py --batch       # also ingest CSV
    python3 scripts/run_pipeline.py --quality     # also run quality checker
    python3 scripts/run_pipeline.py --all         # everything

Why this exists: in LocalStack free tier, the Kinesis→Lambda event source
mapping trigger needs Lambda to be deployed as a zip. This script calls the
handlers directly in Python, giving the same result faster and without packaging.
"""

import argparse
import base64
import io
import json
import os
import sys
import time
from pathlib import Path

# ── path setup ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "src" / "lambdas"))

# ── env vars for LocalStack ───────────────────────────────────────────────────
os.environ.setdefault("AWS_ACCESS_KEY_ID",             "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY",         "test")
os.environ.setdefault("AWS_DEFAULT_REGION",            "us-east-1")
os.environ.setdefault("AWS_ENDPOINT_URL",              "http://localhost:4566")
os.environ.setdefault("KINESIS_STREAM_NAME",           "gps-eventos")
os.environ.setdefault("DYNAMO_TABLE_NAME",             "gps-last-seen")
os.environ.setdefault("DEDUP_TABLE_NAME",              "gps-dedup")
os.environ.setdefault("SNS_TOPIC_ARN",                 "arn:aws:sns:us-east-1:000000000000:gps-alertas")
os.environ.setdefault("SILVER_BUCKET",                 "gps-silver")
os.environ.setdefault("BRONZE_BUCKET",                 "gps-bronze")
os.environ.setdefault("GOLD_BUCKET",                   "gps-gold")
os.environ.setdefault("SIGNAL_LOSS_THRESHOLD_MINUTES", "10")

import boto3

EP  = os.environ["AWS_ENDPOINT_URL"]
_kw = dict(endpoint_url=EP,
           region_name=os.environ["AWS_DEFAULT_REGION"],
           aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
           aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"])

# ── helpers ───────────────────────────────────────────────────────────────────

def _kinesis_event_from_records(records: list[dict]) -> dict:
    """Wrap dicts into the Kinesis Lambda event format."""
    return {
        "Records": [
            {
                "kinesis": {
                    "data": base64.b64encode(json.dumps(r).encode()).decode(),
                    "partitionKey": r.get("equipo_id", "CAM_000"),
                    "sequenceNumber": f"seq_{i}",
                },
                "eventSource": "aws:kinesis",
            }
            for i, r in enumerate(records)
        ]
    }


def generate_gps_events(n: int = 50) -> list[dict]:
    """Generate n GPS events using the simulator."""
    import random
    from datetime import datetime, timezone
    from producer.simulator import _make_valid_event, _make_invalid_event, EQUIPOS
    events = []
    for i in range(n):
        equipo = random.choice(EQUIPOS)
        if random.random() < 0.1:  # 10% invalid
            events.append(_make_invalid_event(equipo))
        else:
            events.append(_make_valid_event(equipo))
    return events


def read_kinesis_events(max_records: int = 500) -> list[dict]:
    """Read pending events from all Kinesis shards."""
    kinesis = boto3.client("kinesis", **_kw)
    stream = os.environ["KINESIS_STREAM_NAME"]

    shards = kinesis.describe_stream_summary(StreamName=stream)
    shard_count = shards["StreamDescriptionSummary"]["OpenShardCount"]

    all_records = []
    for i in range(shard_count):
        shard_id = f"shardId-{str(i).zfill(12)}"
        try:
            it = kinesis.get_shard_iterator(
                StreamName=stream, ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON"
            )["ShardIterator"]
            while it and len(all_records) < max_records:
                resp = kinesis.get_records(ShardIterator=it, Limit=100)
                for r in resp["Records"]:
                    try:
                        all_records.append(json.loads(r["Data"].decode()))
                    except Exception:
                        pass
                it = resp.get("NextShardIterator")
                if not resp["Records"]:
                    break
        except Exception:
            pass
    return all_records


def step_validate(events: list[dict]) -> dict:
    """Run validate_gps handler on a list of events."""
    # Reset lazy clients so env vars take effect
    import lambdas.validate_gps.handler as mod
    mod._dynamo = None
    mod._s3 = None

    from lambdas.validate_gps.handler import handler
    kinesis_event = _kinesis_event_from_records(events)
    result = handler(kinesis_event, None)
    return result


def step_batch_csv() -> dict:
    """Upload sample CSV to bronze and run ingest_maintenance."""
    s3 = boto3.client("s3", **_kw)
    csv_path = ROOT / "data" / "mantenimiento_sample.csv"
    key = f"mantenimientos/mantenimiento_demo.csv"
    s3.upload_file(str(csv_path), os.environ["BRONZE_BUCKET"], key)
    print(f"  ↑ Uploaded {csv_path.name} → s3://gps-bronze/{key}")

    import batch.ingest_maintenance as mod
    mod._s3 = None
    from batch.ingest_maintenance import handler
    result = handler({"Records": [{"s3": {"bucket": {"name": "gps-bronze"},
                                           "object": {"key": key}}}]}, None)
    return result


def step_quality(datasets: list[str] | None = None) -> dict:
    """Run quality checker on silver datasets."""
    import quality.checker as mod
    mod._s3 = None

    from quality.checker import read_silver, validate, write_metrics
    datasets = datasets or ["gps_eventos", "mantenimientos"]
    results = {}
    for ds in datasets:
        prefix_map = {"gps_eventos": "gps_eventos/", "mantenimientos": "mantenimientos/"}
        df = read_silver(os.environ["SILVER_BUCKET"], prefix_map[ds])
        if df.empty:
            results[ds] = {"status": "no_data"}
            continue
        _, _, metrics = validate(df, ds)
        write_metrics(metrics, os.environ["GOLD_BUCKET"])
        results[ds] = metrics
    return results


def step_signal_loss() -> dict:
    """Run detect_signal_loss handler."""
    import lambdas.detect_signal_loss.handler as mod
    mod._dynamo = None
    mod._sns = None
    from lambdas.detect_signal_loss.handler import handler
    return handler({}, None)


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Run GPS pipeline locally")
    parser.add_argument("--generate", action="store_true",
                        help="Generate 50 GPS events (don't read from Kinesis)")
    parser.add_argument("--events", type=int, default=50,
                        help="Number of events to generate (default: 50)")
    parser.add_argument("--batch",   action="store_true", help="Also run CSV batch ingestion")
    parser.add_argument("--quality", action="store_true", help="Also run quality checker")
    parser.add_argument("--signals", action="store_true", help="Also run signal loss detection")
    parser.add_argument("--all",     action="store_true", help="Run everything")
    args = parser.parse_args()

    if args.all:
        args.generate = args.batch = args.quality = args.signals = True

    print("\n" + "═"*52)
    print("  GPS Pipeline — Demo Local")
    print("═"*52)

    # ── Step 1: GPS events ─────────────────────────────────────────────────
    print(f"\n{'─'*52}")
    if args.generate:
        print(f"[1/4] Generando {args.events} eventos GPS...")
        events = generate_gps_events(args.events)
        print(f"  ✓ {len(events)} eventos generados ({int(len(events)*0.1)} inválidos esperados)")
    else:
        print("[1/4] Leyendo eventos de Kinesis...")
        events = read_kinesis_events(max_records=500)
        if not events:
            print("  ⚠ No hay eventos en Kinesis. Usa --generate para crear eventos de prueba.")
            print("    O corre el producer en otra terminal:")
            print("    PYTHONPATH=src:src/lambdas AWS_ACCESS_KEY_ID=test ...")
            print("    python3 -m producer.simulator")
            events = generate_gps_events(args.events)
            print(f"  → Generando {len(events)} eventos de todos modos...")
        else:
            print(f"  ✓ {len(events)} eventos leídos de Kinesis")

    # ── Step 2: Validate GPS ───────────────────────────────────────────────
    print(f"\n{'─'*52}")
    print("[2/4] Validando eventos GPS (validate_gps Lambda)...")
    t = time.time()
    result = step_validate(events)
    elapsed = time.time() - t
    print(f"  ✓ Procesados en {elapsed:.1f}s")
    print(f"  ├── Válidos   → silver/gps_eventos/:   {result['valid']:>4} registros")
    print(f"  └── Rechazados → bronze_rejected/:     {result['rejected']:>4} registros")

    # ── Step 3: Batch CSV (opcional) ───────────────────────────────────────
    if args.batch or args.all:
        print(f"\n{'─'*52}")
        print("[3/4] Procesando CSV de mantenimiento (ingest_maintenance)...")
        b = step_batch_csv()
        if b.get("files"):
            f = b["files"][0]
            print(f"  ✓ CSV procesado")
            print(f"  ├── Válidos   → silver/mantenimientos/:  {f['valid']:>4} filas")
            print(f"  └── Rechazadas → bronze_rejected/:       {f['rejected']:>4} filas")
    else:
        print(f"\n{'─'*52}")
        print("[3/4] Batch CSV omitido  (usa --batch para incluirlo)")

    # ── Step 4: Quality checker (opcional) ─────────────────────────────────
    if args.quality or args.all:
        print(f"\n{'─'*52}")
        print("[4/4] Calculando métricas de calidad (Pandera → gold/)...")
        q = step_quality()
        for ds, m in q.items():
            if m.get("status") == "no_data":
                print(f"  ⚠ {ds}: sin datos en silver/")
                continue
            print(f"  ✓ {ds}:")
            print(f"     Total: {m['total_rows']} filas | Válidas: {m['valid_pct']:.1f}%"
                  f" | Duplicados: {m['duplicates_pct']:.1f}%")
    else:
        print(f"\n{'─'*52}")
        print("[4/4] Quality checker omitido  (usa --quality para incluirlo)")

    # ── Signal loss (opcional) ─────────────────────────────────────────────
    if args.signals:
        print(f"\n{'─'*52}")
        print("[+] Detección de pérdida de señal...")
        s = step_signal_loss()
        if s["lost"] == 0:
            print("  ✓ Todos los equipos con señal reciente")
        else:
            print(f"  ⚠ {s['lost']} equipo(s) sin señal: {s['equipos']}")

    # ── Summary ────────────────────────────────────────────────────────────
    print(f"\n{'═'*52}")
    print("  RESUMEN")
    print(f"{'─'*52}")
    s3 = boto3.client("s3", **_kw)
    gps_n  = len(s3.list_objects_v2(Bucket="gps-silver",  Prefix="gps_eventos/").get("Contents", []))
    mant_n = len(s3.list_objects_v2(Bucket="gps-silver",  Prefix="mantenimientos/").get("Contents", []))
    gold_n = len(s3.list_objects_v2(Bucket="gps-gold",    Prefix="quality_metrics/").get("Contents", []))
    dyn_n  = boto3.client("dynamodb", **_kw).scan(TableName="gps-last-seen", Select="COUNT")["Count"]
    print(f"  silver/gps_eventos/:    {gps_n:>4} Parquet files")
    print(f"  silver/mantenimientos/: {mant_n:>4} Parquet files")
    print(f"  gold/quality_metrics/:  {gold_n:>4} archivos")
    print(f"  DynamoDB last-seen:     {dyn_n:>4} equipos rastreados")
    print(f"\n  Dashboard: http://localhost:8501")
    print("  Para ver el dashboard:")
    print("  source .venv/bin/activate")
    print("  PYTHONPATH=src:src/lambdas streamlit run src/dashboard/app.py")
    print("═"*52 + "\n")


if __name__ == "__main__":
    main()
