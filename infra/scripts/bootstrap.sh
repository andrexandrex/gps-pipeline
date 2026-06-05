#!/bin/bash
# Idempotent bootstrap: creates all AWS resources against LocalStack.
# Safe to re-run; uses --no-cli-pager to avoid pagination hangs in CI.
set -euo pipefail

EP="--endpoint-url ${AWS_ENDPOINT_URL:-http://localhost:4566}"
REGION="us-east-1"
ACCOUNT="000000000000"   # LocalStack default

echo "[bootstrap] Creating S3 buckets (medallion architecture)..."
for BUCKET in gps-bronze gps-silver gps-gold; do
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" $EP \
    --no-cli-pager 2>/dev/null || echo "  $BUCKET already exists, skipping"
done

# Enable versioning on bronze for reprocessing safety
aws s3api put-bucket-versioning \
  --bucket gps-bronze \
  --versioning-configuration Status=Enabled $EP --no-cli-pager

echo "[bootstrap] Creating Kinesis stream..."
aws kinesis create-stream \
  --stream-name gps-eventos \
  --shard-count 2 \
  --region "$REGION" $EP --no-cli-pager 2>/dev/null || echo "  stream already exists"

echo "[bootstrap] Creating DynamoDB table (last-seen tracking)..."
aws dynamodb create-table \
  --table-name gps-last-seen \
  --attribute-definitions AttributeName=equipo_id,AttributeType=S \
  --key-schema AttributeName=equipo_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region "$REGION" $EP --no-cli-pager 2>/dev/null || echo "  table already exists"

echo "[bootstrap] Creating SNS topic for alerts..."
aws sns create-topic --name gps-alertas --region "$REGION" $EP --no-cli-pager 2>/dev/null || echo "  topic already exists"

echo "[bootstrap] Creating SQS DLQ for Lambda failures..."
aws sqs create-queue \
  --queue-name gps-validate-dlq \
  --region "$REGION" $EP --no-cli-pager 2>/dev/null || echo "  DLQ already exists"

echo "[bootstrap] Creating DynamoDB table (deduplication with TTL)..."
aws dynamodb create-table \
  --table-name gps-dedup \
  --attribute-definitions AttributeName=record_id,AttributeType=S \
  --key-schema AttributeName=record_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region "$REGION" $EP --no-cli-pager 2>/dev/null || echo "  gps-dedup already exists"

# Enable TTL so dedup entries self-expire after 24h without a manual purge job
aws dynamodb update-time-to-live \
  --table-name gps-dedup \
  --time-to-live-specification "Enabled=true,AttributeName=ttl" \
  --region "$REGION" $EP --no-cli-pager 2>/dev/null || true

echo "[bootstrap] Creating S3 prefixes (touch placeholder objects)..."
for PREFIX in bronze/gps_eventos/ bronze/mantenimientos/ silver/gps_eventos/ silver/mantenimientos/ gold/quality_metrics/; do
  aws s3api put-object --bucket gps-bronze --key "${PREFIX}.keep" $EP --no-cli-pager 2>/dev/null || true
done

echo "[bootstrap] Creating Glue database and catalog tables..."
aws glue create-database \
  --database-input '{"Name":"gps_pipeline","Description":"GPS Pipeline Ancash Peru"}' \
  --region "$REGION" $EP --no-cli-pager 2>/dev/null || echo "  gps_pipeline db already exists"

# gps_eventos table — points to silver/gps_eventos/
aws glue create-table \
  --database-name gps_pipeline \
  --table-input '{
    "Name": "gps_eventos",
    "StorageDescriptor": {
      "Columns": [
        {"Name":"equipo_id","Type":"string"},
        {"Name":"latitude","Type":"double"},
        {"Name":"longitude","Type":"double"},
        {"Name":"timestamp","Type":"string"},
        {"Name":"speed_kmh","Type":"double"},
        {"Name":"heading","Type":"double"}
      ],
      "Location": "s3://gps-silver/gps_eventos/",
      "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      "SerdeInfo": {"SerializationLibrary":"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}
    },
    "PartitionKeys": [
      {"Name":"year","Type":"int"},{"Name":"month","Type":"int"},
      {"Name":"day","Type":"int"},{"Name":"hour","Type":"int"}
    ],
    "TableType": "EXTERNAL_TABLE"
  }' \
  --region "$REGION" $EP --no-cli-pager 2>/dev/null || echo "  gps_eventos table already exists"

# mantenimientos table — points to silver/mantenimientos/
aws glue create-table \
  --database-name gps_pipeline \
  --table-input '{
    "Name": "mantenimientos",
    "StorageDescriptor": {
      "Columns": [
        {"Name":"equipo_id","Type":"string"},
        {"Name":"fecha_mantenimiento","Type":"string"},
        {"Name":"tipo_falla","Type":"string"},
        {"Name":"descripcion","Type":"string"},
        {"Name":"tecnico","Type":"string"},
        {"Name":"estado","Type":"string"}
      ],
      "Location": "s3://gps-silver/mantenimientos/",
      "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      "SerdeInfo": {"SerializationLibrary":"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}
    },
    "TableType": "EXTERNAL_TABLE"
  }' \
  --region "$REGION" $EP --no-cli-pager 2>/dev/null || echo "  mantenimientos table already exists"

echo "[bootstrap] Done. Resources ready on ${AWS_ENDPOINT_URL:-http://localhost:4566}"
