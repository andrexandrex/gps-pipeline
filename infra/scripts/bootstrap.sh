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

echo "[bootstrap] Done. Resources ready on ${AWS_ENDPOINT_URL:-http://localhost:4566}"
