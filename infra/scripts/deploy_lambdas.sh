#!/bin/bash
# Deploy Lambda function code to LocalStack or real AWS.
#
# Real AWS mode (default when AWS_ENDPOINT_URL is unset):
#   Terraform already created the functions and set env vars.
#   This script only pushes the new zip code so they update atomically.
#
# LocalStack mode (AWS_ENDPOINT_URL=http://localhost:4566):
#   Creates functions from scratch with all environment variables.
#
set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
BUILD="${BUILD_DIR:-$(cd "$(dirname "$0")/../../build" && pwd)}"

# ── Detect mode ───────────────────────────────────────────────────────────────
if [ -z "${AWS_ENDPOINT_URL:-}" ]; then
  echo "[deploy] Mode: real AWS (Terraform manages function config)"
  EP_FLAG=""
  REAL_AWS=true
else
  echo "[deploy] Mode: LocalStack (${AWS_ENDPOINT_URL})"
  EP_FLAG="--endpoint-url ${AWS_ENDPOINT_URL}"
  REAL_AWS=false
fi

# ── Real AWS: just update code ─────────────────────────────────────────────────
if [ "$REAL_AWS" = true ]; then
  for NAME in validate_gps detect_signal_loss ingest_maintenance; do
    ZIP="${BUILD}/${NAME}.zip"
    if [ ! -f "$ZIP" ]; then
      echo "[deploy] WARNING: $ZIP not found — skipping $NAME"
      continue
    fi
    echo "[deploy] Updating $NAME code..."
    aws lambda update-function-code \
      --function-name "$NAME" \
      --zip-file "fileb://$ZIP" \
      --region "$REGION" \
      --no-cli-pager
    # Wait for update to propagate before next function
    aws lambda wait function-updated \
      --function-name "$NAME" \
      --region "$REGION" 2>/dev/null || true
    echo "  ✓ $NAME updated"
  done
  echo "[deploy] All Lambdas updated."
  exit 0
fi

# ── LocalStack: full create/update ───────────────────────────────────────────
ACCOUNT="${AWS_ACCOUNT_ID:-000000000000}"
ROLE_ARN="arn:aws:iam::${ACCOUNT}:role/lambda-gps-role"
SRC="$(cd "$(dirname "$0")/../../src" && pwd)"
TMP="${BUILD}/lambda-staging"

SILVER_BUCKET="${SILVER_BUCKET:-gps-silver}"
BRONZE_BUCKET="${BRONZE_BUCKET:-gps-bronze}"
SNS_ARN="${SNS_TOPIC_ARN:-arn:aws:sns:${REGION}:${ACCOUNT}:gps-alertas}"

echo "[deploy] Creating Lambda IAM role..."
aws iam create-role \
  --role-name lambda-gps-role \
  --assume-role-policy-document \
  '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
  $EP_FLAG --region "$REGION" --no-cli-pager 2>/dev/null || echo "  role exists"
aws iam attach-role-policy \
  --role-name lambda-gps-role \
  --policy-arn arn:aws:iam::aws:policy/AdministratorAccess \
  $EP_FLAG --region "$REGION" --no-cli-pager 2>/dev/null || true

# Helper: package one Lambda
package() {
  local NAME="$1"
  local DIR="$2"
  local ZIP="${BUILD}/${NAME}.zip"
  echo "[deploy] Packaging $NAME..."
  rm -rf "${TMP}/${NAME}" && mkdir -p "${TMP}/${NAME}"
  pip install --quiet --target "${TMP}/${NAME}" boto3 pandas pyarrow python-dotenv 2>&1 | tail -2
  cp -r "${DIR}"/. "${TMP}/${NAME}/"
  [ -d "$SRC/lambdas/common" ] && cp -r "$SRC/lambdas/common" "${TMP}/${NAME}/common"
  (cd "${TMP}/${NAME}" && zip -qr "$ZIP" .)
  echo "  → $ZIP ($(du -sh "$ZIP" | cut -f1))"
}

# Helper: create or update a Lambda
deploy() {
  local NAME="$1"
  local HANDLER="$2"
  local ZIP="${BUILD}/${NAME}.zip"
  local ENV_VARS="$3"

  if aws lambda get-function --function-name "$NAME" \
       $EP_FLAG --region "$REGION" --no-cli-pager &>/dev/null; then
    aws lambda update-function-code --function-name "$NAME" \
      --zip-file "fileb://$ZIP" $EP_FLAG --region "$REGION" --no-cli-pager
    aws lambda update-function-configuration --function-name "$NAME" \
      --environment "Variables={${ENV_VARS}}" $EP_FLAG --region "$REGION" --no-cli-pager
    echo "  ✓ $NAME updated"
  else
    aws lambda create-function --function-name "$NAME" \
      --runtime python3.12 --role "$ROLE_ARN" --handler "$HANDLER" \
      --zip-file "fileb://$ZIP" --timeout 60 --memory-size 256 \
      --environment "Variables={${ENV_VARS}}" \
      $EP_FLAG --region "$REGION" --no-cli-pager
    echo "  ✓ $NAME created"
  fi
}

mkdir -p "$TMP"

# validate_gps
package "validate_gps" "$SRC/lambdas/validate_gps"
deploy  "validate_gps" "handler.handler" \
  "AWS_ENDPOINT_URL=http://localstack:4566,DYNAMO_TABLE_NAME=gps-last-seen,DEDUP_TABLE_NAME=gps-dedup,SNS_TOPIC_ARN=${SNS_ARN},SILVER_BUCKET=${SILVER_BUCKET},BRONZE_BUCKET=${BRONZE_BUCKET},SIGNAL_LOSS_THRESHOLD_MINUTES=10,LOG_LEVEL=INFO"

# detect_signal_loss
package "detect_signal_loss" "$SRC/lambdas/detect_signal_loss"
deploy  "detect_signal_loss" "handler.handler" \
  "AWS_ENDPOINT_URL=http://localstack:4566,DYNAMO_TABLE_NAME=gps-last-seen,SNS_TOPIC_ARN=${SNS_ARN},SILVER_BUCKET=${SILVER_BUCKET},SIGNAL_LOSS_THRESHOLD_MINUTES=10,AUTO_MAINTENANCE_THRESHOLD_MINUTES=30,LOG_LEVEL=INFO"

# ingest_maintenance
package "ingest_maintenance" "$SRC/batch"
deploy  "ingest_maintenance" "ingest_maintenance.handler" \
  "AWS_ENDPOINT_URL=http://localstack:4566,SILVER_BUCKET=${SILVER_BUCKET},BRONZE_BUCKET=${BRONZE_BUCKET},LOG_LEVEL=INFO"

# SQS → validate_gps ESM
echo "[deploy] Wiring SQS gps-eventos → validate_gps..."
QUEUE_ARN=$(aws sqs get-queue-attributes \
  --queue-url "$(aws sqs get-queue-url --queue-name gps-eventos $EP_FLAG --region "$REGION" \
    --query QueueUrl --output text --no-cli-pager)" \
  --attribute-names QueueArn $EP_FLAG --region "$REGION" \
  --query Attributes.QueueArn --output text --no-cli-pager 2>/dev/null || echo "")

if [ -n "$QUEUE_ARN" ]; then
  EXISTING=$(aws lambda list-event-source-mappings \
    --function-name validate_gps --event-source-arn "$QUEUE_ARN" \
    $EP_FLAG --region "$REGION" \
    --query 'EventSourceMappings[0].UUID' --output text --no-cli-pager 2>/dev/null || echo "None")
  if [ "$EXISTING" = "None" ] || [ -z "$EXISTING" ]; then
    aws lambda create-event-source-mapping \
      --function-name validate_gps \
      --event-source-arn "$QUEUE_ARN" \
      --batch-size 10 \
      $EP_FLAG --region "$REGION" --no-cli-pager
    echo "  ✓ SQS ESM created"
  else
    echo "  ESM already exists ($EXISTING)"
  fi
fi

# EventBridge → detect_signal_loss
echo "[deploy] Wiring EventBridge → detect_signal_loss..."
aws events put-rule --name gps-signal-loss-check \
  --schedule-expression "rate(5 minutes)" --state ENABLED \
  $EP_FLAG --region "$REGION" --no-cli-pager 2>/dev/null || true
LAMBDA_ARN="arn:aws:lambda:${REGION}:${ACCOUNT}:function:detect_signal_loss"
aws events put-targets --rule gps-signal-loss-check \
  --targets "[{\"Id\":\"1\",\"Arn\":\"$LAMBDA_ARN\"}]" \
  $EP_FLAG --region "$REGION" --no-cli-pager 2>/dev/null || true
aws lambda add-permission --function-name detect_signal_loss \
  --statement-id events-invoke --action lambda:InvokeFunction \
  --principal events.amazonaws.com $EP_FLAG --region "$REGION" --no-cli-pager 2>/dev/null || true

echo "[deploy] Done — all Lambdas deployed."
