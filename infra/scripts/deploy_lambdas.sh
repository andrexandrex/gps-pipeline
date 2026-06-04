#!/bin/bash
# Package and deploy Lambda functions to LocalStack (or real AWS).
# Idempotent: uses create-or-update pattern.
# Requires: python3, pip, zip, aws-cli
set -euo pipefail

EP="${AWS_ENDPOINT_URL:-http://localhost:4566}"
EP_FLAG="--endpoint-url $EP"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
ACCOUNT="000000000000"
ROLE_ARN="arn:aws:iam::${ACCOUNT}:role/lambda-gps-role"
SRC="$(cd "$(dirname "$0")/../../src" && pwd)"
BUILD_DIR="/tmp/gps-lambda-build"

# Env vars injected into every Lambda at runtime
LAMBDA_ENV="Variables={
  AWS_ENDPOINT_URL=http://localstack:4566,
  DYNAMO_TABLE_NAME=gps-last-seen,
  DEDUP_TABLE_NAME=gps-dedup,
  SNS_TOPIC_ARN=arn:aws:sns:${REGION}:${ACCOUNT}:gps-alertas,
  SILVER_BUCKET=gps-silver,
  BRONZE_BUCKET=gps-bronze,
  SIGNAL_LOSS_THRESHOLD_MINUTES=10,
  LOG_LEVEL=INFO
}"

# ── Step 1: create IAM role (LocalStack accepts any policy) ─────────────────
echo "[deploy] Creating Lambda IAM role..."
aws iam create-role \
  --role-name lambda-gps-role \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
  $EP_FLAG --region "$REGION" --no-cli-pager 2>/dev/null || echo "  role already exists"

aws iam attach-role-policy \
  --role-name lambda-gps-role \
  --policy-arn arn:aws:iam::aws:policy/AdministratorAccess \
  $EP_FLAG --region "$REGION" --no-cli-pager 2>/dev/null || true

# ── Helper: package one lambda ───────────────────────────────────────────────
package_lambda() {
  local NAME="$1"        # e.g. validate_gps
  local HANDLER_DIR="$SRC/lambdas/$NAME"
  local ZIP_PATH="$BUILD_DIR/${NAME}.zip"

  echo "[deploy] Packaging $NAME..."
  rm -rf "$BUILD_DIR/$NAME"
  mkdir -p "$BUILD_DIR/$NAME"

  # Install Python dependencies into the package directory
  pip install --quiet --target "$BUILD_DIR/$NAME" \
    boto3 pandas pyarrow python-dotenv 2>&1 | tail -3

  # Copy handler + shared common module
  cp -r "$HANDLER_DIR"/. "$BUILD_DIR/$NAME/"
  cp -r "$SRC/lambdas/common" "$BUILD_DIR/$NAME/common"

  (cd "$BUILD_DIR/$NAME" && zip -qr "$ZIP_PATH" .)
  echo "  → $ZIP_PATH ($(du -sh "$ZIP_PATH" | cut -f1))"
}

# ── Helper: create or update a Lambda function ───────────────────────────────
deploy_lambda() {
  local NAME="$1"
  local HANDLER="$2"   # e.g. handler.handler
  local ZIP_PATH="$BUILD_DIR/${NAME}.zip"

  if aws lambda get-function --function-name "$NAME" $EP_FLAG --region "$REGION" \
       --no-cli-pager &>/dev/null; then
    echo "[deploy] Updating $NAME..."
    aws lambda update-function-code \
      --function-name "$NAME" \
      --zip-file "fileb://$ZIP_PATH" \
      $EP_FLAG --region "$REGION" --no-cli-pager
    aws lambda update-function-configuration \
      --function-name "$NAME" \
      --environment "$LAMBDA_ENV" \
      $EP_FLAG --region "$REGION" --no-cli-pager
  else
    echo "[deploy] Creating $NAME..."
    aws lambda create-function \
      --function-name "$NAME" \
      --runtime python3.12 \
      --role "$ROLE_ARN" \
      --handler "$HANDLER" \
      --zip-file "fileb://$ZIP_PATH" \
      --timeout 60 \
      --memory-size 256 \
      --environment "$LAMBDA_ENV" \
      $EP_FLAG --region "$REGION" --no-cli-pager
  fi
}

mkdir -p "$BUILD_DIR"

# ── validate_gps ─────────────────────────────────────────────────────────────
package_lambda "validate_gps"
deploy_lambda  "validate_gps" "handler.handler"

# ── detect_signal_loss ────────────────────────────────────────────────────────
package_lambda "detect_signal_loss"
deploy_lambda  "detect_signal_loss" "handler.handler"

# ── Kinesis event source mapping for validate_gps ────────────────────────────
echo "[deploy] Wiring Kinesis → validate_gps..."
STREAM_ARN=$(aws kinesis describe-stream-summary \
  --stream-name gps-eventos $EP_FLAG --region "$REGION" \
  --query StreamDescriptionSummary.StreamARN --output text --no-cli-pager)

DLQ_ARN=$(aws sqs get-queue-attributes \
  --queue-url "http://localhost:4566/000000000000/gps-validate-dlq" \
  --attribute-names QueueArn $EP_FLAG --region "$REGION" \
  --query Attributes.QueueArn --output text --no-cli-pager 2>/dev/null \
  || echo "arn:aws:sqs:${REGION}:${ACCOUNT}:gps-validate-dlq")

# Check if mapping already exists to stay idempotent
EXISTING_UUID=$(aws lambda list-event-source-mappings \
  --function-name validate_gps \
  --event-source-arn "$STREAM_ARN" \
  $EP_FLAG --region "$REGION" \
  --query 'EventSourceMappings[0].UUID' --output text --no-cli-pager 2>/dev/null || echo "None")

if [ "$EXISTING_UUID" = "None" ] || [ -z "$EXISTING_UUID" ]; then
  aws lambda create-event-source-mapping \
    --function-name validate_gps \
    --event-source-arn "$STREAM_ARN" \
    --starting-position TRIM_HORIZON \
    --batch-size 100 \
    --maximum-batching-window-in-seconds 10 \
    --maximum-retry-attempts 2 \
    --bisect-batch-on-function-error \
    --destination-config "{\"OnFailure\":{\"Destination\":\"$DLQ_ARN\"}}" \
    $EP_FLAG --region "$REGION" --no-cli-pager
  echo "  Event source mapping created"
else
  echo "  Event source mapping already exists ($EXISTING_UUID)"
fi

# ── EventBridge rule for detect_signal_loss ───────────────────────────────────
echo "[deploy] Wiring EventBridge → detect_signal_loss (every 5 min)..."
aws events put-rule \
  --name gps-signal-loss-check \
  --schedule-expression "rate(5 minutes)" \
  --state ENABLED \
  $EP_FLAG --region "$REGION" --no-cli-pager 2>/dev/null || echo "  rule already exists"

LAMBDA_ARN="arn:aws:lambda:${REGION}:${ACCOUNT}:function:detect_signal_loss"
aws events put-targets \
  --rule gps-signal-loss-check \
  --targets "[{\"Id\":\"1\",\"Arn\":\"$LAMBDA_ARN\"}]" \
  $EP_FLAG --region "$REGION" --no-cli-pager 2>/dev/null || echo "  target already set"

aws lambda add-permission \
  --function-name detect_signal_loss \
  --statement-id events-invoke \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  $EP_FLAG --region "$REGION" --no-cli-pager 2>/dev/null || echo "  permission already exists"

echo "[deploy] All Lambdas deployed successfully."
