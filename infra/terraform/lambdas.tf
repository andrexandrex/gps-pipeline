# ── IAM role ────────────────────────────────────────────────────────────────
resource "aws_iam_role" "lambda_gps" {
  name = "lambda-gps-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_gps_admin" {
  role       = aws_iam_role.lambda_gps.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
  # Production: replace with least-privilege policy scoped to specific resources
}

# ── DynamoDB dedup table ─────────────────────────────────────────────────────
resource "aws_dynamodb_table" "dedup" {
  name         = "gps-dedup"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "record_id"

  attribute {
    name = "record_id"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}

# ── Lambda: validate_gps ─────────────────────────────────────────────────────
data "archive_file" "validate_gps_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../../src/lambdas/validate_gps"
  output_path = "${path.module}/../../build/validate_gps.zip"
}

resource "aws_lambda_function" "validate_gps" {
  function_name    = "validate_gps"
  role             = aws_iam_role.lambda_gps.arn
  handler          = "handler.handler"
  runtime          = "python3.12"
  filename         = data.archive_file.validate_gps_zip.output_path
  source_code_hash = data.archive_file.validate_gps_zip.output_base64sha256
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      AWS_ENDPOINT_URL                = var.use_localstack ? "http://localstack:4566" : ""
      DYNAMO_TABLE_NAME               = aws_dynamodb_table.last_seen.name
      DEDUP_TABLE_NAME                = aws_dynamodb_table.dedup.name
      SNS_TOPIC_ARN                   = aws_sns_topic.alertas.arn
      SILVER_BUCKET                   = aws_s3_bucket.silver.bucket
      BRONZE_BUCKET                   = aws_s3_bucket.bronze.bucket
      SIGNAL_LOSS_THRESHOLD_MINUTES   = "10"
      LOG_LEVEL                       = "INFO"
    }
  }

  dead_letter_config {
    target_arn = aws_sqs_queue.validate_dlq.arn
  }
}

# ── Kinesis → validate_gps trigger ──────────────────────────────────────────
resource "aws_lambda_event_source_mapping" "kinesis_to_validate" {
  event_source_arn                   = aws_kinesis_stream.gps_eventos.arn
  function_name                      = aws_lambda_function.validate_gps.arn
  starting_position                  = "TRIM_HORIZON"
  batch_size                         = 100
  maximum_batching_window_in_seconds = 10

  # On error: retry 2x, then split batch to isolate bad record
  maximum_retry_attempts       = 2
  bisect_batch_on_function_error = true

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.validate_dlq.arn
    }
  }
}

# ── Lambda: detect_signal_loss ───────────────────────────────────────────────
data "archive_file" "detect_signal_loss_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../../src/lambdas/detect_signal_loss"
  output_path = "${path.module}/../../build/detect_signal_loss.zip"
}

resource "aws_lambda_function" "detect_signal_loss" {
  function_name    = "detect_signal_loss"
  role             = aws_iam_role.lambda_gps.arn
  handler          = "handler.handler"
  runtime          = "python3.12"
  filename         = data.archive_file.detect_signal_loss_zip.output_path
  source_code_hash = data.archive_file.detect_signal_loss_zip.output_base64sha256
  timeout          = 60
  memory_size      = 128

  environment {
    variables = {
      AWS_ENDPOINT_URL              = var.use_localstack ? "http://localstack:4566" : ""
      DYNAMO_TABLE_NAME             = aws_dynamodb_table.last_seen.name
      SNS_TOPIC_ARN                 = aws_sns_topic.alertas.arn
      SIGNAL_LOSS_THRESHOLD_MINUTES = "10"
      LOG_LEVEL                     = "INFO"
    }
  }
}

# ── EventBridge rule → detect_signal_loss (every 5 min) ─────────────────────
resource "aws_cloudwatch_event_rule" "signal_loss_schedule" {
  name                = "gps-signal-loss-check"
  schedule_expression = "rate(5 minutes)"
}

resource "aws_cloudwatch_event_target" "signal_loss_target" {
  rule = aws_cloudwatch_event_rule.signal_loss_schedule.name
  arn  = aws_lambda_function.detect_signal_loss.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.detect_signal_loss.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.signal_loss_schedule.arn
}

# ── Lambda: ingest_maintenance ────────────────────────────────────────────────
data "archive_file" "ingest_maintenance_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../../src/batch"
  output_path = "${path.module}/../../build/ingest_maintenance.zip"
}

resource "aws_lambda_function" "ingest_maintenance" {
  function_name    = "ingest_maintenance"
  role             = aws_iam_role.lambda_gps.arn
  handler          = "ingest_maintenance.handler"
  runtime          = "python3.12"
  filename         = data.archive_file.ingest_maintenance_zip.output_path
  source_code_hash = data.archive_file.ingest_maintenance_zip.output_base64sha256
  timeout          = 120   # CSV parsing can take longer than GPS validation
  memory_size      = 512   # pandas + pyarrow benefit from extra memory

  environment {
    variables = {
      AWS_ENDPOINT_URL = var.use_localstack ? "http://localstack:4566" : ""
      SILVER_BUCKET    = aws_s3_bucket.silver.bucket
      BRONZE_BUCKET    = aws_s3_bucket.bronze.bucket
      LOG_LEVEL        = "INFO"
    }
  }
}

# Allow S3 to invoke the Lambda when a CSV lands in bronze/mantenimientos/
resource "aws_lambda_permission" "allow_s3_ingest" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest_maintenance.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.bronze.arn
}

# S3 event notification: bronze bucket PutObject in mantenimientos/ → Lambda
resource "aws_s3_bucket_notification" "bronze_csv_trigger" {
  bucket = aws_s3_bucket.bronze.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.ingest_maintenance.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "mantenimientos/"
    filter_suffix       = ".csv"
  }

  depends_on = [aws_lambda_permission.allow_s3_ingest]
}
