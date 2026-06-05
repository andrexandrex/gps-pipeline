# ── CloudWatch Alarms ────────────────────────────────────────────────────────
# Why CloudWatch and not a third-party tool: nativo en AWS, cero costo de
# integración, métricas ya existen sin instrumentación extra.
# Alternativa: Datadog/Grafana + CloudWatch source — más visualización pero
# añade costo y dependencia externa.

locals {
  alarm_actions = [aws_sns_topic.alertas.arn]
}

# Lambda errors — validate_gps
resource "aws_cloudwatch_metric_alarm" "validate_gps_errors" {
  alarm_name          = "validate-gps-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Sum"
  threshold           = 5
  alarm_description   = "validate_gps raised >5 errors in 1 min"
  alarm_actions       = local.alarm_actions
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.validate_gps.function_name
  }
}

# Lambda errors — detect_signal_loss
resource "aws_cloudwatch_metric_alarm" "detect_signal_loss_errors" {
  alarm_name          = "detect-signal-loss-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "detect_signal_loss raised any error in 5 min"
  alarm_actions       = local.alarm_actions
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.detect_signal_loss.function_name
  }
}

# SQS message age — equivalent to Kinesis IteratorAge; rises when consumer lags
resource "aws_cloudwatch_metric_alarm" "gps_sqs_message_age" {
  alarm_name          = "gps-eventos-message-age"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "ApproximateAgeOfOldestMessage"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Maximum"
  threshold           = 60  # seconds — consumer >1 min behind
  alarm_description   = "GPS SQS queue oldest message >60s — validate_gps may be throttled or erroring"
  alarm_actions       = local.alarm_actions
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.gps_eventos.name
  }
}

# DLQ depth — any message in DLQ means a record exhausted all retries
resource "aws_cloudwatch_metric_alarm" "dlq_depth" {
  alarm_name          = "gps-validate-dlq-depth"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Messages landed in DLQ — records exhausted all retries"
  alarm_actions       = local.alarm_actions
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.validate_dlq.name
  }
}

# Lambda duration — if P99 duration approaches timeout (60s), raise alert
resource "aws_cloudwatch_metric_alarm" "validate_gps_duration" {
  alarm_name          = "validate-gps-duration-p99"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "Duration"
  namespace           = "AWS/Lambda"
  period              = 60
  extended_statistic  = "p99"
  threshold           = 45000 # 45s — 75% of the 60s timeout
  alarm_description   = "validate_gps P99 duration >45s — approaching timeout"
  alarm_actions       = local.alarm_actions
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.validate_gps.function_name
  }
}
