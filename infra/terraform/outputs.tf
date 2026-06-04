output "kinesis_stream_arn" {
  value = aws_kinesis_stream.gps_eventos.arn
}

output "sns_topic_arn" {
  value = aws_sns_topic.alertas.arn
}

output "dynamodb_table_name" {
  value = aws_dynamodb_table.last_seen.name
}

output "bronze_bucket" { value = aws_s3_bucket.bronze.bucket }
output "silver_bucket" { value = aws_s3_bucket.silver.bucket }
output "gold_bucket"   { value = aws_s3_bucket.gold.bucket }
