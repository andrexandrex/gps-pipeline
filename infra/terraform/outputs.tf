output "kinesis_stream_arn"  { value = aws_kinesis_stream.gps_eventos.arn }
output "sns_topic_arn"       { value = aws_sns_topic.alertas.arn }
output "dynamodb_table_name" { value = aws_dynamodb_table.last_seen.name }
output "bronze_bucket"       { value = aws_s3_bucket.bronze.bucket }
output "silver_bucket"       { value = aws_s3_bucket.silver.bucket }
output "gold_bucket"         { value = aws_s3_bucket.gold.bucket }

# Set after terraform apply -var="use_localstack=false"
output "dashboard_url" {
  value = var.use_localstack ? "http://localhost:8501" : (
    length(aws_instance.dashboard) > 0
      ? "http://${aws_instance.dashboard[0].public_ip}:8501"
      : "deploy EC2 with: terraform apply -var=use_localstack=false"
  )
  description = "Streamlit dashboard URL"
}
