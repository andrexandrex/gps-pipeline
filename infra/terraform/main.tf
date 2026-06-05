terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Switch between LocalStack and real AWS via TF_VAR_use_localstack=true
variable "use_localstack" {
  type    = bool
  default = true
}

variable "aws_region" {
  default = "us-east-1"
}

# Pass your real account ID when deploying to AWS to avoid an STS lookup.
# Defaults to LocalStack's fixed account ID; override with:
#   terraform apply -var="aws_account_id=150465626929"
variable "aws_account_id" {
  default     = "000000000000"
  description = "AWS Account ID. LocalStack default is 000000000000."
}

locals {
  endpoint      = var.use_localstack ? "http://localhost:4566" : null
  # S3 bucket names must be globally unique — append account ID on real AWS.
  # LocalStack keeps the short names (no suffix) so bootstrap.sh stays simple.
  bucket_suffix = var.use_localstack ? "" : "-${var.aws_account_id}"
}

provider "aws" {
  region                      = var.aws_region
  access_key                  = var.use_localstack ? "test" : null
  secret_key                  = var.use_localstack ? "test" : null
  skip_credentials_validation = var.use_localstack
  skip_metadata_api_check     = var.use_localstack
  skip_requesting_account_id  = var.use_localstack

  dynamic "endpoints" {
    for_each = var.use_localstack ? [1] : []
    content {
      s3          = local.endpoint
      kinesis     = local.endpoint
      dynamodb    = local.endpoint
      sns         = local.endpoint
      sqs         = local.endpoint
      lambda      = local.endpoint
      iam         = local.endpoint
      firehose    = local.endpoint
      glue        = local.endpoint
      sts         = local.endpoint   # needed for data.aws_caller_identity
      cloudwatch  = local.endpoint
      events      = local.endpoint
    }
  }
}

# ── S3 Medallion buckets ────────────────────────────────────────────────────
resource "aws_s3_bucket" "bronze" {
  bucket        = "gps-bronze${local.bucket_suffix}"
  force_destroy = true
}

resource "aws_s3_bucket" "silver" {
  bucket        = "gps-silver${local.bucket_suffix}"
  force_destroy = true
}

resource "aws_s3_bucket" "gold" {
  bucket        = "gps-gold${local.bucket_suffix}"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "bronze_versioning" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration { status = "Enabled" }
}

# ── SQS queue: GPS events ingestion ─────────────────────────────────────────
# SQS Standard Queue replaces Kinesis Data Streams as the GPS event bus.
# Functionally equivalent for this workload: Lambda ESM, DLQ on failure,
# at-least-once delivery. SQS has no service subscription requirement.
resource "aws_sqs_queue" "gps_eventos" {
  name                       = "gps-eventos"
  message_retention_seconds  = 86400   # 1 day — GPS data is stale quickly
  visibility_timeout_seconds = 120     # must be >= Lambda timeout (60s)
  receive_wait_time_seconds  = 10      # long polling — reduces empty receives
}

# ── DynamoDB: last-seen per device ──────────────────────────────────────────
resource "aws_dynamodb_table" "last_seen" {
  name         = "gps-last-seen"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "equipo_id"

  attribute {
    name = "equipo_id"
    type = "S"
  }
}

# ── SNS alerts ──────────────────────────────────────────────────────────────
resource "aws_sns_topic" "alertas" {
  name = "gps-alertas"
}

# ── SQS DLQ for Lambda failures ─────────────────────────────────────────────
resource "aws_sqs_queue" "validate_dlq" {
  name                      = "gps-validate-dlq"
  message_retention_seconds = 1209600 # 14 days
}
