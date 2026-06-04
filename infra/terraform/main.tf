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

locals {
  endpoint = var.use_localstack ? "http://localhost:4566" : null
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
      s3       = local.endpoint
      kinesis  = local.endpoint
      dynamodb = local.endpoint
      sns      = local.endpoint
      sqs      = local.endpoint
      lambda   = local.endpoint
      iam      = local.endpoint
      firehose = local.endpoint
      glue     = local.endpoint
    }
  }
}

# ── S3 Medallion buckets ────────────────────────────────────────────────────
resource "aws_s3_bucket" "bronze" {
  bucket        = "gps-bronze"
  force_destroy = true
}

resource "aws_s3_bucket" "silver" {
  bucket        = "gps-silver"
  force_destroy = true
}

resource "aws_s3_bucket" "gold" {
  bucket        = "gps-gold"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "bronze_versioning" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration { status = "Enabled" }
}

# ── Kinesis stream ──────────────────────────────────────────────────────────
resource "aws_kinesis_stream" "gps_eventos" {
  name        = "gps-eventos"
  shard_count = 2
  # 2 shards = ~2 MB/s ingest; scale up for >1000 vehicles
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
