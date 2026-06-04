# GPS Pipeline — Real-time + Batch on AWS

Streaming GPS pipeline (Kinesis → Lambda → DynamoDB/S3) + batch maintenance CSV ingestion, fully reproducible locally via LocalStack.

## Quick start (LocalStack)

```bash
# 1. Clone and enter
cd gps-pipeline

# 2. Copy env vars
cp .env.example .env

# 3. Spin up LocalStack + bootstrap resources
docker compose up -d

# 4. Wait for health (≈30s), then verify
docker compose logs infra-init
```

## Verify resources exist

```bash
# List S3 buckets
aws --endpoint-url=http://localhost:4566 s3 ls

# List Kinesis streams
aws --endpoint-url=http://localhost:4566 kinesis list-streams

# List DynamoDB tables
aws --endpoint-url=http://localhost:4566 dynamodb list-tables
```

## Deploy to real AWS

```bash
# Terraform — disable LocalStack flag
cd infra/terraform
terraform init
terraform apply -var="use_localstack=false"
```

## Architecture

See [docs/architecture.md](docs/architecture.md).

## Stack

| Layer | Technology |
|-------|-----------|
| Streaming ingest | Kinesis Data Streams (2 shards) |
| Validation | Lambda (Python 3.12) |
| State / last-seen | DynamoDB (on-demand) |
| Alerts | SNS |
| Dead letter | SQS DLQ |
| Storage | S3 medallion (bronze/silver/gold, Parquet) |
| Batch ETL | Glue / Pandas |
| Query | Athena |
| Quality | Pandera |
| Local emulation | LocalStack 3.4 |
| IaC | Terraform 5.x |
