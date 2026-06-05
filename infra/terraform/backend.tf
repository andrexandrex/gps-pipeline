terraform {
  # Partial backend config — bucket/key/region passed via -backend-config in CI.
  # For LocalStack: terraform init -backend=false
  # For real AWS:   terraform init -backend-config="bucket=gps-tfstate-<account_id>" ...
  backend "s3" {}
}
