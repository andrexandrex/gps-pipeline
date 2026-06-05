# ── Streamlit Dashboard on EC2 (real AWS only) ───────────────────────────────
# count = 0 in LocalStack, count = 1 in real AWS
# t3.micro = ~$8.50/mes, free-tier eligible el primer año

variable "ec2_key_name" {
  description = "SSH key pair name created in AWS console (EC2 → Key Pairs)"
  default     = "gps-pipeline-key"
}

variable "allowed_ssh_cidr" {
  description = "Your public IP for SSH. Get it with: curl ifconfig.me"
  default     = "0.0.0.0/0"  # restrict to your IP in production
}

# ── IAM role: EC2 reads S3 + DynamoDB + SQS (no credentials stored on instance)
resource "aws_iam_role" "dashboard_ec2" {
  count = var.use_localstack ? 0 : 1
  name  = "gps-dashboard-ec2-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "dashboard_ec2_policy" {
  count = var.use_localstack ? 0 : 1
  name  = "gps-dashboard-policy"
  role  = aws_iam_role.dashboard_ec2[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # S3: read silver + gold, list buckets
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::gps-silver", "arn:aws:s3:::gps-silver/*",
          "arn:aws:s3:::gps-gold",   "arn:aws:s3:::gps-gold/*",
          "arn:aws:s3:::gps-bronze", "arn:aws:s3:::gps-bronze/*",
        ]
      },
      # DynamoDB: scan last-seen table
      {
        Effect   = "Allow"
        Action   = ["dynamodb:Scan", "dynamodb:GetItem"]
        Resource = "arn:aws:dynamodb:*:*:table/gps-*"
      },
      # SQS: read DLQ depth (for Robustez tab)
      {
        Effect   = "Allow"
        Action   = ["sqs:GetQueueAttributes", "sqs:GetQueueUrl"]
        Resource = "arn:aws:sqs:*:*:gps-*"
      },
    ]
  })
}

resource "aws_iam_instance_profile" "dashboard_ec2" {
  count = var.use_localstack ? 0 : 1
  name  = "gps-dashboard-profile"
  role  = aws_iam_role.dashboard_ec2[0].name
}

# ── Security group: public 8501 + your-IP SSH ────────────────────────────────
resource "aws_security_group" "dashboard" {
  count       = var.use_localstack ? 0 : 1
  name        = "gps-dashboard-sg"
  description = "Streamlit dashboard (8501) + SSH (22)"

  ingress {
    description = "Streamlit - public access"
    from_port   = 8501
    to_port     = 8501
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_ssh_cidr]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ── AMI: latest Amazon Linux 2023 (official AWS image) ───────────────────────
data "aws_ami" "al2023" {
  count       = var.use_localstack ? 0 : 1
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# ── EC2 instance ──────────────────────────────────────────────────────────────
resource "aws_instance" "dashboard" {
  count                = var.use_localstack ? 0 : 1
  ami                  = data.aws_ami.al2023[0].id
  instance_type        = "t3.micro"   # $0.0104/h ≈ $7.50/mes
  key_name             = var.ec2_key_name != "" ? var.ec2_key_name : null
  iam_instance_profile = aws_iam_instance_profile.dashboard_ec2[0].name
  vpc_security_group_ids = [aws_security_group.dashboard[0].id]

  # Bootstrap: install Python + pip, then we SSH in and run setup_ec2.sh
  user_data = base64encode(<<-SCRIPT
    #!/bin/bash
    dnf update -y
    dnf install -y git python3.12 python3.12-pip
    echo "EC2 bootstrap complete — SSH in and run: bash infra/scripts/setup_ec2.sh" \
      >> /home/ec2-user/NEXT_STEP.txt
    chown ec2-user:ec2-user /home/ec2-user/NEXT_STEP.txt
  SCRIPT
  )

  tags = {
    Name    = "gps-dashboard"
    Project = "gps-pipeline"
  }
}
