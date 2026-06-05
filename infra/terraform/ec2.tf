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
      # S3: read silver + gold + bronze + state bucket (for app bundle download)
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.silver.arn, "${aws_s3_bucket.silver.arn}/*",
          aws_s3_bucket.gold.arn,   "${aws_s3_bucket.gold.arn}/*",
          aws_s3_bucket.bronze.arn, "${aws_s3_bucket.bronze.arn}/*",
          "arn:aws:s3:::gps-tfstate-${var.aws_account_id}",
          "arn:aws:s3:::gps-tfstate-${var.aws_account_id}/*",
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

# SSM: allows running commands on the instance without SSH keys
resource "aws_iam_role_policy_attachment" "dashboard_ec2_ssm" {
  count      = var.use_localstack ? 0 : 1
  role       = aws_iam_role.dashboard_ec2[0].name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
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

  # Full bootstrap: downloads app bundle from S3 state bucket and starts dashboard.
  # Logs go to /var/log/gps-dashboard-setup.log
  # The CI uploads build/app-bundle.zip to s3://gps-tfstate-<ACCOUNT>/app-bundle.zip
  # BEFORE terraform apply so the bundle is already there when user_data runs.
  user_data = base64encode(<<-SCRIPT
    #!/bin/bash
    exec > /var/log/gps-dashboard-setup.log 2>&1
    set -e
    sleep 10   # let IAM instance profile propagate

    dnf update -y -q
    dnf install -y -q python3.12 python3.12-pip unzip

    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    REGION=$(curl -sf http://169.254.169.254/latest/meta-data/placement/region || echo "us-east-1")
    STATE_BUCKET="gps-tfstate-$${ACCOUNT_ID}"
    APP_DIR="/home/ec2-user/gps-pipeline"

    echo "Downloading app bundle from s3://$${STATE_BUCKET}/app-bundle.zip..."
    aws s3 cp "s3://$${STATE_BUCKET}/app-bundle.zip" /tmp/app-bundle.zip
    mkdir -p "$${APP_DIR}"
    unzip -q /tmp/app-bundle.zip -d "$${APP_DIR}"
    chown -R ec2-user:ec2-user "$${APP_DIR}"

    echo "Installing Python dependencies..."
    python3.12 -m pip install -q -r "$${APP_DIR}/requirements.txt"

    echo "Writing .env..."
    cat > "$${APP_DIR}/.env" << EOF
    AWS_DEFAULT_REGION=$${REGION}
    SILVER_BUCKET=gps-silver-$${ACCOUNT_ID}
    GOLD_BUCKET=gps-gold-$${ACCOUNT_ID}
    BRONZE_BUCKET=gps-bronze-$${ACCOUNT_ID}
    SQS_GPS_QUEUE_NAME=gps-eventos
    DYNAMO_TABLE_NAME=gps-last-seen
    SIGNAL_LOSS_THRESHOLD_MINUTES=10
    AUTO_MAINTENANCE_THRESHOLD_MINUTES=30
    EOF
    chown ec2-user:ec2-user "$${APP_DIR}/.env"

    echo "Installing systemd service..."
    cat > /etc/systemd/system/gps-dashboard.service << UNIT
    [Unit]
    Description=GPS Pipeline Streamlit Dashboard
    After=network.target

    [Service]
    User=ec2-user
    WorkingDirectory=$${APP_DIR}
    EnvironmentFile=$${APP_DIR}/.env
    Environment=PYTHONPATH=$${APP_DIR}/src:$${APP_DIR}/src/lambdas
    ExecStart=/usr/bin/python3.12 -m streamlit run src/dashboard/app.py --server.port=8501 --server.address=0.0.0.0 --server.headless=true
    Restart=always
    RestartSec=10

    [Install]
    WantedBy=multi-user.target
    UNIT

    systemctl daemon-reload
    systemctl enable gps-dashboard
    systemctl start gps-dashboard
    echo "Dashboard started. Check: systemctl status gps-dashboard"
  SCRIPT
  )

  tags = {
    Name    = "gps-dashboard"
    Project = "gps-pipeline"
  }
}
