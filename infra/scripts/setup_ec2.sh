#!/bin/bash
# Run this once after SSH-ing into the EC2 instance.
# Usage: bash infra/scripts/setup_ec2.sh
set -euo pipefail

REPO_URL="${1:-}"   # pass your GitHub URL as first argument, or set below
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
APP_DIR="/home/ec2-user/gps-pipeline"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  GPS Pipeline — EC2 Setup"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── 1. Install system dependencies ───────────────────────────────────────────
echo "[1/5] Installing Python 3.12..."
sudo dnf install -y git python3.12 python3.12-pip 2>/dev/null || \
  sudo apt-get install -y git python3.12 python3-pip 2>/dev/null || true

# ── 2. Clone repo ─────────────────────────────────────────────────────────────
echo "[2/5] Cloning repository..."
if [ -z "$REPO_URL" ]; then
  echo "  ⚠  No repo URL provided. Copy files manually or pass URL:"
  echo "     bash setup_ec2.sh https://github.com/YOUR_USER/gps-pipeline.git"
  echo "  → Skipping clone; assuming code is already in $APP_DIR"
else
  git clone "$REPO_URL" "$APP_DIR" 2>/dev/null || (cd "$APP_DIR" && git pull)
fi

cd "$APP_DIR"

# ── 3. Install Python dependencies ───────────────────────────────────────────
echo "[3/5] Installing Python dependencies..."
python3.12 -m pip install --quiet -r requirements.txt

# ── 4. Create .env for real AWS (no endpoint, uses IAM role from EC2 profile) ─
echo "[4/5] Creating .env for real AWS..."
cat > .env << EOF
# Real AWS — no AWS_ENDPOINT_URL (boto3 uses IAM role from EC2 instance profile)
AWS_DEFAULT_REGION=${REGION}
SILVER_BUCKET=gps-silver
GOLD_BUCKET=gps-gold
BRONZE_BUCKET=gps-bronze
DYNAMO_TABLE_NAME=gps-last-seen
SIGNAL_LOSS_THRESHOLD_MINUTES=10
AUTO_MAINTENANCE_THRESHOLD_MINUTES=30
EOF
echo "  .env created (credentials come from EC2 IAM role — nothing hardcoded)"

# ── 5. Install and start as systemd service ───────────────────────────────────
echo "[5/5] Installing systemd service..."
sudo bash -c "cat > /etc/systemd/system/gps-dashboard.service << 'UNIT'
[Unit]
Description=GPS Pipeline Streamlit Dashboard
After=network.target

[Service]
User=ec2-user
WorkingDirectory=${APP_DIR}
EnvironmentFile=${APP_DIR}/.env
Environment=PYTHONPATH=${APP_DIR}/src:${APP_DIR}/src/lambdas
ExecStart=/usr/bin/python3.12 -m streamlit run src/dashboard/app.py \\
          --server.port=8501 --server.address=0.0.0.0 --server.headless=true
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
UNIT"

sudo systemctl daemon-reload
sudo systemctl enable  gps-dashboard
sudo systemctl restart gps-dashboard

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
PUBLIC_IP=$(curl -sf http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "<public-ip>")
echo "  ✅ Done! Dashboard:"
echo "     http://${PUBLIC_IP}:8501"
echo ""
echo "  Commands:"
echo "  sudo systemctl status  gps-dashboard   # check status"
echo "  sudo journalctl -fu    gps-dashboard   # live logs"
echo "  sudo systemctl restart gps-dashboard   # restart after code update"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
