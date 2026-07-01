#!/bin/bash
# setup_stage.sh — Complete stage server setup from scratch
# Run once on a fresh Oracle Cloud AMD E2.1.Micro (Ubuntu 24) instance
# Usage: bash setup_stage.sh
# Assumes: SSH access as ubuntu, internet connectivity

set -euo pipefail

echo "════════════════════════════════════════"
echo "PredictEngine Stage Server Setup"
echo "════════════════════════════════════════"
echo ""

# ── 1. System packages ───────────────────────────────────────────────
echo "==> Installing system packages..."
sudo apt update && sudo apt install -y \
    vim nano unzip curl wget \
    python3 python3-pip \
    net-tools iptables-persistent
echo "   ✅ System packages installed"
echo ""

# ── 2. Python modules (global, no venv) ──────────────────────────────
echo "==> Installing Python modules..."
sudo pip3 install --break-system-packages \
    fastapi uvicorn websockets aiohttp \
    python-dotenv httpx numpy pandas \
    requests python-telegram-bot circuitbreaker
echo "   ✅ Python modules installed"
echo ""

# ── 3. Verify ────────────────────────────────────────────────────────
echo "==> Verifying modules..."
python3 -c "import fastapi, uvicorn, websockets, aiohttp, dotenv, httpx, numpy, pandas, telegram, circuitbreaker; print('   ✅ All modules OK')"
echo ""

# ── 4. Create engine directory ───────────────────────────────────────
echo "==> Creating engine directory..."
mkdir -p ~/engine/logs
echo "   ✅ ~/engine/logs created"
echo ""

# ── 5. Firewall rules ────────────────────────────────────────────────
echo "==> Configuring firewall (iptables)..."
# Allow port 8001 (dashboard) before the catch-all REJECT rule
sudo iptables -I INPUT 5 -p tcp --dport 8001 -j ACCEPT
sudo iptables -I INPUT 5 -p tcp --dport 8080 -j ACCEPT
sudo netfilter-persistent save
echo "   ✅ Ports 8001 + 8080 open"
echo ""

# ── 6. Systemd services ───────────────────────────────────────────────
echo "==> Creating systemd services..."

# predict-engine
sudo tee /etc/systemd/system/predict-engine.service << 'SERVICE'
[Unit]
Description=PredictEngine Stage
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/engine
EnvironmentFile=/home/ubuntu/engine/.env
ExecStart=/usr/bin/python3 /home/ubuntu/engine/predict_engine.py --multi --dashboard --quiet
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
SERVICE

# predict-monitor
sudo tee /etc/systemd/system/predict-monitor.service << 'SERVICE'
[Unit]
Description=PredictEngine Stage Monitor
After=network.target predict-engine.service

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/engine
EnvironmentFile=/home/ubuntu/engine/.env
ExecStart=/usr/bin/python3 /home/ubuntu/engine/tg_monitor.py --loop
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
SERVICE

# demo-reset
sudo tee /etc/systemd/system/demo-reset.service << 'SERVICE'
[Unit]
Description=Demo Balance Auto-Reset
After=predict-engine.service

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/engine
EnvironmentFile=/home/ubuntu/engine/.env
ExecStart=/usr/bin/python3 /home/ubuntu/engine/demo_balance_reset.py --threshold 100
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
SERVICE

sudo systemctl daemon-reload
sudo systemctl enable predict-engine predict-monitor demo-reset
echo "   ✅ Services created and enabled"
echo ""

# ── 7. OS tuning ─────────────────────────────────────────────────────
echo "==> Applying OS low-latency tuning..."
if [ -f ~/tune_os.sh ]; then
    sudo bash ~/tune_os.sh
    echo "   ✅ OS tuned"
else
    echo "   ⚠️  tune_os.sh not found — run manually: sudo bash tune_os.sh"
fi
echo ""

# ── 8. Hostname ───────────────────────────────────────────────────────
echo "==> Setting hostname to 'stage'..."
sudo hostnamectl set-hostname stage
sudo sed -i 's/0.0.0.0.*/0.0.0.0 stage/' /etc/hosts
echo "   ✅ Hostname set to 'stage'"
echo ""

echo "════════════════════════════════════════"
echo "✅ Stage setup complete"
echo "════════════════════════════════════════"
echo ""
echo "Next steps:"
echo "  1. Copy .env.stage from local:  scp .env.stage user@host.example.com~/engine/.env"
echo "  2. Deploy engine:               bash deploy.sh --full --stage --label 'initial'"
echo "  3. Start services:              sudo systemctl start predict-engine predict-monitor demo-reset"
echo "  4. Check status:                sudo systemctl status predict-engine"
echo "  5. Watch logs:                  journalctl -u predict-engine -f"
