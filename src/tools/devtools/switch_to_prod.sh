#!/bin/bash
# Switch from DEMO to PROD Binance
# Run locally: bash switch_to_prod.sh

SERVER="${DEPLOY_HOST:-user@host.example.com}"
KEY="$HOME/.ssh/oracle_key"
SSH="ssh -i $KEY $SERVER"

echo "⚠️  Switching to REAL Binance (fapi.binance.com)"
echo "   LIVE_MODE=true — orders will use REAL money"
read -p "   Type 'yes' to confirm: " confirm
[ "$confirm" != "yes" ] && echo "Cancelled" && exit 1

# Show current state
echo ""
echo "Current .env:"
$SSH "grep 'LIVE_MODE\|LIVE_ENABLED\|LIVE_ORDER' ~/engine/.env"

# Flip LIVE_MODE
$SSH "sed -i 's/LIVE_MODE=false/LIVE_MODE=true/' ~/engine/.env"

echo ""
echo "Updated .env:"
$SSH "grep 'LIVE_MODE\|LIVE_ENABLED\|LIVE_ORDER' ~/engine/.env"
echo ""
echo "✅ Done. Now run:"
echo "   bash deploy.sh --full --label 'PROD: switch to live fapi.binance.com'"
