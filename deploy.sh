#!/bin/bash
# deploy.sh — Single unified deployment: track changes + deploy + record history
# Usage:
#   bash deploy.sh                                           # hot-reload config only
#   bash deploy.sh --full --label "dashboard v3"            # full deploy with label
#   bash deploy.sh --full --clean-first --label "my change" # clean remote CSVs, then full deploy
#   bash deploy.sh --full --clean-first --watch             # deploy + tail live logs after
#   bash deploy.sh --watch                                  # just tail logs (no deploy)
#   bash deploy.sh --list                                   # show deployment history
#   bash deploy.sh --status                                 # show last deployment

set -euo pipefail

SERVER="${DEPLOY_HOST:-user@host.example.com}"
KEY="$HOME/.ssh/oracle_key"
REMOTE_DIR="~/engine"
STAGE=false

# SSH helpers
CTRL="$HOME/.ssh/ctl-oracle-%r@%h:%p"
SSH="ssh -i $KEY -o ControlMaster=auto -o ControlPath=$CTRL -o ControlPersist=60s $SERVER"
SCP="scp -i $KEY -o ControlPath=$CTRL -o Compression=no"

# Logging
BACKUP_ROOT="./data_backup"
DEPLOY_HIST="$BACKUP_ROOT/deploy_history"
mkdir -p "$BACKUP_ROOT" "$DEPLOY_HIST"
# NOTE: DEPLOYS_LOG and LOG_FILE set after arg parsing (depends on --stage flag)

cleanup() { $SSH -O exit 2>/dev/null || true; }
trap cleanup EXIT

# ── Parse args ──────────────────────────────────────────────────────
FORCE_FULL=false
LABEL=""
LIST_ONLY=false
STATUS_ONLY=false
DRY_RUN=false
CLEAN_FIRST=false
WATCH=false
DEPLOY_ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --full)        FORCE_FULL=true; shift ;;
        --label)       LABEL="$2"; shift 2 ;;
        --list)        LIST_ONLY=true; shift ;;
        --status)      STATUS_ONLY=true; shift ;;
        --dry-run)     DRY_RUN=true; shift ;;
        --clean-first) CLEAN_FIRST=true; shift ;;
        --stage)       STAGE=true; shift ;;
        --watch)       WATCH=true; shift ;;
        *)             DEPLOY_ARGS+=("$1"); shift ;;
    esac
done

# ── Stage server override (must be after arg parsing) ──────────────
if [[ "$STAGE" == "true" ]]; then
    SERVER="${DEPLOY_HOST:-user@host.example.com}"
    echo "⚠️  Deploying to STAGE (0.0.0.0) — sim only, no live orders"
    # Rebuild SSH/SCP with new SERVER
    SSH="ssh -i $KEY -o ControlMaster=auto -o ControlPath=$CTRL -o ControlPersist=60s $SERVER"
    SCP="scp -i $KEY -o ControlPath=$CTRL -o Compression=no"
fi

# ── Env-specific log files (depends on --stage) ────────────────────
ENV_TAG="prod"
[[ "$STAGE" == "true" ]] && ENV_TAG="stage"
DEPLOYS_LOG="$BACKUP_ROOT/deployments_${ENV_TAG}.log"
LOG_FILE="$BACKUP_ROOT/deploy_${ENV_TAG}.log"
exec > >(tee -a "$LOG_FILE") 2>&1

# ── Watch only (no deploy) ──────────────────────────────────────────
if [[ "$WATCH" == "true" ]] && [[ "$FORCE_FULL" == "false" ]] && [[ "$CLEAN_FIRST" == "false" ]]; then
    echo "👁  Tailing predict-engine logs (Ctrl+C to stop)..."
    $SSH "journalctl -u predict-engine -f --no-pager -n 40" || true
    exit 0
fi

# ── List deployments ────────────────────────────────────────────────
if [[ "$LIST_ONLY" == "true" ]]; then
    echo "📋 Deployment History"
    echo "====================="
    if [[ -f "$DEPLOYS_LOG" ]]; then
        cat "$DEPLOYS_LOG"
    else
        echo "No deployments recorded yet"
    fi
    exit 0
fi

# ── Show current status ─────────────────────────────────────────────
if [[ "$STATUS_ONLY" == "true" ]]; then
    echo "📊 Last Deployment"
    echo "=================="
    if [[ -f "$DEPLOYS_LOG" ]]; then
        tail -1 "$DEPLOYS_LOG"
    else
        echo "No deployments recorded yet"
    fi
    exit 0
fi

# ── Main deployment ─────────────────────────────────────────────────
echo "════════════════════════════════════════"
echo "$(date '+%Y-%m-%d %H:%M:%S') deploy.sh start"
echo "════════════════════════════════════════"
echo ""

# Cleanup remote if requested
# CSVs live directly in ~/engine/ (preds_*.csv) and ~/engine/logs/ (signals_*.csv)
if [[ "$CLEAN_FIRST" == "true" ]]; then
    echo "🧹 Cleaning remote CSVs and signal logs..."
    $SSH "rm -f $REMOTE_DIR/preds_*.csv $REMOTE_DIR/preds_b_*.csv $REMOTE_DIR/logs/signals_*.csv" 2>/dev/null || true
    echo "   ✅ Remote cleaned"
    echo ""
fi

DEPLOY_TIME=$(date +%Y%m%d_%H%M%S)
DEPLOY_DATE=$(date '+%Y-%m-%d %H:%M:%S')

if [[ -z "$LABEL" ]]; then
    LABEL="Auto-deploy"
fi

echo "🚀 Deploying: $LABEL"
echo "   Time: $DEPLOY_DATE"
echo ""

# ── Select correct strategies_config for environment ────────────────
if [[ "$STAGE" == "true" ]]; then
    [[ -f "strategies_config_stage.py" ]]   && cp strategies_config_stage.py   strategies_config.py   && echo "   📋 Using strategies_config_stage.py"
    [[ -f "strategies_config_b_stage.py" ]] && cp strategies_config_b_stage.py strategies_config_b.py && echo "   📋 Using strategies_config_b_stage.py"
else
    [[ -f "strategies_config_prod.py" ]]    && cp strategies_config_prod.py    strategies_config.py   && echo "   📋 Using strategies_config_prod.py"
    [[ -f "strategies_config_b_prod.py" ]]  && cp strategies_config_b_prod.py  strategies_config_b.py && echo "   📋 Using strategies_config_b_prod.py"
fi

# ── File lists ──────────────────────────────────────────────────────
HOT_FILES=(strategies_config.py strategies_config_b.py)

[[ "$STAGE" == "true" ]] && ENV_FILE=".env.stage" || ENV_FILE=".env.prod"

ENGINE_FILES=(
    config.py core_signals.py engine.py engine_scanner.py engine_lag.py
    predict_engine.py dashboard_multi.py strategies.py strategies_signals.py
    strategies_engine.py strategies_runtime.py engine_logger.py
    live_execution.py close_all_positions.py tg_monitor.py market_maker_paper.py
)

# Stage-only extra files
STAGE_FILES=()
[[ "$STAGE" == "true" ]] && STAGE_FILES=(demo_balance_reset.py)

# Classify files
HOT_PRESENT=()
ENGINE_PRESENT=()
for f in "${HOT_FILES[@]}"; do [[ -f "$f" ]] && HOT_PRESENT+=("$f"); done
for f in "${ENGINE_FILES[@]}"; do [[ -f "$f" ]] && ENGINE_PRESENT+=("$f"); done

# Decide mode
if $FORCE_FULL || [[ ${#ENGINE_PRESENT[@]} -gt 0 ]]; then
    MODE="full"
elif [[ ${#HOT_PRESENT[@]} -gt 0 ]]; then
    MODE="hot"
else
    echo "❌ No files to deploy"
    exit 1
fi

echo "📦 Mode: $MODE"
printf "   Files: %s\n" "${HOT_PRESENT[@]}" "${ENGINE_PRESENT[@]}"
echo ""

# ── Save config snapshot ────────────────────────────────────────────
for f in "${HOT_PRESENT[@]}"; do
    cp "$f" "$DEPLOY_HIST/${f%.*}_$DEPLOY_TIME.py" 2>/dev/null || true
done

# ── Pre-deploy checks ──────────────────────────────────────────────
preflight_check() {
    local files=("$@")
    local errors=0
    local warnings=0

    echo "==> Pre-deploy checks..."

    for f in "${files[@]}"; do
        [[ -f "$f" ]] || continue

        # 1. Python syntax check
        if [[ "$f" == *.py ]]; then
            if ! python3 -m py_compile "$f" 2>/tmp/syntax_err_$$.txt; then
                echo "    ❌ SYNTAX ERROR: $f"
                cat /tmp/syntax_err_$$.txt
                errors=$((errors+1))
                continue
            fi
            echo "    ✓  syntax OK: $f"
        fi

        # 2. Check for disabled=True on non-disabled strategies
        if [[ "$f" == "strategies_config.py" ]]; then
            DISABLED=$(python3 -c "
import ast, sys
src = open('$f').read()
src2 = src.replace('import engine as E','').replace('from config import FEE_RT, VERSION, SPREAD_MAX_PCT','')
g = {}
try:
    exec(src2, g)
    strats = g.get('STRATEGIES', [])
    disabled = [s.label for s in strats if s.disabled]
    active   = [s.label for s in strats if not s.disabled]
    print(f'active={len(active)} disabled={len(disabled)} labels_disabled={disabled}')
except Exception as e:
    print(f'error={e}')
" 2>/dev/null)
            echo "    ✓  config: $DISABLED"
        fi

        # 3. Check strategies_engine.py for required gate methods
        if [[ "$f" == "strategies_engine.py" ]]; then
            for gate in _k_gate _w_gate _y_gate _b_gate _cgy_gate _wb_gate; do
                if ! grep -q "def $gate" "$f"; then
                    echo "    ❌ MISSING METHOD: $gate in $f"
                    errors=$((errors+1))
                else
                    echo "    ✓  method: $gate"
                fi
            done
            if grep -q "self\.preds = \[\]" "$f"; then
                echo "    ❌ CRITICAL: self.preds initialized as list, must be deque"
                errors=$((errors+1))
            fi
            if ! grep -q "'disabled':self.cfg.disabled" "$f"; then
                echo "    ⚠️  WARNING: 'disabled' not in snapshot() — dashboard filter will break"
                warnings=$((warnings+1))
            else
                echo "    ✓  snapshot includes disabled field"
            fi
            # Check _apply_reload attributes — missing ones crash on hot-reload
            # Check fire() has force_sim param — required by strategies_runtime v18
            if ! grep -q "def fire(self.*force_sim" "$f"; then
                echo "    ❌ MISSING: fire() lacks force_sim param — strategies_runtime will crash"
                errors=$((errors+1))
            else
                echo "    ✓  fire() has force_sim param"
            fi
            for attr in _session_start_cum _cum_net _bnb_cum_pnl _bnb_cum_comm; do
                if ! grep -q "self\.$attr" "$f"; then
                    echo "    ❌ MISSING ATTR: self.$attr — hot-reload will crash"
                    errors=$((errors+1))
                else
                    echo "    ✓  attr: $attr"
                fi
            done
        fi

        # 4. Check live_execution.py config
        if [[ "$f" == "live_execution.py" ]]; then
            LIVE_CHECK=$(python3 -c "
import ast
src = open('$f').read()
tree = ast.parse(src)
# Check TESTNET_BASE points to demo, not old testnet
if 'testnet.binancefuture.com' in src:
    print('WARN: still references testnet.binancefuture.com — should be demo-fapi.binance.com')
elif 'demo-fapi.binance.com' in src:
    print('ok: demo-fapi.binance.com')
else:
    print('WARN: no Binance base URL found')
" 2>/dev/null)
            echo "    ✓  live_execution: $LIVE_CHECK"
        fi

        # 5. Check strategies_config_b.py only references valid labels
        if [[ "$f" == "strategies_config_b.py" ]]; then
            INVALID=$(python3 -c "
import re
src = open('$f').read()
tweaks = re.findall(r\"_tweak\('([A-Z]+)'\", src)
removed = {'T','R','X','O','P','N'}
bad = [l for l in tweaks if l in removed]
if bad: print('invalid_labels=' + ','.join(bad))
else: print('ok')
" 2>/dev/null)
            if [[ "$INVALID" == ok ]] || [[ -z "$INVALID" ]]; then
                echo "    ✓  config_b: no removed strategy references"
            else
                echo "    ❌ config_b references removed strategies: $INVALID"
                errors=$((errors+1))
            fi
        fi

        # 6. Check for common crash patterns
        if [[ "$f" == *.py ]]; then
            if grep -v "^#\|^ *#" "$f" | grep -q "deque(" && ! grep -q "from collections import.*deque\|import collections" "$f"; then
                echo "    ⚠️  WARNING: deque used but not imported in $f"
                warnings=$((warnings+1))
            fi
        fi
    done

    rm -f /tmp/syntax_err_$$.txt

    echo ""
    if [[ $errors -gt 0 ]]; then
        echo "    ❌ $errors error(s) — aborting deploy"
        return 1
    elif [[ $warnings -gt 0 ]]; then
        echo "    ⚠️  $warnings warning(s) — deploying anyway"
        return 0
    else
        echo "    ✅ All checks passed"
        return 0
    fi
}

# ── HOT RELOAD ──────────────────────────────────────────────────────
if [[ "$MODE" == "hot" ]]; then
    echo "==> Hot-reload config files (no restart)..."

    if $DRY_RUN; then
        echo "    [dry-run] Would push: ${HOT_PRESENT[*]}"
        exit 0
    fi

    if ! preflight_check "${HOT_PRESENT[@]}"; then
        exit 1
    fi

    for f in "${HOT_PRESENT[@]}"; do
        $SCP "$f" "$SERVER:$REMOTE_DIR/$f"
        echo "    ✅ $f"
    done

    sleep 4
    STATUS=$($SSH "sudo systemctl is-active predict-engine 2>/dev/null || echo inactive")

    if [[ "$STATUS" == "active" ]]; then
        echo "    ✅ Service still running"
    else
        echo "    ⚠️  Service $STATUS, restarting..."
        $SSH "sudo systemctl restart predict-engine"
        sleep 2
    fi

    MODE_TAG="hot"
fi

# ── FULL DEPLOY ─────────────────────────────────────────────────────
if [[ "$MODE" == "full" ]]; then
    echo "==> Full deploy (with restart)..."

    SEND=()
    for f in "${HOT_PRESENT[@]}" "${ENGINE_PRESENT[@]}" "${STAGE_FILES[@]}"; do
        [[ -f "$f" ]] && SEND+=("$f")
    done

    [[ ${#SEND[@]} -eq 0 ]] && { echo "❌ No files to send"; exit 1; }

    if $DRY_RUN; then
        echo "    [dry-run] Would deploy ${#SEND[@]} files and restart"
        exit 0
    fi

    if ! preflight_check "${SEND[@]}"; then
        exit 1
    fi

    # Zip and upload
    TMPDIR=$(mktemp -d)
    ZIP="$TMPDIR/deploy_$DEPLOY_TIME.zip"

    zip -q "$ZIP" "${SEND[@]}"
    SHA=$(sha256sum "$ZIP" | awk '{print $1}')
    SIZE=$(du -sh "$ZIP" | cut -f1)

    echo "    📦 Zip: $SIZE  SHA256: ${SHA:0:16}..."

    REMOTE_ZIP="$REMOTE_DIR/$(basename $ZIP)"

    $SCP "$ZIP" "$SERVER:$REMOTE_ZIP"
    echo "    ✅ Uploaded"

    REMOTE_SHA=$($SSH "sha256sum $REMOTE_ZIP | awk '{print \$1}'")
    if [[ "$SHA" != "$REMOTE_SHA" ]]; then
        echo "❌ Checksum mismatch!"
        $SSH "rm -f $REMOTE_ZIP"
        exit 1
    fi

    $SSH "cd $REMOTE_DIR && unzip -qo $(basename $ZIP) && rm -f $(basename $ZIP)"
    echo "    ✅ Extracted"

    # Push correct .env (renamed to .env on remote)
    $SCP "$ENV_FILE" "$SERVER:$REMOTE_DIR/.env"
    echo "    ✅ .env pushed ($ENV_FILE → .env)"

    $SSH "sudo systemctl restart predict-engine"
    sleep 3
    echo "    ✅ Restarted"

    rm -rf "$TMPDIR"
    MODE_TAG="full"
fi

# ── Record deployment ────────────────────────────────────────────────
echo ""
echo "==> Recording deployment..."
echo "$DEPLOY_TIME | $MODE_TAG | $LABEL" >> "$DEPLOYS_LOG"
echo "    ✅ Recorded: $DEPLOY_TIME"

# ── Verify service ─────────────────────────────────────────────────
echo ""
echo "==> Verifying service..."
sleep 2
STATUS_AFTER=$($SSH "sudo systemctl is-active predict-engine 2>/dev/null || echo inactive")
DASH_PORT=8080
[[ "$STAGE" == "true" ]] && DASH_PORT=8001
DASH_IP=$(echo "$SERVER" | cut -d@ -f2)
HEALTH=$($SSH "curl -s -o /dev/null -w '%{http_code}' localhost:$DASH_PORT" 2>/dev/null || echo "000")

if [[ "$STATUS_AFTER" == "active" ]]; then
    echo "    ✅ Service: active"
else
    echo "    ❌ Service: $STATUS_AFTER"
fi

if [[ "$HEALTH" == "200" ]] || [[ "$HEALTH" == "302" ]]; then
    echo "    ✅ Dashboard: http://$DASH_IP:$DASH_PORT"
else
    echo "    ⚠️  Dashboard returned HTTP $HEALTH"
fi

echo ""
echo "════════════════════════════════════════"
echo "✅ Deployment complete: $LABEL"
echo "════════════════════════════════════════"
echo ""

# ── Watch logs after deploy ─────────────────────────────────────────
if [[ "$WATCH" == "true" ]]; then
    echo "👁  Tailing logs (Ctrl+C to stop)..."
    echo ""
    $SSH "journalctl -u predict-engine -f --no-pager -n 30" || true
else
    echo "Next steps:"
    echo "  Watch logs:  bash deploy.sh --watch"
    echo "  Analyze:     python3 analyze.sh --since deploy"
    echo "  History:     bash deploy.sh --list"
fi
