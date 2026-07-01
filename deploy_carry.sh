#!/usr/bin/env bash
# deploy_carry.sh — deploy the cash-and-carry PAPER stack to stage.
#
# Deploys to ~/carry/ on the stage box (isolated from ~/engine), and restarts the
# paper engine + dashboard tmux sessions. It does NOT start live trading: carry_live.py
# is armed manually with explicit switches, by design. Prod is refused outright — live
# carry is a deliberate manual step with real keys, never a scripted deploy.
#
# Usage:
#   ./deploy_carry.sh                      # deploy files + restart paper services on stage
#   ./deploy_carry.sh --no-restart         # deploy files only
#   ./deploy_carry.sh --open-port          # also open dashboard port 8090 in iptables
#   ./deploy_carry.sh --label "msg"        # annotate the deploy log
set -euo pipefail

# ── config ────────────────────────────────────────────────────────────────────
SSH_HOST="${CARRY_SSH:-stage}"            # uses your `ssh stage` alias
REMOTE_DIR="~/carry"
DASH_PORT="${CARRY_DASH_PORT:-8090}"
LOCAL_SRC="$(cd "$(dirname "$0")" && pwd)"   # run from inside the carry_live_stack dir
LOG="deployments_carry.log"

RESTART=1; OPEN_PORT=0; LABEL=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-restart) RESTART=0 ;;
    --open-port)  OPEN_PORT=1 ;;
    --label)      LABEL="$2"; shift ;;
    --prod)       echo "REFUSED: live carry on prod is a manual, deliberate step with real keys — never scripted."; exit 1 ;;
    *) echo "unknown arg: $1"; exit 1 ;;
  esac
  shift
done

# ── files to ship (paper stack + executors + fixed live_execution, kept local to ~/carry) ──
FILES=(carry_paper.py carry_dashboard.py carry_exec.py spot_exec.py carry_live.py live_execution.py README.md)
for f in "${FILES[@]}"; do
  [[ -f "$LOCAL_SRC/$f" ]] || { echo "missing $f in $LOCAL_SRC"; exit 1; }
done

echo "→ deploying carry PAPER stack to $SSH_HOST:$REMOTE_DIR"
ssh "$SSH_HOST" "mkdir -p $REMOTE_DIR $REMOTE_DIR/backtest"

# ship files via tar-over-ssh (no rsync dependency on either side)
TAR_INC=("${FILES[@]}")
[[ -d "$LOCAL_SRC/backtest" ]] && TAR_INC+=(backtest)
tar -czf - -C "$LOCAL_SRC" "${TAR_INC[@]}" | ssh "$SSH_HOST" "tar -xzf - -C $REMOTE_DIR"

# sanity: byte-compile remotely so a bad push fails loudly before we restart anything
echo "→ remote syntax check"
ssh "$SSH_HOST" "cd $REMOTE_DIR && python3 -m py_compile carry_paper.py carry_dashboard.py carry_exec.py spot_exec.py carry_live.py live_execution.py && echo '  ok'"

if [[ "$OPEN_PORT" == "1" ]]; then
  echo "→ opening dashboard port $DASH_PORT (iptables ACCEPT before REJECT)"
  ssh "$SSH_HOST" "sudo iptables -C INPUT -p tcp --dport $DASH_PORT -j ACCEPT 2>/dev/null || sudo iptables -I INPUT 1 -p tcp --dport $DASH_PORT -j ACCEPT"
fi

if [[ "$RESTART" == "1" ]]; then
  echo "→ restarting paper engine + dashboard (tmux)"
  ssh "$SSH_HOST" bash -s <<REMOTE
    cd $REMOTE_DIR
    tmux kill-session -t carry 2>/dev/null || true
    tmux kill-session -t carrydash 2>/dev/null || true
    tmux new -s carry -d
    tmux send-keys -t carry 'cd $REMOTE_DIR && python3 carry_paper.py --loop --interval 900 --k 8 --entry-bp 2 --exit-bp 0.5 --notional 100' Enter
    tmux new -s carrydash -d
    tmux send-keys -t carrydash 'cd $REMOTE_DIR && python3 carry_dashboard.py --port $DASH_PORT' Enter
    sleep 1
    echo "  sessions:"; tmux ls | sed 's/^/    /'
REMOTE
  echo "→ dashboard: http://\$(ssh $SSH_HOST 'curl -s ifconfig.me 2>/dev/null || hostname -I'):$DASH_PORT"
fi

# ── log it (your deploy-log discipline) ────────────────────────────────────────
STAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "$STAMP  stage  paper-stack  restart=$RESTART  port=$OPEN_PORT  ${LABEL}" >> "$LOG"
echo "✓ deployed (PAPER only). Live trading is NOT started — arm carry_live.py manually with"
echo "  --live + LIVE_MODE=true + SPOT_LIVE=true + real keys when you deliberately choose to."
