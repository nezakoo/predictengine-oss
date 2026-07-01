#!/usr/bin/env python3
"""
demo_balance_reset.py — Auto-reset Binance demo account when balance drops to threshold.

Runs as a loop on stage server. When balance <= RESET_THRESHOLD:
  1. Close all open positions
  2. Call Binance demo reset API
  3. Send Telegram notification
  4. Wait COOLDOWN_SEC before checking again

Usage:
  python3 demo_balance_reset.py              # run loop (default threshold $100)
  python3 demo_balance_reset.py --threshold 200
  python3 demo_balance_reset.py --once       # check once and exit
  python3 demo_balance_reset.py --force      # force reset now regardless of balance

Systemd: add to predict-monitor.service or run as separate service.
"""
import os, sys, time, hashlib, hmac, urllib.parse, argparse, json
import requests
from pathlib import Path
from datetime import datetime, timezone

# ── Config ───────────────────────────────────────────────────────────
def _load_env(path=".env"):
    for line in Path(path).read_text().splitlines() if Path(path).exists() else []:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line: continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

_load_env(".env.stage")                     # demo keys (local runs) — load most-specific first
_load_env("/home/ubuntu/engine/.env")       # stage box path
_load_env(".env")                           # generic fallback (lowest priority)

API_KEY    = os.environ.get("BINANCE_API_KEY", "")
API_SECRET = os.environ.get("BINANCE_API_SECRET", "")
TG_TOKEN   = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT    = os.environ.get("TG_CHAT_ID", "")
TG_PREFIX  = os.environ.get("TELEGRAM_PREFIX", "[STAGE]")
BASE_URL   = "https://demo-fapi.binance.com"

RESET_THRESHOLD = float(os.environ.get("DEMO_RESET_THRESHOLD", "100"))
CHECK_INTERVAL  = 60    # seconds between balance checks
COOLDOWN_SEC    = 300   # seconds to wait after a reset before checking again

# ── Helpers ──────────────────────────────────────────────────────────
def _sign(params):
    qs = urllib.parse.urlencode(params)
    return hmac.new(API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()

def _req(method, path, params=None):
    params = params or {}
    params["timestamp"] = int(time.time() * 1000)
    params["signature"] = _sign(params)
    headers = {"X-MBX-APIKEY": API_KEY}
    url = BASE_URL + path
    r = requests.get(url, headers=headers, params=params, timeout=10) if method == "GET" \
        else requests.post(url, headers=headers, params=params, timeout=10)
    return r.json()

def _tg(msg):
    if not TG_TOKEN or not TG_CHAT: 
        print(f"[tg] {msg}")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": f"{TG_PREFIX} {msg}", "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        print(f"[tg] failed: {e}")

def get_balance() -> float:
    data = _req("GET", "/fapi/v2/balance")
    if not isinstance(data, list):
        print(f"[balance] error: {data}")
        return -1
    for asset in data:
        if asset.get("asset") == "USDT":
            return float(asset.get("balance", 0))
    return 0.0

def get_open_positions() -> list:
    data = _req("GET", "/fapi/v2/positionRisk")
    if not isinstance(data, list): return []
    return [p for p in data if float(p.get("positionAmt", 0)) != 0]

def close_position(sym, amt):
    side = "SELL" if float(amt) > 0 else "BUY"
    qty  = abs(float(amt))
    resp = _req("POST", "/fapi/v1/order", {
        "symbol": sym, "side": side, "type": "MARKET",
        "quantity": qty, "reduceOnly": "true",
    })
    if "orderId" in resp:
        return True
    # Fallback: closePosition
    resp2 = _req("POST", "/fapi/v1/order", {
        "symbol": sym, "side": side, "type": "MARKET", "closePosition": "true",
    })
    return "orderId" in resp2

def reset_demo_balance():
    """
    Binance demo account reset endpoint.
    POST /fapi/v1/account/reset (demo-fapi only)
    Resets balance to $10,000 USDT.
    """
    try:
        params = {"timestamp": int(time.time() * 1000)}
        params["signature"] = _sign(params)
        r = requests.post(
            BASE_URL + "/fapi/v1/account/reset",
            headers={"X-MBX-APIKEY": API_KEY},
            params=params,
            timeout=10,
        )
        data = r.json()
        if r.status_code == 200 or "code" not in data:
            return True, data
        return False, data
    except Exception as e:
        return False, str(e)

def do_reset(reason="balance threshold"):
    now = datetime.now(timezone.utc).strftime("%H:%M UTC")
    print(f"[{now}] 🔄 Triggering demo reset ({reason})...")

    # 1. Close all positions
    positions = get_open_positions()
    if positions:
        print(f"  Closing {len(positions)} open position(s)...")
        for p in positions:
            ok = close_position(p["symbol"], p["positionAmt"])
            status = "✅" if ok else "❌"
            print(f"  {status} {p['symbol']}")
        time.sleep(2)  # let closes settle

    # 2. Reset balance
    ok, resp = reset_demo_balance()
    if ok:
        print(f"  ✅ Balance reset → $10,000 USDT")
        _tg(f"🔄 <b>Demo balance reset</b>\nReason: {reason}\nNew balance: ~$10,000 USDT\n{now}")
        return True
    else:
        print(f"  ❌ Reset failed: {resp}")
        _tg(f"❌ <b>Demo reset FAILED</b>\n{resp}\n{now}")
        return False

# ── Main ─────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--threshold", type=float, default=RESET_THRESHOLD,
                        help=f"Reset when balance <= this (default: {RESET_THRESHOLD})")
    parser.add_argument("--once",  action="store_true", help="Check once and exit")
    parser.add_argument("--force", action="store_true", help="Force reset now")
    args = parser.parse_args()

    if not API_KEY or not API_SECRET:
        print("❌ BINANCE_API_KEY/SECRET not set")
        sys.exit(1)

    threshold = args.threshold
    print(f"Demo balance monitor | threshold=${threshold} | interval={CHECK_INTERVAL}s")

    if args.force:
        do_reset("forced via --force flag")
        return

    last_reset = 0.0

    while True:
        try:
            balance = get_balance()
            now = datetime.now(timezone.utc).strftime("%H:%M:%S")

            if balance < 0:
                print(f"[{now}] ⚠️  Could not fetch balance")
            elif balance <= threshold:
                if time.time() - last_reset > COOLDOWN_SEC:
                    print(f"[{now}] Balance ${balance:.2f} ≤ ${threshold} — resetting...")
                    if do_reset(f"balance ${balance:.2f} ≤ ${threshold}"):
                        last_reset = time.time()
                else:
                    remaining = int(COOLDOWN_SEC - (time.time() - last_reset))
                    print(f"[{now}] Balance ${balance:.2f} (cooldown {remaining}s)")
            else:
                print(f"[{now}] Balance ${balance:.2f} ✅")

            if args.once:
                break

            time.sleep(CHECK_INTERVAL)

        except KeyboardInterrupt:
            print("\nStopped.")
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(30)

if __name__ == "__main__":
    main()
