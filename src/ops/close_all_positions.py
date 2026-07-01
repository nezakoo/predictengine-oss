#!/usr/bin/env python3
"""
close_all_positions.py — Force-close all open Binance demo positions.
Run this before a deploy or when the UI's "Close Positions" button fails
with PERCENT_PRICE errors.

Usage:
    python3 close_all_positions.py            # dry-run: show what would be closed
    python3 close_all_positions.py --execute  # actually close all positions
"""
import os, sys, hashlib, hmac, time, urllib.parse, argparse
import requests
from pathlib import Path

def _read_env_file(path):
    """Read a specific .env file into a dict. Explicit — no os.environ merging,
    so a destructive tool can't be silently pointed at the wrong account."""
    d = {}
    p = Path(path)
    if p.exists():
        for line in p.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line: continue
            k, v = line.split("=", 1)
            d[k.strip()] = v.strip().strip('"').strip("'")
    return d

# Resolved in main() once the --live flag is known. No module-level Binance state.
API_KEY = API_SECRET = ""
BASE_URL = "https://demo-fapi.binance.com"
LIVE_MODE = False

def _sign(params):
    qs = urllib.parse.urlencode(params)
    return hmac.new(API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()

def _req(method, path, params=None):
    params = params or {}
    params["timestamp"] = int(time.time() * 1000)
    params["signature"] = _sign(params)
    headers = {"X-MBX-APIKEY": API_KEY}
    url = BASE_URL + path
    if method == "GET":
        r = requests.get(url, headers=headers, params=params, timeout=10)
    else:
        r = requests.post(url, headers=headers, params=params, timeout=10)
    return r.json()

def get_open_positions():
    data = _req("GET", "/fapi/v2/positionRisk")
    if not isinstance(data, list):
        print(f"❌ positionRisk error: {data}")
        return []
    return [p for p in data if float(p.get("positionAmt", 0)) != 0]

def close_position(sym, amt, dry_run=True):
    side = "SELL" if float(amt) > 0 else "BUY"
    qty  = abs(float(amt))
    if dry_run:
        print(f"  [DRY] Would close {sym}: {side} qty={qty}")
        return True

    # Attempt 1: standard reduceOnly market close
    resp = _req("POST", "/fapi/v1/order", {
        "symbol": sym, "side": side, "type": "MARKET",
        "quantity": qty, "reduceOnly": "true",
    })
    if "orderId" in resp:
        print(f"  ✅ Closed {sym}: {side} qty={qty} → orderId={resp['orderId']}")
        return True

    # Attempt 2: closePosition=true (bypasses PERCENT_PRICE on demo)
    if resp.get("code") in (-4131, -1111, -4003):
        print(f"  ↩  Retrying {sym} with closePosition=true...")
        resp2 = _req("POST", "/fapi/v1/order", {
            "symbol": sym, "side": side, "type": "MARKET", "closePosition": "true",
        })
        if "orderId" in resp2:
            print(f"  ✅ Closed {sym} (closePosition) → orderId={resp2['orderId']}")
            return True
        resp = resp2  # fall through to attempt 3

    # Attempt 3: plain market order without reduceOnly (works on all position types)
    print(f"  ↩  Retrying {sym} plain market order (no reduceOnly)...")
    resp3 = _req("POST", "/fapi/v1/order", {
        "symbol": sym, "side": side, "type": "MARKET", "quantity": qty,
    })
    if "orderId" in resp3:
        print(f"  ✅ Closed {sym} (plain market) → orderId={resp3['orderId']}")
        return True

    print(f"  ❌ Failed {sym} (all 3 attempts): {resp3}")
    return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true", help="Actually close (default is dry-run)")
    parser.add_argument("--live", action="store_true",
                        help="Target PROD / real money (fapi.binance.com). Default is DEMO.")
    args = parser.parse_args()

    # Resolve target explicitly from the flag — never from a stray LIVE_MODE in some .env.
    global API_KEY, API_SECRET, BASE_URL, LIVE_MODE
    LIVE_MODE = args.live
    BASE_URL  = "https://fapi.binance.com" if LIVE_MODE else "https://demo-fapi.binance.com"
    env_file  = ".env.prod" if LIVE_MODE else ".env.stage"
    cfg = _read_env_file(env_file)
    if not cfg.get("BINANCE_API_KEY"):                       # local file absent? try the box's active env
        cfg = _read_env_file(os.path.expanduser("~/engine/.env")) or cfg
    API_KEY    = cfg.get("BINANCE_API_KEY", "")
    API_SECRET = cfg.get("BINANCE_API_SECRET", "")
    if not API_KEY or not API_SECRET:
        print(f"❌ No BINANCE_API_KEY/SECRET in {env_file} or ~/engine/.env — refusing to run.")
        print(f"   Run on the {'prod' if LIVE_MODE else 'stage'} box, or ensure {env_file} has keys.")
        sys.exit(1)

    dry_run = not args.execute
    mode = "🔴 LIVE — REAL MONEY" if LIVE_MODE else "🟡 DEMO"
    print(f"\n{'DRY RUN — ' if dry_run else ''}Closing all positions on {mode}")
    print(f"Base URL: {BASE_URL}   (env: {env_file})\n")

    positions = get_open_positions()
    if not positions:
        print("✅ No open positions found.")
        return

    print(f"Found {len(positions)} open position(s):\n")
    for pos in positions:
        sym = pos["symbol"]
        amt = pos["positionAmt"]
        entry = pos.get("entryPrice", "?")
        pnl   = pos.get("unRealizedProfit", "?")
        side  = "LONG" if float(amt) > 0 else "SHORT"
        print(f"  {sym:20s} {side:5s}  qty={abs(float(amt))}  entry={entry}  uPnL={pnl}")

    print()
    if dry_run:
        print("👆 Run with --execute to actually close these.\n")
        return

    prompt = (f"⚠️  Close all {len(positions)} REAL position(s) on PROD? type 'CLOSE LIVE' to confirm: "
              if LIVE_MODE else
              f"Close all {len(positions)} position(s)? [y/N] ")
    confirm = input(prompt).strip()
    ok_confirm = (confirm == "CLOSE LIVE") if LIVE_MODE else (confirm.lower() == "y")
    if not ok_confirm:
        print("Aborted.")
        return

    print()
    ok = all(close_position(p["symbol"], p["positionAmt"], dry_run=False) for p in positions)
    print(f"\n{'✅ All closed.' if ok else '⚠️  Some failed — check output above.'}")

if __name__ == "__main__":
    main()
