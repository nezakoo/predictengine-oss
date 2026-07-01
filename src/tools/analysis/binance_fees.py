#!/usr/bin/env python3
"""
Fetch recent FUTURES trades (with fees) from Binance.

Usage:
    python binance_fees.py                   # both envs
    python binance_fees.py --env prod        # prod only
    python binance_fees.py --env stage       # stage only
    python binance_fees.py --csv             # export to fees_output.csv
    python binance_fees.py --limit 200       # number of trades (default 100)
"""

import time
import hmac
import hashlib
import argparse
import csv
from datetime import datetime, timezone
from dotenv import dotenv_values
import requests

ENVIRONMENTS = {
    "prod":  {"env_file": ".env.prod",  "base_url": "https://fapi.binance.com"},
    "stage": {"env_file": ".env.stage", "base_url": "https://testnet.binancefuture.com"},
}

# ── HTTP ──────────────────────────────────────────────────────────────────────

def sign(params: dict, secret: str) -> str:
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()

def signed_get(base: str, path: str, params: dict, api_key: str, secret: str):
    params["timestamp"]  = int(time.time() * 1000)
    params["recvWindow"] = 10000
    params["signature"]  = sign(params, secret)
    return requests.get(
        f"{base}{path}",
        headers={"X-MBX-APIKEY": api_key},
        params=params,
        timeout=10,
    )

# ── Auth ──────────────────────────────────────────────────────────────────────

def verify_auth(base: str, api_key: str, secret: str) -> bool:
    r = signed_get(base, "/fapi/v2/account", {}, api_key, secret)
    if r.status_code != 200:
        print(f"  ❌ Auth FAILED: HTTP {r.status_code} → {r.json()}")
        return False
    d = r.json()
    print(f"  ✅ Auth OK  |  "
          f"makerFee: {float(d.get('feeTier', 0)) or 'tier ' + str(d.get('feeTier', '?'))}  "
          f"totalUnrealizedProfit: {float(d.get('totalUnrealizedProfit', 0)):.4f}  "
          f"canTrade: {d.get('canTrade')}")
    return True

def bnb_price_usdt(base: str) -> float:
    """Current BNBUSDT mark price (public, no auth) — to convert BNB-denominated fees to USDT."""
    try:
        r = requests.get(f"{base}/fapi/v1/ticker/price", params={"symbol": "BNBUSDT"}, timeout=10)
        if r.status_code == 200:
            return float(r.json()["price"])
    except Exception:
        pass
    return 0.0

# ── Symbols ───────────────────────────────────────────────────────────────────

def fetch_income(base: str, api_key: str, secret: str, limit: int) -> list[dict]:
    """/fapi/v1/income with pagination across 30-day window."""
    all_records = []
    end_time = int(time.time() * 1000)
    start_time = end_time - (30 * 24 * 60 * 60 * 1000)  # 30 days back

    while True:
        r = signed_get(base, "/fapi/v1/income",
                       {"incomeType": "COMMISSION",
                        "limit": 1000,
                        "startTime": start_time,
                        "endTime": end_time},
                       api_key, secret)
        if r.status_code != 200:
            print(f"  ❌ Failed to fetch income: {r.status_code} → {r.json()}")
            break
        batch = r.json()
        if not batch:
            break
        all_records.extend(batch)
        if len(batch) < 1000 or len(all_records) >= limit:
            break
        # paginate: move end_time to just before earliest record
        end_time = min(t["time"] for t in batch) - 1
        time.sleep(0.1)

    return all_records[:limit]

def format_trade(t: dict, env: str) -> dict:
    ts = datetime.fromtimestamp(t["time"] / 1000, tz=timezone.utc)
    return {
        "env":    env,
        "time":   ts.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "symbol": t["symbol"],
        "income": t["income"],        # negative = fee paid
        "asset":  t["asset"],
        "info":   t.get("info", ""),  # tradeId reference
        "tradeId": t.get("tradeId", ""),
    }

# ── Core ──────────────────────────────────────────────────────────────────────

def run_env(env_name: str, config: dict, limit: int) -> list[dict]:
    cfg     = dotenv_values(config["env_file"])
    api_key = cfg.get("BINANCE_API_KEY", "").strip()
    secret  = cfg.get("BINANCE_API_SECRET", "").strip()
    base    = config["base_url"]

    if not api_key or not secret:
        print(f"[{env_name}] ⚠️  Missing keys in {config['env_file']} — skipping.")
        return []

    print(f"\n[{env_name}] {base}")
    if not verify_auth(base, api_key, secret):
        return []

    print(f"  Fetching last {limit} commission entries...")
    raw = fetch_income(base, api_key, secret, limit)
    if not raw:
        return []

    trades = [format_trade(t, env_name) for t in raw]
    trades.sort(key=lambda x: x["time"], reverse=True)
    print(f"  ✓ Got {len(trades)} commission record(s)")
    return trades

# ── Output ────────────────────────────────────────────────────────────────────

def print_table(trades: list[dict]):
    if not trades:
        print("\nNo trades found.")
        return
    header = (f"{'ENV':<6} {'TIME':<22} {'SYMBOL':<12} {'FEE':<14} {'ASSET':<6} {'TRADE ID'}")
    sep = "─" * len(header)
    print(f"\n{sep}\n{header}\n{sep}")
    for t in trades:
        print(f"{t['env']:<6} {t['time']:<22} {t['symbol']:<12} "
              f"{float(t['income']):<14.8f} {t['asset']:<6} {t['tradeId']}")
    print(f"{sep}\nTotal: {len(trades)} record(s)")

def export_csv(trades: list[dict], path: str = "fees_output.csv"):
    if not trades:
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=trades[0].keys())
        writer.writeheader()
        writer.writerows(trades)
    print(f"\n✅ Exported {len(trades)} rows → {path}")

# ── Entry ─────────────────────────────────────────────────────────────────────


# ── Effective fee rate calculator ─────────────────────────────────────────────

def calc_effective_rates(base: str, api_key: str, secret: str,
                         symbols: list[str], limit: int = 100):
    print(f"\n{'─'*60}")
    print(f"  Effective fee rates (commission / notional):")
    print(f"{'─'*60}")

    bnb_px = bnb_price_usdt(base)   # for BNB->USDT fee conversion
    if bnb_px:
        print(f"  (BNB fees converted at BNBUSDT={bnb_px:.2f})")
    else:
        print(f"  ⚠️  could not fetch BNB price — BNB-denominated fees may be understated")

    all_rates = []
    for symbol in symbols:
        r = signed_get(base, "/fapi/v1/userTrades",
                       {"symbol": symbol, "limit": limit},
                       api_key, secret)
        if r.status_code != 200:
            continue
        trades = r.json()
        if not trades:
            continue

        rates = []
        for t in trades:
            notional = float(t["qty"]) * float(t["price"])
            commission = abs(float(t["commission"]))
            # Convert BNB-denominated commission to USDT so the rate is a true %.
            if t["commissionAsset"] == "BNB" and bnb_px:
                commission *= bnb_px
            if notional > 0:
                rate_pct = (commission / notional) * 100
                rates.append({
                    "symbol": symbol,
                    "role":   "maker" if t["maker"] else "taker",
                    "asset":  t["commissionAsset"],
                    "rate":   rate_pct,
                    "fee":    commission,
                    "notional": notional,
                })

        if not rates:
            continue

        maker = [r["rate"] for r in rates if r["role"] == "maker"]
        taker = [r["rate"] for r in rates if r["role"] == "taker"]
        usdt  = [r["rate"] for r in rates if r["asset"] == "USDT"]
        bnb   = [r["rate"] for r in rates if r["asset"] == "BNB"]

        print(f"\n  {symbol} ({len(rates)} trades):")
        if taker: print(f"    taker avg: {sum(taker)/len(taker):.4f}%  "
                        f"min: {min(taker):.4f}%  max: {max(taker):.4f}%  (n={len(taker)})")
        if maker: print(f"    maker avg: {sum(maker)/len(maker):.4f}%  "
                        f"min: {min(maker):.4f}%  max: {max(maker):.4f}%  (n={len(maker)})")
        if usdt:  print(f"    USDT fees: {len(usdt)} trades")
        if bnb:   print(f"    BNB fees:  {len(bnb)} trades  (10% futures discount active)")

        all_rates.extend(rates)
        time.sleep(0.1)

    taker_all = [r["rate"] for r in all_rates if r["role"] == "taker"]
    if all_rates:
        all_pct = [r["rate"] for r in all_rates]
        print(f"\n  {'─'*40}")
        print(f"  OVERALL across {len(all_rates)} trades:")
        print(f"    avg: {sum(all_pct)/len(all_pct):.4f}%")
        print(f"    min: {min(all_pct):.4f}%  max: {max(all_pct):.4f}%")
        maker_all = [r["rate"] for r in all_rates if r["role"] == "maker"]
        if taker_all: print(f"    taker avg: {sum(taker_all)/len(taker_all):.4f}%  (n={len(taker_all)})")
        if maker_all: print(f"    maker avg: {sum(maker_all)/len(maker_all):.4f}%  (n={len(maker_all)})")
    # Return blended per-side taker rate for FEE_RT derivation
    return (sum(taker_all)/len(taker_all)) if taker_all else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env",   choices=["prod", "stage", "both"], default="both")
    parser.add_argument("--csv",   action="store_true")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--rates", nargs="*", metavar="SYMBOL",
                        help="Compute effective fee rate per trade. "
                             "Optionally pass symbols: --rates ARBUSDT ONDOUSDT. "
                             "Omit symbols to auto-pick top 3 from income.")
    parser.add_argument("--emit-fee-rt", action="store_true",
                        help="Print FEE_RT=<round-trip %%> line for .env, derived from measured taker rate.")
    args = parser.parse_args()

    envs = list(ENVIRONMENTS.items()) if args.env == "both" \
        else [(args.env, ENVIRONMENTS[args.env])]

    all_trades = []
    for env_name, config in envs:
        all_trades.extend(run_env(env_name, config, args.limit))

    print_table(all_trades)
    if args.csv:
        export_csv(all_trades)

    if args.rates is not None or args.emit_fee_rt:
        from collections import Counter
        for env_name, config in envs:
            cfg     = dotenv_values(config["env_file"])
            api_key = cfg.get("BINANCE_API_KEY", "").strip()
            secret  = cfg.get("BINANCE_API_SECRET", "").strip()
            base    = config["base_url"]
            if not api_key or not secret:
                continue
            if args.rates:
                symbols = [s.upper() for s in args.rates]
            else:
                counts  = Counter(t["symbol"] for t in all_trades if t["env"] == env_name)
                symbols = [s for s, _ in counts.most_common(3)]
            if symbols:
                print(f"\n[{env_name}] rate check: {', '.join(symbols)}")
                taker_side = calc_effective_rates(base, api_key, secret, symbols, limit=args.limit)
                if args.emit_fee_rt:
                    if taker_side:
                        fee_rt = round(taker_side * 2, 4)   # round-trip = 2 sides
                        print(f"\n  ➜ measured taker {taker_side:.4f}%/side → round-trip FEE_RT={fee_rt}")
                        print(f"     Put this in .env.{env_name} (and .env.stage, to match prod):  FEE_RT={fee_rt}")
                    else:
                        print(f"\n  ⚠️  no taker fills found for {env_name} — cannot derive FEE_RT")

if __name__ == "__main__":
    main()
