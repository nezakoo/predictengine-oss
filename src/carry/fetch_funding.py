#!/usr/bin/env python3
"""
fetch_funding.py — pull DEEP funding history + 8h price for a multi-regime carry test.
Writes into the cache used by funding_carry_backtest.py:
    SYMBOL_funding.csv   ts_ms,rate     (every 8h, full history back to listing)
    SYMBOL_8h.csv        ts_ms,close    (8h klines, full history)

Funding history is 1 weight/request and goes back years, so this is cheap.

Usage:
  python3 fetch_funding.py --out tools/backtest/ohlcv_cache --top 60
  python3 funding_carry_backtest.py --cache-dir tools/backtest/ohlcv_cache --lookback 3 --hold 9 --k 8
"""
import argparse, os, time, csv, urllib.request, json

BASE = "https://fapi.binance.com"


def get(path):
    with urllib.request.urlopen(BASE + path, timeout=30) as r:
        return json.loads(r.read())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="tools/backtest/ohlcv_cache")
    ap.add_argument("--top", type=int, default=60, help="max symbols to keep (by 24h volume, among eligible)")
    ap.add_argument("--listed-before", default=None,
                    help="keep only coins whose FIRST funding is before this date (YYYY-MM-DD), "
                         "e.g. 2023-01-01 to span multiple regimes incl. a funding mania")
    ap.add_argument("--probe-cap", type=int, default=220, help="how many liquid candidates to age-probe")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    info = get("/fapi/v1/exchangeInfo")
    perps = [s["symbol"] for s in info["symbols"]
             if s["symbol"].endswith("USDT") and s.get("contractType") == "PERPETUAL" and s["status"] == "TRADING"]
    vol = {t["symbol"]: float(t["quoteVolume"]) for t in get("/fapi/v1/ticker/24hr")}
    ranked = sorted([s for s in perps if s in vol], key=lambda s: -vol[s])

    if args.listed_before:
        import datetime as _dt
        cutoff = int(_dt.datetime.strptime(args.listed_before, "%Y-%m-%d")
                     .replace(tzinfo=_dt.timezone.utc).timestamp() * 1000)
        print(f"age-probing up to {args.probe_cap} liquid candidates for first-funding < {args.listed_before} ...")
        eligible = []
        for s in ranked[:args.probe_cap]:
            try:
                first = get(f"/fapi/v1/fundingRate?symbol={s}&startTime=1577836800000&limit=1")
            except Exception:
                first = None
            if first and first[0]["fundingTime"] < cutoff:
                eligible.append(s)
            time.sleep(0.08)
        syms = eligible[:args.top]
        print(f"{len(eligible)} eligible (old enough); keeping top {len(syms)} by volume")
    else:
        syms = ranked[:args.top]
    print(f"{len(syms)} symbols")

    for i, sym in enumerate(syms):
        # deep funding history (paginate BACKWARD by endTime until exhausted)
        fund, end, prev = [], None, None
        for _ in range(60):
            q = f"/fapi/v1/fundingRate?symbol={sym}&limit=1000"
            if end:
                q += f"&endTime={end}"
            try:
                k = get(q)
            except Exception:
                break
            if not k:
                break
            fund = [(r["fundingTime"], r["fundingRate"]) for r in k] + fund
            end = k[0]["fundingTime"] - 1
            if end == prev:                 # not advancing -> reached the start
                break
            prev = end
            time.sleep(0.12)
        fund = sorted(set(fund))            # pages may overlap; dedup+sort
        with open(os.path.join(args.out, f"{sym}_funding.csv"), "w", newline="") as f:
            w = csv.writer(f); w.writerow(["ts_ms", "rate"])
            for t, r in fund:
                w.writerow([t, r])

        # 8h klines, full history (paginate back)
        rows, end = [], None
        for _ in range(12):
            q = f"/fapi/v1/klines?symbol={sym}&interval=8h&limit=1000"
            if end:
                q += f"&endTime={end}"
            try:
                k = get(q)
            except Exception:
                break
            if not isinstance(k, list) or not k:
                break
            rows = k + rows
            end = k[0][0] - 1
            if len(k) < 1000:
                break
            time.sleep(0.12)
        with open(os.path.join(args.out, f"{sym}_8h.csv"), "w", newline="") as f:
            w = csv.writer(f); w.writerow(["ts_ms", "close"])
            for r in rows:
                w.writerow([r[0], r[4]])
        print(f"[{i+1}/{len(syms)}] {sym}: {len(fund)} funding, {len(rows)} 8h bars")
    print(f"done -> {args.out}/  (SYMBOL_funding.csv + SYMBOL_8h.csv)")


if __name__ == "__main__":
    main()
