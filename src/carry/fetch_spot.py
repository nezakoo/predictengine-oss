#!/usr/bin/env python3
"""
fetch_spot.py — pull historical SPOT 8h klines for coins already in the cache,
so the carry replay can compute REAL basis = perp/spot - 1 (incl. past crashes).

Reads which coins exist (from *_funding.csv), fetches spot 8h klines for each,
writes SYMBOL_spot_8h.csv (ts_ms,close). Coins with no matching spot market
(e.g. 1000XXX perps whose spot symbol differs) are skipped -- they can't be
directly cash-and-carried anyway.

Usage:
  python3 fetch_spot.py --cache-dir tools/backtest/ohlcv_cache
"""
import argparse, glob, os, time, csv, json, urllib.request

SPOT = "https://api.binance.com"


def get(url):
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.loads(r.read())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache-dir", default="tools/backtest/ohlcv_cache")
    args = ap.parse_args()

    coins = sorted(os.path.basename(f).replace("_funding.csv", "")
                   for f in glob.glob(os.path.join(args.cache_dir, "*_funding.csv")))
    spot_syms = {s["symbol"] for s in get(f"{SPOT}/api/v3/exchangeInfo")["symbols"]
                 if s["status"] == "TRADING"}
    print(f"{len(coins)} cached coins; fetching spot 8h klines where a spot market exists")

    got, skipped = 0, []
    for i, sym in enumerate(coins):
        if sym not in spot_syms:
            skipped.append(sym); continue
        rows, end = [], None
        for _ in range(40):
            q = f"{SPOT}/api/v3/klines?symbol={sym}&interval=8h&limit=1000"
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
            time.sleep(0.1)
        if not rows:
            skipped.append(sym); continue
        with open(os.path.join(args.cache_dir, f"{sym}_spot_8h.csv"), "w", newline="") as f:
            w = csv.writer(f); w.writerow(["ts_ms", "close"])
            for r in rows:
                w.writerow([r[0], r[4]])
        got += 1
        print(f"[{i+1}/{len(coins)}] {sym}: {len(rows)} spot 8h bars")
    print(f"\ndone: {got} spot files written; skipped {len(skipped)} (no spot market): {', '.join(skipped) if skipped else '-'}")


if __name__ == "__main__":
    main()
