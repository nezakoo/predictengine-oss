#!/usr/bin/env python3
"""
resample_cache.py — turn 1m kline cache into higher-TF close CSVs for xsmom_backtest.
Reads  ohlcv_cache/SYMBOL_1m.csv  (any ts+close column naming)
Writes ohlcv/SYMBOL.csv  with  ts,close  at the chosen --bar (e.g. 1h, 4h, 1d).

Usage:
  python3 resample_cache.py --src ohlcv_cache --out ohlcv --bar 1h
"""
import argparse, glob, os, sys
import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="ohlcv_cache")
    ap.add_argument("--out", default="ohlcv")
    ap.add_argument("--bar", default="1h", help="pandas offset: 1h, 4h, 1d ...")
    args = ap.parse_args()

    files = sorted(glob.glob(os.path.join(args.src, "*_1m.csv")))
    if not files:
        sys.exit(f"No *_1m.csv in {args.src}/")
    os.makedirs(args.out, exist_ok=True)

    done = 0
    for path in files:
        sym = os.path.basename(path).replace("_1m.csv", "")
        try:
            df = pd.read_csv(path)
        except Exception as e:
            print(f"  [skip] {sym}: {e}"); continue
        cols = {c.lower(): c for c in df.columns}
        tcol = cols.get("ts") or cols.get("ts_ms") or cols.get("open_time") or cols.get("time") or cols.get("timestamp")
        ccol = cols.get("close") or cols.get("c")
        if not tcol or not ccol:
            print(f"  [skip] {sym}: need ts+close, saw {list(df.columns)}"); continue
        tnum = pd.to_numeric(df[tcol], errors="coerce")
        if tnum.notna().all():
            unit = "ms" if float(tnum.median()) > 1e12 else "s"
            idx = pd.to_datetime(tnum, unit=unit, errors="coerce")
        else:
            idx = pd.to_datetime(df[tcol], errors="coerce")
        s = pd.Series(pd.to_numeric(df[ccol], errors="coerce").values, index=idx).dropna().sort_index()
        s = s[~s.index.duplicated(keep="last")]
        res = s.resample(args.bar).last().dropna()
        if len(res) < 10:
            print(f"  [skip] {sym}: only {len(res)} {args.bar} bars"); continue
        ts_sec = (res.index - pd.Timestamp("1970-01-01")) // pd.Timedelta(seconds=1)
        out = pd.DataFrame({"ts": ts_sec.astype("int64"), "close": res.values})
        out.to_csv(os.path.join(args.out, f"{sym}.csv"), index=False)
        done += 1
        print(f"  {sym}: {len(s)} 1m -> {len(res)} {args.bar} bars")
    print(f"\nWrote {done} symbols to {args.out}/ at {args.bar}")


if __name__ == "__main__":
    main()
