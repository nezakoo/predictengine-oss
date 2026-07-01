#!/usr/bin/env python3
"""
xsmom_backtest.py — Cross-sectional momentum backtester (longer-horizon pivot)
==============================================================================
Same honesty discipline as the scalping toolchain:
  - real round-trip fee subtracted from every position (default 0.093%),
  - OUT-OF-SAMPLE split: params are read on train, the verdict is read on test,
  - a NULL (shuffled-rank) baseline so we know the edge isn't random selection.

Hypothesis: rank a coin universe by trailing return over `lookback` bars; long the
top `k`, short the bottom `k`; hold `hold` bars; repeat. Edge (if any) is
persistence of relative strength. Low trade frequency => fees ~negligible.

Data format (one CSV per symbol in --data-dir, filename = SYMBOL.csv):
    ts,open,high,low,close,volume        # ts = unix seconds or ISO; header flexible
Only `close` and `ts` are required. Bars must be a consistent timeframe per file.

Verdict bar (must clear ALL):
  1. test-set avg net return per rebalance > 0 AND beats the null baseline,
  2. edge survives on the test split, not just train (no overfit),
  3. Sharpe meaningfully > 0 on test.
If it doesn't clear these on a real universe + multi-year history, it's not there.
"""
import argparse, glob, os, sys
import numpy as np
import pandas as pd


def load_panel(data_dir: str, min_bars: int = 50) -> pd.DataFrame:
    """Build a close-price panel: index=timestamp, columns=symbol."""
    series = {}
    for path in sorted(glob.glob(os.path.join(data_dir, "*.csv"))):
        sym = os.path.splitext(os.path.basename(path))[0]
        try:
            df = pd.read_csv(path)
        except Exception as e:
            print(f"  [skip] {sym}: {e}"); continue
        cols = {c.lower(): c for c in df.columns}
        tcol = cols.get("ts_ms") or cols.get("ts") or cols.get("timestamp") or cols.get("time") or cols.get("open_time")
        ccol = cols.get("close") or cols.get("c")
        if not tcol or not ccol:
            print(f"  [skip] {sym}: need ts+close, saw {list(df.columns)}"); continue
        tnum = pd.to_numeric(df[tcol], errors="coerce")
        if tnum.notna().all():
            unit = "ms" if float(tnum.median()) > 1e12 else "s"
            ts = pd.to_datetime(tnum, unit=unit, errors="coerce")
        else:
            ts = pd.to_datetime(df[tcol], errors="coerce")
        s = pd.Series(pd.to_numeric(df[ccol], errors="coerce").values, index=ts).dropna()
        s = s[~s.index.duplicated(keep="last")].sort_index()
        if len(s) >= min_bars:
            series[sym] = s
    if not series:
        sys.exit("No usable symbol CSVs found.")
    panel = pd.DataFrame(series).sort_index()
    panel = panel.ffill(limit=2)            # tolerate small gaps, not large ones
    print(f"Loaded {panel.shape[1]} symbols, {panel.shape[0]} bars "
          f"[{panel.index.min()} .. {panel.index.max()}]")
    return panel


def compute_regime(panel: pd.DataFrame, regime_sym: str, ma_period: int) -> pd.Series:
    """Return a regime series aligned to panel.index:
       +1 = bullish (close > MA),  -1 = bearish (close < MA),  0 = MA not yet warm.
    Falls back to neutral (0) if regime_sym not in panel."""
    if regime_sym not in panel.columns:
        print(f"  [regime] {regime_sym} not in panel — regime filter disabled")
        return pd.Series(0, index=panel.index)
    px = panel[regime_sym]
    ma = px.rolling(ma_period, min_periods=ma_period).mean()
    regime = pd.Series(0, index=panel.index)
    regime[px > ma] = 1
    regime[px < ma] = -1
    return regime


def backtest(panel: pd.DataFrame, lookback: int, hold: int, k: int,
             fee_rt: float, shuffle: bool = False, seed: int = 0,
             regime: pd.Series = None) -> dict:
    """Returns per-rebalance net returns and aggregate metrics.
    Each rebalance: rank by trailing `lookback` return, long top k / short bottom k,
    equal weight, dollar-neutral. Forward return over `hold` bars. Every selected
    position pays a full round-trip fee (fee_rt%) — full turnover assumed.

    If `regime` is provided (pd.Series aligned to panel.index):
      +1 => only open longs (skip shorts)
      -1 => only open shorts (skip longs)
       0 => skip rebalance entirely
    """
    closes = panel.values
    n_bars, n_sym = closes.shape
    rng = np.random.default_rng(seed)
    fee = fee_rt / 100.0

    # Align regime to panel index positions
    reg_vals = None
    if regime is not None:
        reg_aligned = regime.reindex(panel.index).fillna(0).values

    long_nets, short_nets, combo_nets, ts_marks = [], [], [], []
    t = lookback
    while t + hold < n_bars:
        # Regime gate
        reg = int(reg_aligned[t]) if regime is not None else 0
        if regime is not None and reg == 0:
            t += hold
            continue

        past = closes[t]
        prev = closes[t - lookback]
        fwd  = closes[t + hold]
        valid = np.isfinite(past) & np.isfinite(prev) & np.isfinite(fwd) & (prev > 0) & (past > 0)
        idx = np.where(valid)[0]
        if len(idx) >= 2 * k:
            order = rng.permutation(len(idx)) if shuffle else np.argsort(
                past[idx] / prev[idx] - 1.0)
            bottom = idx[order[:k]]
            top    = idx[order[-k:]]

            # Regime-conditional: skip legs not allowed by regime
            do_long  = (regime is None or reg == +1)
            do_short = (regime is None or reg == -1)

            l = (fwd[top]  / past[top]  - 1.0).mean() - fee if do_long  else None
            s = (past[bot] / fwd[bot]   - 1.0).mean() - fee if do_short and (bot := bottom).size else None

            if l is not None: long_nets.append(l)
            if s is not None: short_nets.append(s)
            # combo: average of whatever legs fired this rebalance
            active = [x for x in [l, s] if x is not None]
            if active:
                combo_nets.append(sum(active) / len(active))
                ts_marks.append(panel.index[t])
        t += hold

    def agg(arr):
        a = np.array(arr)
        if a.size == 0:
            return {}
        sharpe = (a.mean() / a.std()) if a.std() > 0 else 0.0
        eq = np.cumprod(1 + a); dd = (eq / np.maximum.accumulate(eq) - 1).min()
        return {"n": int(a.size), "avg_net": float(a.mean()), "wr": float((a > 0).mean()),
                "cum": float(eq[-1] - 1), "sharpe": float(sharpe), "maxdd": float(dd)}

    return {"long": agg(long_nets), "short": agg(short_nets), "combo": agg(combo_nets)}


def show(tag, r):
    for leg in ("long", "short", "combo"):
        m = r.get(leg, {})
        if not m: continue
        flag = "✅" if (m["avg_net"] > 0 and m["sharpe"] > 0) else "  "
        print(f"  {flag} {tag:5s} {leg:5s}  n={m['n']:4d}  avg_net={m['avg_net']*100:+.4f}%  "
              f"WR={m['wr']*100:4.1f}%  cum={m['cum']*100:+8.1f}%  Sharpe={m['sharpe']:+.2f}  maxDD={m['maxdd']*100:.1f}%")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True, help="folder of SYMBOL.csv files")
    ap.add_argument("--lookback", type=int, default=30, help="trailing bars for ranking")
    ap.add_argument("--hold", type=int, default=7, help="bars between rebalances")
    ap.add_argument("--k", type=int, default=3, help="long top-k / short bottom-k")
    ap.add_argument("--fee-rt", type=float, default=0.093, help="round-trip fee %% per position")
    ap.add_argument("--train-frac", type=float, default=0.6)
    ap.add_argument("--regime", action="store_true",
                    help="Enable regime filter: long only above MA, short only below")
    ap.add_argument("--regime-sym", default="BTCUSDT_1d",
                    help="Symbol for regime MA (default: BTCUSDT_1d)")
    ap.add_argument("--regime-ma", type=int, default=20,
                    help="MA period for regime (default: 20)")
    args = ap.parse_args()

    panel = load_panel(args.data_dir)
    split = int(len(panel) * args.train_frac)
    train, test = panel.iloc[:split], panel.iloc[split:]

    regime_train = regime_test = None
    regime_label = ""
    if args.regime:
        full_regime = compute_regime(panel, args.regime_sym, args.regime_ma)
        regime_train = full_regime.iloc[:split]
        regime_test  = full_regime.iloc[split:]
        bull = int((full_regime == 1).sum())
        bear = int((full_regime == -1).sum())
        regime_label = f"  regime={args.regime_sym} MA={args.regime_ma}"
        print(f"  Regime: {bull} bullish bars ({bull/len(panel)*100:.0f}%)  "
              f"{bear} bearish bars ({bear/len(panel)*100:.0f}%)")

    print(f"\nparams: lookback={args.lookback} hold={args.hold} k={args.k} "
          f"fee_rt={args.fee_rt}%{regime_label}")
    print(f"split: train={len(train)} bars  test={len(test)} bars\n")

    for name, p, reg in (("TRAIN", train, regime_train), ("TEST", test, regime_test)):
        print(f"[{name}]")
        show(name, backtest(p, args.lookback, args.hold, args.k, args.fee_rt, regime=reg))
        show("NULL", backtest(p, args.lookback, args.hold, args.k, args.fee_rt,
                              shuffle=True, seed=1, regime=reg))
        print()
    print("Verdict: TEST combo must be ✅ AND clearly beat its NULL row. "
          "If TRAIN is green but TEST isn't, it's overfit. If NULL is as good as real, the ranking has no edge.")


if __name__ == "__main__":
    main()


def backtest_short_only(panel: pd.DataFrame, lookback: int, hold: int, k: int,
                         fee_rt: float, shuffle: bool = False, seed: int = 0,
                         regime: pd.Series = None) -> dict:
    """Short-only variant: only trade in bear regime, only short leg."""
    closes = panel.values
    n_bars, n_sym = closes.shape
    rng = np.random.default_rng(seed)
    fee = fee_rt / 100.0

    reg_vals = None
    if regime is not None:
        reg_vals = regime.reindex(panel.index).fillna(0).values.astype(int)

    short_nets = []
    t = lookback
    while t + hold < n_bars:
        reg = int(reg_vals[t]) if reg_vals is not None else 0
        if reg_vals is not None and reg != -1:   # only bear regime
            t += hold
            continue
        past = closes[t]
        prev = closes[t - lookback]
        fwd  = closes[t + hold]
        valid = np.isfinite(past) & np.isfinite(prev) & np.isfinite(fwd) & (prev > 0) & (past > 0)
        idx = np.where(valid)[0]
        if len(idx) >= k:
            order = rng.permutation(len(idx)) if shuffle else np.argsort(
                past[idx] / prev[idx] - 1.0)
            bottom = idx[order[:k]]
            s = (past[bottom] / fwd[bottom] - 1.0).mean() - fee
            short_nets.append(s)
        t += hold

    def agg(arr):
        a = np.array(arr)
        if a.size == 0:
            return {}
        sharpe = (a.mean() / a.std()) if a.std() > 0 else 0.0
        eq = np.cumprod(1 + a); dd = (eq / np.maximum.accumulate(eq) - 1).min()
        return {"n": int(a.size), "avg_net": float(a.mean()), "wr": float((a > 0).mean()),
                "cum": float(eq[-1] - 1), "sharpe": float(sharpe), "maxdd": float(dd)}

    return {"short": agg(short_nets)}
