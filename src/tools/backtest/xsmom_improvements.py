#!/usr/bin/env python3
"""
xsmom_improvements.py
Tests two improvements to the bear-regime short strategy:
  1. Funding composite ranking: 0.7*price_rank + 0.3*funding_rank
     Logic: weak price + positive funding (crowded longs) = better short
  2. Volume filter: exclude coins with avg daily volume < MIN_VOL_USD
     Logic: thin coins have high slippage not captured in backtest

Requires:
  - ohlcv_cache_4y/SYMBOL_1d.csv      (price, already fetched)
  - ohlcv_cache_4y/SYMBOL_funding.csv  (funding rates, needs re-fetch without --klines-only)

If funding files are missing, falls back to price-only ranking with a warning.

Usage:
  # First fetch funding data (if not already done):
  python3 ohlcv_fetcher.py --interval 1d --days 1460 --cache-dir ohlcv_cache_4y \\
    --symbols BTCUSDT ETHUSDT ... (same list as before, WITHOUT --klines-only)

  # Then run:
  python3 xsmom_improvements.py --data-dir ohlcv_cache_4y
"""
import sys, os, glob, argparse
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from xsmom_backtest import load_panel, compute_regime

# ── Load funding panel ────────────────────────────────────────────────────────

def load_funding_panel(data_dir: str, price_panel: pd.DataFrame) -> pd.DataFrame:
    """Load funding rates resampled to daily, aligned to price panel index."""
    series = {}
    missing = []
    for sym_col in price_panel.columns:
        # col name is e.g. BTCUSDT_1d → file is BTCUSDT_funding.csv
        sym = sym_col.replace('_1d', '')
        path = os.path.join(data_dir, f'{sym}_funding.csv')
        if not os.path.exists(path):
            missing.append(sym)
            continue
        try:
            df = pd.read_csv(path)
            if df.empty or 'fundingRate' not in df.columns:
                missing.append(sym); continue
            tnum = pd.to_numeric(df['ts_ms'], errors='coerce')
            ts   = pd.to_datetime(tnum, unit='ms', errors='coerce')
            rates = pd.Series(pd.to_numeric(df['fundingRate'], errors='coerce').values,
                              index=ts).dropna()
            rates = rates[~rates.index.duplicated(keep='last')].sort_index()
            # Resample to daily: sum of 3 funding payments per day (8h each)
            daily = rates.resample('1D').sum()
            series[sym_col] = daily
        except Exception as e:
            missing.append(sym)

    if missing:
        print(f"  [funding] Missing for {len(missing)} coins: {missing[:5]}{'...' if len(missing)>5 else ''}")

    if not series:
        return pd.DataFrame(index=price_panel.index)

    panel = pd.DataFrame(series).reindex(price_panel.index).ffill(limit=3)
    print(f"  [funding] Loaded {len(series)} symbols, "
          f"{panel.notna().all(axis=1).sum()} complete daily rows")
    return panel


def load_volume_panel(data_dir: str, price_panel: pd.DataFrame) -> pd.DataFrame:
    """Load daily quote volume (USD) aligned to price panel."""
    series = {}
    for sym_col in price_panel.columns:
        sym = sym_col.replace('_1d', '')
        path = os.path.join(data_dir, f'{sym}_1d.csv')
        if not os.path.exists(path):
            continue
        try:
            df = pd.read_csv(path)
            cols = {c.lower(): c for c in df.columns}
            tcol = cols.get('ts_ms') or cols.get('ts')
            vcol = cols.get('quote_volume') or cols.get('volume')
            if not tcol or not vcol: continue
            tnum = pd.to_numeric(df[tcol], errors='coerce')
            ts   = pd.to_datetime(tnum, unit='ms', errors='coerce')
            vol  = pd.Series(pd.to_numeric(df[vcol], errors='coerce').values,
                             index=ts).dropna()
            vol  = vol[~vol.index.duplicated(keep='last')].sort_index()
            series[sym_col] = vol
        except Exception:
            continue
    if not series:
        return pd.DataFrame(index=price_panel.index)
    return pd.DataFrame(series).reindex(price_panel.index).ffill(limit=3)


# ── Core backtest (short-only, bear regime) ───────────────────────────────────

def run_short_only(panel, lookback, hold, k, fee_rt,
                   regime_arr, funding_panel=None,
                   funding_weight=0.3, min_vol_usd=0,
                   volume_panel=None, shuffle=False, seed=0):
    """
    Bear-regime short-only backtest with optional funding composite and volume filter.
    Returns list of (net_return, timestamp).
    """
    closes  = panel.values
    n_bars  = closes.shape[0]
    rng     = np.random.default_rng(seed)
    fee     = fee_rt / 100.0
    has_funding = (funding_panel is not None and not funding_panel.empty
                   and len(funding_panel.columns) > 0)
    has_volume  = (volume_panel is not None and not volume_panel.empty
                   and min_vol_usd > 0)

    nets, timestamps = [], []
    t = lookback
    while t + hold < n_bars:
        reg = int(regime_arr[t]) if t < len(regime_arr) else 0
        if reg != -1:
            t += hold; continue

        past = closes[t]; prev = closes[t - lookback]; fwd = closes[t + hold]
        valid = (np.isfinite(past) & np.isfinite(prev) & np.isfinite(fwd)
                 & (prev > 0) & (past > 0))

        # Volume filter: exclude low-liquidity coins
        if has_volume:
            vol_row = volume_panel.iloc[t].values
            # Use 30-day rolling avg vol — approximate with current bar
            vol_ok  = np.isfinite(vol_row) & (vol_row >= min_vol_usd)
            valid   = valid & vol_ok

        idx = np.where(valid)[0]
        if len(idx) < k:
            t += hold; continue

        # Price momentum score (lower = weaker = better short candidate)
        price_ret = past[idx] / prev[idx] - 1.0

        if has_funding and not shuffle:
            # Funding score: more positive funding = more crowded long = better short
            fund_row = funding_panel.iloc[t].values
            fund_vals = fund_row[idx]
            fund_ok   = np.isfinite(fund_vals)

            if fund_ok.sum() >= k:
                # Rank both signals (0=best short, n=worst short)
                price_ranks   = np.argsort(np.argsort(price_ret))        # lower = weaker price
                # Higher funding = more crowded long = better short = lower rank wanted
                fund_ranks    = np.argsort(np.argsort(-fund_vals))        # lower = more positive funding
                fund_ranks[~fund_ok] = len(idx) // 2                     # neutral for missing

                composite = (1 - funding_weight) * price_ranks + funding_weight * fund_ranks
                order     = np.argsort(composite)                         # lowest composite = best short
            else:
                order = np.argsort(price_ret)   # fallback to price only
        else:
            order = rng.permutation(len(idx)) if shuffle else np.argsort(price_ret)

        bottom = idx[order[:k]]
        net    = (past[bottom] / fwd[bottom] - 1.0).mean() - fee
        nets.append(net); timestamps.append(panel.index[t])
        t += hold

    return nets, timestamps


def agg(arr):
    a = np.array(arr)
    if a.size == 0:
        return {'n': 0, 'avg': 0, 'wr': 0, 'cum': 0, 'sharpe': 0, 'maxdd': 0}
    sh = (a.mean() / a.std()) if a.std() > 0 else 0
    eq = np.cumprod(1 + a)
    dd = (eq / np.maximum.accumulate(eq) - 1).min()
    return {'n': int(a.size), 'avg': float(a.mean()), 'wr': float((a > 0).mean()),
            'cum': float(eq[-1] - 1), 'sharpe': float(sh), 'maxdd': float(dd)}


def print_row(label, r, null_r=None):
    flag = '✅' if r['avg'] > 0 and r['sharpe'] > 0 else '  '
    edge = f"  edge={( r['avg']-null_r['avg'])*100:+.2f}pp" if null_r else ''
    print(f"  {flag} {label:<36} n={r['n']:3d}  avg={r['avg']*100:+.3f}%  "
          f"WR={r['wr']*100:4.1f}%  Sharpe={r['sharpe']:+.2f}"
          f"  cum={r['cum']*100:+7.1f}%{edge}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--data-dir',      required=True)
    ap.add_argument('--lookback',      type=int,   default=14)
    ap.add_argument('--hold',          type=int,   default=7)
    ap.add_argument('--k',             type=int,   default=5)
    ap.add_argument('--fee-rt',        type=float, default=0.093)
    ap.add_argument('--train-frac',    type=float, default=0.7)
    ap.add_argument('--regime-ma',     type=int,   default=50)
    ap.add_argument('--fund-weight',   type=float, default=0.3,
                    help='Weight of funding in composite (0=price only, 1=funding only)')
    ap.add_argument('--min-vol',       type=float, default=50e6,
                    help='Min daily volume USD to include coin (default 50M)')
    args = ap.parse_args()

    # ── Load data ─────────────────────────────────────────────────────────────
    panel = load_panel(args.data_dir)
    print()
    funding_panel = load_funding_panel(args.data_dir, panel)
    volume_panel  = load_volume_panel(args.data_dir, panel)

    # Volume filter stats
    if not volume_panel.empty:
        avg_vol = volume_panel.mean()
        n_above = (avg_vol >= args.min_vol).sum()
        print(f"  [volume] {n_above}/{len(avg_vol)} coins above ${args.min_vol/1e6:.0f}M avg daily vol")
        thin = avg_vol[avg_vol < args.min_vol].index.tolist()
        if thin:
            print(f"  [volume] Excluded: {[c.replace('_1d','') for c in thin]}")

    # ── Regime and split ──────────────────────────────────────────────────────
    btc        = panel['BTCUSDT_1d']
    ma50       = btc.rolling(50,  min_periods=50).mean()
    ma200      = btc.rolling(200, min_periods=200).mean()
    # BTC<MA50 AND BTC<MA200 — the best filter from regime_debug
    full_regime = pd.Series(0, index=panel.index)
    full_regime[(btc < ma50) & (btc < ma200)] = -1

    split        = int(len(panel) * args.train_frac)
    test_regime  = full_regime.iloc[split:].values
    train_regime = full_regime.iloc[:split].values
    test_panel   = panel.iloc[split:]
    train_panel  = panel.iloc[:split]
    test_fund    = funding_panel.iloc[split:]  if not funding_panel.empty else None
    train_fund   = funding_panel.iloc[:split]  if not funding_panel.empty else None
    test_vol     = volume_panel.iloc[split:]   if not volume_panel.empty else None
    train_vol    = volume_panel.iloc[:split]   if not volume_panel.empty else None

    bear_bars = (full_regime == -1).sum()
    print(f"\n  Regime (BTC<MA50+MA200): {bear_bars} bear bars "
          f"({bear_bars/len(panel)*100:.0f}% of history)")
    print(f"  Split: train={len(train_panel)} bars  test={len(test_panel)} bars")

    # ── Run all variants ──────────────────────────────────────────────────────
    variants = [
        ('price only (base)',         False, 0.0,              0),
        ('price only + vol filter',   False, 0.0,              args.min_vol),
        (f'funding {args.fund_weight:.0%} + price {1-args.fund_weight:.0%}',
                                      True,  args.fund_weight, 0),
        (f'funding {args.fund_weight:.0%} + price + vol filter',
                                      True,  args.fund_weight, args.min_vol),
    ]

    print()
    print('=' * 72)
    print('  VARIANT COMPARISON (TEST SET)')
    print('=' * 72)
    print(f"  {'variant':<36} {'n':>4} {'avg%':>8} {'WR%':>6} "
          f"{'Sharpe':>7} {'cum%':>8} {'edge':>8}")
    print('  ' + '-' * 72)

    results = {}
    for name, use_fund, fw, min_vol in variants:
        fund_p = test_fund  if use_fund and test_fund is not None else None
        vol_p  = test_vol   if min_vol > 0 else None

        r_nets, _ = run_short_only(test_panel, args.lookback, args.hold, args.k,
                                    args.fee_rt, test_regime, fund_p, fw, min_vol, vol_p)
        n_nets, _ = run_short_only(test_panel, args.lookback, args.hold, args.k,
                                    args.fee_rt, test_regime, None, 0, 0, None,
                                    shuffle=True, seed=1)
        r = agg(r_nets); n = agg(n_nets)
        results[name] = (r, n)

        flag  = '✅' if r['avg'] > 0 and r['sharpe'] > 0 else '  '
        edge  = (r['avg'] - n['avg']) * 100
        eflag = '+' if edge > 0 else ''
        print(f"  {flag} {name:<36} {r['n']:>4} {r['avg']*100:>+7.3f}% "
              f"{r['wr']*100:>5.1f}% {r['sharpe']:>+6.2f}  "
              f"{r['cum']*100:>+7.1f}%  {eflag}{edge:.2f}pp")

    # ── Year-by-year for each variant ─────────────────────────────────────────
    print()
    print('=' * 72)
    print('  YEAR-BY-YEAR (full dataset, all variants)')
    print('=' * 72)

    full_regime_arr = full_regime.values
    for name, use_fund, fw, min_vol in variants:
        fund_p = funding_panel if use_fund and not funding_panel.empty else None
        vol_p  = volume_panel  if min_vol > 0 and not volume_panel.empty else None

        nets, tss = run_short_only(panel, args.lookback, args.hold, args.k,
                                    args.fee_rt, full_regime_arr, fund_p, fw,
                                    min_vol, vol_p)
        by_year = {}
        for net, ts in zip(nets, tss):
            by_year.setdefault(ts.year, []).append(net)

        print(f"\n  [{name}]")
        print(f"  {'year':>6} {'n':>4} {'avg':>8} {'WR':>6} {'cum':>8}")
        for y in sorted(by_year):
            r = agg(by_year[y])
            flag = '✅' if r['avg'] > 0 else '  '
            print(f"  {flag}  {y:>6} {r['n']:>4} {r['avg']*100:>+7.2f}% "
                  f"{r['wr']*100:>5.1f}% {r['cum']*100:>+7.1f}%")

    # ── Train set sanity check ────────────────────────────────────────────────
    print()
    print('=' * 72)
    print('  TRAIN SET (sanity check — should match direction of test)')
    print('=' * 72)
    print(f"  {'variant':<36} {'n':>4} {'avg%':>8} {'Sharpe':>7} {'edge':>8}")
    print('  ' + '-' * 55)
    for name, use_fund, fw, min_vol in variants:
        fund_p = train_fund if use_fund and train_fund is not None else None
        vol_p  = train_vol  if min_vol > 0 else None
        r_nets, _ = run_short_only(train_panel, args.lookback, args.hold, args.k,
                                    args.fee_rt, train_regime, fund_p, fw, min_vol, vol_p)
        n_nets, _ = run_short_only(train_panel, args.lookback, args.hold, args.k,
                                    args.fee_rt, train_regime, None, 0, 0, None,
                                    shuffle=True, seed=1)
        r = agg(r_nets); n = agg(n_nets)
        edge = (r['avg'] - n['avg']) * 100
        flag = '✅' if r['avg'] > 0 and r['sharpe'] > 0 else '  '
        print(f"  {flag} {name:<36} {r['n']:>4} {r['avg']*100:>+7.3f}%  "
              f"{r['sharpe']:>+6.2f}  {edge:>+6.2f}pp")

    # ── Final recommendation ──────────────────────────────────────────────────
    print()
    print('=' * 72)
    print('  VERDICT')
    print('=' * 72)
    base_r, base_n = results.get('price only (base)', ({}, {}))
    best_name = max(
        [n for n, _, _, _ in variants],
        key=lambda n: results.get(n, ({},))[0].get('sharpe', -99)
    )
    best_r, _ = results.get(best_name, ({}, {}))
    print(f"\n  Base (price only):  avg={base_r.get('avg',0)*100:+.3f}%  "
          f"Sharpe={base_r.get('sharpe',0):+.2f}")
    print(f"  Best variant:       [{best_name}]")
    print(f"                      avg={best_r.get('avg',0)*100:+.3f}%  "
          f"Sharpe={best_r.get('sharpe',0):+.2f}")
    improvement = (best_r.get('sharpe', 0) - base_r.get('sharpe', 0))
    print(f"  Sharpe improvement: {improvement:+.2f}")
    if improvement > 0.05:
        print(f"\n  ✅ Use [{best_name}] — meaningful Sharpe improvement")
    else:
        print(f"\n  ➜  Improvement marginal — stick with price-only ranking")
    print()


if __name__ == '__main__':
    main()
