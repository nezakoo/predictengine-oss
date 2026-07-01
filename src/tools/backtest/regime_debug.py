#!/usr/bin/env python3
"""
regime_debug.py
Diagnoses why 2024 was bad: shows each bear-regime rebalance in 2024
with BTC price context, to understand what regime filter improvements might help.
"""
import sys
import numpy as np
import pandas as pd
sys.path.insert(0, '.')
from xsmom_backtest import load_panel, compute_regime

def analyse(data_dir, lookback=14, hold=7, k=5, fee_rt=0.093, regime_ma=50):
    panel = load_panel(data_dir)
    fee   = fee_rt / 100.0
    closes = panel.values
    n_bars  = closes.shape[0]

    # Regime signals to test
    btc = panel["BTCUSDT_1d"]
    ma50  = btc.rolling(50,  min_periods=50).mean()
    ma200 = btc.rolling(200, min_periods=200).mean()
    ma20  = btc.rolling(20,  min_periods=20).mean()

    # Additional filters
    btc_ret_4w = btc.pct_change(28)          # 4-week BTC return
    btc_ret_2w = btc.pct_change(14)          # 2-week BTC return
    btc_vol_20 = btc.pct_change().rolling(20).std() * np.sqrt(365)  # annualised vol

    # Base regime: BTC < MA50
    base_regime = pd.Series(0, index=panel.index)
    base_regime[btc < ma50] = -1

    print("\n" + "="*72)
    print("  2024 BEAR-REGIME REBALANCES — what went wrong")
    print("="*72)
    print(f"  {'date':<12} {'net%':>7} {'BTC':>8} {'MA50':>8} {'MA200':>8} "
          f"{'4wBTC%':>8} {'2wBTC%':>8} {'note'}")
    print("  " + "-"*72)

    reg_vals = base_regime.reindex(panel.index).fillna(0).values.astype(int)
    t = lookback
    while t + hold < n_bars:
        if reg_vals[t] != -1:
            t += hold; continue
        ts = panel.index[t]
        if ts.year != 2024:
            t += hold; continue

        past = closes[t]; prev = closes[t-lookback]; fwd = closes[t+hold]
        valid = np.isfinite(past)&np.isfinite(prev)&np.isfinite(fwd)&(prev>0)&(past>0)
        idx = np.where(valid)[0]
        if len(idx) >= k:
            order = np.argsort(past[idx]/prev[idx]-1.0)
            bot = idx[order[:k]]
            net = (past[bot]/fwd[bot]-1.0).mean() - fee
        else:
            net = None

        btc_px   = btc.iloc[t]
        ma50_px  = ma50.iloc[t]
        ma200_px = ma200.iloc[t] if not np.isnan(ma200.iloc[t]) else 0
        r4w = btc_ret_4w.iloc[t]*100 if not np.isnan(btc_ret_4w.iloc[t]) else 0
        r2w = btc_ret_2w.iloc[t]*100 if not np.isnan(btc_ret_2w.iloc[t]) else 0

        # Flags
        notes = []
        if btc_px > ma200_px and ma200_px > 0: notes.append("BTC>MA200")
        if r4w > 0:  notes.append(f"4w+{r4w:.0f}%")
        if r2w > 0:  notes.append(f"2w+{r2w:.0f}%")

        flag = "❌" if (net is not None and net < 0) else "✅"
        net_str = f"{net*100:+.2f}%" if net is not None else "n/a"
        print(f"  {flag} {str(ts.date()):<12} {net_str:>7}  "
              f"{btc_px:>8,.0f} {ma50_px:>8,.0f} {ma200_px:>8,.0f} "
              f"{r4w:>+7.1f}% {r2w:>+7.1f}%  {', '.join(notes)}")
        t += hold

    # ── Test improved regime filters ─────────────────────────────────────────
    print("\n" + "="*72)
    print("  IMPROVED REGIME FILTERS — full test set comparison")
    print("="*72)

    split = int(len(panel) * 0.7)
    test_p = panel.iloc[split:]
    test_closes = test_p.values

    filters = {
        "base: BTC<MA50":
            (btc < ma50).iloc[split:].values.astype(int) * -1,
        "BTC<MA50 AND BTC<MA200":
            ((btc < ma50) & (btc < ma200)).iloc[split:].values.astype(int) * -1,
        "BTC<MA50 AND 4wBTC<0":
            ((btc < ma50) & (btc_ret_4w < 0)).iloc[split:].values.astype(int) * -1,
        "BTC<MA50 AND 2wBTC<0":
            ((btc < ma50) & (btc_ret_2w < 0)).iloc[split:].values.astype(int) * -1,
        "BTC<MA50 AND BTC<MA200 AND 4wBTC<0":
            ((btc < ma50) & (btc < ma200) & (btc_ret_4w < 0)).iloc[split:].values.astype(int) * -1,
        "BTC<MA20 (tighter)":
            (btc < ma20).iloc[split:].values.astype(int) * -1,
    }

    print(f"\n  {'filter':<38} {'n':>4} {'avg':>8} {'null':>8} {'edge':>7} {'sharpe':>7} {'WR':>6}")
    print("  " + "-"*72)

    rng = np.random.default_rng(1)
    for fname, reg_arr in filters.items():
        nets = []; null_nets = []
        t = lookback
        while t + hold < len(test_p):
            reg = int(reg_arr[t]) if t < len(reg_arr) else 0
            if reg != -1:
                t += hold; continue
            past = test_closes[t]; prev = test_closes[t-lookback]; fwd = test_closes[t+hold]
            valid = np.isfinite(past)&np.isfinite(prev)&np.isfinite(fwd)&(prev>0)&(past>0)
            idx = np.where(valid)[0]
            if len(idx) >= k:
                order_r = np.argsort(past[idx]/prev[idx]-1.0)
                order_n = rng.permutation(len(idx))
                bot_r = idx[order_r[:k]]; bot_n = idx[order_n[:k]]
                nets.append((past[bot_r]/fwd[bot_r]-1.0).mean()-fee)
                null_nets.append((past[bot_n]/fwd[bot_n]-1.0).mean()-fee)
            t += hold

        if not nets:
            print(f"  {'  '+fname:<38} {'—':>4}")
            continue

        a = np.array(nets); an = np.array(null_nets)
        sh = a.mean()/a.std() if a.std()>0 else 0
        edge = (a.mean()-an.mean())*100
        flag = "✅" if a.mean()>0 and edge>0 else "  "
        print(f"  {flag}  {fname:<36} {len(a):>4} {a.mean()*100:>+7.2f}% "
              f"{an.mean()*100:>+7.2f}% {edge:>+6.2f}pp {sh:>+6.2f} "
              f"{(a>0).mean()*100:>5.1f}%")

    # ── Year-by-year for each filter ──────────────────────────────────────────
    print("\n" + "="*72)
    print("  YEAR-BY-YEAR for best filters (full dataset)")
    print("="*72)

    best_filters = {
        "BTC<MA50":
            (btc < ma50).values.astype(int) * -1,
        "BTC<MA50+MA200+4w<0":
            ((btc < ma50) & (btc < ma200) & (btc_ret_4w < 0)).values.astype(int) * -1,
        "BTC<MA50 AND 4wBTC<0":
            ((btc < ma50) & (btc_ret_4w < 0)).values.astype(int) * -1,
    }

    for fname, reg_arr in best_filters.items():
        print(f"\n  [{fname}]")
        print(f"  {'year':>6} {'n':>4} {'avg':>8} {'WR':>6} {'cum':>8}")
        by_year = {}
        t = lookback
        while t + hold < n_bars:
            reg = int(reg_arr[t]) if t < len(reg_arr) else 0
            if reg != -1:
                t += hold; continue
            ts = panel.index[t]
            past = closes[t]; prev = closes[t-lookback]; fwd = closes[t+hold]
            valid = np.isfinite(past)&np.isfinite(prev)&np.isfinite(fwd)&(prev>0)&(past>0)
            idx = np.where(valid)[0]
            if len(idx) >= k:
                order = np.argsort(past[idx]/prev[idx]-1.0)
                bot = idx[order[:k]]
                net = (past[bot]/fwd[bot]-1.0).mean() - fee
                by_year.setdefault(ts.year, []).append(net)
            t += hold

        for y in sorted(by_year):
            a = np.array(by_year[y])
            eq = np.cumprod(1+a)
            flag = "✅" if a.mean()>0 else "  "
            print(f"  {flag}  {y:>6} {len(a):>4} {a.mean()*100:>+7.2f}% "
                  f"{(a>0).mean()*100:>5.1f}% {(eq[-1]-1)*100:>+7.1f}%")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--lookback", type=int, default=14)
    ap.add_argument("--hold",     type=int, default=7)
    ap.add_argument("--k",        type=int, default=5)
    ap.add_argument("--fee-rt",   type=float, default=0.093)
    ap.add_argument("--regime-ma",type=int, default=50)
    args = ap.parse_args()
    analyse(args.data_dir, args.lookback, args.hold, args.k, args.fee_rt, args.regime_ma)
