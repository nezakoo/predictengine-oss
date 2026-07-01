#!/usr/bin/env python3
"""
regime_short_analysis.py
Analyses the bear-regime short-only strategy in detail:
  1. Short-only vs null short across param grid
  2. Realistic P&L estimate at different capital levels
  3. Per-year breakdown to check regime dependence
"""
import sys, argparse
import numpy as np
import pandas as pd
from xsmom_backtest import load_panel, compute_regime, backtest

def short_only(panel, lookback, hold, k, fee_rt, shuffle=False, seed=0, regime=None):
    closes = panel.values
    n_bars  = closes.shape[0]
    rng     = np.random.default_rng(seed)
    fee     = fee_rt / 100.0
    reg_vals = regime.reindex(panel.index).fillna(0).values.astype(int) if regime is not None else None

    nets = []
    timestamps = []
    t = lookback
    while t + hold < n_bars:
        reg = int(reg_vals[t]) if reg_vals is not None else -1
        if reg != -1:
            t += hold; continue
        past = closes[t]; prev = closes[t - lookback]; fwd = closes[t + hold]
        valid = np.isfinite(past) & np.isfinite(prev) & np.isfinite(fwd) & (prev > 0) & (past > 0)
        idx = np.where(valid)[0]
        if len(idx) >= k:
            order = rng.permutation(len(idx)) if shuffle else np.argsort(past[idx]/prev[idx]-1.0)
            bot = idx[order[:k]]
            nets.append((past[bot]/fwd[bot]-1.0).mean() - fee)
            timestamps.append(panel.index[t])
        t += hold
    return nets, timestamps


def agg(arr):
    a = np.array(arr)
    if a.size == 0:
        return {"n":0,"avg":0,"wr":0,"cum":0,"sharpe":0,"maxdd":0}
    sh = (a.mean()/a.std()) if a.std()>0 else 0
    eq = np.cumprod(1+a); dd = (eq/np.maximum.accumulate(eq)-1).min()
    return {"n":int(a.size),"avg":float(a.mean()),"wr":float((a>0).mean()),
            "cum":float(eq[-1]-1),"sharpe":float(sh),"maxdd":float(dd)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--lookback", type=int, default=14)
    ap.add_argument("--hold",     type=int, default=7)
    ap.add_argument("--k",        type=int, default=5)
    ap.add_argument("--fee-rt",   type=float, default=0.093)
    ap.add_argument("--train-frac", type=float, default=0.7)
    ap.add_argument("--regime-ma",  type=int, default=50)
    ap.add_argument("--capital",    type=float, default=1000,
                    help="Total capital in USDT (default 1000)")
    ap.add_argument("--pos-size",   type=float, default=100,
                    help="USDT notional per position (default 100)")
    args = ap.parse_args()

    panel = load_panel(args.data_dir)
    split = int(len(panel) * args.train_frac)
    train_p, test_p = panel.iloc[:split], panel.iloc[split:]

    full_regime  = compute_regime(panel, "BTCUSDT_1d", args.regime_ma)
    train_regime = full_regime.iloc[:split]
    test_regime  = full_regime.iloc[split:]

    LB, HOLD, K, FEE = args.lookback, args.hold, args.k, args.fee_rt

    # ── 1. Short-only: ranked vs null ────────────────────────────────────────
    print("\n" + "="*64)
    print("  SHORT-ONLY (bear regime only): ranked vs null")
    print("="*64)
    for name, p, reg in (("TRAIN", train_p, train_regime), ("TEST", test_p, test_regime)):
        r_nets, r_ts = short_only(p, LB, HOLD, K, FEE, regime=reg)
        n_nets, _    = short_only(p, LB, HOLD, K, FEE, shuffle=True, seed=1, regime=reg)
        r = agg(r_nets); n = agg(n_nets)
        flag = "✅" if r["avg"] > n["avg"] and r["sharpe"] > 0 else "  "
        print(f"\n[{name}]")
        print(f"  {flag} RANKED  n={r['n']:3d}  avg={r['avg']*100:+.3f}%  "
              f"WR={r['wr']*100:4.1f}%  Sharpe={r['sharpe']:+.2f}  "
              f"cum={r['cum']*100:+7.1f}%  maxDD={r['maxdd']*100:.1f}%")
        flag_n = "  "
        print(f"  {flag_n} NULL    n={n['n']:3d}  avg={n['avg']*100:+.3f}%  "
              f"WR={n['wr']*100:4.1f}%  Sharpe={n['sharpe']:+.2f}  "
              f"cum={n['cum']*100:+7.1f}%  maxDD={n['maxdd']*100:.1f}%")
        print(f"       Edge over null: {(r['avg']-n['avg'])*100:+.3f}pp/rebalance")

    # ── 2. Param grid ────────────────────────────────────────────────────────
    print("\n" + "="*64)
    print("  PARAM GRID — short-only TEST edge over null")
    print("="*64)
    print(f"  {'lb':>4} {'hold':>5} {'k':>3}  {'ranked':>8} {'null':>8} {'edge':>7} {'sharpe':>7} {'n':>4}")
    print("  " + "-"*55)
    for lb in [7, 14, 21]:
        for hold in [3, 7, 14]:
            for k in [3, 5, 8]:
                r_nets, _ = short_only(test_p, lb, hold, k, FEE, regime=test_regime)
                n_nets, _ = short_only(test_p, lb, hold, k, FEE, shuffle=True, seed=1, regime=test_regime)
                r = agg(r_nets); n = agg(n_nets)
                if r["n"] < 5: continue
                edge = (r["avg"] - n["avg"]) * 100
                flag = "✅" if edge > 0 and r["sharpe"] > 0 else "  "
                print(f"  {flag}{lb:>4} {hold:>5} {k:>3}  "
                      f"{r['avg']*100:>+7.2f}% {n['avg']*100:>+7.2f}% "
                      f"{edge:>+6.2f}pp {r['sharpe']:>+6.2f}  {r['n']:>4}")

    # ── 3. Year-by-year breakdown ─────────────────────────────────────────────
    print("\n" + "="*64)
    print("  YEAR-BY-YEAR breakdown (short-only ranked, full dataset)")
    print("="*64)
    full_regime_s = compute_regime(panel, "BTCUSDT_1d", args.regime_ma)
    all_nets, all_ts = short_only(panel, LB, HOLD, K, FEE, regime=full_regime_s)
    by_year = {}
    for net, ts in zip(all_nets, all_ts):
        y = ts.year
        by_year.setdefault(y, []).append(net)
    print(f"  {'year':>6} {'n':>4} {'avg':>8} {'WR':>7} {'cum':>8} {'sharpe':>7}")
    print("  " + "-"*45)
    for y in sorted(by_year):
        r = agg(by_year[y])
        flag = "✅" if r["avg"] > 0 else "  "
        print(f"  {flag}{y:>6} {r['n']:>4} {r['avg']*100:>+7.2f}% "
              f"{r['wr']*100:>6.1f}% {r['cum']*100:>+7.1f}% {r['sharpe']:>+6.2f}")

    # ── 4. Realistic P&L estimate ─────────────────────────────────────────────
    print("\n" + "="*64)
    print("  REALISTIC P&L ESTIMATE")
    print("="*64)
    r_nets_test, _ = short_only(test_p, LB, HOLD, K, FEE, regime=test_regime)
    r = agg(r_nets_test)

    capital    = args.capital
    pos_size   = args.pos_size
    n_pos      = K
    deployed   = pos_size * n_pos
    leverage   = deployed / capital

    # bear regime is ~44% of time
    bear_frac       = (full_regime == -1).mean()
    rebal_per_year  = 365 / HOLD
    bear_rebal_yr   = rebal_per_year * bear_frac

    # P&L per rebalance = avg_net% * pos_size * k  (pos_size is notional per leg)
    pnl_per_rebal   = r["avg"] * pos_size * n_pos
    annual_pnl      = pnl_per_rebal * bear_rebal_yr
    annual_ret_pct  = annual_pnl / capital * 100

    print(f"\n  Capital:          ${capital:>8,.0f} USDT")
    print(f"  Position size:    ${pos_size:>8,.0f} USDT notional per coin")
    print(f"  Positions (k):    {n_pos}")
    print(f"  Total deployed:   ${deployed:>8,.0f} USDT ({leverage:.1f}x of capital)")
    print(f"  Fee RT:           {FEE:.3f}%")
    print()
    print(f"  Test avg/rebal:   {r['avg']*100:>+.3f}%  (Sharpe={r['sharpe']:+.2f}, n={r['n']})")
    print(f"  Bear regime:      {bear_frac*100:.0f}% of bars")
    print(f"  Active rebal/yr:  {bear_rebal_yr:.0f}")
    print()
    print(f"  P&L per rebal:    ${pnl_per_rebal:>+.2f}")
    print(f"  Estimated annual: ${annual_pnl:>+,.0f}  ({annual_ret_pct:+.1f}% on capital)")
    print()

    # Scenarios
    print("  Scenarios (annual, on test avg):")
    print(f"  {'Capital':>10} {'Pos/coin':>10} {'Annual $':>10} {'Annual %':>10}")
    print("  " + "-"*44)
    for cap, pos in [(500,50),(1000,100),(5000,200),(10000,500),(25000,1000)]:
        dep = pos * n_pos
        lev = dep / cap
        if lev > 10: continue  # skip unrealistic leverage
        pnl = r["avg"] * pos * n_pos * bear_rebal_yr
        ret = pnl / cap * 100
        print(f"  ${cap:>9,.0f} ${pos:>9,.0f}   ${pnl:>+9,.0f}  {ret:>+9.1f}%")

    print()
    print("  ⚠  Caveats:")
    print("     - Based on 4yr test set OOS results (n rebalances shown above)")
    print("     - Survivorship bias: 29 coins all survived 2022-2026")
    print("     - Bear regime identification is real-time (no lookahead)")
    print("     - Does not account for slippage on illiquid coins")
    print("     - Past bear-regime performance may not repeat in future cycles")


if __name__ == "__main__":
    main()
