#!/usr/bin/env python3
"""
funding_carry_backtest.py — cross-sectional funding carry (longer-horizon experiment #2)
========================================================================================
Hypothesis: perps pay funding every 8h. Short the coins paying the most (high +funding),
long the coins paying the least (most -funding); you RECEIVE funding on both legs.
Dollar-neutral long/short hedges most market beta. Edge exists only if collected funding
exceeds (trading fees from rebalancing) + (adverse price drift of the positions).

This is structurally a LOW-TURNOVER strategy: funding is ~0.01-0.05%/8h, the round-trip
fee is ~0.093%, so you must HOLD many periods per rebalance for funding to clear fees.
That's the whole point of the --hold sweep.

Same honesty rules as xsmom: real fee per position, train/test out-of-sample split,
shuffled-rank null baseline, AND a P&L decomposition (funding vs price vs fees) so we can
see whether any profit is real carry or just price beta.

Data: a cache dir with BOTH per symbol:
  SYMBOL_funding.csv  (ts + funding rate, every 8h)   — any common column naming
  SYMBOL_1m.csv       (ts_ms + close)                  — for price at each funding stamp

Sign convention (Binance): +funding => longs pay shorts. So SHORT a +funding coin to
receive it; LONG a -funding coin to receive it. funding_income = -position_sign * rate.

Usage:
  python3 funding_carry_backtest.py --cache-dir tools/backtest/ohlcv_cache \
        --lookback 3 --hold 3 --k 5 --fee-rt 0.093
"""
import argparse, glob, os, sys
import numpy as np
import pandas as pd


def _col(df, names):
    cols = {c.lower(): c for c in df.columns}
    for n in names:
        if n in cols:
            return cols[n]
    return None


def _ts(series):
    tnum = pd.to_numeric(series, errors="coerce")
    if tnum.notna().all():
        unit = "ms" if float(tnum.median()) > 1e12 else "s"
        return pd.to_datetime(tnum, unit=unit, errors="coerce")
    return pd.to_datetime(series, errors="coerce")


def load(cache_dir, min_coins=20):
    """Return aligned funding-rate panel and price panel, indexed by funding timestamp."""
    frates, prices = {}, {}
    for fp in sorted(glob.glob(os.path.join(cache_dir, "*_funding.csv"))):
        sym = os.path.basename(fp).replace("_funding.csv", "")
        try:
            fdf = pd.read_csv(fp)
        except Exception:
            continue
        tcol = _col(fdf, ["ts_ms", "fundingtime", "ts", "timestamp", "time"])
        rcol = _col(fdf, ["rate", "fundingrate", "funding_rate", "last_funding_rate", "r"])
        if not tcol or not rcol:
            print(f"  [skip] {sym} funding: need ts+rate, saw {list(fdf.columns)}"); continue
        fr = pd.Series(pd.to_numeric(fdf[rcol], errors="coerce").values, index=_ts(fdf[tcol])).dropna()
        fr.index = fr.index.floor("8h")                       # snap to grid so coins align
        fr = fr[~fr.index.duplicated(keep="last")].sort_index()

        pp = os.path.join(cache_dir, f"{sym}_8h.csv")
        if not os.path.exists(pp):
            pp = os.path.join(cache_dir, f"{sym}_1m.csv")
        if not os.path.exists(pp):
            print(f"  [skip] {sym}: no price file (_8h.csv or _1m.csv)"); continue
        pdf = pd.read_csv(pp)
        ptc = _col(pdf, ["ts_ms", "open_time", "ts", "timestamp", "time"])
        pcc = _col(pdf, ["close", "c"])
        ps = pd.Series(pd.to_numeric(pdf[pcc], errors="coerce").values, index=_ts(pdf[ptc])).dropna().sort_index()
        ps = ps[~ps.index.duplicated(keep="last")]
        # price at each funding stamp = last close at or before it
        px = ps.reindex(fr.index, method="ffill")
        ok = px.notna()
        if ok.sum() >= 20:
            frates[sym] = fr[ok]
            prices[sym] = px[ok]
    if not frates:
        sys.exit("No usable symbol pairs (need both SYMBOL_funding.csv and SYMBOL_1m.csv).")
    F = pd.DataFrame(frates).sort_index()
    P = pd.DataFrame(prices).reindex(F.index)
    # keep only periods where enough coins coexist (drops sparse early history)
    dense = (F.notna() & P.notna()).sum(axis=1) >= min_coins
    F, P = F[dense], P[dense]
    if len(F) < 50:
        sys.exit(f"Only {len(F)} dense periods with >={min_coins} coins. Lower --min-coins or pull more symbols.")
    print(f"Loaded {F.shape[1]} symbols, {F.shape[0]} dense funding periods (>= {min_coins} coins) "
          f"[{F.index.min()} .. {F.index.max()}]")
    print(f"median funding rate: {np.nanmedian(F.values)*100:+.4f}% / period   "
          f"cross-sectional spread (p90-p10, median over time): "
          f"{np.nanmedian(np.nanpercentile(F.values,90,axis=1)-np.nanpercentile(F.values,10,axis=1))*100:.4f}%")
    return F, P


def backtest(F, P, lookback, hold, k, fee_rt, shuffle=False, seed=0):
    fr = F.values; px = P.values
    n_t, n_s = fr.shape
    rng = np.random.default_rng(seed)
    fee = fee_rt / 100.0
    blocks = []          # (net, funding_income, price_pnl, fee_paid)
    t = lookback
    while t + hold < n_t:
        with np.errstate(invalid="ignore"):
            signal = np.nanmean(fr[t - lookback:t], axis=0)      # trailing avg funding
        fwd_fund = np.nansum(fr[t:t + hold], axis=0)             # funding collected over block
        p0, p1 = px[t], px[t + hold]
        valid = np.isfinite(signal) & np.isfinite(fwd_fund) & np.isfinite(p0) & np.isfinite(p1) & (p0 > 0)
        idx = np.where(valid)[0]
        if len(idx) < 2 * k:
            t += hold; continue
        order = rng.permutation(len(idx)) if shuffle else np.argsort(signal[idx])
        low  = idx[order[:k]]     # most negative funding -> LONG  (sign +1)
        high = idx[order[-k:]]    # most positive funding -> SHORT (sign -1)
        pr = px[t + hold] / px[t] - 1.0
        # long-low leg: receive -(+1)*rate = -fund(negative)= +; price pnl = +pr
        long_fund = (-1.0) * fwd_fund[low]                 # = +|fund| since fund<0
        long_pnl  = pr[low]
        # short-high leg: receive -(-1)*rate = +fund; price pnl = -pr
        short_fund = (+1.0) * fwd_fund[high]
        short_pnl  = -pr[high]
        fund_income = np.concatenate([long_fund, short_fund]).mean()
        price_pnl   = np.concatenate([long_pnl, short_pnl]).mean()
        net = fund_income + price_pnl - fee
        blocks.append((net, fund_income, price_pnl, fee))
        t += hold
    return blocks


def agg(blocks):
    if not blocks:
        return {}
    a = np.array([b[0] for b in blocks])
    fund = np.mean([b[1] for b in blocks]); price = np.mean([b[2] for b in blocks]); fee = np.mean([b[3] for b in blocks])
    sharpe = a.mean() / a.std() if a.std() > 0 else 0.0
    eq = np.cumprod(1 + a)
    return {"n": len(a), "avg_net": a.mean()*100, "wr": (a > 0).mean()*100, "cum": (eq[-1]-1)*100,
            "sharpe": sharpe, "fund": fund*100, "price": price*100, "fee": fee*100}


def show(tag, m):
    if not m: print(f"  {tag}: no blocks"); return
    flag = "✅" if (m["avg_net"] > 0 and m["sharpe"] > 0) else "  "
    print(f"  {flag} {tag:6s} n={m['n']:3d}  net={m['avg_net']:+.4f}%  WR={m['wr']:4.1f}%  "
          f"cum={m['cum']:+7.2f}%  Sharpe={m['sharpe']:+.2f}   | funding={m['fund']:+.4f}%  "
          f"price={m['price']:+.4f}%  fee={m['fee']:.4f}%")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache-dir", required=True)
    ap.add_argument("--lookback", type=int, default=3, help="periods of funding to average for ranking")
    ap.add_argument("--hold", type=int, default=3, help="periods to hold before rebalancing (8h each)")
    ap.add_argument("--k", type=int, default=5)
    ap.add_argument("--fee-rt", type=float, default=0.093)
    ap.add_argument("--train-frac", type=float, default=0.6)
    ap.add_argument("--min-coins", type=int, default=20, help="min coexisting coins per period")
    args = ap.parse_args()

    F, P = load(args.cache_dir, args.min_coins)
    split = int(len(F) * args.train_frac)
    print(f"\nparams: lookback={args.lookback} hold={args.hold} k={args.k} fee_rt={args.fee_rt}%  "
          f"(1 period = 8h, so hold={args.hold} = {args.hold*8}h)")
    print(f"split: train={split} periods  test={len(F)-split} periods\n")
    for name, sl in (("TRAIN", slice(0, split)), ("TEST", slice(split, None))):
        Fs, Ps = F.iloc[sl], P.iloc[sl]
        print(f"[{name}]")
        show(name, agg(backtest(Fs, Ps, args.lookback, args.hold, args.k, args.fee_rt)))
        show("NULL", agg(backtest(Fs, Ps, args.lookback, args.hold, args.k, args.fee_rt, shuffle=True, seed=1)))
        print()
    print("Read: TEST net must be ✅ AND clearly beat NULL. The decomposition is the honesty check —\n"
          "if 'funding' is small and 'price' is carrying it, that's beta/luck, not carry. Real carry =\n"
          "funding clearly exceeds fee, and survives out-of-sample. Sweep --hold (3/9/21) to amortize fees.")


if __name__ == "__main__":
    main()
