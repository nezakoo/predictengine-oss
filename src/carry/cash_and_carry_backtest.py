#!/usr/bin/env python3
"""
cash_and_carry_backtest.py — delta-neutral funding harvest (carry, done right)
==============================================================================
For a coin with positive funding: SHORT perp (collect funding) + LONG spot (cancel price).
Net price exposure ~ 0, so P&L = funding_collected - fees. No directional bleed.

What this tests: (1) does funding collected on the highest-funding coins clear the
two-leg fees, and (2) does high funding PERSIST (so selecting on trailing funding predicts
forward funding) -- the actual edge. Out-of-sample split + random-selection null.

IMPORTANT simplification: the spot leg is assumed to hedge the perp price move perfectly
(price residual = 0). Real cash-and-carry has BASIS risk (perp/spot don't track exactly)
and needs a spot account, ~2x capital, and wallet transfers. So this is the OPTIMISTIC
ceiling -- if it's not clearly positive here, it won't survive real basis/borrow costs.

Data: cache dir with SYMBOL_funding.csv (+ SYMBOL_8h.csv or _1m.csv for existence/density).

Usage:
  python3 cash_and_carry_backtest.py --cache-dir tools/backtest/ohlcv_cache \
        --lookback 3 --hold 9 --k 8 --fee-total 0.186 --min-coins 25
"""
import argparse, glob, os, sys
import numpy as np
import pandas as pd


def _col(df, names):
    cols = {c.lower(): c for c in df.columns}
    for n in names:
        if n in cols: return cols[n]
    return None

def _ts(s):
    t = pd.to_numeric(s, errors="coerce")
    if t.notna().all():
        return pd.to_datetime(t, unit="ms" if float(t.median()) > 1e12 else "s", errors="coerce")
    return pd.to_datetime(s, errors="coerce")


def load(cache_dir, min_coins):
    frates, exists = {}, {}
    for fp in sorted(glob.glob(os.path.join(cache_dir, "*_funding.csv"))):
        sym = os.path.basename(fp).replace("_funding.csv", "")
        try: fdf = pd.read_csv(fp)
        except Exception: continue
        tcol = _col(fdf, ["ts_ms","fundingtime","ts","timestamp","time"])
        rcol = _col(fdf, ["rate","fundingrate","funding_rate","last_funding_rate","r"])
        if not tcol or not rcol: continue
        fr = pd.Series(pd.to_numeric(fdf[rcol],errors="coerce").values, index=_ts(fdf[tcol])).dropna()
        fr.index = fr.index.floor("8h")
        fr = fr[~fr.index.duplicated(keep="last")].sort_index()
        # need a price file to confirm the coin is actually tradable at those times
        pp = os.path.join(cache_dir, f"{sym}_8h.csv")
        if not os.path.exists(pp): pp = os.path.join(cache_dir, f"{sym}_1m.csv")
        if not os.path.exists(pp): continue
        pdf = pd.read_csv(pp)
        ptc=_col(pdf,["ts_ms","open_time","ts","timestamp","time"]); pcc=_col(pdf,["close","c"])
        ps = pd.Series(pd.to_numeric(pdf[pcc],errors="coerce").values, index=_ts(pdf[ptc])).dropna().sort_index()
        live = ps.reindex(fr.index, method="ffill").notna()
        if live.sum() >= 20:
            frates[sym] = fr[live]; exists[sym] = live[live]
    if not frates: sys.exit("No usable symbols (need SYMBOL_funding.csv + a price file).")
    F = pd.DataFrame(frates).sort_index()
    dense = F.notna().sum(axis=1) >= min_coins
    F = F[dense]
    if len(F) < 50: sys.exit(f"Only {len(F)} dense periods >= {min_coins} coins. Lower --min-coins / pull older coins.")
    print(f"Loaded {F.shape[1]} symbols, {F.shape[0]} dense periods (>= {min_coins} coins) "
          f"[{F.index.min()} .. {F.index.max()}]")
    pos = (F.values > 0)
    print(f"median funding {np.nanmedian(F.values)*100:+.4f}%/period | "
          f"share of coin-periods with +funding: {pos.mean()*100:.0f}% | "
          f"top-decile funding (median over time): "
          f"{np.nanmedian(np.nanpercentile(F.values,90,axis=1))*100:+.4f}%/period")
    return F


def backtest(F, lookback, hold, k, fee_total, shuffle=False, seed=0):
    fr = F.values; n_t, n_s = fr.shape
    rng = np.random.default_rng(seed); fee = fee_total/100.0
    blocks=[]
    t=lookback
    while t+hold < n_t:
        with np.errstate(invalid="ignore"):
            sig = np.nanmean(fr[t-lookback:t], axis=0)         # trailing funding
        fwd = np.nansum(fr[t:t+hold], axis=0)                  # funding collected over hold
        cnt = np.isfinite(fr[t:t+hold]).sum(axis=0)            # periods actually present
        valid = np.isfinite(sig) & (cnt >= hold*0.5)
        idx = np.where(valid)[0]
        if len(idx) < k: t += hold; continue
        if shuffle:
            pick = idx[rng.permutation(len(idx))[:k]]
        else:
            order = np.argsort(sig[idx])
            pick = idx[order[-k:]]                             # highest trailing funding
            pick = pick[sig[pick] > 0]                          # only positive-funding (short perp collects)
            if len(pick) == 0: t += hold; continue
        collected = fwd[pick].mean()                            # funding earned (price hedged to 0)
        net = collected - fee
        blocks.append((net, collected, fee))
        t += hold
    return blocks


def agg(blocks, periods_per_block, hold_hours):
    if not blocks: return {}
    a=np.array([b[0] for b in blocks]); fund=np.mean([b[1] for b in blocks]); fee=np.mean([b[2] for b in blocks])
    eq=np.cumprod(1+a)
    blocks_per_year = (365*24)/hold_hours
    apr = a.mean()*blocks_per_year*100
    return {"n":len(a),"net":a.mean()*100,"wr":(a>0).mean()*100,"cum":(eq[-1]-1)*100,
            "fund":fund*100,"fee":fee*100,"apr":apr,
            "sharpe": (a.mean()/a.std()) if a.std()>0 else 0.0}

def show(tag,m,hold):
    if not m: print(f"  {tag}: no blocks"); return
    flag="✅" if (m["net"]>0 and m["sharpe"]>0) else "  "
    print(f"  {flag} {tag:5s} n={m['n']:3d}  net/block={m['net']:+.4f}%  WR={m['wr']:4.0f}%  "
          f"~APR={m['apr']:+6.1f}%  | funding={m['fund']:+.4f}%  fee={m['fee']:.4f}%  Sharpe={m['sharpe']:+.2f}")


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cache-dir", required=True)
    ap.add_argument("--lookback", type=int, default=3)
    ap.add_argument("--hold", type=int, default=9, help="periods to hold (8h each)")
    ap.add_argument("--k", type=int, default=8, help="number of highest-funding coins to harvest")
    ap.add_argument("--fee-total", type=float, default=0.186,
                    help="total round-trip fee BOTH legs %% (perp+spot). 0.186=taker both; lower for maker/BNB")
    ap.add_argument("--min-coins", type=int, default=25)
    ap.add_argument("--train-frac", type=float, default=0.6)
    args=ap.parse_args()

    F=load(args.cache_dir, args.min_coins)
    split=int(len(F)*args.train_frac)
    hh=args.hold*8
    print(f"\nparams: lookback={args.lookback} hold={args.hold} ({hh}h) k={args.k} "
          f"fee_total={args.fee_total}% (both legs)\nsplit: train={split}  test={len(F)-split}\n")
    for name, sl in (("TRAIN", slice(0,split)), ("TEST", slice(split,None))):
        Fs=F.iloc[sl]
        print(f"[{name}]")
        show(name, agg(backtest(Fs,args.lookback,args.hold,args.k,args.fee_total), args.hold, hh), args.hold)
        show("NULL", agg(backtest(Fs,args.lookback,args.hold,args.k,args.fee_total,shuffle=True,seed=1), args.hold, hh), args.hold)
        print()
    print("Read: TEST net/block must be ✅ AND clearly beat NULL (selecting high funding must beat random).\n"
          "funding>fee is the harvest; APR is the optimistic ceiling (no basis/borrow). Sweep --hold and --fee-total.")


if __name__=="__main__":
    main()
