#!/usr/bin/env python3
"""
cash_carry_basis_replay.py — cash-and-carry over REAL history WITH real basis P&L
=================================================================================
Upgrades cash_and_carry_backtest (which assumed basis=0) by loading historical SPOT
prices and computing the actual basis P&L the live paper run showed is large.

Per position per block: short perp + long spot on a high-funding coin, hold `hold`
periods. P&L = funding_collected + (basis_entry - basis_exit) - fees, where
basis = perp/spot - 1. Reports not just the average but the TAIL (worst blocks,
percentiles) -- because carry dies in the tail, not the mean.

Needs in cache per coin: SYMBOL_funding.csv, SYMBOL_8h.csv (perp), SYMBOL_spot_8h.csv.

LIMITS this still can't fix (read before trusting a green result):
  * SURVIVORSHIP: coins delisted after crashes are absent -> the basis tail shown
    here is MILDER than reality.
  * EXECUTION-IN-CRISIS: assumes you transact at the 8h close. In a real cascade you
    often can't fill/exit -> real losses exceed what's shown. Only live/demo tests this.

Usage:
  python3 cash_carry_basis_replay.py --cache-dir tools/backtest/ohlcv_cache \
        --lookback 3 --hold 9 --k 8 --fee-total 0.186 --min-coins 20
"""
import argparse, glob, os, sys
import numpy as np
import pandas as pd


def _col(df, names):
    c = {x.lower(): x for x in df.columns}
    for n in names:
        if n in c: return c[n]
    return None

def _ts(s):
    t = pd.to_numeric(s, errors="coerce")
    if t.notna().all():
        return pd.to_datetime(t, unit="ms" if float(t.median()) > 1e12 else "s", errors="coerce")
    return pd.to_datetime(s, errors="coerce")

def _series(path, tnames, cnames):
    df = pd.read_csv(path)
    tc, cc = _col(df, tnames), _col(df, cnames)
    if not tc or not cc: return None
    s = pd.Series(pd.to_numeric(df[cc], errors="coerce").values, index=_ts(df[tc])).dropna()
    s.index = s.index.floor("8h")
    return s[~s.index.duplicated(keep="last")].sort_index()


def load(cache_dir, min_coins):
    F, PP, SP = {}, {}, {}
    for fp in sorted(glob.glob(os.path.join(cache_dir, "*_funding.csv"))):
        sym = os.path.basename(fp).replace("_funding.csv", "")
        perp_fp = os.path.join(cache_dir, f"{sym}_8h.csv")
        spot_fp = os.path.join(cache_dir, f"{sym}_spot_8h.csv")
        if not (os.path.exists(perp_fp) and os.path.exists(spot_fp)):
            continue
        fr = _series(fp, ["ts_ms","fundingtime","ts","timestamp","time"],
                         ["rate","fundingrate","funding_rate","last_funding_rate","r"])
        pp = _series(perp_fp, ["ts_ms","open_time","ts","timestamp","time"], ["close","c"])
        sp = _series(spot_fp, ["ts_ms","open_time","ts","timestamp","time"], ["close","c"])
        if fr is None or pp is None or sp is None: continue
        idx = fr.index
        ppr = pp.reindex(idx, method="ffill"); spr = sp.reindex(idx, method="ffill")
        ok = ppr.notna() & spr.notna() & (spr > 0)
        if ok.sum() >= 20:
            F[sym] = fr[ok]; PP[sym] = ppr[ok]; SP[sym] = spr[ok]
    if not F: sys.exit("No coins with funding + perp + spot. Run fetch_spot.py first.")
    F = pd.DataFrame(F).sort_index()
    PP = pd.DataFrame(PP).reindex(F.index); SP = pd.DataFrame(SP).reindex(F.index)
    B = PP / SP - 1.0                                   # real basis
    dense = (F.notna() & B.notna()).sum(axis=1) >= min_coins
    F, B = F[dense], B[dense]
    if len(F) < 50: sys.exit(f"Only {len(F)} dense periods >= {min_coins} coins.")
    print(f"Loaded {F.shape[1]} coins (funding+perp+spot), {F.shape[0]} dense periods "
          f"[{F.index.min()} .. {F.index.max()}]")
    print(f"median funding {np.nanmedian(F.values)*100:+.4f}%/period | "
          f"median basis {np.nanmedian(B.values)*100:+.4f}% | "
          f"basis abs p95 {np.nanpercentile(np.abs(B.values[np.isfinite(B.values)]),95)*100:.3f}%")
    return F, B


def backtest(F, B, lookback, hold, k, fee_total, shuffle=False, seed=0):
    fr, bs = F.values, B.values
    n_t, n_s = fr.shape
    rng = np.random.default_rng(seed); fee = fee_total/100.0
    rows = []  # (net, funding, basis_pnl)
    t = lookback
    while t + hold < n_t:
        with np.errstate(invalid="ignore"):
            sig = np.nanmean(fr[t-lookback:t], axis=0)
        fwd = np.nansum(fr[t:t+hold], axis=0)
        b0, b1 = bs[t], bs[t+hold]
        cnt = np.isfinite(fr[t:t+hold]).sum(axis=0)
        valid = np.isfinite(sig) & np.isfinite(b0) & np.isfinite(b1) & (cnt >= hold*0.5)
        idx = np.where(valid)[0]
        if len(idx) < k: t += hold; continue
        if shuffle:
            pick = idx[rng.permutation(len(idx))[:k]]
        else:
            pick = idx[np.argsort(sig[idx])[-k:]]
            pick = pick[sig[pick] > 0]
            if len(pick) == 0: t += hold; continue
        funding = fwd[pick].mean()
        basis_pnl = (b0[pick] - b1[pick]).mean()         # short perp + long spot
        net = funding + basis_pnl - fee
        rows.append((net, funding, basis_pnl))
        t += hold
    return rows


def agg(rows, hold_hours):
    if not rows: return {}
    net = np.array([r[0] for r in rows]); fund = np.array([r[1] for r in rows]); bas = np.array([r[2] for r in rows])
    eq = np.cumprod(1+net)
    bpy = (365*24)/hold_hours
    return {"n":len(net),"net":net.mean()*100,"wr":(net>0).mean()*100,
            "fund":fund.mean()*100,"basis":bas.mean()*100,
            "apr":net.mean()*bpy*100,"sharpe":(net.mean()/net.std()) if net.std()>0 else 0,
            "net_p5":np.percentile(net,5)*100,"net_min":net.min()*100,
            "basis_min":bas.min()*100,"maxdd":(eq/np.maximum.accumulate(eq)-1).min()*100}

def show(tag,m):
    if not m: print(f"  {tag}: no blocks"); return
    flag="✅" if (m["net"]>0 and m["sharpe"]>0) else "  "
    print(f"  {flag} {tag:5s} n={m['n']:4d} net={m['net']:+.4f}% WR={m['wr']:3.0f}% ~APR={m['apr']:+6.1f}% "
          f"Shrp={m['sharpe']:+.2f} | fund={m['fund']:+.4f}% basis={m['basis']:+.4f}%")
    print(f"          TAIL: worst block net={m['net_min']:+.3f}%  p5={m['net_p5']:+.3f}%  "
          f"worst basis={m['basis_min']:+.3f}%  maxDD={m['maxdd']:.1f}%")


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cache-dir", required=True)
    ap.add_argument("--lookback", type=int, default=3)
    ap.add_argument("--hold", type=int, default=9)
    ap.add_argument("--k", type=int, default=8)
    ap.add_argument("--fee-total", type=float, default=0.186)
    ap.add_argument("--min-coins", type=int, default=20)
    ap.add_argument("--train-frac", type=float, default=0.6)
    args=ap.parse_args()
    F,B = load(args.cache_dir, args.min_coins)
    split=int(len(F)*args.train_frac); hh=args.hold*8
    print(f"\nparams: lookback={args.lookback} hold={args.hold} ({hh}h) k={args.k} fee_total={args.fee_total}%")
    print(f"split: train={split} test={len(F)-split}\n")
    for name, sl in (("TRAIN", slice(0,split)), ("TEST", slice(split,None))):
        Fs, Bs = F.iloc[sl], B.iloc[sl]
        print(f"[{name}]")
        show(name, agg(backtest(Fs,Bs,args.lookback,args.hold,args.k,args.fee_total), hh))
        show("NULL", agg(backtest(Fs,Bs,args.lookback,args.hold,args.k,args.fee_total,shuffle=True,seed=1), hh))
        print()
    print("Now WITH real basis. Read: TEST net green + beats NULL, AND the TAIL is survivable\n"
          "(worst block / maxDD you could actually sit through). Survivorship makes the real tail worse;\n"
          "execution-in-crisis makes it worse still. A bad tail here = do NOT deploy. Sweep --hold/--fee-total.")


if __name__=="__main__":
    main()
