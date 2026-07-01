#!/usr/bin/env python3
"""
carry_paper.py — live PAPER cash-and-carry funding harvester (phase 1: no real orders)
======================================================================================
Runs the validated carry strategy on LIVE Binance funding + prices, executes NOTHING,
and measures the two things the backtest assumed away:
    1. BASIS drift  (perp vs spot don't track perfectly -> real P&L the backtest set to 0)
    2. funding actually collected vs fees, on a real evolving universe.

Strategy (delta-neutral): for each selected coin, SHORT perp (collect funding) + LONG spot
(cancel price). Net price exposure ~ 0; P&L = funding collected +/- basis drift - fees.

Selection: hold the top-k coins by current funding rate that (a) have a spot market and
(b) have funding above --entry-bp per 8h; drop a held coin when its funding falls below
--exit-bp. Equal notional per coin.

This is PAPER. It logs what it WOULD do and tracks realized paper P&L decomposed into
funding / basis / fees, so you can compare live reality to the +6.5% APR optimistic test.
Real order execution is a separate, later phase you turn on deliberately — not here.

Run:
  python3 carry_paper.py --once                 # one tick (use in cron, e.g. every 15m)
  python3 carry_paper.py --loop --interval 900  # run continuously
  python3 carry_paper.py --report               # print current paper book + stats
State: carry_state.json   Log: carry_log.csv
"""
import argparse, json, os, time, csv, urllib.request
from datetime import datetime, timezone

FAPI = "https://fapi.binance.com"
SPOT = "https://api.binance.com"
STATE = "carry_state.json"
LOG = "carry_log.csv"
EQUITY = "carry_equity.csv"
MAKER_BP = 1.0   # assumed per-leg, per-side maker fee in basis points (0.01%). Entry+exit+2 legs = 4x.


def get(url):
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.loads(r.read())


def eligible_universe():
    """Perps that also have a USDT spot market (needed for the hedge leg)."""
    perp = {s["symbol"] for s in get(f"{FAPI}/fapi/v1/exchangeInfo")["symbols"]
            if s["symbol"].endswith("USDT") and s.get("contractType") == "PERPETUAL" and s["status"] == "TRADING"}
    spot = {s["symbol"] for s in get(f"{SPOT}/api/v3/exchangeInfo")["symbols"]
            if s["symbol"].endswith("USDT") and s["status"] == "TRADING"}
    return perp & spot


def market_snapshot(universe):
    """Return {sym: {funding, perp, spot, next_funding}} for the eligible universe."""
    prem = {p["symbol"]: p for p in get(f"{FAPI}/fapi/v1/premiumIndex")}
    spot = {s["symbol"]: float(s["price"]) for s in get(f"{SPOT}/api/v3/ticker/price")}
    out = {}
    for s in universe:
        if s in prem and s in spot:
            out[s] = {"funding": float(prem[s]["lastFundingRate"]),
                      "perp": float(prem[s]["markPrice"]),
                      "spot": spot[s],
                      "next_funding": int(prem[s]["nextFundingTime"])}
    return out


def load_state():
    if os.path.exists(STATE):
        return json.load(open(STATE))
    return {"positions": {}, "cum_funding": 0.0, "cum_fees": 0.0, "cum_basis": 0.0,
            "realized": 0.0, "started": None, "ticks": 0}


def save_state(st):
    json.dump(st, open(STATE, "w"), indent=2)


def log_event(row):
    new = not os.path.exists(LOG)
    with open(LOG, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["ts", "event", "sym", "funding_bp", "perp", "spot", "basis_bp", "notional", "detail"])
        w.writerow(row)


def basis_bp(perp, spot):
    return (perp / spot - 1.0) * 1e4 if spot else 0.0


def tick(args):
    st = load_state()
    if st["started"] is None:
        st["started"] = datetime.now(timezone.utc).isoformat()
    now = int(time.time() * 1000)
    nowiso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    fee_leg = MAKER_BP / 1e4

    uni = eligible_universe()
    snap = market_snapshot(uni)
    if not snap:
        print("no market data"); return

    ranked = sorted(snap.items(), key=lambda kv: -kv[1]["funding"])
    entry_thr = args.entry_bp / 1e4
    exit_thr = args.exit_bp / 1e4
    target = [s for s, d in ranked if d["funding"] > entry_thr][:args.k]

    pos = st["positions"]

    # 1) accrue funding on held positions whose funding window passed since last tick
    for s, p in list(pos.items()):
        if s in snap and now >= p["next_funding"]:
            inc = snap[s]["funding"] * args.notional      # short perp on +funding => receive
            p["funding_collected"] += inc
            st["cum_funding"] += inc
            p["next_funding"] = snap[s]["next_funding"]
            log_event([nowiso, "FUND", s, f"{snap[s]['funding']*1e4:.2f}", snap[s]["perp"],
                       snap[s]["spot"], f"{basis_bp(snap[s]['perp'], snap[s]['spot']):.2f}",
                       args.notional, f"income={inc:+.4f}"])

    # 2) close held positions that fell below exit threshold (or lost spot/data)
    for s in list(pos.keys()):
        if s not in snap or snap[s]["funding"] < exit_thr:
            p = pos[s]
            if s in snap:
                b_now = basis_bp(snap[s]["perp"], snap[s]["spot"])
                basis_pnl = (p["entry_basis_bp"] - b_now) / 1e4 * args.notional  # short perp+long spot
            else:
                basis_pnl = 0.0
            fees = 2 * fee_leg * args.notional            # exit: 2 legs
            st["cum_basis"] += basis_pnl
            st["cum_fees"] += fees
            st["realized"] += p["funding_collected"] + basis_pnl - p["entry_fees"] - fees
            log_event([nowiso, "CLOSE", s, "", snap.get(s, {}).get("perp", ""),
                       snap.get(s, {}).get("spot", ""), "", args.notional,
                       f"funding={p['funding_collected']:+.4f} basis={basis_pnl:+.4f} fees={p['entry_fees']+fees:.4f}"])
            del pos[s]

    # 3) open new targets not yet held
    for s in target:
        if s not in pos:
            d = snap[s]
            fees = 2 * fee_leg * args.notional            # entry: 2 legs
            st["cum_fees"] += fees
            pos[s] = {"entry_ts": now, "entry_perp": d["perp"], "entry_spot": d["spot"],
                      "entry_basis_bp": basis_bp(d["perp"], d["spot"]), "notional": args.notional,
                      "funding_collected": 0.0, "entry_fees": fees, "next_funding": d["next_funding"]}
            log_event([nowiso, "OPEN", s, f"{d['funding']*1e4:.2f}", d["perp"], d["spot"],
                       f"{basis_bp(d['perp'], d['spot']):.2f}", args.notional, "short perp + long spot"])

    st["ticks"] += 1
    # record live snapshot for the dashboard (unrealized basis + net + equity point)
    unreal = 0.0
    for s, p in pos.items():
        if s in snap:
            unreal += (p["entry_basis_bp"] - basis_bp(snap[s]["perp"], snap[s]["spot"])) / 1e4 * p["notional"]
    net = st["cum_funding"] + st["cum_basis"] + unreal - st["cum_fees"]
    st["unreal_basis"] = unreal
    st["net"] = net
    st["last_update"] = nowiso
    new = not os.path.exists(EQUITY)
    with open(EQUITY, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["ts", "open", "cum_funding", "cum_basis", "unreal_basis", "cum_fees", "net"])
        w.writerow([nowiso, len(pos), round(st["cum_funding"], 4), round(st["cum_basis"], 4),
                    round(unreal, 4), round(st["cum_fees"], 4), round(net, 4)])
    save_state(st)
    report(st, snap)


def report(st, snap=None):
    pos = st["positions"]
    # unrealized basis on open positions
    unreal_basis = 0.0
    if snap:
        for s, p in pos.items():
            if s in snap:
                unreal_basis += (p["entry_basis_bp"] - basis_bp(snap[s]["perp"], snap[s]["spot"])) / 1e4 * p["notional"]
    net = st["cum_funding"] + st["cum_basis"] + unreal_basis - st["cum_fees"]
    # crude APR: net / capital-deployed / days * 365.  capital = 2*notional per position (both legs)
    days = 1
    if st["started"]:
        days = max(1/24, (datetime.now(timezone.utc) - datetime.fromisoformat(st["started"])).total_seconds()/86400)
    cap = max(1.0, 2 * (len(pos) or 1) * (pos[next(iter(pos))]["notional"] if pos else 100))
    apr = net / cap / days * 365 * 100
    print(f"\n=== carry paper book @ {datetime.now(timezone.utc):%Y-%m-%d %H:%M}Z  (tick {st['ticks']}, {days:.2f}d) ===")
    print(f"open positions: {len(pos)}  -> {', '.join(sorted(pos)) or '(none)'}")
    print(f"cum funding:  {st['cum_funding']:+.4f}")
    print(f"cum basis:    {st['cum_basis']:+.4f}   unrealized basis: {unreal_basis:+.4f}")
    print(f"cum fees:     {st['cum_fees']:.4f}   (maker {MAKER_BP}bp/leg)")
    print(f"NET (paper):  {net:+.4f}   ~APR(on 2x notional/coin): {apr:+.1f}%")
    print("watch: if 'basis' swings as large as 'funding', the perfect-hedge assumption fails and the edge is basis-bound.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="run a single tick")
    ap.add_argument("--loop", action="store_true", help="run continuously")
    ap.add_argument("--report", action="store_true", help="print current book and exit")
    ap.add_argument("--interval", type=int, default=900, help="seconds between ticks in --loop")
    ap.add_argument("--k", type=int, default=8, help="basket size (top-k by funding)")
    ap.add_argument("--entry-bp", type=float, default=2.0, help="enter if funding > this (bp per 8h; 1bp=0.01%%)")
    ap.add_argument("--exit-bp", type=float, default=0.5, help="drop a coin if funding < this (bp per 8h)")
    ap.add_argument("--notional", type=float, default=100.0, help="paper notional per coin per leg (USDT)")
    args = ap.parse_args()

    if args.report:
        try:
            snap = market_snapshot(eligible_universe())
        except Exception as e:
            print(f"(couldn't fetch live prices for basis: {e})"); snap = None
        report(load_state(), snap); return
    if args.loop:
        print(f"paper carry loop: every {args.interval}s, k={args.k}, entry>{args.entry_bp}bp, exit<{args.exit_bp}bp")
        while True:
            try:
                tick(args)
            except Exception as e:
                print(f"tick error: {e}")
            time.sleep(args.interval)
    else:
        tick(args)


if __name__ == "__main__":
    main()
