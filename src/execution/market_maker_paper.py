#!/usr/bin/env python3
"""
Phase 1 — Paper Market Maker (go/no-go measurement, ZERO risk).

Runs as a stage-only shadow observer. It computes two-sided quotes around the
live mid, then SIMULATES fills against the real trade tape (no orders are ever
placed). For every simulated fill it accounts:

    net_pnl = gross_spread_captured  -  maker_fees  -  adverse_selection

and logs per-symbol so you can answer the only question that matters before
building the execution engine: does spread capture beat fees + adverse
selection on your coins, at your real maker fee?

This is "AS-lite": vol-scaled spread + inventory skew (the essence of market
making) but NOT the full Avellaneda-Stoikov kappa calibration — kappa needs
real fill data, which this phase GENERATES. If the edge isn't here in paper,
we stop before writing post-only orders + the user-data fill stream.

Run (stage):  set ENGINE_ENV=stage; the predict-engine task starts it.
Report:       python3 market_maker_paper.py --report
"""
import os, csv, time, math, argparse
from collections import deque
from datetime import datetime
from pathlib import Path

import engine as E
try:
    from core_signals import detect_regime
except Exception:
    def detect_regime(sym): return ('neutral', 0.0)

# ── CONFIG (tune these; set MAKER_FEE from binance_fees.py actuals) ───────────
MM_MAKER_FEE_PCT  = 0.02     # maker fee PER SIDE, %. Binance USDT-M VIP0 ≈ 0.02 (less w/ BNB).
                              # Round-trip maker = 2× this. THE edge bar to beat.
MM_QUOTE_SIZE_USD = 50.0     # notional per quote (paper)
MM_MAX_INVENTORY  = 300.0    # USD notional inventory cap; stop quoting the side that grows it
MM_SPREAD_FLOOR   = 0.030    # min half-spread %, never quote tighter than this
MM_VOL_MULT       = 0.80     # half-spread = max(FLOOR, VOL_MULT × σ%)
MM_INV_SKEW_PCT   = 0.060    # max reservation-price shift % at full inventory
MM_VOL_WINDOW_SEC = 60.0     # realized-vol lookback
MM_REQUOTE_MS     = 250      # loop / re-quote interval
MM_REGIME_GATE    = True     # only quote in chop/neutral (MM dies in trends)
MM_FILL_THROUGH   = True     # True: trade must cross our resting price to fill (conservative)
MM_QUEUE_MODEL    = True     # FIFO queue: you sit BEHIND resting depth at your price level.
                              # Fills require cumulative crossing volume to consume the queue
                              # ahead first. This is the realism that surfaces adverse selection.
MM_QUEUE_FACTOR   = 1.0      # multiplier on resting depth treated as queue-ahead (×1 = full FIFO,
                              # >1 = more conservative, <1 = assume partial queue priority)

# Regimes where we actively quote vs. regimes where we only carry residual risk.
MM_QUOTE_REGIMES  = ('chop', 'neutral')
MM_RISK_REGIMES   = ('trend_up', 'trend_down', 'breakout', 'squeeze', 'cascade')

# Curated liquid mid-cap universe — wide-enough spreads to beat fees, deep enough
# to fill. NOT BTC/ETH (spreads < fee). Filtered to whatever is actually streaming.
# NARROWED 2026-06-12: the optimistic broad-book run was net-negative (−58), but
# DOT (+54) and OP (+47) were robustly positive with LOW adverse/gross, and DYDX
# was marginal (+2.9). Restricting to these isolates the only open question:
# do the deep-book survivors hold up under the FIFO queue haircut IN CHOP?
# (Full broad list kept below, commented, for reference.)
MM_SYMBOLS = ['DOTUSDT', 'OPUSDT', 'DYDXUSDT']
# MM_SYMBOLS = [
#     'ARBUSDT','OPUSDT','TIAUSDT','ONDOUSDT','INJUSDT','APTUSDT','SUIUSDT',
#     'NEARUSDT','LINKUSDT','AVAXUSDT','DOTUSDT','ATOMUSDT','PENDLEUSDT','DYDXUSDT',
# ]

_LOG_DIR = Path(__file__).parent / 'logs' / 'mm_paper'
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_CSV_PATH = _LOG_DIR / f"mm_paper_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"


class PaperMM:
    """Per-symbol paper market-making state + accounting."""
    __slots__ = ('sym','inv_units','cash','fills','buys','sells','gross_spread',
                 'fees','last_ts','quoting','bid_px','ask_px','t_quoting','t_total',
                 't_capped','t_start',
                 'q_bid','q_ask','q_bid_px','q_ask_px',          # FIFO queue state
                 'reg_stats','prev_net','cur_regime')            # regime-stratified accounting

    def __init__(self, sym):
        self.sym = sym
        self.inv_units    = 0.0     # signed inventory in base units
        self.cash         = 0.0     # realized cash flow (fees already netted)
        self.fills = self.buys = self.sells = 0
        self.gross_spread = 0.0     # Σ |fill_px − mid_at_fill| × units  (theoretical edge)
        self.fees         = 0.0     # Σ maker fees paid
        self.last_ts      = 0.0     # last processed tape ts
        self.quoting      = False
        self.bid_px = self.ask_px = None
        self.t_quoting = self.t_total = self.t_capped = 0.0
        # FIFO queue: USD ahead of us at the price level we're resting on
        self.q_bid = self.q_ask = 0.0
        self.q_bid_px = self.q_ask_px = None
        # per-regime accumulators: regime -> dict(fills, gross, fees, net, time)
        self.reg_stats = {}
        self.prev_net  = 0.0
        self.cur_regime = 'neutral'
        self.t_start  = time.time()

    # — volatility from price_hist (stdev of log-returns), as % —
    def _vol_pct(self, st, now_ms):
        ph = [(ts, p) for ts, p in st['price_hist']
              if now_ms - ts < MM_VOL_WINDOW_SEC * 1000 and p > 0]
        if len(ph) < 8:
            return None
        rets = [math.log(ph[i][1] / ph[i-1][1]) for i in range(1, len(ph))
                if ph[i-1][1] > 0]
        if len(rets) < 6:
            return None
        m = sum(rets) / len(rets)
        var = sum((r - m) ** 2 for r in rets) / (len(rets) - 1)
        return math.sqrt(var) * 100.0

    def _q_norm(self, mid):
        notional = self.inv_units * mid
        return max(-1.0, min(1.0, notional / MM_MAX_INVENTORY)) if MM_MAX_INVENTORY else 0.0

    def update(self, dt):
        st = E.sym_state.get(self.sym)
        self.t_total += dt
        if not st:
            self.quoting = False; self._attribute(dt, None); return
        bb, ba, px = st.get('best_bid'), st.get('best_ask'), st.get('price')
        if not bb or not ba or not px or bb <= 0 or ba <= bb:
            self.quoting = False; self._attribute(dt, None); return
        mid = (bb + ba) / 2
        now_ms = time.time() * 1000

        # regime is computed EVERY tick (not just for the gate) so that PnL on
        # residual inventory during trends is attributed to the trend bucket.
        regime, _ = detect_regime(self.sym)
        self.cur_regime = regime

        # regime gate — only quote in mean-reverting conditions. We still mark
        # inventory below, so a trend that catches us holding shows up as bleed.
        if MM_REGIME_GATE and regime not in MM_QUOTE_REGIMES:
            self.quoting = False
            self.bid_px = self.ask_px = None
            self.q_bid_px = self.q_ask_px = None   # pulling quotes loses queue position
            self._drain_tape(st)
            self._attribute(dt, mid)
            return

        vol = self._vol_pct(st, now_ms)
        if vol is None:
            self.quoting = False; self._drain_tape(st); self._attribute(dt, mid); return

        half = max(MM_SPREAD_FLOOR, MM_VOL_MULT * vol)        # half-spread %
        qn   = self._q_norm(mid)
        res  = mid * (1 + (-qn * MM_INV_SKEW_PCT) / 100.0)    # inventory-skewed reservation px
        bid  = min(res * (1 - half / 100.0), bb)              # post-only: never cross
        ask  = max(res * (1 + half / 100.0), ba)

        # inventory cap: quote only the side that reduces |inventory|
        quote_bid = qn < 1.0
        quote_ask = qn > -1.0
        if abs(qn) >= 1.0:
            self.t_capped += dt
        self.bid_px = bid if quote_bid else None
        self.ask_px = ask if quote_ask else None
        if not quote_bid: self.q_bid_px = None
        if not quote_ask: self.q_ask_px = None
        self.quoting = quote_bid or quote_ask
        self.t_quoting += dt

        self._simulate_fills(st, mid, quote_bid, quote_ask)
        self._attribute(dt, mid)

    def _attribute(self, dt, mid):
        """Add elapsed time and the net-PnL change since last update to the
        CURRENT regime's bucket. Sum of per-regime net == total net (conserved)."""
        s = self.reg_stats.setdefault(self.cur_regime,
                {'fills': 0, 'gross': 0.0, 'fees': 0.0, 'net': 0.0, 'time': 0.0})
        s['time'] += dt
        if mid is not None:
            net = self.cash + self.inv_units * mid
            s['net'] += (net - self.prev_net)
            self.prev_net = net

    def _drain_tape(self, st):
        tape = st.get('trade_tape')
        if tape:
            self.last_ts = tape[-1][0]

    def _simulate_fills(self, st, mid, quote_bid, quote_ask):
        tape = st.get('trade_tape')
        if not tape:
            return
        bids_f = st.get('bids_f') or {}
        asks_f = st.get('asks_f') or {}

        # (Re)seed queue-ahead whenever our resting price changes: we join the BACK,
        # behind all resting USD at our price level or better. Fills require crossing
        # volume to consume that queue first — this is what surfaces adverse selection.
        if MM_QUEUE_MODEL:
            if quote_bid and self.bid_px and self.bid_px != self.q_bid_px:
                self.q_bid = sum(p * s for p, s in bids_f.items()
                                 if p >= self.bid_px) * MM_QUEUE_FACTOR
                self.q_bid_px = self.bid_px
            if quote_ask and self.ask_px and self.ask_px != self.q_ask_px:
                self.q_ask = sum(p * s for p, s in asks_f.items()
                                 if p <= self.ask_px) * MM_QUEUE_FACTOR
                self.q_ask_px = self.ask_px

        bid_done = ask_done = False
        new_last = self.last_ts
        for ts, tpx, val, is_buy in list(tape):
            if ts <= self.last_ts:
                continue
            new_last = max(new_last, ts)
            # resting BID fills when an aggressive SELL trades down through it
            if quote_bid and not bid_done and self.bid_px and not is_buy:
                hit = (tpx <= self.bid_px) if MM_FILL_THROUGH else (tpx < self.bid_px)
                if hit:
                    if MM_QUEUE_MODEL and self.q_bid > 0:
                        self.q_bid -= val          # consume queue ahead of us
                    if not (MM_QUEUE_MODEL and self.q_bid > 0):
                        self._fill('buy', self.bid_px, mid)
                        bid_done = True; self.q_bid_px = None   # re-quote → back of queue
            # resting ASK fills when an aggressive BUY trades up through it
            if quote_ask and not ask_done and self.ask_px and is_buy:
                hit = (tpx >= self.ask_px) if MM_FILL_THROUGH else (tpx > self.ask_px)
                if hit:
                    if MM_QUEUE_MODEL and self.q_ask > 0:
                        self.q_ask -= val
                    if not (MM_QUEUE_MODEL and self.q_ask > 0):
                        self._fill('sell', self.ask_px, mid)
                        ask_done = True; self.q_ask_px = None
            if bid_done and ask_done:
                break
        self.last_ts = new_last

    def _fill(self, side, price, mid_at_fill):
        units = MM_QUOTE_SIZE_USD / price
        fee   = MM_QUOTE_SIZE_USD * MM_MAKER_FEE_PCT / 100.0
        self.fills += 1
        self.fees  += fee
        s = self.reg_stats.setdefault(self.cur_regime,
                {'fills': 0, 'gross': 0.0, 'fees': 0.0, 'net': 0.0, 'time': 0.0})
        s['fills'] += 1
        s['fees']  += fee
        if side == 'buy':
            self.inv_units += units
            self.cash      -= price * units
            g = (mid_at_fill - price) * units            # bought below mid
            self.gross_spread += g; s['gross'] += g
            self.buys += 1
        else:
            self.inv_units -= units
            self.cash      += price * units
            g = (price - mid_at_fill) * units            # sold above mid
            self.gross_spread += g; s['gross'] += g
            self.sells += 1
        self.cash -= fee

    # — accounting —
    def _mid(self):
        st = E.sym_state.get(self.sym)
        if st and st.get('best_bid') and st.get('best_ask'):
            return (st['best_bid'] + st['best_ask']) / 2
        return 0.0

    def regime_rows(self):
        """One row per regime bucket (cumulative). adverse = gross − fees − net."""
        out = []
        for reg, s in self.reg_stats.items():
            adverse = s['gross'] - s['fees'] - s['net']
            out.append({'sym': self.sym, 'regime': reg, 'fills': s['fills'],
                        'gross': round(s['gross'], 4), 'fees': round(s['fees'], 4),
                        'adverse': round(adverse, 4), 'net': round(s['net'], 4),
                        'time_s': round(s['time'], 1)})
        return out

    def overall(self):
        mid = self._mid()
        net = self.cash + self.inv_units * mid
        return {'fills': self.fills, 'net': net,
                'uptime': round(100 * self.t_quoting / self.t_total, 1) if self.t_total else 0.0,
                'capped': round(100 * self.t_capped / self.t_total, 1) if self.t_total else 0.0}


_books: dict[str, PaperMM] = {}


async def mm_paper_loop(coins=None):
    """Stage-only paper-MM task. Hook into predict_engine tasks list (ENGINE_ENV=stage)."""
    import asyncio
    print(f"[mm-paper] started → {_CSV_PATH.name} "
          f"(maker={MM_MAKER_FEE_PCT}%/side, size=${MM_QUOTE_SIZE_USD}, "
          f"regime_gate={MM_REGIME_GATE})", flush=True)
    last = time.time()
    last_snap = 0.0
    while True:
        await asyncio.sleep(MM_REQUOTE_MS / 1000)
        now = time.time(); dt = now - last; last = now
        universe = [s for s in MM_SYMBOLS if s in E.sym_state]
        for sym in universe:
            mm = _books.get(sym) or _books.setdefault(sym, PaperMM(sym))
            try:
                mm.update(dt)
            except Exception as ex:
                print(f"[mm-paper] {sym} update error: {ex}", flush=True)
        # snapshot to CSV every 60s
        if now - last_snap >= 60 and _books:
            last_snap = now
            _snapshot()


def _snapshot():
    rows = []
    for b in _books.values():
        rows.extend(b.regime_rows())
    if not rows:
        return
    write_header = not _CSV_PATH.exists()
    with open(_CSV_PATH, 'a', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['ts', 'sym', 'regime', 'fills',
                                          'gross', 'fees', 'adverse', 'net', 'time_s'])
        if write_header:
            w.writeheader()
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for r in rows:
            w.writerow({'ts': ts, **r})
    ov = [b.overall() for b in _books.values()]
    tot_net = sum(o['net'] for o in ov)
    tot_fill = sum(o['fills'] for o in ov)
    print(f"[mm-paper] snapshot: {len(rows)} syms, {tot_fill} fills, "
          f"net=${tot_net:+.4f} (gross−fees−adverse)", flush=True)


# ── Standalone report (go/no-go readout, regime-stratified) ──────────────────
def report(path=None):
    """Regime-stratified edge readout. Reads last cumulative row per (sym, regime).
    The verdict requires real trend/volatile coverage AND that trend-period
    inventory bleed doesn't erase the chop earnings."""
    from collections import defaultdict
    p = Path(path) if path else _latest_csv()
    if not p or not p.exists():
        print("no paper-MM CSV found in logs/mm_paper/"); return
    latest = {}
    with open(p) as f:
        for row in csv.DictReader(f):
            latest[(row['sym'], row['regime'])] = row   # last cumulative per (sym,regime)

    reg = defaultdict(lambda: {'fills': 0, 'gross': 0.0, 'fees': 0.0,
                               'adverse': 0.0, 'net': 0.0, 'time': 0.0})
    sym = defaultdict(lambda: {'fills': 0, 'net': 0.0})
    for (s, rg), r in latest.items():
        a = reg[rg]
        a['fills'] += int(r['fills']); a['gross'] += float(r['gross'])
        a['fees'] += float(r['fees']); a['adverse'] += float(r['adverse'])
        a['net'] += float(r['net']);   a['time'] += float(r['time_s'])
        sym[s]['fills'] += int(r['fills']); sym[s]['net'] += float(r['net'])

    print(f"\n=== Paper MM edge (regime-stratified) — {p.name} ===")
    print(f"(maker {MM_MAKER_FEE_PCT}%/side; FIFO queue={MM_QUEUE_MODEL}; net = gross − fees − adverse)\n")
    hdr = f"{'regime':<12}{'fills':>7}{'gross':>10}{'fees':>9}{'adverse':>10}{'NET':>10}{'time_min':>10}"
    print(hdr); print('-' * len(hdr))
    quote_net = risk_net = 0.0; quote_fills = risk_fills = 0; risk_time = 0.0
    for rg in sorted(reg, key=lambda r: -reg[r]['net']):
        a = reg[rg]
        print(f"{rg:<12}{a['fills']:>7}{a['gross']:>10.3f}{a['fees']:>9.3f}"
              f"{a['adverse']:>10.3f}{a['net']:>+10.3f}{a['time']/60:>10.1f}")
        if rg in MM_QUOTE_REGIMES:
            quote_net += a['net']; quote_fills += a['fills']
        elif rg in MM_RISK_REGIMES:
            risk_net += a['net']; risk_fills += a['fills']; risk_time += a['time']
    print('-' * len(hdr))
    total_net = sum(a['net'] for a in reg.values())
    total_fills = sum(a['fills'] for a in reg.values())
    print(f"{'TOTAL':<12}{total_fills:>7}{'':>29}{total_net:>+10.3f}")

    print(f"\n  quoting (chop/neutral):  net={quote_net:+.3f}  fills={quote_fills}")
    print(f"  risk (trend/breakout/…): net={risk_net:+.3f}  fills={risk_fills}  time={risk_time/60:.1f}min")

    # top contributors / detractors by symbol
    ranked = sorted(sym.items(), key=lambda kv: -kv[1]['net'])
    if ranked:
        best = ', '.join(f"{s}({d['net']:+.2f})" for s, d in ranked[:3])
        worst = ', '.join(f"{s}({d['net']:+.2f})" for s, d in ranked[-3:])
        print(f"  best: {best}   worst: {worst}")

    if risk_time / 60 < 30:
        verdict = (f"INSUFFICIENT COVERAGE — only {risk_time/60:.0f} min in trend/volatile regimes. "
                   "Keep running; the chop number alone is not a green light.")
    elif quote_net <= 0:
        verdict = "NO EDGE — not profitable even in chop after the queue haircut. Stop."
    elif total_net <= 0:
        verdict = "NO EDGE — chop earnings are erased by trend-period inventory bleed. Stop."
    elif risk_net < -0.5 * quote_net:
        verdict = ("MARGINAL — trend bleed eats >half the chop earnings. Tighten the regime gate / "
                   "inventory cap and re-measure before committing to Phase 2.")
    else:
        verdict = "EDGE HOLDS across regimes — Phase 2 (real demo quoting) is justified."
    print(f"\nVERDICT: {verdict}\n")


def _latest_csv():
    files = sorted(_LOG_DIR.glob('mm_paper_*.csv'))
    return files[-1] if files else None


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--report', action='store_true', help='print edge readout and exit')
    ap.add_argument('--csv', help='specific CSV path for --report')
    args = ap.parse_args()
    if args.report:
        report(args.csv)
    else:
        print("This module runs as a task inside predict-engine (ENGINE_ENV=stage).")
        print("For the readout:  python3 market_maker_paper.py --report")
