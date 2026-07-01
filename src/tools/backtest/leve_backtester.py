"""
PredictEngine — L Strategy Backtester
======================================
Backtests the L (candle S/R) strategy against real Binance 1m klines.

Unlike ohlcv_replay.py, this tool ONLY tests L because L's signal is
entirely computable from klines:
  - S/R levels: TPO profile from price_hist (1h lookback)
  - VPIN: from taker buy volume (klines col 9)
  - ATR: from klines H/L/C
  - Exits: trail/SL simulated against subsequent candle prices

No sub-second data required. No order book. No microburst.

Usage:
  # Basic backtest — all symbols, full cache range:
  python level_backtester.py

  # Specific params:
  python level_backtester.py --vpin-min 0.55 --short-only

  # Compare two configs:
  python level_backtester.py --compare vpin_min 0.50 0.55
  python level_backtester.py --compare short_only False True
  python level_backtester.py --compare trail_dist 0.28 0.20

  # Date range:
  python level_backtester.py --from 2026-05-15 --to 2026-06-10

  # Walk-forward (21 train / 9 validate):
  python level_backtester.py --walk-forward

  # Specific symbols:
  python level_backtester.py --symbols WLDUSDT HYPEUSDT NEARUSDT

Output: per-symbol WR, direction breakdown, exit distribution, net PnL

Run before deploying L config changes:
  python level_backtester.py --compare short_only False True
"""

import sys, os, csv, math, time, argparse
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ── Config defaults (mirrors strategies_config.py L params) ──────────────────

DEFAULTS = {
    'vpin_min':      0.50,
    'min_vol_atr':   0.25,
    'trail_dist':    0.28,    # % trailing stop distance
    'atr_sl_mult':   2.0,     # SL = entry_price ± ATR * mult
    'atr_tp_mult':   1.6,     # TP = entry_price ± ATR * mult (999=disabled)
    'cooldown_sec':  60.0,
    'short_only':    False,
    'long_only':     False,
    'sr_dist_max':   0.04,    # % max distance from level to fire
    'sr_min_touches': 7,      # minimum TPO hits to qualify as level
    'fee_rt':        0.08,    # % round-trip fee
    'spread_max':    0.05,    # % max spread
    'warmup_candles': 120,    # candles before first fire (build price_hist)
    'max_hold_candles': 120,  # max 120 candles (2h) before timeout exit
}

CACHE_DIR = Path('ohlcv_cache')


# ── OHLCV loader ──────────────────────────────────────────────────────────────

def load_candles(symbol: str) -> list:
    p = CACHE_DIR / f'{symbol}_1m.csv'
    if not p.exists():
        return []
    rows = []
    try:
        with open(p, newline='') as fh:
            for row in csv.DictReader(fh):
                try:
                    rows.append({
                        'ts_ms':   int(row['ts_ms']),
                        'open':    float(row['open']),
                        'high':    float(row['high']),
                        'low':     float(row['low']),
                        'close':   float(row['close']),
                        'volume':  float(row['volume']),
                        'tb_vol':  float(row.get('taker_buy_base_vol') or row.get('tb_vol') or 0),
                    })
                except (ValueError, KeyError):
                    pass
    except Exception:
        return []
    rows.sort(key=lambda r: r['ts_ms'])
    return rows


# ── Signal computation from klines ───────────────────────────────────────────

def compute_vpin(price_hist: deque, vpin_buckets: deque, vpin_acc: dict) -> Optional[float]:
    """
    Compute VPIN from vpin_buckets (same logic as core_signals.calc_vpin path 1).
    Returns 0.0-1.0 or None if insufficient data.
    """
    buckets = list(vpin_buckets)
    if len(buckets) >= 5:
        recent = buckets[-20:]
        val = recent[0]
        for b in recent[1:]:
            val = 0.30 * b + 0.70 * val
        return round(val, 3)
    return None


def compute_atr(klines: list, n: int = 14) -> float:
    """Wilder's ATR from recent klines dicts {h,l,c}."""
    if len(klines) < 4:
        return 0.0
    bars = klines[-n - 1:]
    trs = []
    for i in range(1, len(bars)):
        hi, lo, prev_c = bars[i]['high'], bars[i]['low'], bars[i - 1]['close']
        if prev_c == 0:
            continue
        tr = max(hi - lo, abs(hi - prev_c), abs(lo - prev_c)) / prev_c * 100
        trs.append(tr)
    if not trs:
        return 0.0
    atr = sum(trs) / len(trs)
    for tr in trs[1:]:
        atr = (atr * (n - 1) + tr) / n
    return round(atr, 4)


def build_sr_levels(price_hist: list, sr_dist_max: float, sr_min_touches: int) -> list:
    """
    TPO-based S/R level detection.
    Mirrors _build_sr_levels from strategies_signals.py.
    price_hist: list of (ts_ms, price) tuples, last 1h
    Returns list of {'price': float, 'touches': int, 'kind': 'both'}
    """
    if len(price_hist) < 100:
        return []

    px_first = price_hist[0][1]
    if px_first == 0:
        return []
    bucket_size = px_first * 0.0010

    profile = defaultdict(int)
    for _, px in price_hist:
        if px == 0:
            continue
        b = round(px / bucket_size) * bucket_size
        profile[b] += 1

    levels = []
    sorted_buckets = sorted(profile.items(), key=lambda x: x[0])
    min_count = len(price_hist) * 0.05

    for i in range(1, len(sorted_buckets) - 1):
        _, prev_c = sorted_buckets[i - 1]
        curr_b, curr_c = sorted_buckets[i]
        _, next_c = sorted_buckets[i + 1]

        if curr_c > prev_c and curr_c > next_c and curr_c >= min_count:
            if curr_c >= sr_min_touches:
                levels.append({
                    'price':   curr_b,
                    'touches': curr_c,
                    'kind':    'both',
                })

    return sorted(levels, key=lambda x: -x['touches'])


def find_level_signal(price: float, last_close: float, levels: list,
                      vpin: Optional[float], sr_dist_max: float) -> Optional[dict]:
    """
    Detect S/R level signal at current price.
    Mirrors _find_level_signal from strategies_signals.py.
    """
    for lv in levels:
        dist = abs(price - lv['price']) / lv['price'] * 100

        # Bounce signal: price near level
        if dist <= sr_dist_max:
            if lv['kind'] in ('support', 'both') and price >= lv['price']:
                return {'dir': 'long', 'type': 'bounce', 'level': lv}
            if lv['kind'] in ('resistance', 'both') and price <= lv['price']:
                return {'dir': 'short', 'type': 'bounce', 'level': lv}

        # Break signal: candle close meaningfully through level
        close_pct = (last_close - lv['price']) / lv['price'] * 100
        if lv['kind'] in ('resistance', 'both') and close_pct > 0.08:
            if vpin is not None and vpin < 0.50:
                continue
            return {'dir': 'long', 'type': 'break', 'level': lv}
        if lv['kind'] in ('support', 'both') and close_pct < -0.08:
            if vpin is not None and vpin < 0.50:
                continue
            return {'dir': 'short', 'type': 'break', 'level': lv}

    return None


# ── Trade simulation ──────────────────────────────────────────────────────────

def simulate_exit(candles: list, entry_idx: int, entry_price: float,
                  direction: str, cfg: dict) -> dict:
    """
    Simulate trail/SL/TP exit against subsequent candles.
    Returns {'exit_reason': str, 'exit_price': float, 'net_pct': float,
             'dur_candles': int, 'mfe': float}
    """
    trail_dist = cfg['trail_dist']
    atr_sl     = cfg['atr_sl_mult'] * compute_atr(candles[max(0, entry_idx-15):entry_idx+1])
    atr_tp     = cfg['atr_tp_mult']
    fee        = cfg['fee_rt']
    max_hold   = cfg['max_hold_candles']

    if atr_sl == 0:
        atr_sl = trail_dist * 2

    if direction == 'long':
        sl_price   = entry_price * (1 - atr_sl / 100)
        tp_price   = entry_price * (1 + atr_tp * atr_sl / 100) if atr_tp < 900 else float('inf')
        trail_high = entry_price
    else:
        sl_price   = entry_price * (1 + atr_sl / 100)
        tp_price   = entry_price * (1 - atr_tp * atr_sl / 100) if atr_tp < 900 else 0.0
        trail_low  = entry_price

    mfe = 0.0  # max favourable excursion

    for i in range(entry_idx + 1, min(entry_idx + max_hold + 1, len(candles))):
        c = candles[i]
        hi, lo = c['high'], c['low']

        if direction == 'long':
            mfe = max(mfe, (hi - entry_price) / entry_price * 100)
            # Trail stop update
            if hi > trail_high:
                trail_high = hi
            trail_sl = trail_high * (1 - trail_dist / 100)

            # Check SL hit
            if lo <= sl_price:
                gross = (sl_price - entry_price) / entry_price * 100
                return {'exit_reason': 'sl', 'exit_price': sl_price,
                        'net_pct': gross - fee, 'dur_candles': i - entry_idx, 'mfe': mfe}
            # Check TP hit
            if hi >= tp_price:
                gross = (tp_price - entry_price) / entry_price * 100
                return {'exit_reason': 'tp', 'exit_price': tp_price,
                        'net_pct': gross - fee, 'dur_candles': i - entry_idx, 'mfe': mfe}
            # Check trail stop
            if lo <= trail_sl and trail_high > entry_price:
                gross = (trail_sl - entry_price) / entry_price * 100
                return {'exit_reason': 'trail', 'exit_price': trail_sl,
                        'net_pct': gross - fee, 'dur_candles': i - entry_idx, 'mfe': mfe}

        else:  # short
            mfe = max(mfe, (entry_price - lo) / entry_price * 100)
            # Trail stop update
            if lo < trail_low:
                trail_low = lo
            trail_sl = trail_low * (1 + trail_dist / 100)

            # Check SL
            if hi >= sl_price:
                gross = (entry_price - sl_price) / entry_price * 100
                return {'exit_reason': 'sl', 'exit_price': sl_price,
                        'net_pct': gross - fee, 'dur_candles': i - entry_idx, 'mfe': mfe}
            # Check TP
            if lo <= tp_price:
                gross = (entry_price - tp_price) / entry_price * 100
                return {'exit_reason': 'tp', 'exit_price': tp_price,
                        'net_pct': gross - fee, 'dur_candles': i - entry_idx, 'mfe': mfe}
            # Check trail
            if hi >= trail_sl and trail_low < entry_price:
                gross = (entry_price - trail_sl) / entry_price * 100
                return {'exit_reason': 'trail', 'exit_price': trail_sl,
                        'net_pct': gross - fee, 'dur_candles': i - entry_idx, 'mfe': mfe}

    # Timeout exit — use last close
    last_close = candles[min(entry_idx + max_hold, len(candles) - 1)]['close']
    if direction == 'long':
        gross = (last_close - entry_price) / entry_price * 100
    else:
        gross = (entry_price - last_close) / entry_price * 100
    return {'exit_reason': 'time', 'exit_price': last_close,
            'net_pct': gross - fee, 'dur_candles': max_hold, 'mfe': mfe}


# ── Per-symbol backtest ───────────────────────────────────────────────────────

def backtest_symbol(symbol: str, candles: list, cfg: dict,
                    start_ms: int = 0, end_ms: int = 0) -> list:
    """
    Run L strategy backtest on one symbol's candle series.
    Returns list of trade dicts.
    """
    trades = []
    warmup   = cfg['warmup_candles']
    cooldown = cfg['cooldown_sec']
    sr_dist  = cfg['sr_dist_max']
    sr_min   = cfg['sr_min_touches']
    vpin_min = cfg['vpin_min']
    atr_min  = cfg['min_vol_atr']
    spread_max = cfg['spread_max']

    # Rolling state
    price_hist   = deque(maxlen=600)   # ~10min of 1s ticks; we use candle closes
    vpin_buckets = deque(maxlen=50)
    vpin_acc     = {'buy': 0.0, 'sell': 0.0, 'total': 0.0}
    recent_klines = deque(maxlen=30)

    last_fire_ts  = 0.0
    in_trade      = False

    for idx, c in enumerate(candles):
        ts_ms = c['ts_ms']

        # Date range filter
        if start_ms and ts_ms < start_ms:
            continue
        if end_ms and ts_ms > end_ms:
            break

        # Feed price_hist — use 60 sub-ticks per candle for dense history
        o, h, l, cl = c['open'], c['high'], c['low'], c['close']
        rng = h - l
        for sub_i in range(60):
            t = sub_i / 59
            if cl >= o:
                if t < 0.25:    px = o + (l - o) * (t / 0.25)
                elif t < 0.75:  px = l + (h - l) * ((t - 0.25) / 0.50)
                else:           px = h + (cl - h) * ((t - 0.75) / 0.25)
            else:
                if t < 0.25:    px = o + (h - o) * (t / 0.25)
                elif t < 0.75:  px = h + (l - h) * ((t - 0.25) / 0.50)
                else:           px = l + (cl - l) * ((t - 0.75) / 0.25)
            sub_ts = ts_ms + sub_i * 1000
            price_hist.append((sub_ts, px))

        # VPIN bucket update
        vol = c['volume']
        tb  = c['tb_vol']
        buy_frac = tb / vol if vol > 0 else 0.5
        vpin_acc['buy']   += vol * buy_frac
        vpin_acc['sell']  += vol * (1 - buy_frac)
        vpin_acc['total'] += vol
        if vpin_acc['total'] > 0:
            imb = abs(vpin_acc['buy'] / vpin_acc['total'] - 0.5) * 2
            vpin_buckets.append(imb)
            vpin_acc = {'buy': 0.0, 'sell': 0.0, 'total': 0.0}

        recent_klines.append(c)

        # Warmup
        if idx < warmup:
            continue

        if in_trade:
            continue  # position lock — one trade at a time

        # Cooldown check
        if (ts_ms / 1000) - last_fire_ts < cooldown:
            continue

        # Compute signals
        vpin  = compute_vpin(price_hist, vpin_buckets, vpin_acc)
        atr   = compute_atr(list(recent_klines))
        spread = 0.01  # approximation — 0.01% synthetic spread

        # Gate checks
        if vpin is not None and vpin < vpin_min:
            continue
        if atr < atr_min:
            continue
        if spread > spread_max:
            continue

        # S/R levels (rebuild every candle — cheap since price_hist is small)
        ph_list = list(price_hist)
        # Use last 1h of sub-ticks (3600 entries at 1/sec)
        now_ms = ts_ms + 59_000
        ph_1h = [(ts, px) for ts, px in ph_list if now_ms - ts < 3_600_000]
        levels = build_sr_levels(ph_1h, sr_dist, sr_min)
        if not levels:
            continue

        # Find signal
        sig = find_level_signal(cl, cl, levels, vpin, sr_dist)
        if sig is None:
            continue

        direction = sig['dir']

        # Direction filter
        if cfg['long_only'] and direction == 'short':
            continue
        if cfg['short_only'] and direction == 'long':
            continue

        # Score gate (use level touch count as proxy for score)
        score_proxy = sig['level']['touches'] * 0.5  # ~3.5 per touch
        if score_proxy < cfg.get('min_score_proxy', 0):
            continue

        # Simulate exit
        entry_price = cl
        exit_info   = simulate_exit(candles, idx, entry_price, direction, cfg)

        in_trade = False   # single candle position (simplified)
        last_fire_ts = ts_ms / 1000

        trades.append({
            'symbol':      symbol,
            'ts_fired':    ts_ms,
            'dir':         direction,
            'level_type':  sig['type'],
            'level_price': sig['level']['price'],
            'entry_price': entry_price,
            'vpin':        vpin,
            'atr':         atr,
            'exit_reason': exit_info['exit_reason'],
            'net_pct':     exit_info['net_pct'],
            'dur_candles': exit_info['dur_candles'],
            'mfe':         exit_info['mfe'],
            'win':         1 if exit_info['net_pct'] > 0 else 0,
        })

    return trades


# ── Stats + reporting ─────────────────────────────────────────────────────────

RESET = '\033[0m'; BOLD = '\033[1m'; CYAN = '\033[96m'
GREEN = '\033[92m'; RED  = '\033[91m'; DIM  = '\033[2m'; YELLOW = '\033[93m'

def _c(col, txt): return f'{col}{txt}{RESET}'
def _wr(v):   return _c(GREEN if v >= 50 else (YELLOW if v >= 40 else RED), f'{v:.1f}%')
def _net(v):  return _c(GREEN if v >= 0 else RED, f'{v:+.5f}%')
def _netc(v): return _c(GREEN if v >= 0 else RED, f'{v:+.3f}%')


def compute_stats(trades: list) -> dict:
    if not trades:
        return {'n': 0, 'wins': 0, 'wr': 0.0, 'avg_net': 0.0, 'cum_net': 0.0}
    n       = len(trades)
    wins    = sum(t['win'] for t in trades)
    cum_net = sum(t['net_pct'] for t in trades)
    return {
        'n': n, 'wins': wins,
        'wr': wins / n * 100,
        'avg_net': cum_net / n,
        'cum_net': cum_net,
    }


def print_results(trades: list, label: str = 'Backtest results', cfg: dict = None):
    s = compute_stats(trades)
    if s['n'] == 0:
        print(f'\n  {label}: no trades fired')
        return

    print(_c(BOLD + CYAN, f'\n{"━"*64}'))
    print(_c(BOLD, f'  {label}'))
    if cfg:
        cfg_str = f"vpin≥{cfg['vpin_min']} trail={cfg['trail_dist']}% " \
                  f"{'short_only ' if cfg['short_only'] else ''}{'long_only ' if cfg['long_only'] else ''}"
        print(_c(DIM, f'  config: {cfg_str}'))
    print(_c(CYAN, f'{"━"*64}'))
    print(f'  n={s["n"]:,}  WR={_wr(s["wr"])}  avg={_net(s["avg_net"])}  cum={_netc(s["cum_net"])}')

    # Direction breakdown
    for d in ('long', 'short'):
        dt = [t for t in trades if t['dir'] == d]
        if dt:
            ds = compute_stats(dt)
            print(f'  {d:<6} n={ds["n"]:>4,}  {_wr(ds["wr"])}  {_net(ds["avg_net"])}  {_netc(ds["cum_net"])}')

    # Exit breakdown
    exit_counts = defaultdict(list)
    for t in trades:
        exit_counts[t['exit_reason']].append(t)
    print(f'\n  {"exit":<8} {"n":>5} {"WR%":>7} {"avg_net":>9}')
    for reason, et in sorted(exit_counts.items(), key=lambda x: -len(x[1])):
        es = compute_stats(et)
        print(f'  {reason:<8} {es["n"]:>5,} {_wr(es["wr"])} {_net(es["avg_net"])}')

    # Level type breakdown
    type_counts = defaultdict(list)
    for t in trades:
        type_counts[t['level_type']].append(t)
    if len(type_counts) > 1:
        print(f'\n  {"type":<8} {"n":>5} {"WR%":>7} {"avg_net":>9}')
        for ltype, lt in sorted(type_counts.items(), key=lambda x: -len(x[1])):
            ls = compute_stats(lt)
            print(f'  {ltype:<8} {ls["n"]:>5,} {_wr(ls["wr"])} {_net(ls["avg_net"])}')

    # Top symbols
    sym_stats = defaultdict(list)
    for t in trades:
        sym_stats[t['symbol'].replace('USDT', '')].append(t)
    sym_sorted = sorted(sym_stats.items(),
                        key=lambda x: compute_stats(x[1])['avg_net'], reverse=True)
    if sym_sorted:
        print(f'\n  Top coins: ', end='')
        parts = []
        for sym, st2 in sym_sorted[:5]:
            ss = compute_stats(st2)
            parts.append(f'{sym}:{_net(ss["avg_net"])}({ss["n"]}T)')
        print('  '.join(parts))


def print_compare(trades_a: list, trades_b: list, param: str, val_a, val_b):
    sa = compute_stats(trades_a)
    sb = compute_stats(trades_b)
    print(_c(BOLD + CYAN, f'\n{"━"*64}'))
    print(_c(BOLD, f'  COMPARE  {param}: {val_a!r}  →  {val_b!r}'))
    print(_c(CYAN, f'{"━"*64}'))
    print(f'  {"":20} {str(val_a):>12}  {str(val_b):>12}  {"delta":>10}')
    print(f'  {"─"*20}  {"─"*12}  {"─"*12}  {"─"*10}')

    def _row(name, va, vb, fmt, better='high'):
        d = vb - va
        col = (GREEN if d > 0 else (RED if d < 0 else DIM)) if better == 'high' \
              else (GREEN if d < 0 else (RED if d > 0 else DIM))
        print(f'  {name:<20}  {fmt(va):>12}  {fmt(vb):>12}  {_c(col, fmt(d)):>10}')

    _row('n trades',  float(sa['n']),       float(sb['n']),       lambda v: f'{int(v):,}',  'neutral')
    _row('WR%',       sa['wr'],             sb['wr'],             lambda v: f'{v:.1f}%')
    _row('avg_net',   sa['avg_net'],        sb['avg_net'],        lambda v: f'{v:+.5f}%')
    _row('cum_net',   sa['cum_net'],        sb['cum_net'],        lambda v: f'{v:+.3f}%')

    # Per-direction
    for d in ('long', 'short'):
        ta = [t for t in trades_a if t['dir'] == d]
        tb = [t for t in trades_b if t['dir'] == d]
        if ta or tb:
            da = compute_stats(ta); db = compute_stats(tb)
            _row(f'{d} avg_net', da['avg_net'], db['avg_net'], lambda v: f'{v:+.5f}%')

    verdict = ''
    if sb['avg_net'] > sa['avg_net'] and sb['n'] >= sa['n'] * 0.5:
        verdict = _c(GREEN, f'  → {val_b!r} is BETTER ({sb["avg_net"] - sa["avg_net"]:+.5f}% per trade)')
    elif sa['avg_net'] > sb['avg_net']:
        verdict = _c(RED, f'  → {val_a!r} is better (change would HURT)')
    else:
        verdict = _c(DIM, '  → No meaningful difference')
    print(f'\n{verdict}')


# ── Walk-forward ──────────────────────────────────────────────────────────────

def walk_forward(all_candles: dict, cfg: dict, train_days: int, val_days: int):
    all_ts = [c['ts_ms'] for sym_c in all_candles.values() for c in sym_c]
    if not all_ts:
        return
    global_start = min(all_ts)
    global_end   = max(all_ts)
    train_ms = train_days * 86_400_000
    val_ms   = val_days   * 86_400_000

    wi = 0
    cursor = global_start
    while cursor + train_ms + val_ms <= global_end + 120_000:
        t_start = cursor
        t_end   = cursor + train_ms
        v_start = t_end
        v_end   = v_start + val_ms

        def _fmt(ms):
            return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d')

        print(f'\n  Window {wi + 1}: train {_fmt(t_start)}→{_fmt(t_end)}  '
              f'validate {_fmt(v_start)}→{_fmt(v_end)}')

        val_trades = []
        for sym, candles in all_candles.items():
            val_trades.extend(
                backtest_symbol(sym, candles, cfg,
                                start_ms=v_start, end_ms=v_end)
            )

        print_results(val_trades, label=f'Window {wi + 1} validation', cfg=cfg)
        cursor += val_ms
        wi += 1
        if wi >= 5:  # cap at 5 windows
            break


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='L strategy backtester — S/R levels from real klines',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python level_backtester.py
  python level_backtester.py --short-only
  python level_backtester.py --vpin-min 0.55
  python level_backtester.py --compare vpin_min 0.50 0.55
  python level_backtester.py --compare short_only False True
  python level_backtester.py --compare trail_dist 0.28 0.20
  python level_backtester.py --from 2026-05-15 --to 2026-06-10
  python level_backtester.py --walk-forward --train-days 21 --val-days 9
        """
    )
    parser.add_argument('--symbols',     nargs='+', default=None)
    parser.add_argument('--vpin-min',    type=float, default=DEFAULTS['vpin_min'])
    parser.add_argument('--trail-dist',  type=float, default=DEFAULTS['trail_dist'])
    parser.add_argument('--atr-sl-mult', type=float, default=DEFAULTS['atr_sl_mult'])
    parser.add_argument('--short-only',  action='store_true')
    parser.add_argument('--long-only',   action='store_true')
    parser.add_argument('--from',        dest='date_from', default=None)
    parser.add_argument('--to',          dest='date_to',   default=None)
    parser.add_argument('--compare',     nargs=3, metavar=('PARAM', 'VAL_A', 'VAL_B'),
                        help='Compare two param values: --compare vpin_min 0.50 0.55')
    parser.add_argument('--walk-forward', action='store_true')
    parser.add_argument('--train-days',  type=int, default=21)
    parser.add_argument('--val-days',    type=int, default=9)
    parser.add_argument('--cache-dir',   default='ohlcv_cache')
    parser.add_argument('--warmup',      type=int, default=DEFAULTS['warmup_candles'])
    args = parser.parse_args()

    global CACHE_DIR
    CACHE_DIR = Path(args.cache_dir)

    print(_c(BOLD + CYAN, '\nPredictEngine — L Strategy Backtester'))

    # Build config
    cfg = dict(DEFAULTS)
    cfg['vpin_min']    = args.vpin_min
    cfg['trail_dist']  = args.trail_dist
    cfg['atr_sl_mult'] = args.atr_sl_mult
    cfg['short_only']  = args.short_only
    cfg['long_only']   = args.long_only
    cfg['warmup_candles'] = args.warmup

    # Date range
    start_ms = end_ms = 0
    if args.date_from:
        dt = datetime.strptime(args.date_from, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        start_ms = int(dt.timestamp() * 1000)
    if args.date_to:
        dt = datetime.strptime(args.date_to, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        end_ms = int(dt.timestamp() * 1000)

    # Load symbols
    sym_list = args.symbols
    if not sym_list:
        if CACHE_DIR.exists():
            sym_list = [p.name.replace('_1m.csv', '')
                        for p in sorted(CACHE_DIR.glob('*_1m.csv'))]
    if not sym_list:
        print(f'[ERROR] No klines cache found. Run: python ohlcv_fetcher.py')
        sys.exit(1)

    # Filter out BTC — L doesn't trade BTC (in blacklist)
    sym_list = [s for s in sym_list if s not in ('BTCUSDT', 'ETHUSDT', 'SOLUSDT')]

    print(_c(DIM, f'  Loading {len(sym_list)} symbols...'))
    all_candles = {}
    total = 0
    for sym in sym_list:
        c = load_candles(sym)
        if c:
            all_candles[sym] = c
            total += len(c)
    print(_c(DIM, f'  {total:,} candles loaded'))

    if not all_candles:
        print('[ERROR] No candles loaded.')
        sys.exit(1)

    # Walk-forward mode
    if args.walk_forward:
        walk_forward(all_candles, cfg, args.train_days, args.val_days)
        return

    # Compare mode
    if args.compare:
        param, val_a_str, val_b_str = args.compare
        # Type coerce
        val_a: object = val_a_str
        val_b: object = val_b_str
        if param in DEFAULTS:
            default = DEFAULTS[param]
            try:
                if isinstance(default, bool):
                    val_a = val_a_str.lower() in ('true', '1', 'yes')
                    val_b = val_b_str.lower() in ('true', '1', 'yes')
                elif isinstance(default, float):
                    val_a = float(val_a_str)
                    val_b = float(val_b_str)
                elif isinstance(default, int):
                    val_a = int(val_a_str)
                    val_b = int(val_b_str)
            except (ValueError, TypeError):
                pass

        print(_c(DIM, f'  Running {param}={val_a!r}...'))
        cfg_a = dict(cfg); cfg_a[param] = val_a
        trades_a = []
        for sym, candles in all_candles.items():
            trades_a.extend(backtest_symbol(sym, candles, cfg_a, start_ms, end_ms))

        print(_c(DIM, f'  Running {param}={val_b!r}...'))
        cfg_b = dict(cfg); cfg_b[param] = val_b
        trades_b = []
        for sym, candles in all_candles.items():
            trades_b.extend(backtest_symbol(sym, candles, cfg_b, start_ms, end_ms))

        print_results(trades_a, label=f'{param}={val_a!r}', cfg=cfg_a)
        print_results(trades_b, label=f'{param}={val_b!r}', cfg=cfg_b)
        print_compare(trades_a, trades_b, param, val_a, val_b)
        return

    # Standard backtest
    print(_c(DIM, '  Running backtest...'))
    all_trades = []
    for sym, candles in all_candles.items():
        all_trades.extend(backtest_symbol(sym, candles, cfg, start_ms, end_ms))

    print_results(all_trades, label='L Strategy Backtest', cfg=cfg)
    print()


if __name__ == '__main__':
    main()
