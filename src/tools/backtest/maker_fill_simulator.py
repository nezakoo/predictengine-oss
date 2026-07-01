#!/usr/bin/env python3
"""
maker_fill_simulator.py — Phase 0: Validate maker fill viability
================================================================
For each historical signal (from signals_with_outcomes.csv), simulates
whether a passive limit order would have filled, at what price, and
what the resulting P&L would be vs taker execution.

Requires 1-second kline data fetched around each signal time.
Uses Binance /fapi/v1/klines with interval=1s (max 1000 bars = ~16min).

Output:
  maker_sim_results.csv  — per-signal fill result
  maker_sim_summary.txt  — per-strategy verdict

Usage:
  # First run with --fetch to download 1s bars (slow, Binance rate-limited)
  python3 maker_fill_simulator.py \\
    --signals tools/analysis/signals_with_outcomes.csv \\
    --cache-dir tools/backtest/maker_1s_cache \\
    --strategies E CGYL Q \\
    --fetch

  # Then run analysis on cached data
  python3 maker_fill_simulator.py \\
    --signals tools/analysis/signals_with_outcomes.csv \\
    --cache-dir tools/backtest/maker_1s_cache \\
    --strategies E CGYL Q
"""

import argparse, csv, json, os, sys, time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
import requests

BASE_URL = 'https://fapi.binance.com'

# ── Config ────────────────────────────────────────────────────────────────────
FILL_WINDOWS   = {'E': 120, 'CGYL': 180, 'Q': 3600, 'default': 60}
CHASE_LIMIT    = 2          # max re-posts
PRICE_OFFSET   = 0.0002     # post limit 0.02% inside mid (improvement)
TAKER_RT       = 0.093 / 100
MAKER_RT       = 0.029 / 100   # VIP1 + BNB
MIN_SIGNALS    = 30         # skip strategy if fewer signals

# ── Binance 1s kline fetcher ──────────────────────────────────────────────────
def fetch_1s_bars(sym: str, start_ms: int, n_bars: int, cache_dir: Path) -> list:
    """
    Fetch n_bars of 1-second klines starting at start_ms.
    Caches to cache_dir/SYMBOL/START_MS.json.
    Returns list of dicts: {ts_ms, open, high, low, close}.
    """
    cache_path = cache_dir / sym / f'{start_ms}.json'
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    if cache_path.exists():
        try:
            return json.loads(cache_path.read_text())
        except:
            pass

    try:
        r = requests.get(
            BASE_URL + '/fapi/v1/klines',
            params={'symbol': sym, 'interval': '1s',
                    'startTime': start_ms, 'limit': min(n_bars, 1000)},
            timeout=10
        )
        raw = r.json()
        if not isinstance(raw, list):
            return []
        bars = [{'ts_ms': int(k[0]), 'open': float(k[1]), 'high': float(k[2]),
                  'low': float(k[3]), 'close': float(k[4])} for k in raw]
        cache_path.write_text(json.dumps(bars))
        return bars
    except Exception as e:
        print(f'  fetch_1s {sym}@{start_ms}: {e}')
        return []


# ── Simulate a single maker fill ──────────────────────────────────────────────
def simulate_maker_fill(sym: str, direction: str, entry_price: float,
                        bars_1s: list, fill_window_s: int,
                        taker_net_pct: float) -> dict:
    """
    Simulate passive limit order execution.

    For a LONG: post limit at entry_price * (1 - PRICE_OFFSET)  → fill when low <= limit
    For a SHORT: post limit at entry_price * (1 + PRICE_OFFSET) → fill when high >= limit

    Returns dict with fill result.
    """
    if not bars_1s:
        return {'filled': False, 'reason': 'no_data'}

    is_long   = direction.lower() in ('long', 'buy')
    # Post slightly better than market (inside spread)
    if is_long:
        limit = entry_price * (1 - PRICE_OFFSET)
    else:
        limit = entry_price * (1 + PRICE_OFFSET)

    fill_bar  = None
    chase_count = 0
    current_limit = limit
    deadline_bar  = fill_window_s  # bars = seconds for 1s data

    for i, bar in enumerate(bars_1s[:deadline_bar]):
        if is_long:
            if bar['low'] <= current_limit:
                fill_bar = bar
                fill_price = current_limit
                break
            # Chase: if price moves >0.05% away, re-post
            if bar['close'] > current_limit * 1.0005 and chase_count < CHASE_LIMIT:
                current_limit = bar['close'] * (1 - PRICE_OFFSET)
                chase_count += 1
        else:
            if bar['high'] >= current_limit:
                fill_bar = bar
                fill_price = current_limit
                break
            if bar['close'] < current_limit * 0.9995 and chase_count < CHASE_LIMIT:
                current_limit = bar['close'] * (1 + PRICE_OFFSET)
                chase_count += 1

    if not fill_bar:
        return {'filled': False, 'reason': 'timeout', 'fill_window_s': fill_window_s,
                'chase_count': chase_count}

    fill_delay_s = (fill_bar['ts_ms'] - bars_1s[0]['ts_ms']) / 1000

    # Adverse selection: how much did price move against us after fill?
    # Look at next 10 bars after fill
    fill_idx = bars_1s.index(fill_bar)
    post_fill = bars_1s[fill_idx+1:fill_idx+11]
    if post_fill:
        if is_long:
            worst = min(b['low'] for b in post_fill)
            adv_sel = max(0, (fill_price - worst) / fill_price)
        else:
            worst = max(b['high'] for b in post_fill)
            adv_sel = max(0, (worst - fill_price) / fill_price)
    else:
        adv_sel = 0.0

    # Taker entry vs maker entry price difference
    if is_long:
        price_improvement = (entry_price - fill_price) / entry_price  # positive = better
    else:
        price_improvement = (fill_price - entry_price) / entry_price

    # Estimate maker net P&L using taker outcome but adjusting entry
    # taker_net_pct already has taker fee deducted
    # maker net = taker_net_pct - taker_fee + maker_fee + price_improvement - adv_sel
    maker_net_pct = (taker_net_pct / 100
                     + TAKER_RT            # add back taker fee
                     - MAKER_RT            # subtract maker fee
                     + price_improvement   # better fill price
                     - adv_sel             # adverse selection cost
                     ) * 100

    return {
        'filled':           True,
        'fill_delay_s':     round(fill_delay_s, 1),
        'fill_price':       round(fill_price, 8),
        'price_improvement': round(price_improvement * 100, 4),
        'adverse_sel_pct':  round(adv_sel * 100, 4),
        'chase_count':      chase_count,
        'taker_net_pct':    round(taker_net_pct, 4),
        'maker_net_pct':    round(maker_net_pct, 4),
        'maker_vs_taker':   round(maker_net_pct - taker_net_pct, 4),
    }


# ── Load signals ──────────────────────────────────────────────────────────────
def load_signals(path: str, strategies: list) -> list:
    """Load signals_with_outcomes.csv filtered to target strategies."""
    signals = []
    strat_set = set(strategies)
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('strategy') not in strat_set:
                continue
            if not row.get('net_pct') or not row.get('ts_fired'):
                continue
            try:
                # Parse ts_fired: YYYYMMDD_HHMMSS
                ts = datetime.strptime(row['ts_fired'], '%Y%m%d_%H%M%S')
                ts_ms = int(ts.replace(tzinfo=timezone.utc).timestamp() * 1000)
                signals.append({
                    'strategy':  row['strategy'],
                    'symbol':    row['symbol'],
                    'direction': row.get('direction', row.get('dir', 'long')),
                    'ts_ms':     ts_ms,
                    'entry_price': float(row.get('entry_price', 0) or 0),
                    'net_pct':   float(row['net_pct']),
                    'win':       int(row.get('win', 0)),
                })
            except Exception:
                continue
    return signals


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--signals',    required=True, help='signals_with_outcomes.csv')
    ap.add_argument('--cache-dir',  default='tools/backtest/maker_1s_cache')
    ap.add_argument('--strategies', nargs='+', default=['E', 'CGYL', 'Q'])
    ap.add_argument('--fetch',      action='store_true', help='Download 1s bars')
    ap.add_argument('--max-signals',type=int, default=500,
                    help='Max signals per strategy to simulate (default 500)')
    ap.add_argument('--out',        default='maker_sim_results.csv')
    ap.add_argument('--fee-rt-taker', type=float, default=0.093)
    ap.add_argument('--fee-rt-maker', type=float, default=0.029)
    args = ap.parse_args()

    global TAKER_RT, MAKER_RT
    TAKER_RT = args.fee_rt_taker / 100
    MAKER_RT = args.fee_rt_maker / 100

    cache_dir = Path(args.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    print(f'\nMaker Fill Simulator')
    print(f'  Strategies: {args.strategies}')
    print(f'  Taker RT: {args.fee_rt_taker}%  Maker RT: {args.fee_rt_maker}%')
    print(f'  Price offset: {PRICE_OFFSET*100:.3f}%  Chase limit: {CHASE_LIMIT}')
    print()

    signals = load_signals(args.signals, args.strategies)
    print(f'  Loaded {len(signals)} signals')
    if not signals:
        print('  No signals found — check --strategies match CSV values')
        sys.exit(1)

    # Group by strategy, sample if too many
    import random
    by_strat = defaultdict(list)
    for s in signals:
        by_strat[s['strategy']].append(s)

    results = []
    strat_summary = {}

    for strat, sigs in by_strat.items():
        random.shuffle(sigs)
        sigs = sigs[:args.max_signals]
        fill_window = FILL_WINDOWS.get(strat, FILL_WINDOWS['default'])
        n_bars_needed = fill_window + 60  # extra buffer

        print(f'\n  [{strat}]  n={len(sigs)}  fill_window={fill_window}s')
        filled = unfilled = 0
        taker_nets = []; maker_nets = []

        for i, sig in enumerate(sigs):
            sym = sig['symbol']
            ts_ms = sig['ts_ms']

            # Fetch or load 1s bars
            if args.fetch:
                bars = fetch_1s_bars(sym, ts_ms, n_bars_needed, cache_dir)
                time.sleep(0.12)  # ~8 req/s to stay under rate limit
            else:
                cache_path = cache_dir / sym / f'{ts_ms}.json'
                if cache_path.exists():
                    try: bars = json.loads(cache_path.read_text())
                    except: bars = []
                else:
                    bars = []

            if not bars:
                if i < 5:
                    print(f'    {sym}@{ts_ms}: no bars'
                          f'{"" if not args.fetch else " (fetch failed)"}')
                continue

            result = simulate_maker_fill(
                sym, sig['direction'], sig['entry_price'],
                bars, fill_window, sig['net_pct']
            )
            result.update({'strategy': strat, 'symbol': sym,
                           'ts_ms': ts_ms, 'win': sig['win']})
            results.append(result)

            if result['filled']:
                filled += 1
                maker_nets.append(result['maker_net_pct'])
                taker_nets.append(result['taker_net_pct'])
            else:
                unfilled += 1

            if (i+1) % 50 == 0:
                fr = filled/(filled+unfilled)*100 if (filled+unfilled) else 0
                print(f'    {i+1}/{len(sigs)}  fill_rate={fr:.0f}%', end='\r')

        total = filled + unfilled
        fill_rate = filled / total * 100 if total else 0
        avg_maker = sum(maker_nets)/len(maker_nets) if maker_nets else 0
        avg_taker = sum(taker_nets)/len(taker_nets) if taker_nets else 0

        import numpy as np
        sharpe = 0
        if maker_nets:
            a = np.array(maker_nets) / 100
            sharpe = float(a.mean()/a.std()) if a.std()>0 else 0

        strat_summary[strat] = {
            'n_total':   total,
            'n_filled':  filled,
            'fill_rate': fill_rate,
            'avg_taker': avg_taker,
            'avg_maker': avg_maker,
            'improvement': avg_maker - avg_taker,
            'sharpe':    sharpe,
            'positive':  avg_maker > 0 and fill_rate >= 60,
        }

        verdict = '✅ VIABLE' if strat_summary[strat]['positive'] else '❌ NOT VIABLE'
        print(f'\n    fill_rate={fill_rate:.0f}%  '
              f'avg_taker={avg_taker:+.4f}%  '
              f'avg_maker={avg_maker:+.4f}%  '
              f'improvement={avg_maker-avg_taker:+.4f}%  '
              f'sharpe={sharpe:+.2f}  {verdict}')

    # ── Write results CSV ─────────────────────────────────────────────────────
    if results:
        fields = ['strategy','symbol','ts_ms','win','filled','reason',
                  'fill_delay_s','fill_price','price_improvement',
                  'adverse_sel_pct','chase_count',
                  'taker_net_pct','maker_net_pct','maker_vs_taker']
        with open(args.out, 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
            w.writeheader()
            w.writerows(results)
        print(f'\n  Results written → {args.out}')

    # ── Print summary ─────────────────────────────────────────────────────────
    print(f'\n{"="*65}')
    print(f'  MAKER FILL SIMULATION SUMMARY')
    print(f'{"="*65}')
    print(f'  {"strat":<8} {"n":>5} {"fill%":>7} {"taker":>8} '
          f'{"maker":>8} {"improv":>8} {"sharpe":>7}  verdict')
    print(f'  {"-"*65}')
    for strat, s in strat_summary.items():
        verdict = '✅ VIABLE' if s['positive'] else '❌ NOT VIABLE'
        print(f'  {strat:<8} {s["n_total"]:>5} {s["fill_rate"]:>6.0f}%  '
              f'{s["avg_taker"]:>+7.4f}%  {s["avg_maker"]:>+7.4f}%  '
              f'{s["improvement"]:>+7.4f}%  {s["sharpe"]:>+6.2f}  {verdict}')

    print(f'\n  Decision gate: fill_rate >= 60% AND avg_maker > 0')
    viable = [s for s, v in strat_summary.items() if v['positive']]
    if viable:
        print(f'\n  ✅ Proceed to Phase 1 with: {viable}')
    else:
        print(f'\n  ❌ No strategy passes — maker execution not viable')
        print(f'     Recommendation: abandon maker approach')

    # Save summary JSON for Phase 1 decision
    with open('maker_sim_summary.json', 'w') as f:
        json.dump(strat_summary, f, indent=2)
    print(f'  Summary → maker_sim_summary.json')


if __name__ == '__main__':
    main()
