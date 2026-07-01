"""
PredictEngine — Distribution Builder (Phase 3)
================================================
Reads signals_with_outcomes.csv (from signal_outcome_joiner.py) and produces
sim_dists.json with REAL per-strategy distributions grounded in actual trade
outcomes — replacing the hand-tuned fallback_dists in synth_market_sim.py.

Supersedes sim_learn_dists.py: that tool only learned from fired/blocked rows
(no outcomes). This tool uses matched fired→closed rows with real WR and PnL.

Usage:
  # Step 1 — generate outcomes file (if not already done):
  python signal_outcome_joiner.py --backup-dir ./data_backup

  # Step 2 — build distributions:
  python dist_builder.py

  # Step 3 — use in synth_market_sim:
  python synth_market_sim.py --dists sim_dists.json --ticks 200000
  python synth_market_sim.py --strategy B --ticks 200000 --compare vpin_min 0.55 0.65

Output: sim_dists.json
Format: drop-in replacement for fallback_dists in synth_market_sim.py.

New fields added beyond fallback_dists:
  win_rate        — real WR from matched trades
  avg_net_pct     — real avg net per trade (after fees)
  exit_dist       — {trail, sl, tp, inertia, time, rev} as fractions
  avg_net_by_exit — avg net per exit reason {trail: X, sl: Y, ...}
  avg_dur_sec     — avg trade duration
  long_win_rate   — WR for long trades only
  short_win_rate  — WR for short trades only
  n_trades        — matched trade count (confidence indicator)
  source          — 'signals_with_outcomes' (vs 'fallback' for old dists)
"""

import csv
import json
import math
import sys
import argparse
from collections import defaultdict
from pathlib import Path
_HERE = Path(__file__).parent  # tools/backtest/


# ── Stats helpers ─────────────────────────────────────────────────────────────

def _mean(vals):
    return sum(vals) / len(vals) if vals else 0.0

def _std(vals):
    if len(vals) < 2: return 0.0
    m = _mean(vals)
    return math.sqrt(sum((v - m) ** 2 for v in vals) / (len(vals) - 1))

def _percentile(vals, p):
    if not vals: return 0.0
    s = sorted(vals)
    i = (len(s) - 1) * p / 100
    lo = int(i); hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (i - lo)


# ── Loader ────────────────────────────────────────────────────────────────────

def load_outcomes(path: str) -> list:
    rows = []
    try:
        with open(path, newline='', encoding='utf-8') as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                net_str = row.get('net_pct', '').strip()
                if not net_str:
                    continue
                try:
                    net_pct = float(net_str)
                except ValueError:
                    continue

                def _f(k):
                    v = row.get(k, '').strip()
                    try: return float(v) if v else None
                    except ValueError: return None

                win_str = row.get('win', '').strip()
                win = int(win_str) if win_str in ('0', '1') else None

                rows.append({
                    'strategy':    row.get('strategy', '').strip(),
                    'dir':         row.get('dir', '').strip(),
                    'vpin':        _f('vpin'),
                    'atr':         _f('atr'),
                    'conf':        _f('conf'),
                    'score':       _f('score'),
                    'net_pct':     net_pct,
                    'win':         win,
                    'exit_reason': row.get('exit_reason', '').strip(),
                    'dur_sec':     _f('dur_sec'),
                })
    except FileNotFoundError:
        print(f'[ERROR] {path} not found.', file=sys.stderr)
        print('  Run: python signal_outcome_joiner.py --backup-dir ./data_backup', file=sys.stderr)
        sys.exit(1)
    return rows


# ── Per-strategy dist builder ─────────────────────────────────────────────────

def build_dists(rows: list, min_trades: int = 20) -> dict:
    """
    Build per-strategy distribution dict from matched trade rows.
    Returns dict compatible with synth_market_sim.py's dists format.
    """
    by_strat = defaultdict(list)
    for r in rows:
        if r['strategy']:
            by_strat[r['strategy']].append(r)

    out = {}
    for label, trades in sorted(by_strat.items()):
        n = len(trades)
        if n < min_trades:
            continue

        # ── Signal distributions (for regime injection sampling) ──
        vpins  = [r['vpin']  for r in trades if r['vpin']  is not None]
        atrs   = [r['atr']   for r in trades if r['atr']   is not None]
        confs  = [r['conf']  for r in trades if r['conf']  is not None]
        scores = [r['score'] for r in trades if r['score'] is not None]
        durs   = [r['dur_sec'] for r in trades if r['dur_sec'] is not None]

        # ── Direction split ──
        longs  = [r for r in trades if r['dir'] == 'long']
        shorts = [r for r in trades if r['dir'] == 'short']
        long_pct = len(longs) / n

        # ── Win rates ──
        wins_all   = [r for r in trades if r['win'] == 1]
        wins_long  = [r for r in longs  if r['win'] == 1]
        wins_short = [r for r in shorts if r['win'] == 1]

        win_rate       = len(wins_all)   / n          * 100
        long_win_rate  = len(wins_long)  / max(len(longs),  1) * 100
        short_win_rate = len(wins_short) / max(len(shorts), 1) * 100

        # ── Net PnL ──
        nets     = [r['net_pct'] for r in trades]
        avg_net  = _mean(nets)

        # ── Exit reason distribution ──
        exit_counts = defaultdict(int)
        exit_nets   = defaultdict(list)
        for r in trades:
            er = r['exit_reason'] or 'unknown'
            exit_counts[er] += 1
            exit_nets[er].append(r['net_pct'])

        total_exits = sum(exit_counts.values())
        exit_dist = {k: round(v / total_exits, 4)
                     for k, v in exit_counts.items() if total_exits > 0}
        avg_net_by_exit = {k: round(_mean(v), 6)
                           for k, v in exit_nets.items()}

        # ── conf_std fallback if too few conf rows ──
        conf_std = _std(confs) if len(confs) >= 5 else 10.0
        score_std = _std(scores) if len(scores) >= 5 else 20.0

        out[label] = {
            # ── Fields synth_market_sim already reads (drop-in compatible) ──
            'fired':       n,
            'score_mean':  round(_mean(scores), 3)  if scores else 0.0,
            'score_std':   round(score_std, 3),
            'vpin_mean':   round(_mean(vpins), 4)   if vpins  else 0.55,
            'vpin_std':    round(_std(vpins),  4)   if len(vpins) >= 2 else 0.08,
            'vpin_p10':    round(_percentile(vpins, 10), 4) if vpins else 0.40,
            'vpin_p90':    round(_percentile(vpins, 90), 4) if vpins else 0.80,
            'atr_mean':    round(_mean(atrs),  4)   if atrs   else 0.35,
            'atr_std':     round(_std(atrs),   4)   if len(atrs)  >= 2 else 0.10,
            'conf_mean':   round(_mean(confs), 2)   if confs  else 60.0,
            'conf_std':    round(conf_std, 2),
            'long_pct':    round(long_pct, 4),

            # ── New outcome fields ──
            'n_trades':        n,
            'win_rate':        round(win_rate, 2),
            'long_win_rate':   round(long_win_rate,  2),
            'short_win_rate':  round(short_win_rate, 2),
            'avg_net_pct':     round(avg_net, 6),
            'avg_dur_sec':     round(_mean(durs), 1) if durs else 120.0,
            'exit_dist':       exit_dist,
            'avg_net_by_exit': avg_net_by_exit,

            # Percentiles for richer sampling
            'net_p25':     round(_percentile(nets, 25), 5),
            'net_p50':     round(_percentile(nets, 50), 5),
            'net_p75':     round(_percentile(nets, 75), 5),
            'net_std':     round(_std(nets), 5),

            'source': 'signals_with_outcomes',
        }

    return out


# ── Reporter ──────────────────────────────────────────────────────────────────

def print_dists(dists: dict) -> None:
    RESET = '\033[0m'; BOLD = '\033[1m'; CYAN = '\033[96m'
    GREEN = '\033[92m'; RED  = '\033[91m'; DIM  = '\033[2m'
    YELLOW = '\033[93m'

    def _c(col, txt): return f'{col}{txt}{RESET}'
    def _wr(v): return _c(GREEN if v >= 50 else (YELLOW if v >= 40 else RED), f'{v:.1f}%')
    def _net(v): return _c(GREEN if v >= 0 else RED, f'{v:+.4f}%')

    print(_c(BOLD + CYAN, f'\n{"━"*70}'))
    print(_c(BOLD, '  DISTRIBUTION BUILDER — real outcome distributions'))
    print(_c(CYAN, f'{"━"*70}'))
    print(f'  Built {len(dists)} strategy distributions from signals_with_outcomes.csv\n')

    print(f'  {"Strat":<6} {"n":>5} {"WR%":>7} {"long_WR":>8} {"sht_WR":>8} '
          f'{"avg_net":>9} {"vpin_μ":>7} {"exits (top 3)"}')
    print(f'  {"─"*5}  {"─"*5}  {"─"*6}  {"─"*7}  {"─"*7}  {"─"*8}  {"─"*6}  {"─"*30}')

    for label, d in sorted(dists.items()):
        exits = sorted(d['exit_dist'].items(), key=lambda x: -x[1])[:3]
        exit_str = '  '.join(f"{k}:{v*100:.0f}%" for k, v in exits)
        print(f'  {label:<6} {d["n_trades"]:>5,} {_wr(d["win_rate"])} '
              f'{_wr(d["long_win_rate"])} {_wr(d["short_win_rate"])} '
              f'{_net(d["avg_net_pct"])} {d["vpin_mean"]:>7.3f}  '
              f'{_c(DIM, exit_str)}')

    print()

    # Highlight key findings
    print(_c(BOLD, '  Key findings for --compare mode:'))
    for label, d in sorted(dists.items()):
        if d['n_trades'] < 100:
            continue
        lwr = d['long_win_rate']
        swr = d['short_win_rate']
        delta = abs(lwr - swr)
        if delta >= 5.0:
            better = 'LONG' if lwr > swr else 'SHORT'
            flag = _c(YELLOW, f'  ← {better} preferred ({delta:.1f}pp gap)')
        else:
            flag = ''
        if d['avg_net_pct'] > -0.02:
            net_flag = _c(GREEN, '  ← near breakeven')
        elif d['avg_net_pct'] < -0.08:
            net_flag = _c(RED, '  ← high fee drag')
        else:
            net_flag = ''
        if flag or net_flag:
            print(f'  {label}: WR={d["win_rate"]:.1f}% avg={d["avg_net_pct"]:+.4f}%{flag}{net_flag}')
    print()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Build real outcome distributions for synth_market_sim.py',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python dist_builder.py
  python dist_builder.py --input signals_with_outcomes.csv --out sim_dists.json
  python dist_builder.py --min-trades 50
  python dist_builder.py --strategy B W L K

Then run:
  python synth_market_sim.py --dists sim_dists.json --ticks 200000
  python synth_market_sim.py --strategy B --ticks 200000 --compare vpin_min 0.55 0.65
        """
    )
    parser.add_argument('--input',      default=str(_HERE.parent / 'analysis' / 'signals_with_outcomes.csv'))
    parser.add_argument('--out',        default=str(_HERE / 'sim_dists.json'))
    parser.add_argument('--min-trades', type=int, default=20,
                        help='Min matched trades to include a strategy (default: 20)')
    parser.add_argument('--strategy',   nargs='+', default=None,
                        help='Filter to specific strategies')
    args = parser.parse_args()

    print(f'  Loading {args.input}...', file=sys.stderr)
    rows = load_outcomes(args.input)

    if args.strategy:
        rows = [r for r in rows if r['strategy'] in args.strategy]

    print(f'  {len(rows):,} matched trades loaded', file=sys.stderr)

    dists = build_dists(rows, min_trades=args.min_trades)

    print_dists(dists)

    with open(args.out, 'w') as fh:
        json.dump(dists, fh, indent=2)

    print(f'  Wrote {len(dists)} distributions → {args.out}', file=sys.stderr)
    print(f'  Run: python synth_market_sim.py --dists {args.out}', file=sys.stderr)


if __name__ == '__main__':
    main()
