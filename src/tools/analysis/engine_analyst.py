"""
PredictEngine — Unified Analyst
================================
Single entry point for all strategy analysis, deploy tracking,
and pre-deploy validation.

Commands:
  report          Full report card for all strategies — WR, direction, exits, verdict
  sweep           Auto gate sweep — find optimal vpin/conf/score per strategy
  symbols         Per-symbol breakdown — blacklist candidates and top performers
  restore         Test disabled strategies with optimised configs
  deploy log      Record a deploy with before/after stats
  deploy check    Pre-deploy validation — DEPLOY / HOLD / NEUTRAL verdict
  deploy history  Show all recorded deploys and their outcomes

Usage:
  python engine_analyst.py report
  python engine_analyst.py report --strategy B W L
  python engine_analyst.py sweep
  python engine_analyst.py sweep --strategy B
  python engine_analyst.py symbols --strategy B --min-trades 10
  python engine_analyst.py restore --strategy Z L G
  python engine_analyst.py deploy check --strategy B --param long_only --before False --after True
  python engine_analyst.py deploy check --strategy L --param short_only --before False --after True
  python engine_analyst.py deploy log --label "B long-only deployed" --strategies B
  python engine_analyst.py deploy history

All outputs are also saved to analysis_log.json (appended, never overwritten).
"""

import sys, os, csv, json, math, argparse, time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ── Colour helpers ─────────────────────────────────────────────────────────────
RESET  = '\033[0m'; BOLD = '\033[1m'; CYAN = '\033[96m'
GREEN  = '\033[92m'; RED  = '\033[91m'; DIM  = '\033[2m'; YELLOW = '\033[93m'

def _c(col, t): return f'{col}{t}{RESET}'
def _wr(v):    return _c(GREEN if v>=55 else (YELLOW if v>=45 else RED), f'{v:.1f}%')
def _net(v):   return _c(GREEN if v>=0 else RED, f'{v:+.4f}%')
def _netc(v):  return _c(GREEN if v>=0 else RED, f'{v:+.2f}%')

LOG_FILE = Path('analysis_log.json')

# ── Data loading ───────────────────────────────────────────────────────────────

def load_outcomes(path: str = 'signals_with_outcomes.csv',
                  strategies: list = None) -> list:
    rows = []
    try:
        with open(path, newline='', encoding='utf-8') as fh:
            for row in csv.DictReader(fh):
                net = row.get('net_pct', '').strip()
                if not net: continue
                try: net_pct = float(net)
                except ValueError: continue
                strat = row.get('strategy', '').strip()
                if strategies and strat not in strategies: continue
                win_s = row.get('win', '').strip()
                def _f(k):
                    v = row.get(k,'').strip()
                    try: return float(v) if v else None
                    except: return None
                rows.append({
                    'strategy':    strat,
                    'symbol':      row.get('symbol','').strip(),
                    'dir':         row.get('dir','').strip(),
                    'vpin':        _f('vpin'),
                    'conf':        _f('conf'),
                    'score':       _f('score'),
                    'net_pct':     net_pct,
                    'win':         int(win_s) if win_s in ('0','1') else None,
                    'exit_reason': row.get('exit_reason','').strip(),
                    'dur_sec':     _f('dur_sec'),
                    'ts_fired':    row.get('ts_fired','').strip(),
                })
    except FileNotFoundError:
        print(f'[ERROR] {path} not found. Run: python signal_outcome_joiner.py --backup-dir ./data_backup')
        sys.exit(1)
    return rows


def stats(rows: list) -> dict:
    if not rows: return {'n':0,'wins':0,'wr':0.0,'avg_net':0.0,'cum_net':0.0,'expect':0.0}
    n    = len(rows)
    wins = sum(1 for r in rows if r.get('win')==1)
    cum  = sum(r['net_pct'] for r in rows)
    return {'n':n,'wins':wins,'wr':wins/n*100,'avg_net':cum/n,'cum_net':cum,'expect':cum/n}


def ts_to_week(ts_str: str) -> str:
    """Convert ts_fired to ISO week string."""
    import re
    m = re.match(r'^(\d{8})_', ts_str)
    if m:
        try:
            dt = datetime.strptime(m.group(1), '%Y%m%d')
            return dt.strftime('%Y-W%W')
        except: pass
    return 'unknown'


# ── Verdict engine ─────────────────────────────────────────────────────────────

FEE_RT = 0.08   # % round-trip

def verdict(s: dict, n_min: int = 50) -> tuple:
    """
    Return (label, colour) verdict for a strategy's stats.
    PROMOTE  — profitable or very close, solid WR
    KEEP     — losing but best in class, worth running
    TUNE     — clear signal but wrong parameters
    DISABLE  — no edge visible
    RESTORE? — disabled but data shows potential
    """
    if s['n'] < n_min:
        return ('INSUFFICIENT DATA', DIM)
    avg = s['avg_net']
    wr  = s['wr']
    if avg >= 0:
        return ('PROMOTE ★', GREEN)
    if avg >= -FEE_RT * 0.5 and wr >= 52:
        return ('PROMOTE ★', GREEN)
    if avg >= -FEE_RT and wr >= 50:
        return ('KEEP ✓', CYAN)
    if wr >= 50:
        return ('TUNE ↑', YELLOW)
    if wr >= 45 and avg >= -FEE_RT * 1.5:
        return ('TUNE ↑', YELLOW)
    return ('DISABLE ✗', RED)


# ── COMMAND: report ────────────────────────────────────────────────────────────

def cmd_report(rows: list, strategies: list = None, min_trades: int = 20):
    strats = strategies or sorted(set(r['strategy'] for r in rows))

    print(_c(BOLD+CYAN, f'\n{"━"*72}'))
    print(_c(BOLD, '  STRATEGY REPORT CARD'))
    print(_c(CYAN, f'{"━"*72}'))
    print(f'  {"Strat":<6} {"n":>5} {"WR%":>7} {"avg_net":>9} {"cum_net":>9} '
          f'{"long_WR":>8} {"sht_WR":>7}  verdict')
    print(f'  {"─"*5}  {"─"*5}  {"─"*6}  {"─"*8}  {"─"*8}  {"─"*7}  {"─"*6}  {"─"*20}')

    report_data = []
    for label in strats:
        sr = [r for r in rows if r['strategy']==label]
        if len(sr) < min_trades: continue
        s  = stats(sr)
        ls = stats([r for r in sr if r['dir']=='long'])
        ss = stats([r for r in sr if r['dir']=='short'])
        vd, vc = verdict(s)
        dir_gap = abs(ls['avg_net'] - ss['avg_net'])
        dir_flag = _c(YELLOW,' ←dir') if dir_gap > 0.02 and ls['n']>=10 and ss['n']>=10 else ''
        print(f'  {label:<6} {s["n"]:>5,} {_wr(s["wr"])} {_net(s["avg_net"])} '
              f'{_netc(s["cum_net"])} '
              f'{"—":>8}' if ls['n']==0 else
              f'  {label:<6} {s["n"]:>5,} {_wr(s["wr"])} {_net(s["avg_net"])} '
              f'{_netc(s["cum_net"])} {_wr(ls["wr"])} {_wr(ss["wr"])}  '
              f'{_c(vc, vd)}{dir_flag}')
        report_data.append({'strategy':label,'stats':s,'verdict':vd})

    # Exit mix summary
    print(f'\n  {"Strat":<6} {"trail%":>7} {"sl%":>6} {"tp%":>5} {"time%":>6} {"inertia%":>9}')
    print(f'  {"─"*5}  {"─"*6}  {"─"*5}  {"─"*4}  {"─"*5}  {"─"*8}')
    for label in strats:
        sr = [r for r in rows if r['strategy']==label]
        if len(sr) < min_trades: continue
        exits = defaultdict(int)
        for r in sr: exits[r['exit_reason']] += 1
        tot = sum(exits.values()) or 1
        def _ep(k): return exits.get(k,0)/tot*100
        print(f'  {label:<6} {_ep("trail"):>6.0f}% {_ep("sl"):>5.0f}% '
              f'{_ep("tp"):>4.0f}% {_ep("time"):>5.0f}% {_ep("inertia"):>8.0f}%')

    # Week-over-week trend for key strategies
    live = [l for l in strats if l in ('B','W','CGY','K')]
    if live:
        print(f'\n  Week-over-week WR (key strategies):')
        weeks = sorted(set(ts_to_week(r['ts_fired']) for r in rows if r['ts_fired']))
        if len(weeks) > 1:
            print(f'  {"Strat":<6}  ' + '  '.join(f'{w:<10}' for w in weeks[-4:]))
            for label in live:
                sr = [r for r in rows if r['strategy']==label]
                if not sr: continue
                parts = []
                for w in weeks[-4:]:
                    wr = [r for r in sr if ts_to_week(r['ts_fired'])==w]
                    if len(wr) >= 5:
                        ws = stats(wr)
                        parts.append(f'{_wr(ws["wr"])}({ws["n"]})')
                    else:
                        parts.append(f'{"—":<10}')
                print(f'  {label:<6}  ' + '  '.join(parts))

    return report_data


# ── COMMAND: sweep ─────────────────────────────────────────────────────────────

VPIN_THRESHOLDS  = [0.25,0.30,0.35,0.40,0.45,0.50,0.55,0.60,0.65,0.70,0.75,0.80]
CONF_THRESHOLDS  = [20,30,40,50,55,60,65,70]
SCORE_THRESHOLDS = [0,5,10,15,20,25,30,40,50]


def sweep_field(rows: list, field: str, thresholds: list, min_trades: int=30) -> list:
    valid = [r for r in rows if r.get(field) is not None]
    results = []
    base_n = len([r for r in valid if r[field] >= thresholds[0]])
    base_s = stats([r for r in valid if r[field] >= thresholds[0]])
    for thr in thresholds:
        filtered = [r for r in valid if r[field] >= thr]
        if len(filtered) < min_trades: break
        s = stats(filtered)
        results.append({
            'threshold': thr,
            'n': s['n'],
            'removed': base_n - s['n'],
            'wr': s['wr'],
            'avg_net': s['avg_net'],
            'delta_wr': s['wr'] - base_s['wr'],
            'delta_avg': s['avg_net'] - base_s['avg_net'],
        })
    return results


def find_optimal(sweep_results: list) -> dict:
    """Find threshold that maximises avg_net with at least 30 trades."""
    if not sweep_results: return {}
    best = max(sweep_results, key=lambda x: x['avg_net'])
    return best


def cmd_sweep(rows: list, strategies: list = None, min_trades: int = 30):
    strats = strategies or sorted(set(r['strategy'] for r in rows))
    findings = []

    print(_c(BOLD+CYAN, f'\n{"━"*72}'))
    print(_c(BOLD, '  GATE SWEEP — optimal thresholds per strategy'))
    print(_c(CYAN, f'{"━"*72}'))

    for label in strats:
        sr = [r for r in rows if r['strategy']==label]
        if len(sr) < min_trades: continue

        current = stats(sr)
        print(f'\n  {_c(BOLD, label)}  (n={current["n"]:,}  WR={current["wr"]:.1f}%  '
              f'avg={current["avg_net"]:+.4f}%)')

        for field, thresholds in [('vpin', VPIN_THRESHOLDS),
                                   ('conf', CONF_THRESHOLDS),
                                   ('score', SCORE_THRESHOLDS)]:
            valid_count = sum(1 for r in sr if r.get(field) is not None)
            if valid_count < min_trades: continue

            results = sweep_field(sr, field, thresholds, min_trades)
            if not results: continue

            optimal = find_optimal(results)
            delta_avg = optimal['avg_net'] - current['avg_net']

            if abs(delta_avg) < 0.001:
                continue  # skip if no meaningful improvement possible

            col = GREEN if delta_avg > 0 else DIM
            print(f'  {field:>6}≥{optimal["threshold"]:<5.2f}  '
                  f'n={optimal["n"]:>4,}  {_wr(optimal["wr"])}  '
                  f'{_net(optimal["avg_net"])}  '
                  f'delta={_c(col, f"{delta_avg:+.4f}%")}  '
                  f'({optimal["removed"]:,} trades filtered)')

            findings.append({
                'strategy': label,
                'field': field,
                'current_avg': current['avg_net'],
                'optimal_threshold': optimal['threshold'],
                'optimal_avg': optimal['avg_net'],
                'delta_avg': delta_avg,
                'trades_filtered': optimal['removed'],
            })

    # Summary of actionable findings
    actionable = [f for f in findings if f['delta_avg'] > 0.002]
    if actionable:
        print(_c(BOLD+CYAN, f'\n  Actionable findings (delta > 0.002%/trade):'))
        for f in sorted(actionable, key=lambda x: -x['delta_avg']):
            _da = f"{f['delta_avg']:+.4f}%/trade"
            print(f'  {f["strategy"]} {f["field"]}≥{f["optimal_threshold"]:.2f}  '
                  f'{_c(GREEN, _da)}  '
                  f'({f["trades_filtered"]:,} trades filtered)')

    return findings


# ── COMMAND: symbols ───────────────────────────────────────────────────────────

def cmd_symbols(rows: list, strategies: list = None, min_trades: int = 10):
    strats = strategies or sorted(set(r['strategy'] for r in rows))

    print(_c(BOLD+CYAN, f'\n{"━"*72}'))
    print(_c(BOLD, '  PER-SYMBOL ANALYSIS'))
    print(_c(CYAN, f'{"━"*72}'))

    blacklist_candidates = []
    whitelist_candidates = []

    for label in strats:
        sr = [r for r in rows if r['strategy']==label]
        if not sr: continue

        by_sym = defaultdict(list)
        for r in sr:
            by_sym[r['symbol'].replace('USDT','')].append(r)

        sym_stats = [(sym, stats(trades))
                     for sym, trades in by_sym.items()
                     if len(trades) >= min_trades]
        if not sym_stats: continue

        sym_stats.sort(key=lambda x: x[1]['avg_net'])
        worst = sym_stats[:5]
        best  = sym_stats[-5:][::-1]

        overall = stats(sr)
        print(f'\n  {_c(BOLD, label)}  (avg={_net(overall["avg_net"])})')

        print(f'  {"worst":}  →  ', end='')
        for sym, s in worst:
            longs  = stats([r for r in by_sym[sym] if r['dir']=='long'])
            shorts = stats([r for r in by_sym[sym] if r['dir']=='short'])
            both_neg = longs['avg_net'] < 0 and shorts['avg_net'] < 0 and \
                       longs['n'] >= 3 and shorts['n'] >= 3
            flag = _c(RED,' ✗BLACKLIST') if both_neg and s['avg_net'] < -0.10 else ''
            print(f'{sym}:{_net(s["avg_net"])}({s["n"]}T){flag}  ', end='')
            if both_neg and s['avg_net'] < -0.10:
                blacklist_candidates.append({'strategy':label,'symbol':sym+'USDT',
                                              'avg_net':s['avg_net'],'n':s['n']})
        print()
        print(f'  {"best":}   →  ', end='')
        for sym, s in best:
            flag = _c(GREEN,' ★WHITELIST') if s['avg_net'] > 0.05 and s['n'] >= 15 else ''
            print(f'{sym}:{_net(s["avg_net"])}({s["n"]}T){flag}  ', end='')
            if s['avg_net'] > 0.05 and s['n'] >= 15:
                whitelist_candidates.append({'strategy':label,'symbol':sym+'USDT',
                                              'avg_net':s['avg_net'],'n':s['n']})
        print()

    if blacklist_candidates:
        print(_c(BOLD+RED, '\n  Blacklist candidates (both dirs negative, avg<-0.10%):'))
        for c in sorted(blacklist_candidates, key=lambda x: x['avg_net']):
            print(f'  {c["strategy"]} {c["symbol"]:<20} {_net(c["avg_net"])} ({c["n"]}T)')

    if whitelist_candidates:
        print(_c(BOLD+GREEN, '\n  Consistent winners (avg>+0.05%, n>=15):'))
        for c in sorted(whitelist_candidates, key=lambda x: -x['avg_net']):
            print(f'  {c["strategy"]} {c["symbol"]:<20} {_net(c["avg_net"])} ({c["n"]}T)')

    return {'blacklist': blacklist_candidates, 'whitelist': whitelist_candidates}


# ── COMMAND: restore ───────────────────────────────────────────────────────────

RESTORE_TARGETS = {
    'Z': {'note': 'longs 60.5%WR — try long_only',
          'sweeps': [('vpin', VPIN_THRESHOLDS)],
          'direction_test': True},
    'L': {'note': 'shorts near breakeven — try short_only',
          'sweeps': [('vpin', VPIN_THRESHOLDS)],
          'direction_test': True},
    'G': {'note': 'improving at vpin>=0.70',
          'sweeps': [('vpin', VPIN_THRESHOLDS)],
          'direction_test': True},
    'Q': {'note': 'funding fade — check if profitable at vpin>=0.60',
          'sweeps': [('vpin', VPIN_THRESHOLDS)],
          'direction_test': False},
    'K': {'note': 'too many trades — raise vpin to reduce volume',
          'sweeps': [('vpin', VPIN_THRESHOLDS)],
          'direction_test': False},
    'E': {'note': 'EMA cross — too few trades, no action yet',
          'sweeps': [],
          'direction_test': True},
}


def cmd_restore(rows: list, strategies: list = None, min_trades: int = 30):
    targets = strategies or list(RESTORE_TARGETS.keys())

    print(_c(BOLD+CYAN, f'\n{"━"*72}'))
    print(_c(BOLD, '  STRATEGY RESTORATION ANALYSIS'))
    print(_c(CYAN, f'{"━"*72}'))
    print(_c(DIM, '  Testing disabled/sim strategies for promotion potential\n'))

    promotions = []

    for label in targets:
        sr = [r for r in rows if r['strategy']==label]
        if not sr:
            print(f'  {label}: no data')
            continue

        s = stats(sr)
        info = RESTORE_TARGETS.get(label, {})
        note = info.get('note', '')

        print(f'  {_c(BOLD, label)}  n={s["n"]:,}  WR={_wr(s["wr"])}  '
              f'avg={_net(s["avg_net"])}  {_c(DIM, note)}')

        best_config = {'avg_net': s['avg_net'], 'wr': s['wr'], 'n': s['n'],
                       'params': 'current'}

        # Direction test
        if info.get('direction_test') and len(sr) >= min_trades:
            longs  = [r for r in sr if r['dir']=='long']
            shorts = [r for r in sr if r['dir']=='short']
            ls = stats(longs); ss = stats(shorts)
            if ls['n'] >= min_trades and ss['n'] >= min_trades:
                better_dir = 'long_only' if ls['avg_net'] > ss['avg_net'] else 'short_only'
                better_s   = ls if ls['avg_net'] > ss['avg_net'] else ss
                dir_delta  = abs(ls['avg_net'] - ss['avg_net'])
                if dir_delta > 0.02:
                    col = GREEN if better_s['avg_net'] > s['avg_net'] else YELLOW
                    print(f'    {_c(col, better_dir)}: WR={better_s["wr"]:.1f}% '
                          f'avg={better_s["avg_net"]:+.4f}%  '
                          f'(delta {dir_delta:+.4f}%/trade)')
                    if better_s['avg_net'] > best_config['avg_net']:
                        best_config = {'avg_net': better_s['avg_net'],
                                       'wr': better_s['wr'],
                                       'n': better_s['n'],
                                       'params': better_dir}

        # Gate sweep for optimal threshold
        for field, thresholds in info.get('sweeps', []):
            valid = [r for r in sr if r.get(field) is not None]
            if len(valid) < min_trades: continue
            results = sweep_field(valid, field, thresholds, min_trades)
            optimal = find_optimal(results)
            if not optimal: continue
            delta = optimal['avg_net'] - s['avg_net']
            if abs(delta) >= 0.001:
                col = GREEN if delta > 0 else DIM
                print(f'    {field}≥{optimal["threshold"]:.2f}: '
                      f'WR={optimal["wr"]:.1f}% avg={_net(optimal["avg_net"])}  '
                      f'delta={_c(col, f"{delta:+.4f}%")}  '
                      f'n={optimal["n"]:,}')
                if optimal['avg_net'] > best_config['avg_net']:
                    best_config = {'avg_net': optimal['avg_net'],
                                   'wr': optimal['wr'],
                                   'n': optimal['n'],
                                   'params': f'{field}>={optimal["threshold"]}'}

        # Overall verdict
        vd, vc = verdict(best_config, n_min=min_trades)
        gap = best_config['avg_net'] - s['avg_net']
        print(f'    best config [{best_config["params"]}]: '
              f'WR={best_config["wr"]:.1f}% avg={best_config["avg_net"]:+.4f}%  '
              f'{_c(vc, vd)}')

        if best_config['avg_net'] > -FEE_RT:
            promotions.append({'strategy': label, 'best_config': best_config,
                                'verdict': vd})
        print()

    if promotions:
        print(_c(BOLD+GREEN, '  Strategies worth promoting:'))
        for p in promotions:
            print(f'  {p["strategy"]}: {p["best_config"]["params"]}  '
                  f'avg={p["best_config"]["avg_net"]:+.4f}%  {p["verdict"]}')

    return promotions


# ── COMMAND: deploy ────────────────────────────────────────────────────────────

def load_deploy_log() -> list:
    if LOG_FILE.exists():
        try:
            with open(LOG_FILE) as f:
                return json.load(f)
        except Exception:
            return []
    return []


def save_deploy_log(entries: list):
    with open(LOG_FILE, 'w') as f:
        json.dump(entries, f, indent=2)


def cmd_deploy_check(rows: list, strategy: str, param: str,
                     before, after, min_trades: int = 30) -> dict:
    """Compare before/after stats for a specific config change."""
    print(_c(BOLD+CYAN, f'\n{"━"*64}'))
    print(_c(BOLD, f'  PRE-DEPLOY CHECK  {strategy}: {param} {before!r} → {after!r}'))
    print(_c(CYAN, f'{"━"*64}'))

    sr = [r for r in rows if r['strategy']==strategy]
    if len(sr) < min_trades:
        print(f'  {_c(YELLOW, f"SKIP — only {len(sr)} trades for {strategy}")}')
        return {'verdict': 'SKIP', 'reason': 'insufficient data'}

    # Determine what filter to apply
    before_rows = sr
    after_rows  = sr

    # Boolean direction filters
    if param in ('long_only', 'short_only'):
        after_bool = str(after).lower() in ('true','1','yes')
        if param == 'long_only' and after_bool:
            after_rows = [r for r in sr if r['dir']=='long']
        elif param == 'short_only' and after_bool:
            after_rows = [r for r in sr if r['dir']=='short']

    # Numeric gate filters
    elif param in ('vpin_min', 'vpin'):
        try:
            b_val = float(before); a_val = float(after)
            before_rows = [r for r in sr if r.get('vpin') is not None and r['vpin'] >= b_val]
            after_rows  = [r for r in sr if r.get('vpin') is not None and r['vpin'] >= a_val]
        except (TypeError, ValueError): pass

    elif param in ('min_conf', 'conf'):
        try:
            b_val = float(before); a_val = float(after)
            before_rows = [r for r in sr if r.get('conf') is not None and r['conf'] >= b_val]
            after_rows  = [r for r in sr if r.get('conf') is not None and r['conf'] >= a_val]
        except (TypeError, ValueError): pass

    sb = stats(before_rows)
    sa = stats(after_rows)
    delta_avg = sa['avg_net'] - sb['avg_net']
    delta_wr  = sa['wr']      - sb['wr']
    filtered  = sb['n'] - sa['n']

    # Print comparison
    print(f'  {"":22} {"before":>12}  {"after":>12}  {"delta":>10}')
    print(f'  {"─"*22}  {"─"*12}  {"─"*12}  {"─"*10}')

    def _row(name, va, vb, fmt, better='high'):
        d = vb - va
        col = (GREEN if d > 0 else (RED if d < -0.0001 else DIM)) if better=='high' \
              else (GREEN if d < 0 else (RED if d > 0.0001 else DIM))
        print(f'  {name:<22}  {fmt(va):>12}  {fmt(vb):>12}  {_c(col, fmt(d)):>10}')

    _row('n trades',   float(sb['n']), float(sa['n']), lambda v: f'{int(v):,}', 'neutral')
    _row('WR%',        sb['wr'],       sa['wr'],       lambda v: f'{v:.1f}%')
    _row('avg_net',    sb['avg_net'],  sa['avg_net'],  lambda v: f'{v:+.5f}%')
    _row('cum_net',    sb['cum_net'],  sa['cum_net'],  lambda v: f'{v:+.3f}%')
    print(f'  trades filtered: {filtered:,}  ({filtered/max(sb["n"],1)*100:.0f}% removed)')

    # Direction breakdown if applicable
    if param not in ('long_only','short_only'):
        for d in ('long','short'):
            ab = stats([r for r in before_rows if r['dir']==d])
            aa = stats([r for r in after_rows  if r['dir']==d])
            if ab['n'] >= 10:
                dd = aa['avg_net'] - ab['avg_net']
                col = GREEN if dd > 0 else (RED if dd < -0.001 else DIM)
                print(f'  {d:<6} {ab["avg_net"]:+.5f}% → {aa["avg_net"]:+.5f}%  '
                      f'({_c(col, f"{dd:+.5f}%")})')

    # Verdict
    if sa['n'] < min_trades:
        v_label = 'HOLD — too few trades after filter'
        v_col   = RED
    elif delta_avg > 0.005 and delta_wr > 0:
        v_label = f'DEPLOY ★ — avg +{delta_avg:+.4f}%/trade, WR +{delta_wr:.1f}pp'
        v_col   = GREEN
    elif delta_avg > 0 and delta_wr >= -1:
        v_label = f'DEPLOY ✓ — marginal improvement ({delta_avg:+.5f}%/trade)'
        v_col   = CYAN
    elif delta_avg < -0.005:
        v_label = f'HOLD ✗ — avg {delta_avg:+.4f}%/trade (worse)'
        v_col   = RED
    elif abs(delta_avg) < 0.002:
        v_label = 'NEUTRAL — change has no meaningful effect'
        v_col   = DIM
    else:
        v_label = f'CAUTION — small delta ({delta_avg:+.5f}%/trade), review'
        v_col   = YELLOW

    print(f'\n  {_c(BOLD+v_col, v_label)}')

    return {
        'strategy': strategy, 'param': param,
        'before': str(before), 'after': str(after),
        'before_stats': sb, 'after_stats': sa,
        'delta_avg': delta_avg, 'delta_wr': delta_wr,
        'trades_filtered': filtered,
        'verdict': v_label,
    }


def cmd_deploy_log(rows: list, label: str, strategies: list,
                   notes: str = ''):
    """Record a deploy snapshot with current stats."""
    entries = load_deploy_log()
    ts      = datetime.now(tz=timezone.utc).isoformat()

    snap = {}
    for s in strategies:
        sr = [r for r in rows if r['strategy']==s]
        if sr:
            snap[s] = stats(sr)

    entry = {
        'ts':         ts,
        'label':      label,
        'strategies': strategies,
        'stats':      snap,
        'notes':      notes,
    }
    entries.append(entry)
    save_deploy_log(entries)

    print(_c(BOLD+CYAN, f'\n  Deploy logged: {label}'))
    for s, st in snap.items():
        print(f'  {s}: n={st["n"]:,}  WR={st["wr"]:.1f}%  avg={st["avg_net"]:+.4f}%')
    print(f'  Saved to {LOG_FILE}')

    return entry


def cmd_deploy_history(rows: list):
    """Show all logged deploys with before/after comparisons."""
    entries = load_deploy_log()
    if not entries:
        print(f'  No deploy history found in {LOG_FILE}')
        return

    print(_c(BOLD+CYAN, f'\n{"━"*72}'))
    print(_c(BOLD, f'  DEPLOY HISTORY  ({len(entries)} entries)'))
    print(_c(CYAN, f'{"━"*72}'))

    for i, e in enumerate(entries):
        dt  = e.get('ts','')[:16].replace('T',' ')
        lbl = e.get('label','?')
        print(f'\n  [{i+1}] {dt}  {_c(BOLD, lbl)}')
        for s, st in e.get('stats', {}).items():
            # Compare to current data
            current = stats([r for r in rows if r['strategy']==s])
            if current['n'] > 0:
                d = current['avg_net'] - st['avg_net']
                col = GREEN if d > 0 else (RED if d < -0.001 else DIM)
                print(f'  {s}: at-log WR={st["wr"]:.1f}% avg={st["avg_net"]:+.4f}%  '
                      f'→  now WR={current["wr"]:.1f}% avg={current["avg_net"]:+.4f}%  '
                      f'({_c(col, f"{d:+.4f}%")})')
            else:
                print(f'  {s}: WR={st["wr"]:.1f}% avg={st["avg_net"]:+.4f}%  (no current data)')
        if e.get('notes'):
            print(f'  notes: {_c(DIM, e["notes"])}')


# ── Logging helper ────────────────────────────────────────────────────────────

def _append_log(command: str, data: object):
    """Append any analysis result to analysis_log.json."""
    entries = load_deploy_log()
    def _clean(obj):
        if isinstance(obj, dict):  return {k: _clean(v) for k, v in obj.items()}
        if isinstance(obj, list):  return [_clean(v) for v in obj]
        if isinstance(obj, float): return round(obj, 6)
        return obj
    entries.append({
        'ts': datetime.now(tz=timezone.utc).isoformat(),
        'command': command,
        'data': _clean(data),
    })
    save_deploy_log(entries)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        prog='engine_analyst.py',
        description='PredictEngine unified analyst',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  report                     Full strategy report card
  sweep                      Auto gate sweep — optimal thresholds
  symbols                    Per-symbol analysis — blacklist/whitelist
  restore                    Test disabled strategies
  deploy check               Pre-deploy validation
  deploy log                 Record current stats snapshot
  deploy history             Show all past deploys

Examples:
  python engine_analyst.py report
  python engine_analyst.py report --strategy B W
  python engine_analyst.py sweep --strategy B
  python engine_analyst.py symbols --strategy B --min-trades 8
  python engine_analyst.py restore --strategy Z L G
  python engine_analyst.py deploy check --strategy B --param long_only --before False --after True
  python engine_analyst.py deploy check --strategy L --param short_only --before False --after True
  python engine_analyst.py deploy log --label "B long-only v1" --strategies B W
  python engine_analyst.py deploy history
        """
    )
    parser.add_argument('command', choices=['report','sweep','symbols','restore','deploy'],
                        help='Command: report / sweep / symbols / restore / deploy')
    parser.add_argument('subcommand', nargs='?', default='',
                        help='Sub-command for deploy: check / log / history')
    parser.add_argument('--strategy',    nargs='+', default=None)
    parser.add_argument('--param',       default=None)
    parser.add_argument('--before',      default=None)
    parser.add_argument('--after',       default=None)
    parser.add_argument('--label',       default=None)
    parser.add_argument('--notes',       default='')
    parser.add_argument('--strategies',  nargs='+', default=None,
                        help='Alias for --strategy (used with deploy log)')
    parser.add_argument('--min-trades',  type=int, default=30)
    parser.add_argument('--outcomes',    default='signals_with_outcomes.csv')
    parser.add_argument('--log-file',    default='analysis_log.json')
    parser.add_argument('--no-log',      action='store_true',
                        help="Don't write to analysis_log.json")
    args = parser.parse_args()

    global LOG_FILE
    LOG_FILE = Path(args.log_file)

    cmd  = args.command.lower()
    sub  = (args.subcommand or '').lower()
    strats = args.strategy or args.strategies

    print(_c(BOLD+CYAN, '\nPredictEngine — Analyst'))
    print(_c(DIM, f'  {datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}'))

    # Load data
    rows = load_outcomes(args.outcomes, strategies=strats)
    print(_c(DIM, f'  {len(rows):,} trades loaded'))

    result = None

    if cmd == 'report':
        result = cmd_report(rows, strats, args.min_trades)

    elif cmd == 'sweep':
        result = cmd_sweep(rows, strats, args.min_trades)

    elif cmd == 'symbols':
        result = cmd_symbols(rows, strats, args.min_trades)

    elif cmd == 'restore':
        targets = strats or list(RESTORE_TARGETS.keys())
        result  = cmd_restore(rows, targets, args.min_trades)

    elif cmd == 'deploy':
        if sub == 'check':
            if not all([args.strategy, args.param, args.before is not None,
                        args.after is not None]):
                print('[ERROR] deploy check requires --strategy --param --before --after')
                sys.exit(1)
            result = cmd_deploy_check(rows, args.strategy[0], args.param,
                                      args.before, args.after, args.min_trades)
        elif sub == 'log':
            if not (args.label and (strats)):
                print('[ERROR] deploy log requires --label and --strategies')
                sys.exit(1)
            result = cmd_deploy_log(rows, args.label, strats, args.notes)
        elif sub == 'history':
            cmd_deploy_history(rows)
        else:
            print(f'[ERROR] Unknown deploy sub-command: {sub!r}')
            print('  Use: deploy check / deploy log / deploy history')
            sys.exit(1)
    else:
        print(f'[ERROR] Unknown command: {cmd!r}')
        parser.print_help()
        sys.exit(1)

    # Persist to log
    if result is not None and not args.no_log:
        _append_log(f'{cmd} {sub}'.strip(), result)

    print()


if __name__ == '__main__':
    main()
