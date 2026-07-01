"""
PredictEngine — Signal Replay / Gate Sweep
==========================================
Reads signals_with_outcomes.csv (from signal_outcome_joiner.py) and
answers gate-tuning questions against real historical outcomes.

This is NOT a backtest — it replays already-fired trades through
hypothetical gate filters to measure what WR/net would have been
if stricter (or looser) gates had been active at fire time.

Usage:
  # Generate the input file first:
  python signal_outcome_joiner.py --backup-dir ./data_backup

  # VPIN sweep — find optimal vpin gate per strategy:
  python signal_replay.py --sweep vpin

  # Full gate sweep across all numeric fields:
  python signal_replay.py --sweep all

  # Compare two specific gate configs:
  python signal_replay.py --strategy B --filter "vpin>=0.55" --compare "vpin>=0.65"

  # Chained gates (AND):
  python signal_replay.py --strategy CGYL --filter "vpin>=0.0" --compare "vpin>=0.70 AND conf>=55"

  # WR by direction per strategy:
  python signal_replay.py --by direction

  # WR by exit reason:
  python signal_replay.py --by exit_reason

  # WR by hour-of-day (market timing):
  python signal_replay.py --by hour

  # Deep dive on one strategy:
  python signal_replay.py --strategy B --sweep vpin --by direction exit_reason

  # Minimum trades threshold (default 30):
  python signal_replay.py --sweep vpin --min-trades 50

Key principle:
  gate_sweep(vpin>=0.65) answers: "of the trades that fired with vpin>=0.65,
  what was the WR and avg net?" — it does NOT simulate whether MORE trades
  would have fired at looser gates (those trades are already in the data).
  It CAN answer whether FEWER trades at a tighter gate would have better outcomes.

Columns used from signals_with_outcomes.csv:
  ts_fired, strategy, symbol, dir, vpin, atr, conf, score,
  entry_price, tp_pct, sl_pct, ts_closed, exit_reason, net_pct, dur_sec, win
"""

import csv
import sys
import argparse
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
_HERE = Path(__file__).parent  # tools/analysis/
from typing import Optional


# ── Colours ───────────────────────────────────────────────────────────────────

RESET  = '\033[0m'
GREEN  = '\033[92m'
RED    = '\033[91m'
CYAN   = '\033[96m'
BOLD   = '\033[1m'
DIM    = '\033[2m'
YELLOW = '\033[93m'

def _c(col, txt): return f'{col}{txt}{RESET}'
def _wr(v):  return _c(GREEN if v >= 50 else (YELLOW if v >= 40 else RED), f'{v:.1f}%')
def _net(v): return _c(GREEN if v >= 0 else RED, f'{v:+.4f}%')
def _exp(v): return _c(GREEN if v >= 0 else RED, f'{v:+.5f}%')


# ── Data loading ──────────────────────────────────────────────────────────────

def load(path: str, strategy_filter: Optional[list] = None) -> list:
    """Load signals_with_outcomes.csv into list of dicts with typed fields."""
    rows = []
    try:
        with open(path, newline='', encoding='utf-8') as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                # Skip unmatched rows (no net_pct)
                net_str = row.get('net_pct', '').strip()
                if not net_str:
                    continue
                try:
                    net_pct = float(net_str)
                except ValueError:
                    continue

                strategy = row.get('strategy', '').strip()
                if strategy_filter and strategy not in strategy_filter:
                    continue

                win_str = row.get('win', '').strip()
                win = int(win_str) if win_str in ('0', '1') else None

                def _f(k):
                    v = row.get(k, '').strip()
                    try: return float(v) if v else None
                    except ValueError: return None

                # Parse hour from ts_fired
                ts_raw = row.get('ts_fired', '').strip()
                hour = None
                m = re.match(r'^(\d{8})_(\d{2})(\d{2})(\d{2})$', ts_raw)
                if m:
                    hour = int(m.group(2))
                else:
                    m2 = re.match(r'^(\d{2}):(\d{2}):(\d{2})$', ts_raw)
                    if m2:
                        hour = int(m2.group(1))

                rows.append({
                    'ts_fired':    ts_raw,
                    'strategy':    strategy,
                    'symbol':      row.get('symbol', '').strip(),
                    'dir':         row.get('dir', '').strip(),
                    'vpin':        _f('vpin'),
                    'atr':         _f('atr'),
                    'conf':        _f('conf'),
                    'score':       _f('score'),
                    'entry_price': _f('entry_price'),
                    'tp_pct':      _f('tp_pct'),
                    'sl_pct':      _f('sl_pct'),
                    'exit_reason': row.get('exit_reason', '').strip(),
                    'net_pct':     net_pct,
                    'dur_sec':     _f('dur_sec'),
                    'win':         win,
                    'hour':        hour,
                    'match_type':  row.get('match_type', '').strip(),
                })
    except FileNotFoundError:
        print(f'[ERROR] File not found: {path}', file=sys.stderr)
        print('  Run: python signal_outcome_joiner.py --backup-dir ./data_backup', file=sys.stderr)
        sys.exit(1)

    return rows


# ── Gate filter parser ────────────────────────────────────────────────────────

def _parse_single(expr: str):
    """Parse one 'field OP value' condition into a callable(row)->bool."""
    m = re.match(r'^(\w+)\s*(>=|<=|>|<|==|!=)\s*(.+)$', expr.strip())
    if not m:
        raise ValueError(f'Cannot parse filter: {expr!r}. Use: field>=value')
    field, op, val_str = m.group(1), m.group(2), m.group(3).strip()
    # Try numeric, fall back to string
    try:
        val = float(val_str)
        numeric = True
    except ValueError:
        val = val_str
        numeric = False

    def _apply(row):
        rv = row.get(field)
        if rv is None:
            return False
        if numeric:
            try:
                rv = float(rv)
            except (TypeError, ValueError):
                return False
        if op == '>=': return rv >= val
        if op == '<=': return rv <= val
        if op == '>':  return rv >  val
        if op == '<':  return rv <  val
        if op == '==': return str(rv) == str(val) or rv == val
        if op == '!=': return str(rv) != str(val) and rv != val
        return False

    return _apply


def parse_filter(expr: str):
    """
    Parse a filter expression into a callable. Multiple conditions may be
    chained with 'AND' (or '&&') — a row must satisfy all of them.
    Supported ops per condition: >=, <=, >, <, ==, !=
    Examples: "vpin>=0.55", "conf>=50", "vpin>=0.70 AND conf>=55", "dir==long"

    Returns (callable, expr, 'and', None) — callers use only the callable.
    """
    parts = re.split(r'\s+(?:AND|&&)\s+', expr.strip(), flags=re.IGNORECASE)
    conds = [_parse_single(p) for p in parts if p.strip()]
    if not conds:
        raise ValueError(f'Cannot parse filter: {expr!r}')

    def _apply(row):
        return all(c(row) for c in conds)

    return _apply, expr, 'and', None


# ── Core stats ────────────────────────────────────────────────────────────────

def stats(rows: list) -> dict:
    """Compute WR, avg_net, expect, cumnet, n from a list of trade rows."""
    if not rows:
        return {'n': 0, 'wins': 0, 'wr': 0.0, 'avg_net': 0.0, 'expect': 0.0, 'cum_net': 0.0}
    wins    = sum(1 for r in rows if r['win'] == 1)
    n       = len(rows)
    cum_net = sum(r['net_pct'] for r in rows)
    avg_net = cum_net / n
    wr      = wins / n * 100
    expect  = avg_net  # same thing, named differently for clarity
    return {'n': n, 'wins': wins, 'wr': wr, 'avg_net': avg_net, 'expect': expect, 'cum_net': cum_net}


# ── Breakdown table ───────────────────────────────────────────────────────────

def breakdown(rows: list, key_fn, key_label: str, min_trades: int = 10,
              sort_by: str = 'key') -> list:
    """
    Group rows by key_fn(row), compute stats per group.
    Returns list of (key, stats_dict) sorted by sort_by.
    """
    groups = defaultdict(list)
    for r in rows:
        k = key_fn(r)
        if k is not None:
            groups[k].append(r)

    results = []
    for k, grp in groups.items():
        if len(grp) < min_trades:
            continue
        s = stats(grp)
        s['key'] = k
        results.append(s)

    if sort_by == 'wr':
        results.sort(key=lambda x: -x['wr'])
    elif sort_by == 'expect':
        results.sort(key=lambda x: -x['expect'])
    elif sort_by == 'n':
        results.sort(key=lambda x: -x['n'])
    else:
        # Try numeric sort, fall back to string
        try:
            results.sort(key=lambda x: float(x['key']))
        except (ValueError, TypeError):
            results.sort(key=lambda x: str(x['key']))

    return results


# ── VPIN bucket sweep ──────────────────────────────────────────────────────────

VPIN_THRESHOLDS = [0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80]

def vpin_sweep(rows: list, min_trades: int = 30) -> list:
    """
    For each vpin threshold T, compute stats for rows where vpin >= T.
    Returns list of (threshold, stats) showing cumulative effect of tightening gate.
    """
    results = []
    for thr in VPIN_THRESHOLDS:
        filtered = [r for r in rows if r['vpin'] is not None and r['vpin'] >= thr]
        if len(filtered) < min_trades:
            break
        s = stats(filtered)
        s['threshold'] = thr
        results.append(s)
    return results


# ── Generic numeric threshold sweep ──────────────────────────────────────────

def threshold_sweep(rows: list, field: str, thresholds: list,
                    direction: str = 'gte', min_trades: int = 30) -> list:
    """
    Sweep a numeric field threshold. direction='gte' means field>=thr,
    direction='lte' means field<=thr.
    """
    results = []
    valid = [r for r in rows if r.get(field) is not None]
    for thr in thresholds:
        if direction == 'gte':
            filtered = [r for r in valid if r[field] >= thr]
        else:
            filtered = [r for r in valid if r[field] <= thr]
        if len(filtered) < min_trades:
            continue
        s = stats(filtered)
        s['threshold'] = thr
        s['field']     = field
        s['direction'] = direction
        results.append(s)
    return results


# ── Printers ──────────────────────────────────────────────────────────────────

def print_header(title: str):
    print(_c(BOLD + CYAN, f'\n{"━"*64}'))
    print(_c(BOLD, f'  {title}'))
    print(_c(CYAN, f'{"━"*64}'))


def print_strategy_summary(rows: list, label: str = 'all'):
    s = stats(rows)
    if s['n'] == 0:
        print(f'  {label}: no data')
        return
    print(f'  {_c(BOLD, label):<18} '
          f'n={s["n"]:>5,}  '
          f'WR={_wr(s["wr"])}  '
          f'avg={_net(s["avg_net"])}  '
          f'cum={_net(s["cum_net"])}')


def print_breakdown_table(results: list, key_label: str, title: str = ''):
    if title:
        print(f'\n  {_c(BOLD, title)}')
    print(f'  {key_label:<14} {"n":>6} {"WR%":>7} {"avg_net":>9} {"cum_net":>10} {"expect":>9}')
    print(f'  {"─"*13}  {"─"*6}  {"─"*6}  {"─"*8}  {"─"*9}  {"─"*8}')
    for s in results:
        print(f'  {str(s["key"]):<14} {s["n"]:>6,} {_wr(s["wr"])} {_net(s["avg_net"])} {_net(s["cum_net"])} {_exp(s["expect"])}')


def print_threshold_sweep(results: list, field: str, direction: str = 'gte'):
    op = '>=' if direction == 'gte' else '<='
    print(f'\n  {_c(BOLD, f"{field} {op} threshold")}')
    print(f'  {"threshold":<12} {"n":>6} {"removed":>8} {"WR%":>7} {"avg_net":>9} {"delta_wr":>9} {"delta_avg":>10}')
    print(f'  {"─"*11}  {"─"*6}  {"─"*7}  {"─"*6}  {"─"*8}  {"─"*8}  {"─"*9}')
    base_n  = results[0]['n']  if results else 0
    base_wr = results[0]['wr'] if results else 0
    base_avg= results[0]['avg_net'] if results else 0
    for s in results:
        removed  = base_n - s['n']
        delta_wr = s['wr']      - base_wr
        delta_avg= s['avg_net'] - base_avg
        dwr_col  = GREEN if delta_wr > 0 else (DIM if delta_wr == 0 else RED)
        davg_col = GREEN if delta_avg > 0 else (DIM if delta_avg == 0 else RED)
        print(f'  {s["threshold"]:<12.2f} {s["n"]:>6,} {removed:>+8,} '
              f'{_wr(s["wr"])} {_net(s["avg_net"])} '
              f'{_c(dwr_col, f"{delta_wr:>+8.1f}pp")} '
              f'{_c(davg_col, f"{delta_avg:>+9.4f}%")}')


def print_compare(rows_base: list, rows_var: list, label_a: str, label_b: str):
    sa = stats(rows_base)
    sb = stats(rows_var)
    print(f'\n  {"":20} {label_a:>12}  {label_b:>12}  {"delta":>10}')
    print(f'  {"─"*20}  {"─"*11}  {"─"*11}  {"─"*10}')

    def _row(name, va, vb, fmt, better='high'):
        d = vb - va
        if better == 'high':
            col = GREEN if d > 0.001 else (RED if d < -0.001 else DIM)
        else:
            col = GREEN if d < -0.001 else (RED if d > 0.001 else DIM)
        print(f'  {name:<20}  {fmt(va):>12}  {fmt(vb):>12}  {_c(col, fmt(d)):>10}')

    _row('n',       sa['n'],       sb['n'],       lambda v: f'{v:,.0f}',  'neutral')
    _row('WR%',     sa['wr'],      sb['wr'],       lambda v: f'{v:.1f}%')
    _row('avg_net', sa['avg_net'], sb['avg_net'],  lambda v: f'{v:+.4f}%')
    _row('cum_net', sa['cum_net'], sb['cum_net'],  lambda v: f'{v:+.3f}%')
    _row('expect',  sa['expect'],  sb['expect'],   lambda v: f'{v:+.5f}%')
    removed = sa['n'] - sb['n']
    kept_pct = sb['n'] / max(sa['n'], 1) * 100
    print(f'\n  Trades removed by tighter gate: {removed:,} ({100-kept_pct:.1f}% filtered)')


# ── Main sweep modes ──────────────────────────────────────────────────────────

def run_vpin_sweep(rows: list, strategies: list, min_trades: int):
    print_header('VPIN Gate Sweep — effect of raising vpin threshold')
    print(_c(DIM, '  Rows where vpin >= threshold (tightening removes low-quality signals)\n'))

    for label in strategies:
        strat_rows = [r for r in rows if r['strategy'] == label]
        vpin_rows  = [r for r in strat_rows if r['vpin'] is not None]
        if len(vpin_rows) < min_trades:
            continue
        results = vpin_sweep(vpin_rows, min_trades=min_trades)
        if not results:
            continue
        print(f'\n  {_c(BOLD, label)}  ({len(strat_rows):,} total trades, {len(vpin_rows):,} with vpin)')
        print_threshold_sweep(results, 'vpin', 'gte')


def run_breakdown(rows: list, strategies: list, by_fields: list, min_trades: int):
    for field in by_fields:
        print_header(f'Breakdown by {field}')

        # Grouping by strategy is global, not per-strategy — emit one combined
        # table across all rows (optionally restricted to --strategy filter).
        if field == 'strategy' or field == 'strat':
            scope = rows if not strategies else [r for r in rows if r['strategy'] in strategies]
            results = breakdown(scope, lambda r: r['strategy'], 'strategy',
                                min_trades=min_trades, sort_by='n')
            if results:
                print_breakdown_table(results, 'strategy')
            continue

        if field == 'direction' or field == 'dir':
            key_fn = lambda r: r['dir'] or 'unknown'
            label  = 'direction'
        elif field == 'exit_reason':
            key_fn = lambda r: r['exit_reason'] or 'unknown'
            label  = 'exit_reason'
        elif field == 'hour':
            key_fn = lambda r: r['hour']
            label  = 'hour (UTC)'
        elif field == 'symbol':
            key_fn = lambda r: r['symbol']
            label  = 'symbol'
        elif field == 'symbol_dir':
            key_fn = lambda r: f"{r['symbol']} {r['dir']}" if r['dir'] else r['symbol']
            label  = 'symbol+dir'
        elif field == 'vpin_bucket':
            def key_fn(r):
                v = r['vpin']
                if v is None: return None
                return f'{int(v*10)/10:.1f}–{int(v*10)/10+0.1:.1f}'
            label = 'vpin bucket'
        else:
            print(f'  Unknown breakdown field: {field}', file=sys.stderr)
            continue

        for label_s in strategies:
            strat_rows = [r for r in rows if r['strategy'] == label_s]
            if len(strat_rows) < min_trades:
                continue
            results = breakdown(strat_rows, key_fn, label, min_trades=min_trades//2)
            if results:
                print_breakdown_table(results, label, title=label_s)


def run_sweep_all(rows: list, strategies: list, min_trades: int):
    """Sweep vpin, conf, score for all strategies with enough data."""
    fields = {
        'vpin':  ([0.25,0.30,0.35,0.40,0.45,0.50,0.55,0.60,0.65,0.70,0.75,0.80], 'gte'),
        'conf':  ([20,30,40,50,55,60,65,70,75,80], 'gte'),
        'score': ([0,5,10,15,20,25,30,40,50], 'gte'),
    }
    for field, (thresholds, direction) in fields.items():
        print_header(f'{field.upper()} Gate Sweep')
        for label in strategies:
            strat_rows = [r for r in rows if r['strategy'] == label]
            valid = [r for r in strat_rows if r.get(field) is not None]
            if len(valid) < min_trades:
                continue
            results = threshold_sweep(valid, field, thresholds, direction, min_trades)
            if results:
                print(f'\n  {_c(BOLD, label)}  ({len(valid):,} rows with {field})')
                print_threshold_sweep(results, field, direction)


def run_compare(rows: list, filter_a: str, filter_b: str, strategies: list, min_trades: int):
    print_header(f'Gate Compare: {filter_a!r}  vs  {filter_b!r}')
    fn_a, *_ = parse_filter(filter_a)
    fn_b, *_ = parse_filter(filter_b)

    for label in strategies:
        strat_rows = [r for r in rows if r['strategy'] == label]
        rows_a = [r for r in strat_rows if fn_a(r)]
        rows_b = [r for r in strat_rows if fn_b(r)]
        if len(rows_a) < min_trades and len(rows_b) < min_trades:
            continue
        print(f'\n  {_c(BOLD, label)}')
        print_compare(rows_a, rows_b, filter_a, filter_b)


# ── Entry point ───────────────────────────────────────────────────────────────

def run_symbol_dir(rows: list, strategies: list, min_trades: int):
    """
    For each strategy, print a per-symbol long vs short comparison table.
    Sorted by long avg_net descending so best longs appear first.
    Highlights coins where direction asymmetry is large (>0.02% delta).
    """
    print_header('Per-symbol long vs short breakdown')
    print(_c(DIM, '  Sorted by long avg_net. Flags coins with strong direction asymmetry.\n'))

    for label in strategies:
        strat_rows = [r for r in rows if r['strategy'] == label]
        if len(strat_rows) < min_trades:
            continue

        # Group by symbol
        from collections import defaultdict
        by_sym = defaultdict(lambda: {'long': [], 'short': []})
        for r in strat_rows:
            d = r['dir']
            if d in ('long', 'short'):
                sym = r['symbol'].replace('USDT', '')
                by_sym[sym][d].append(r)

        # Build rows — only include symbols with data in at least one direction
        table = []
        for sym, dirs in by_sym.items():
            longs  = dirs['long']
            shorts = dirs['short']
            ls = stats(longs)  if longs  else None
            ss = stats(shorts) if shorts else None
            if ls is None and ss is None:
                continue
            # Require at least min_trades/3 in at least one direction
            threshold = max(3, min_trades // 3)
            if (ls is None or ls['n'] < threshold) and (ss is None or ss['n'] < threshold):
                continue
            table.append((sym, ls, ss))

        if not table:
            continue

        # Sort by long avg_net descending (None sorts last)
        table.sort(key=lambda x: -(x[1]['avg_net'] if x[1] else -999))

        print(f'\n  {_c(BOLD, label)}  ({len(strat_rows):,} trades across {len(table)} coins)\n')
        print(f'  {"coin":<14} {"long_n":>7} {"long_WR":>8} {"long_avg":>9}  {"short_n":>7} {"short_WR":>8} {"short_avg":>9}  {"Δavg":>8}  note')
        print(f'  {"─"*13}  {"─"*6}  {"─"*7}  {"─"*8}  {"─"*6}  {"─"*7}  {"─"*8}  {"─"*7}  {"─"*20}')

        for sym, ls, ss in table:
            l_n   = ls['n']   if ls else 0
            l_wr  = ls['wr']  if ls else 0.0
            l_avg = ls['avg_net'] if ls else None
            s_n   = ss['n']   if ss else 0
            s_wr  = ss['wr']  if ss else 0.0
            s_avg = ss['avg_net'] if ss else None

            # Delta: long_avg - short_avg (positive = longs better)
            if l_avg is not None and s_avg is not None:
                delta = l_avg - s_avg
            else:
                delta = None

            # Flags
            note = ''
            if l_avg is not None and s_avg is not None:
                if abs(delta) >= 0.05:
                    note = _c(YELLOW, f'{"LONG" if delta > 0 else "SHORT"} only ← big asymmetry')
                elif l_avg > 0 and s_avg < 0:
                    note = _c(GREEN, 'long profitable')
                elif s_avg > 0 and l_avg < 0:
                    note = _c(GREEN, 'short profitable')
                elif l_avg > 0 and s_avg > 0:
                    note = _c(GREEN, 'both profitable')
            elif l_avg is not None and l_avg > 0:
                note = _c(GREEN, 'long profitable (no short data)')
            elif s_avg is not None and s_avg > 0:
                note = _c(GREEN, 'short profitable (no long data)')

            l_str  = f'{l_n:>6,}  {_wr(l_wr)} {_net(l_avg) if l_avg is not None else "      —"}' if l_n else f'{"—":>6}  {"—":>7} {"—":>8}'
            s_str  = f'{s_n:>6,}  {_wr(s_wr)} {_net(s_avg) if s_avg is not None else "      —"}' if s_n else f'{"—":>6}  {"—":>7} {"—":>8}'
            d_str  = _net(delta) if delta is not None else '       —'

            print(f'  {sym:<14}  {l_str}   {s_str}  {d_str}  {note}')


# ── Fee re-baseline ───────────────────────────────────────────────────────────

def apply_refee(rows: list, logged_fee: float, true_fee: float) -> None:
    """Re-baseline net_pct from the fee the engine logged at to the true fee.

    net_logged = gross - logged_fee  =>  gross = net_logged + logged_fee
    net_true   = gross - true_fee    =  net_logged - (true_fee - logged_fee)
    win is recomputed on the corrected net.
    """
    delta = logged_fee - true_fee          # negative when true > logged
    for r in rows:
        r['net_pct'] = r['net_pct'] + delta
        r['win'] = 1 if r['net_pct'] > 0 else 0


# ── SL-cut counterfactual ─────────────────────────────────────────────────────

_SL_CUT_THRESHOLDS = {
    'conf':      [25, 32, 40, 50, 55, 60, 70, 75],
    'score':     [10, 20, 25, 30, 40, 50, 60],
    'abs_score': [10, 20, 30, 40, 50, 60, 70],
    'vpin':      [0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75],
}

def _exit_counts(rows: list) -> dict:
    c = defaultdict(int)
    for r in rows:
        c[(r['exit_reason'] or 'unknown')] += 1
    return c

def run_sl_cut(rows: list, strategies: list, field: str, min_trades: int) -> None:
    """For each gate threshold on `field`, show how the exit mix and expectancy
    change — specifically how many SL-bound trades the gate removes vs how many
    trail winners it sacrifices. A surgical gate kills SL without killing trail."""
    print_header(f'SL-cut counterfactual — gate on {field} (keep {field} >= thr)')
    thresholds = _SL_CUT_THRESHOLDS.get(field, _SL_CUT_THRESHOLDS['conf'])

    for label in strategies:
        srows = [r for r in rows if r['strategy'] == label]
        # derive abs_score on demand
        if field == 'abs_score':
            for r in srows:
                r['abs_score'] = abs(r['score']) if r.get('score') is not None else None
        valid = [r for r in srows if r.get(field) is not None]
        if len(valid) < min_trades:
            continue

        base = _exit_counts(valid)
        base_sl    = base.get('sl', 0)
        base_trail = base.get('trail', 0)
        base_stats = stats(valid)
        print(f"\n  {_c(BOLD, label)}  baseline: n={len(valid)} "
              f"SL={base_sl} ({base_sl/len(valid)*100:.0f}%)  "
              f"trail={base_trail} ({base_trail/len(valid)*100:.0f}%)  "
              f"expect={_exp(base_stats['expect'])}")
        print(f"    {'thr':>6} {'n_kept':>8} {'SL%':>6} {'trail%':>7} {'expect':>10} "
              f"{'SLcut':>7} {'trailcut':>9} {'SL/trail':>9}")
        for thr in thresholds:
            kept = [r for r in valid if r[field] >= thr]
            if len(kept) < min_trades:
                continue
            kc = _exit_counts(kept)
            ksl, ktr = kc.get('sl', 0), kc.get('trail', 0)
            sl_cut    = base_sl - ksl
            trail_cut = base_trail - ktr
            ratio = (sl_cut / trail_cut) if trail_cut > 0 else float('inf')
            st = stats(kept)
            ratio_str = '∞' if ratio == float('inf') else f'{ratio:.1f}'
            # color the ratio: surgical (>=3 SL per trail) green, <1 red
            rc = GREEN if (ratio == float('inf') or ratio >= 3) else (YELLOW if ratio >= 1 else RED)
            print(f"    {thr:>6} {len(kept):>8} "
                  f"{ksl/len(kept)*100:>5.0f}% {ktr/len(kept)*100:>6.0f}% "
                  f"{_exp(st['expect']):>10} {sl_cut:>7} {trail_cut:>9} {_c(rc, ratio_str):>9}")


def main():
    parser = argparse.ArgumentParser(
        description='Signal Replay — gate sweep against real trade outcomes',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python signal_replay.py --sweep vpin
  python signal_replay.py --sweep all
  python signal_replay.py --sweep vpin --strategy B W L
  python signal_replay.py --by direction exit_reason
  python signal_replay.py --by hour --strategy B
  python signal_replay.py --by symbol --strategy B --min-trades 10
  python signal_replay.py --strategy B --filter "vpin>=0.55" --compare "vpin>=0.65"
  python signal_replay.py --strategy CGYL --filter "vpin>=0.0" --compare "vpin>=0.70 AND conf>=55"
        """
    )
    parser.add_argument('--input',      default=str(_HERE / 'signals_with_outcomes.csv'),
                        help='Input CSV (default: signals_with_outcomes.csv)')
    parser.add_argument('--strategy',   nargs='+', default=None,
                        help='Filter to specific strategy labels')
    parser.add_argument('--sweep',      choices=['vpin', 'all'], default=None,
                        help='Run threshold sweep')
    parser.add_argument('--by',         nargs='+', default=None,
                        metavar='FIELD',
                        help='Breakdown by field(s): strategy direction exit_reason hour symbol vpin_bucket')
    parser.add_argument('--filter',     default=None,
                        help='Baseline gate filter for --compare (e.g. "vpin>=0.50")')
    parser.add_argument('--compare',    default=None,
                        help='Variant gate filter for --compare (e.g. "vpin>=0.65 AND conf>=55")')
    parser.add_argument('--min-trades', type=int, default=30,
                        help='Min trades per bucket to show (default: 30)')
    parser.add_argument('--symbol-dir', action='store_true',
                        help='Per-coin long vs short breakdown (use with --strategy)')
    parser.add_argument('--sort',       choices=['key','wr','expect','n'], default='key',
                        help='Sort breakdown rows by (default: key)')
    parser.add_argument('--refee',      default=None, metavar='LOGGED:TRUE',
                        help='Re-baseline net_pct from logged fee to true fee, e.g. "0.05:0.093". '
                             'Shifts every trade by the fee delta and recomputes win.')
    parser.add_argument('--sl-cut',     default=None, metavar='FIELD',
                        help='SL-cut counterfactual: sweep a gate on FIELD (conf|score|abs_score|vpin) '
                             'and show how many SL-bound trades it removes vs trail winners sacrificed.')
    args = parser.parse_args()

    print(_c(BOLD + CYAN, '\nPredictEngine — Signal Replay'))

    # Load data
    rows = load(args.input, strategy_filter=args.strategy)
    if not rows:
        print('[ERROR] No rows loaded. Check input file and --strategy filter.', file=sys.stderr)
        sys.exit(1)
    print(_c(DIM, f'  Loaded {len(rows):,} matched trades from {args.input}'))

    # Fee re-baseline (apply before any stats so every mode reflects the true fee)
    if args.refee:
        try:
            logged_s, true_s = args.refee.split(':')
            logged_f, true_f = float(logged_s), float(true_s)
        except ValueError:
            print(_c(RED, f'  [ERROR] --refee must be LOGGED:TRUE, e.g. 0.05:0.093'), file=sys.stderr)
            sys.exit(1)
        apply_refee(rows, logged_f, true_f)
        print(_c(YELLOW, f'  ↻ re-baselined fee {logged_f}% → {true_f}% '
                         f'(net shifted {logged_f - true_f:+.3f}%/trade, win recomputed)'))

    # Determine strategies to show
    all_strats = sorted(set(r['strategy'] for r in rows))
    strategies = args.strategy if args.strategy else all_strats

    # Overall summary
    print_header('Overall Summary')
    for label in strategies:
        strat_rows = [r for r in rows if r['strategy'] == label]
        print_strategy_summary(strat_rows, label)

    ran_something = False

    # Symbol-direction analysis
    if args.symbol_dir:
        run_symbol_dir(rows, strategies, args.min_trades)
        ran_something = True

    # SL-cut counterfactual
    if args.sl_cut:
        run_sl_cut(rows, strategies, args.sl_cut, args.min_trades)
        ran_something = True

    # Gate sweeps
    if args.sweep == 'vpin':
        run_vpin_sweep(rows, strategies, args.min_trades)
        ran_something = True
    elif args.sweep == 'all':
        run_sweep_all(rows, strategies, args.min_trades)
        ran_something = True

    # Breakdowns
    if args.by:
        run_breakdown(rows, strategies, args.by, args.min_trades)
        ran_something = True

    # Compare
    if args.filter and args.compare:
        run_compare(rows, args.filter, args.compare, strategies, args.min_trades)
        ran_something = True
    elif args.filter or args.compare:
        print(_c(YELLOW, '\n  [WARN] --filter and --compare must be used together'), file=sys.stderr)

    if not ran_something:
        print(_c(DIM, '\n  No analysis mode specified. Try: --sweep vpin  or  --by direction'))
        print(_c(DIM, '  Run with --help for full usage.'))

    print()


if __name__ == '__main__':
    main()
