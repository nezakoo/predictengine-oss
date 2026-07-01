"""
PredictEngine — Signal Outcome Joiner
======================================
Reads ALL signals_combined.csv files across data_backup directories,
matches each 'fired' row to its corresponding 'closed' row by
(strategy, symbol) within a time window, and outputs a clean
signals_with_outcomes.csv for use by signal_replay.py.

Usage:
  # Basic — auto-discovers all backup dirs:
  python signal_outcome_joiner.py --backup-dir ./data_backup

  # Explicit glob:
  python signal_outcome_joiner.py --csv "./data_backup/*/logs/signals_combined.csv"

  # Also cross-reference against positions CSV for more accurate PnL:
  python signal_outcome_joiner.py --backup-dir ./data_backup --positions ./logs/positions_*.csv

  # Dry run — print stats without writing output:
  python signal_outcome_joiner.py --backup-dir ./data_backup --dry-run

  # Adjust match window (default 1800s = 30min):
  python signal_outcome_joiner.py --backup-dir ./data_backup --max-dur 3600

Output: signals_with_outcomes.csv
Columns:
  ts_fired, strategy, symbol, dir, vpin, atr, conf, score, entry_price,
  tp_pct, sl_pct, ts_closed, exit_reason, net_pct, dur_sec,
  win, source_file, match_type

match_type values:
  signal_csv   — matched fired→closed within signals_combined.csv
  positions    — PnL taken from positions_YYYYMMDD.csv (more accurate)
  signal_only  — fired row found but no close matched (open/orphaned)
"""

import csv
import glob
import os
import re
import sys
import argparse
from collections import defaultdict
from datetime import datetime, date
from pathlib import Path
_HERE = Path(__file__).parent  # tools/analysis/
from typing import Optional


# ── Constants ────────────────────────────────────────────────────────────────

MAX_DUR_DEFAULT  = 1800   # 30 min — max time between fired and closed
MATCH_WINDOW_SEC = 10     # closed must be within MAX_DUR + this slack of fired

# ── Timestamp parsing ─────────────────────────────────────────────────────────

def _parse_ts(ts_str: str, file_date: Optional[date] = None) -> Optional[float]:
    """
    Parse a signal CSV timestamp to a Unix float.

    Formats seen in the wild:
      YYYYMMDD_HHMMSS   → e.g. 20260531_050102   (most common)
      HH:MM:SS          → e.g. 16:17:25           (older format, needs file_date)
      HH:MM:SS.ffffff   → with microseconds
    """
    ts_str = ts_str.strip()
    if not ts_str:
        return None

    # YYYYMMDD_HHMMSS
    m = re.match(r'^(\d{8})_(\d{6})$', ts_str)
    if m:
        try:
            return datetime.strptime(ts_str, '%Y%m%d_%H%M%S').timestamp()
        except ValueError:
            pass

    # HH:MM:SS or HH:MM:SS.ffffff
    m = re.match(r'^(\d{2}:\d{2}:\d{2})(\.\d+)?$', ts_str)
    if m:
        if file_date is None:
            return None   # can't resolve without a date
        time_part = m.group(1)
        try:
            dt = datetime.strptime(f'{file_date.strftime("%Y%m%d")} {time_part}', '%Y%m%d %H:%M:%S')
            return dt.timestamp()
        except ValueError:
            pass

    return None


def _date_from_path(path: str) -> Optional[date]:
    """
    Extract a date from a backup directory path.
    Looks for YYYYMMDD pattern in path components.
    e.g. ./data_backup/20260531_010043/logs/signals_combined.csv → 2026-05-31
    """
    for part in Path(path).parts:
        m = re.match(r'^(\d{8})', part)
        if m:
            try:
                return datetime.strptime(m.group(1), '%Y%m%d').date()
            except ValueError:
                pass
    return None


# ── Detail field parsers ──────────────────────────────────────────────────────

def _parse_fired_detail(detail: str, strategy: str) -> dict:
    """
    Extract dir, entry_price, tp_pct, sl_pct from a fired detail string.

    Known formats:
      "short entry=0.39 tp=0.866% sl=0.510%"
      "long entry=0.07 tp=0.400% sl=0.300%"
      "decor short btc=-0.27%"             → W strategy, dir from word after decor
      "decor long btc=0.45%"
      "level break long lvl=69.71"         → L strategy
      "level bounce short lvl=0.80"
      "mtf long score=45"                  → B strategy
      "short"                              → minimal
    """
    out = {'dir': None, 'entry_price': None, 'tp_pct': None, 'sl_pct': None}
    if not detail:
        return out

    dl = detail.lower().strip()

    # Direction — look for 'long' or 'short' anywhere in detail
    if 'long' in dl:
        out['dir'] = 'long'
    elif 'short' in dl:
        out['dir'] = 'short'

    # entry=
    m = re.search(r'entry=([\d.]+)', detail, re.IGNORECASE)
    if m:
        try:
            out['entry_price'] = float(m.group(1))
        except ValueError:
            pass

    # tp=
    m = re.search(r'tp=([\d.]+)%?', detail, re.IGNORECASE)
    if m:
        try:
            out['tp_pct'] = float(m.group(1))
        except ValueError:
            pass

    # sl=
    m = re.search(r'sl=([\d.]+)%?', detail, re.IGNORECASE)
    if m:
        try:
            out['sl_pct'] = float(m.group(1))
        except ValueError:
            pass

    return out


def _parse_closed_detail(detail: str) -> dict:
    """
    Extract exit_reason, net_pct, dur_sec from a closed detail string.

    Known formats:
      "short trail net=+0.0749% dur=27s"
      "long sl net=-0.1234% dur=120s"
      "long tp net=+0.2100% dur=45s"
      "long inertia net=-0.0800% dur=300s"
      "short time net=-0.0500% dur=600s"
      "long rev net=-0.0800% dur=90s"
      "short be net=+0.0000% dur=15s"      → break-even lock
    """
    out = {'exit_reason': None, 'net_pct': None, 'dur_sec': None}
    if not detail:
        return out

    dl = detail.lower().strip()
    tokens = dl.split()

    # Direction is first token, exit_reason is second
    if len(tokens) >= 2 and tokens[0] in ('long', 'short'):
        out['exit_reason'] = tokens[1]

    # net=
    m = re.search(r'net=([+-]?[\d.]+)%?', detail, re.IGNORECASE)
    if m:
        try:
            out['net_pct'] = float(m.group(1))
        except ValueError:
            pass

    # dur=Xs
    m = re.search(r'dur=([\d.]+)s', detail, re.IGNORECASE)
    if m:
        try:
            out['dur_sec'] = float(m.group(1))
        except ValueError:
            pass

    return out


# ── CSV loading ───────────────────────────────────────────────────────────────

def _load_csv_rows(paths: list) -> list:
    """
    Load all rows from a list of signals_combined.csv files.
    Normalises column names, resolves timestamps, tags with source_file.
    Returns list of dicts sorted by ts_unix ascending.
    """
    rows = []
    n_skipped = 0
    n_bad_ts  = 0

    for fpath in paths:
        file_date = _date_from_path(fpath)
        try:
            with open(fpath, newline='', encoding='utf-8', errors='replace') as fh:
                reader = csv.DictReader(fh)
                raw_fields = reader.fieldnames
                if not raw_fields:
                    continue
                # Normalise: strip whitespace from field names
                raw_fields = reader.fieldnames or []
                reader.fieldnames = [f.strip() if isinstance(f, str) else f for f in raw_fields]

                for row in reader:
                    # Strip whitespace from all values
                    row = {k: (v.strip() if v else '') for k, v in row.items()}

                    event    = row.get('event', '').lower()
                    strategy = row.get('strategy', '').strip()
                    symbol   = row.get('symbol', '').strip()
                    ts_raw   = row.get('ts', '').strip()

                    # Only care about fired and closed events
                    if event not in ('fired', 'closed'):
                        n_skipped += 1
                        continue

                    if not strategy or not symbol:
                        n_skipped += 1
                        continue

                    ts_unix = _parse_ts(ts_raw, file_date)
                    if ts_unix is None:
                        n_bad_ts += 1
                        continue

                    # Safe float helper
                    def _f(key, fallback=None):
                        v = row.get(key, '')
                        if not v:
                            return fallback
                        try:
                            return float(v)
                        except ValueError:
                            return fallback

                    rows.append({
                        'ts_unix':   ts_unix,
                        'ts_raw':    ts_raw,
                        'event':     event,
                        'strategy':  strategy,
                        'symbol':    symbol,
                        'detail':    row.get('detail', ''),
                        'vpin':      _f('vpin'),
                        'atr':       _f('atr'),
                        'spread':    _f('spread'),
                        'price':     _f('price'),
                        'conf':      _f('conf'),
                        'score':     _f('score'),
                        'source_file': os.path.basename(os.path.dirname(os.path.dirname(fpath)))
                                        + '/' + os.path.basename(fpath),
                    })
        except Exception as exc:
            print(f'  [WARN] {fpath}: {exc}', file=sys.stderr)

    rows.sort(key=lambda r: r['ts_unix'])
    print(f'  Loaded {len(rows):,} fired+closed rows  '
          f'(skipped={n_skipped:,} non-fired/closed, bad_ts={n_bad_ts:,})',
          file=sys.stderr)
    return rows


# ── Positions CSV loader ──────────────────────────────────────────────────────

def _load_positions(paths: list) -> dict:
    """
    Load positions_YYYYMMDD.csv files.
    Returns dict keyed by (strategy, symbol, entry_ts_approx) → PnL info.

    Columns: ts,event,sym,dir,qty,price,order_id,strategy,realized_pnl,commission,dur_sec,entry_ts
    """
    positions = []
    for fpath in paths:
        try:
            with open(fpath, newline='', encoding='utf-8', errors='replace') as fh:
                reader = csv.DictReader(fh)
                raw_fields = reader.fieldnames
                if not raw_fields:
                    continue
                raw_fields = reader.fieldnames or []
                reader.fieldnames = [f.strip() if isinstance(f, str) else f for f in raw_fields]
                for row in reader:
                    row = {k: (v.strip() if v else '') for k, v in row.items()}
                    event = row.get('event', '').lower()
                    if event != 'close':
                        continue
                    strategy = row.get('strategy', '').strip()
                    sym      = row.get('sym', '').strip()
                    if not strategy or not sym:
                        continue

                    def _f(k, fb=None):
                        v = row.get(k, '')
                        try: return float(v) if v else fb
                        except ValueError: return fb

                    ts_close  = _parse_ts(row.get('ts', ''))
                    entry_ts  = _parse_ts(row.get('entry_ts', ''))
                    if ts_close is None:
                        continue

                    positions.append({
                        'strategy':     strategy,
                        'symbol':       sym,
                        'ts_close':     ts_close,
                        'entry_ts':     entry_ts,
                        'realized_pnl': _f('realized_pnl'),
                        'commission':   _f('commission'),
                        'dur_sec':      _f('dur_sec'),
                        'dir':          row.get('dir', '').strip(),
                    })
        except Exception as exc:
            print(f'  [WARN] positions {fpath}: {exc}', file=sys.stderr)

    print(f'  Loaded {len(positions):,} position close records', file=sys.stderr)
    return positions


# ── Matching engine ───────────────────────────────────────────────────────────

def match_fired_to_closed(rows: list, max_dur: int = MAX_DUR_DEFAULT) -> tuple:
    """
    Match each fired row to its closed row.

    Algorithm:
      1. Separate rows into fired and closed buckets per (strategy, symbol).
      2. For each fired row, find the earliest closed row that:
           - is the same (strategy, symbol)
           - ts_closed >= ts_fired
           - ts_closed <= ts_fired + max_dur
      3. Use a running pointer per key (both lists sorted by ts) to keep O(N) complexity.
      4. A closed row can only match ONE fired row (first-come-first-serve).

    Returns:
      matched   — list of joined dicts
      unmatched — fired rows with no close found
    """
    from collections import defaultdict

    # Group by (strategy, symbol)
    fired_by_key  = defaultdict(list)
    closed_by_key = defaultdict(list)

    for row in rows:
        key = (row['strategy'], row['symbol'])
        if row['event'] == 'fired':
            fired_by_key[key].append(row)
        elif row['event'] == 'closed':
            closed_by_key[key].append(row)

    matched   = []
    unmatched = []
    n_ambiguous = 0

    for key, fired_list in fired_by_key.items():
        closed_list = closed_by_key.get(key, [])
        # Both already sorted by ts_unix (from load step)
        ci = 0  # pointer into closed_list
        used_closes = set()

        for fi, fired in enumerate(fired_list):
            t_fired = fired['ts_unix']
            t_max   = t_fired + max_dur + MATCH_WINDOW_SEC

            # Advance pointer past closes that are before this fire
            while ci < len(closed_list) and closed_list[ci]['ts_unix'] < t_fired:
                ci += 1

            # Find first unused close within window
            best_close = None
            for ci2 in range(ci, len(closed_list)):
                c = closed_list[ci2]
                if c['ts_unix'] > t_max:
                    break
                if id(c) not in used_closes:
                    best_close = c
                    used_closes.add(id(c))
                    break

            # Parse fired detail
            fd = _parse_fired_detail(fired['detail'], fired['strategy'])

            # Use price from fired row if entry_price not in detail
            entry_price = fd['entry_price'] or fired.get('price')

            # Use conf/score/vpin from fired row
            vpin  = fired.get('vpin')
            atr   = fired.get('atr')
            conf  = fired.get('conf')
            score = fired.get('score')

            if best_close is not None:
                cd = _parse_closed_detail(best_close['detail'])
                # dir from closed detail (more reliable for some strategies)
                dir_val = fd['dir'] or _parse_fired_detail(best_close['detail'], fired['strategy']).get('dir')
                # net_pct from closed detail
                net_pct     = cd['net_pct']
                exit_reason = cd['exit_reason']
                dur_sec     = cd['dur_sec']
                if dur_sec is None:
                    dur_sec = round(best_close['ts_unix'] - t_fired, 1)
                win = None
                if net_pct is not None:
                    win = 1 if net_pct > 0 else 0

                matched.append({
                    'ts_fired':    fired['ts_raw'],
                    'ts_fired_u':  t_fired,
                    'strategy':    fired['strategy'],
                    'symbol':      fired['symbol'],
                    'dir':         dir_val or '',
                    'vpin':        vpin,
                    'atr':         atr,
                    'conf':        conf,
                    'score':       score,
                    'entry_price': entry_price,
                    'tp_pct':      fd['tp_pct'],
                    'sl_pct':      fd['sl_pct'],
                    'ts_closed':   best_close['ts_raw'],
                    'ts_closed_u': best_close['ts_unix'],
                    'exit_reason': exit_reason or '',
                    'net_pct':     net_pct,
                    'dur_sec':     dur_sec,
                    'win':         win,
                    'source_file': fired['source_file'],
                    'match_type':  'signal_csv',
                })
            else:
                # No close found — record as unmatched (trade still open or log gap)
                unmatched.append({
                    'ts_fired':    fired['ts_raw'],
                    'ts_fired_u':  t_fired,
                    'strategy':    fired['strategy'],
                    'symbol':      fired['symbol'],
                    'dir':         fd['dir'] or '',
                    'vpin':        vpin,
                    'atr':         atr,
                    'conf':        conf,
                    'score':       score,
                    'entry_price': entry_price,
                    'tp_pct':      fd['tp_pct'],
                    'sl_pct':      fd['sl_pct'],
                    'ts_closed':   '',
                    'ts_closed_u': None,
                    'exit_reason': '',
                    'net_pct':     None,
                    'dur_sec':     None,
                    'win':         None,
                    'source_file': fired['source_file'],
                    'match_type':  'signal_only',
                })

    return matched, unmatched


def enrich_from_positions(matched: list, positions: list, max_ts_delta: float = 60.0) -> list:
    """
    For each matched trade, try to find a positions CSV record that agrees
    (strategy, symbol, ts_closed within max_ts_delta) and replace net_pct
    with the real Binance realized_pnl (more accurate than parsed signal detail).

    Converts realized_pnl (USDT) to percentage using entry_price and order size.
    Since we don't have qty here, we flag the match but keep net_pct from signal
    and add bnb_pnl_usdt for reference.
    """
    # Index positions by (strategy, symbol) → sorted list
    pos_idx = defaultdict(list)
    for p in positions:
        pos_idx[(p['strategy'], p['symbol'])].append(p)

    n_enriched = 0
    for m in matched:
        key = (m['strategy'], m['symbol'])
        candidates = pos_idx.get(key, [])
        t_close = m['ts_closed_u']
        if t_close is None:
            continue
        best = None
        best_delta = max_ts_delta + 1
        for p in candidates:
            delta = abs(p['ts_close'] - t_close)
            if delta < best_delta:
                best_delta = delta
                best = p
        if best is not None and best_delta <= max_ts_delta:
            m['bnb_pnl_usdt']  = best['realized_pnl']
            m['bnb_comm_usdt'] = best['commission']
            m['match_type']    = 'positions'
            n_enriched += 1

    print(f'  Enriched {n_enriched:,} trades with positions CSV data', file=sys.stderr)
    return matched


# ── Output writer ─────────────────────────────────────────────────────────────

OUTPUT_COLS = [
    'ts_fired', 'strategy', 'symbol', 'dir',
    'vpin', 'atr', 'conf', 'score',
    'entry_price', 'tp_pct', 'sl_pct',
    'ts_closed', 'exit_reason', 'net_pct', 'dur_sec', 'win',
    'bnb_pnl_usdt', 'bnb_comm_usdt',
    'source_file', 'match_type',
]

def _fmt(v) -> str:
    if v is None:
        return ''
    if isinstance(v, float):
        # Keep reasonable precision, strip trailing zeros
        return f'{v:.6g}'
    return str(v)


def write_output(matched: list, unmatched: list, out_path: str, include_unmatched: bool = False) -> None:
    rows_to_write = matched + (unmatched if include_unmatched else [])
    rows_to_write.sort(key=lambda r: r.get('ts_fired_u') or 0)

    with open(out_path, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=OUTPUT_COLS, extrasaction='ignore')
        writer.writeheader()
        for row in rows_to_write:
            # Ensure all output cols exist
            out_row = {col: _fmt(row.get(col)) for col in OUTPUT_COLS}
            writer.writerow(out_row)

    print(f'  Wrote {len(rows_to_write):,} rows → {out_path}', file=sys.stderr)


# ── Stats reporter ────────────────────────────────────────────────────────────

def print_stats(matched: list, unmatched: list) -> None:
    RESET  = '\033[0m'
    GREEN  = '\033[92m'
    RED    = '\033[91m'
    CYAN   = '\033[96m'
    BOLD   = '\033[1m'
    DIM    = '\033[2m'
    YELLOW = '\033[93m'

    def _c(col, txt): return f'{col}{txt}{RESET}'

    total_fired = len(matched) + len(unmatched)
    match_pct   = len(matched) / max(total_fired, 1) * 100

    print(_c(BOLD + CYAN, f'\n{"━"*62}'))
    print(_c(BOLD, f'  SIGNAL OUTCOME JOINER RESULTS'))
    print(_c(CYAN, f'{"━"*62}'))
    print(f'  Total fired rows:  {total_fired:>6,}')
    n_matched   = f'{len(matched):>6,}'
    n_unmatched = f'{len(unmatched):>6,}'
    print(f'  Matched (closed):  {_c(GREEN, n_matched)}  ({match_pct:.1f}%)')
    print(f'  Unmatched (open):  {_c(YELLOW, n_unmatched)}')

    if not matched:
        print(_c(RED, '\n  No matched trades found. Check --backup-dir path.'))
        return

    # Per-strategy breakdown
    from collections import defaultdict
    by_strat = defaultdict(lambda: {'n': 0, 'wins': 0, 'net': 0.0, 'exits': defaultdict(int), 'no_net': 0})
    for m in matched:
        s = by_strat[m['strategy']]
        s['n'] += 1
        if m['net_pct'] is not None:
            if m['win'] == 1: s['wins'] += 1
            s['net'] += m['net_pct']
        else:
            s['no_net'] += 1
        if m['exit_reason']:
            s['exits'][m['exit_reason']] += 1

    print(f'\n  {"Strat":<6} {"trades":>7} {"WR%":>7} {"avg_net":>9} {"exits"}')
    print(f'  {"─"*5}  {"─"*6}  {"─"*6}  {"─"*8}  {"─"*30}')

    for label in sorted(by_strat.keys()):
        s    = by_strat[label]
        n    = s['n']
        n_ok = n - s['no_net']
        wr   = s['wins'] / max(n_ok, 1) * 100
        avg  = s['net'] / max(n_ok, 1)
        wr_c = GREEN if wr >= 50 else (YELLOW if wr >= 35 else RED)
        avg_c= GREEN if avg >= 0 else RED
        exits_str = '  '.join(f"{k}:{v}" for k, v in sorted(s['exits'].items(), key=lambda x: -x[1])[:5])
        print(f'  {label:<6} {n:>7,}  {_c(wr_c, f"{wr:>5.1f}%")}  {_c(avg_c, f"{avg:>+7.4f}%")}  {_c(DIM, exits_str)}')

    # Exit reason overall
    all_exits = defaultdict(int)
    for m in matched:
        if m['exit_reason']:
            all_exits[m['exit_reason']] += 1
    if all_exits:
        total_exits = sum(all_exits.values())
        exit_parts  = '  '.join(
            f"{k}:{v} ({v/total_exits*100:.0f}%)"
            for k, v in sorted(all_exits.items(), key=lambda x: -x[1])
        )
        print(f'\n  Exit mix: {_c(DIM, exit_parts)}')

    # Coverage warning
    no_net = sum(1 for m in matched if m['net_pct'] is None)
    if no_net:
        print(f'\n  {_c(YELLOW, f"⚠ {no_net:,} matched rows missing net_pct (unparseable closed detail)")}')

    print()


# ── Main ──────────────────────────────────────────────────────────────────────

def discover_csv_files(backup_dir: str) -> list:
    """Find all signals_combined.csv files under backup_dir."""
    pattern = os.path.join(backup_dir, '**', 'signals_combined.csv')
    files   = sorted(glob.glob(pattern, recursive=True))
    # Also try non-recursive in case of flat structure
    if not files:
        pattern2 = os.path.join(backup_dir, '*/logs/signals_combined.csv')
        files    = sorted(glob.glob(pattern2))
    return files


def main():
    parser = argparse.ArgumentParser(
        description='Match fired→closed rows in signals_combined.csv',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python signal_outcome_joiner.py --backup-dir ./data_backup
  python signal_outcome_joiner.py --csv "./data_backup/*/logs/signals_combined.csv"
  python signal_outcome_joiner.py --backup-dir ./data_backup --positions "./logs/positions_*.csv"
  python signal_outcome_joiner.py --backup-dir ./data_backup --dry-run
  python signal_outcome_joiner.py --backup-dir ./data_backup --include-unmatched
        """
    )
    parser.add_argument('--backup-dir', default=str(_HERE.parent.parent / 'data_backup'),
                        # Scans ALL sessions: *_prod/ and *_stage/ dirs automatically included
                        help='Root backup directory (default: ./data_backup)')
    parser.add_argument('--csv', nargs='+', default=None,
                        help='Explicit glob pattern(s) for signals_combined.csv files')
    parser.add_argument('--positions', nargs='+', default=None,
                        help='Glob pattern(s) for positions_YYYYMMDD.csv files')
    parser.add_argument('--out', default=str(_HERE / 'signals_with_outcomes.csv'),
                        help='Output CSV path (default: signals_with_outcomes.csv)')
    parser.add_argument('--max-dur', type=int, default=MAX_DUR_DEFAULT,
                        help=f'Max seconds between fired and closed (default: {MAX_DUR_DEFAULT})')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print stats but do not write output file')
    parser.add_argument('--include-unmatched', action='store_true',
                        help='Include unmatched fired rows in output (net_pct empty)')
    parser.add_argument('--strategy', nargs='+', default=None,
                        help='Filter to specific strategy labels (e.g. B W L)')
    args = parser.parse_args()

    RESET = '\033[0m'; BOLD = '\033[1m'; CYAN = '\033[96m'; DIM = '\033[2m'
    def _c(col, txt): return f'{col}{txt}{RESET}'

    print(_c(BOLD + CYAN, '\nPredictEngine — Signal Outcome Joiner'))

    # Discover CSV files
    if args.csv:
        csv_files = []
        for pattern in args.csv:
            csv_files.extend(sorted(glob.glob(pattern)))
        csv_files = sorted(set(csv_files))
    else:
        csv_files = discover_csv_files(args.backup_dir)

    if not csv_files:
        print(f'[ERROR] No signals_combined.csv files found under {args.backup_dir}', file=sys.stderr)
        print('  Try: --csv "./data_backup/*/logs/signals_combined.csv"', file=sys.stderr)
        sys.exit(1)

    print(f'  Found {len(csv_files):,} CSV files', file=sys.stderr)

    # Load rows
    print(_c(DIM, '  Loading signal rows...'), file=sys.stderr)
    rows = _load_csv_rows(csv_files)

    # Strategy filter
    if args.strategy:
        before = len(rows)
        rows   = [r for r in rows if r['strategy'] in args.strategy]
        print(f'  Strategy filter ({", ".join(args.strategy)}): {before:,} → {len(rows):,} rows',
              file=sys.stderr)

    if not rows:
        print('[ERROR] No fired/closed rows found after filtering.', file=sys.stderr)
        sys.exit(1)

    fired_count  = sum(1 for r in rows if r['event'] == 'fired')
    closed_count = sum(1 for r in rows if r['event'] == 'closed')
    print(f'  fired={fired_count:,}  closed={closed_count:,}', file=sys.stderr)

    # Match
    print(_c(DIM, '  Matching fired→closed...'), file=sys.stderr)
    matched, unmatched = match_fired_to_closed(rows, max_dur=args.max_dur)

    # Enrich from positions CSV
    if args.positions:
        pos_files = []
        for pattern in args.positions:
            pos_files.extend(sorted(glob.glob(pattern)))
        if pos_files:
            print(_c(DIM, f'  Loading {len(pos_files)} positions file(s)...'), file=sys.stderr)
            positions = _load_positions(pos_files)
            matched = enrich_from_positions(matched, positions)
        else:
            print('  [WARN] --positions glob matched no files', file=sys.stderr)

    # Stats
    print_stats(matched, unmatched)

    # Write
    if not args.dry_run:
        write_output(matched, unmatched, args.out, include_unmatched=args.include_unmatched)
        print(f'  Output: {args.out}', file=sys.stderr)
    else:
        print('  [DRY RUN] no file written', file=sys.stderr)

    # Exit summary
    print(f'  Done. {len(matched):,} matched trades ready for signal_replay.py', file=sys.stderr)


if __name__ == '__main__':
    main()
