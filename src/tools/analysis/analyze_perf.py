#!/usr/bin/env python3
"""
analyze_perf.py — Strategy performance over time + config correlation

Usage:
  python3 analyze_perf.py                      # all sessions, HTML output
  python3 analyze_perf.py --since 7d           # last 7 days
  python3 analyze_perf.py --since deploy       # since last deploy
  python3 analyze_perf.py --since deploy:2     # since 2nd-to-last deploy
  python3 analyze_perf.py --since 2026-05-22   # since date
  python3 analyze_perf.py --strategy K         # single strategy deep-dive
  python3 analyze_perf.py --data ./data_backup # custom data dir

Output: analysis_perf_YYYYMMDD_HHMMSS.html
"""

import os, sys, re, csv, json, glob, argparse
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path

DATA_DIR       = "./data_backup"
DEPLOYS_LOG    = "./data_backup/deployments.log"
FEE_RT         = 0.08   # round-trip fee %

# ══════════════════════════════════════════════════════════════════
# FORMAT DETECTION & PARSING
# ══════════════════════════════════════════════════════════════════

# Format A columns (v13, no STRATEGY header, VERSION comment line)
FORMAT_A_COLS = [
    'time','sym','dir','conf','score','sigs','n_avail','entry_px',
    'dyn_tp','dyn_sl','vpin_entry','kyle_lam','spread_entry','accel',
    'pct_exit','outcome','net_exit','reason','dur_sec','version'
]

# Format B/C columns (v16+, STRATEGY header present, OUT_ rows)
FORMAT_BC_COLS = [
    'time','sym','dir','conf','score','n_agree','n_avail','entry_px',
    'dyn_tp','dyn_sl','atr_entry','vpin_entry','spread_entry','exit_px',
    'pct_exit','net_exit','outcome','reason','dur_sec','max_dp','min_dp',
    'snap30','snap60','be_activated','be_at_sec','tp_extended','tp_touches','version'
]

def _parse_strategy_header(line):
    """
    Parse '# STRATEGY,MTF Momentum,vpin≥0.45,conf≥25,score≥10.0,...,inertia=9999.0s,max_window=600.0'
    Returns dict of config params + strategy name.
    """
    cfg = {}
    parts = [p.strip() for p in line.lstrip('#').split(',')]
    if len(parts) < 2:
        return cfg
    cfg['strategy_name'] = parts[1]
    for part in parts[2:]:
        # inertia=9999.0s, max_window=600.0, sl_mult=0.7, win_thr=0.3
        m = re.match(r'([a-z_]+)=([\d.]+)', part.lower())
        if m:
            try:
                cfg[m.group(1)] = float(m.group(2))
            except ValueError:
                cfg[m.group(1)] = m.group(2)
        # vpin≥0.45, conf≥25
        m = re.match(r'([a-z_]+)[≥>=]+([\d.]+)', part.lower())
        if m:
            try:
                cfg[f'min_{m.group(1)}'] = float(m.group(2))
            except ValueError:
                pass
    return cfg

def _parse_version_header(line):
    """
    Parse '# VERSION,v=v13,date=2026-05-07,tp=...,sl=...,win_thr=...'
    Returns dict.
    """
    cfg = {}
    for part in line.lstrip('#').split(','):
        if '=' in part:
            k, _, v = part.strip().partition('=')
            cfg[k.strip().lower()] = v.strip()
    return cfg

def _safe_float(v):
    try:
        return float(v) if v not in (None, '', 'nan') else None
    except (ValueError, TypeError):
        return None

def _parse_time(t_str, session_date):
    """
    Parse 'HH:MM:SS' or 'OUT_HH:MM:SS' into a datetime using session_date as base.
    Also handles 'YYYYMMDD_HHMMSS' format from signals.
    """
    t_str = t_str.lstrip('OUT_').strip() if t_str else ''
    # Try HH:MM:SS
    m = re.match(r'^(\d{1,2}):(\d{2}):(\d{2})$', t_str)
    if m and session_date:
        h, mn, s = int(m.group(1)), int(m.group(2)), int(m.group(3))
        dt = session_date.replace(hour=h, minute=mn, second=s, microsecond=0)
        # If time is earlier than session start, it's next day
        if dt < session_date:
            dt += timedelta(days=1)
        return dt
    # Try YYYYMMDD_HHMMSS
    try:
        return datetime.strptime(t_str, '%Y%m%d_%H%M%S')
    except ValueError:
        pass
    # Try unix timestamp
    try:
        return datetime.fromtimestamp(float(t_str))
    except (ValueError, TypeError, OSError):
        pass
    return None

def load_preds_file(filepath, session_dt):
    """
    Load a preds CSV file. Returns list of normalised closed-trade dicts.
    Handles Format A (VERSION header), Format B/C (STRATEGY header or headerless).
    Only returns rows that are closed (have outcome filled).
    """
    trades = []
    strategy_cfg = {}
    fmt = None  # 'A', 'BC', or None (positional)
    col_names = None

    # Extract strategy label from filename
    # Handles:
    #   preds_20260604_0328_W_BTC_Decorrelati...csv        → 'W'   (prod)
    #   preds_b_20260602_2004_C_C_B_max_win...csv          → 'C_b' (_b shadow variant)
    #   preds_20260509_0453_v13__tp_dyn...csv              → 'ALL' (legacy v13)
    fname = os.path.basename(filepath)
    strat_label = '?'
    is_b_variant = fname.startswith('preds_b_')

    if is_b_variant:
        # preds_b_YYYYMMDD_HHMM_LABEL_LABEL_B_...csv
        # The shadow label is the first single uppercase letter after the timestamp
        m = re.search(r'preds_b_\d{8}_\d{4,6}_([A-Z]{1,3})_', fname)
        if m:
            strat_label = m.group(1) + '_b'
    else:
        m = re.search(r'preds_\d{8}_\d{4,6}_([A-Z]{1,3})_', fname)
        if m:
            strat_label = m.group(1)
        else:
            # v13 filenames: preds_YYYYMMDD_HHMM_v13__tp...csv
            m = re.search(r'preds_\d{8}_\d{4,6}_v\d+', fname)
            if m:
                strat_label = 'ALL'

    # File timestamp from filename
    # preds_b_ files: preds_b_YYYYMMDD_HHMM_... (date is after the 'b_' prefix)
    # prod files:     preds_YYYYMMDD_HHMM_...
    m2 = re.search(r'preds_(?:b_)?(\d{8})_(\d{4,6})', fname)
    file_dt = None
    if m2:
        ds, ts = m2.group(1), m2.group(2)
        fmt_str = '%Y%m%d_%H%M%S' if len(ts) == 6 else '%Y%m%d_%H%M'
        try:
            file_dt = datetime.strptime(f'{ds}_{ts}', fmt_str)
        except ValueError:
            pass
    base_dt = file_dt or session_dt

    try:
        with open(filepath, newline='', encoding='utf-8', errors='replace') as fh:
            raw_lines = fh.readlines()
    except Exception:
        return []

    lines = []
    for line in raw_lines:
        s = line.strip()
        if not s:
            continue
        if s.startswith('# STRATEGY'):
            strategy_cfg = _parse_strategy_header(s)
            strategy_cfg['_header_line'] = s
            fmt = 'BC'
        elif s.startswith('# VERSION'):
            strategy_cfg = _parse_version_header(s)
            fmt = 'A'
        elif s.startswith('time,'):
            # named header row
            col_names = [c.strip() for c in s.split(',')]
            if fmt is None:
                fmt = 'BC' if 'n_agree' in col_names else 'A'
        else:
            lines.append(s)

    for line in lines:
        is_closed = line.startswith('OUT_')
        # For Format A: rows without exit data are open/pending
        row_str = line.lstrip('OUT_') if is_closed else line
        parts = row_str.split(',')

        if col_names:
            # Named columns — zip with header
            row = dict(zip(col_names, parts + [''] * max(0, len(col_names) - len(parts))))
            # For Format A, closed trades have pct_exit filled; open don't
            if fmt == 'A':
                is_closed = bool(row.get('pct_exit', '').strip())
        elif fmt == 'BC' or (fmt is None and len(parts) >= 18):
            # Positional Format B/C (headerless old files)
            cols = FORMAT_BC_COLS if len(parts) >= 22 else FORMAT_A_COLS
            row = dict(zip(cols, parts + [''] * max(0, len(cols) - len(parts))))
            if not is_closed:
                continue  # open row, skip
        else:
            continue

        if not is_closed:
            continue

        outcome = row.get('outcome', '').strip().lower()
        if outcome not in ('win', 'lose', 'loss', 'flat'):
            continue  # not a closed trade

        t_str = row.get('time', '').lstrip('OUT_').strip()
        trade_dt = _parse_time(t_str, base_dt)

        pct  = _safe_float(row.get('pct_exit'))
        net  = _safe_float(row.get('net_exit')) or (pct - FEE_RT if pct is not None else None)

        trades.append({
            'dt':           trade_dt,
            'session_dt':   base_dt,
            'strategy':     strat_label,
            'sym':          row.get('sym', '').strip(),
            'dir':          row.get('dir', '').strip(),
            'conf':         _safe_float(row.get('conf')),
            'score':        _safe_float(row.get('score')),
            'vpin':         _safe_float(row.get('vpin_entry')),
            'entry_px':     _safe_float(row.get('entry_px')),
            'pct_exit':     pct,
            'net_exit':     net,
            'outcome':      'lose' if outcome in ('lose', 'loss') else outcome,
            'reason':       row.get('reason', '').strip().lower() or 'unknown',
            'dur_sec':      _safe_float(row.get('dur_sec')),
            'max_dp':       _safe_float(row.get('max_dp')),
            'snap30':       _safe_float(row.get('snap30')),
            # config from header
            'cfg':          strategy_cfg,
        })

    return trades


# ══════════════════════════════════════════════════════════════════
# DEPLOYMENTS LOG
# ══════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════
# CONFIG SNAPSHOT DIFFING (deploy_history/)
# ══════════════════════════════════════════════════════════════════

def _parse_config_snapshot(filepath):
    """
    Parse a strategies_config_YYYYMMDD_HHMMSS.py snapshot.
    Returns dict: {label: {param: value}} extracted from StrategyConfig(...) blocks.
    Uses a line-by-line state machine — avoids multi-line regex fragility.
    """
    params = {}
    try:
        lines = open(filepath, encoding='utf-8', errors='replace').readlines()
    except Exception:
        return params

    in_block = False
    block_lines = []
    depth = 0

    _label_re   = re.compile(r"label\s*=\s*['\"]([A-Z]{1,3})['\"]")
    _num_re     = re.compile(r"(\w+)\s*=\s*(-?\d+(?:\.\d+)?)")
    _disabled_re= re.compile(r"disabled\s*=\s*True")

    for line in lines:
        if not in_block:
            if 'StrategyConfig(' in line:
                in_block = True
                block_lines = [line]
                depth = line.count('(') - line.count(')')
        else:
            block_lines.append(line)
            depth += line.count('(') - line.count(')')
            if depth <= 0:
                block = ''.join(block_lines)
                in_block = False
                block_lines = []

                lm = _label_re.search(block)
                if not lm:
                    continue
                label = lm.group(1)

                cfg = {}
                for m in _num_re.finditer(block):
                    k, v = m.group(1), m.group(2)
                    if k in ('label', 'color', 'version'):
                        continue
                    try:
                        cfg[k] = float(v) if '.' in v else int(v)
                    except ValueError:
                        pass
                if _disabled_re.search(block):
                    cfg['disabled'] = True
                params[label] = cfg

    return params

def _diff_configs(cfg_a, cfg_b):
    """
    Compare two {label: {param: value}} dicts.
    Returns list of human-readable change strings.
    """
    changes = []
    all_labels = set(list(cfg_a.keys()) + list(cfg_b.keys()))
    for label in sorted(all_labels):
        a = cfg_a.get(label, {})
        b = cfg_b.get(label, {})
        if not a and b:
            changes.append(f"{label}: added")
            continue
        if a and not b:
            changes.append(f"{label}: removed")
            continue
        for k in sorted(set(list(a.keys()) + list(b.keys()))):
            va, vb = a.get(k), b.get(k)
            if va != vb and va is not None and vb is not None:
                changes.append(f"{label}.{k}: {va}→{vb}")
            elif va is None and vb is not None:
                changes.append(f"{label}.{k}: +{vb}")
            elif va is not None and vb is None:
                changes.append(f"{label}.{k}: -{va}")
    return changes


def load_deploy_history_snapshots(data_dir):
    """
    Parse deploy_history/strategies_config_YYYYMMDD_HHMMSS.py files.
    Returns list of {dt, filepath, cfg_snapshot, changes_from_prev}.
    """
    history_dir = os.path.join(data_dir, 'deploy_history')
    if not os.path.isdir(history_dir):
        return []

    snapshots = []
    pattern = re.compile(r'strategies_config_(\d{8}_\d{6})\.py$')
    for fname in sorted(os.listdir(history_dir)):
        m = pattern.match(fname)
        if not m:
            continue
        try:
            dt = datetime.strptime(m.group(1), '%Y%m%d_%H%M%S')
        except ValueError:
            continue
        fpath = os.path.join(history_dir, fname)
        snapshots.append({'dt': dt, 'filepath': fpath, 'cfg': None, 'changes': []})

    # Parse configs and diff consecutive pairs
    prev_cfg = {}
    for snap in snapshots:
        snap['cfg'] = _parse_config_snapshot(snap['filepath'])
        if prev_cfg:
            snap['changes'] = _diff_configs(prev_cfg, snap['cfg'])
        prev_cfg = snap['cfg']

    print(f"  Loaded {len(snapshots)} config snapshots from deploy_history/", file=sys.stderr)
    return snapshots


def load_deployments(data_dir=None, path=None):
    """
    Merge all deploy log sources into a single sorted list.
    Sources (in priority order, deduped by timestamp within 60s):
      1. deployments.log    — YYYYMMDD_HHMMSS | kind | note
      2. .deploy_history    — YYYY-MM-DD HH:MM:SS | note
      3. deploy.log         — prose, extract timestamps + headings
      4. deploy_history/    — filenames give timestamps (no notes, but have config diffs)
    """
    if data_dir is None:
        data_dir = os.path.dirname(path) if path else DATA_DIR
    deploys = []
    seen_ts = {}  # ts_rounded -> index in deploys (for dedup)

    def _add(dt, kind, note, source):
        # Dedup: if another entry within 60s exists, merge notes
        key = dt.replace(second=0, microsecond=0)
        if key in seen_ts:
            idx = seen_ts[key]
            if note and note not in deploys[idx]['note']:
                deploys[idx]['note'] += ' / ' + note
            return
        seen_ts[key] = len(deploys)
        deploys.append({'dt': dt, 'kind': kind, 'note': note, 'source': source,
                        'cfg_changes': []})

    # ── 1. deployments.log ──
    dlog = os.path.join(data_dir, 'deployments.log')
    try:
        with open(dlog) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = [p.strip() for p in line.split('|', 2)]
                try:
                    ts = datetime.strptime(parts[0], '%Y%m%d_%H%M%S')
                except ValueError:
                    continue
                kind = parts[1] if len(parts) > 1 else 'full'
                note = parts[2] if len(parts) > 2 else ''
                _add(ts, kind, note, 'deployments.log')
    except FileNotFoundError:
        pass

    # ── 2. .deploy_history (hidden file) ──
    dhist = os.path.join(data_dir, '.deploy_history')
    try:
        with open(dhist) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                # Format: "YYYY-MM-DD HH:MM:SS | note"
                m = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s*\|\s*(.*)', line)
                if m:
                    try:
                        ts = datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S')
                        _add(ts, 'full', m.group(2).strip(), '.deploy_history')
                    except ValueError:
                        pass
    except FileNotFoundError:
        pass

    # ── 3. deploy.log (prose) ──
    deploy_log = os.path.join(data_dir, 'deploy.log')
    try:
        with open(deploy_log, encoding='utf-8', errors='replace') as f:
            content = f.read()
        # Pattern: "2026-05-12 20:02:16  deploy.sh start"
        for m in re.finditer(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+deploy\.sh start', content):
            try:
                ts = datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S')
                _add(ts, 'deploy.sh', 'deploy.sh start', 'deploy.log')
            except ValueError:
                pass
    except FileNotFoundError:
        pass

    # ── 4. deploy_history/ filenames ──
    history_dir = os.path.join(data_dir, 'deploy_history')
    if os.path.isdir(history_dir):
        pattern = re.compile(r'strategies_config_(\d{8}_\d{6})\.py$')
        for fname in sorted(os.listdir(history_dir)):
            m = pattern.match(fname)
            if not m:
                continue
            try:
                ts = datetime.strptime(m.group(1), '%Y%m%d_%H%M%S')
                _add(ts, 'config', '', 'deploy_history')
            except ValueError:
                pass

    deploys_sorted = sorted(deploys, key=lambda x: x['dt'])

    # Attach config diffs from deploy_history snapshots
    snapshots = load_deploy_history_snapshots(data_dir)
    if snapshots:
        snap_by_ts = {s['dt'].replace(second=0, microsecond=0): s for s in snapshots}
        for d in deploys_sorted:
            key = d['dt'].replace(second=0, microsecond=0)
            if key in snap_by_ts:
                d['cfg_changes'] = snap_by_ts[key].get('changes', [])
                if d['cfg_changes'] and not d['note']:
                    # Use first few changes as the note if no note exists
                    d['note'] = '; '.join(d['cfg_changes'][:3])

    total = len(deploys_sorted)
    sourced = {}
    for d in deploys_sorted:
        sourced[d['source']] = sourced.get(d['source'], 0) + 1
    src_str = ', '.join(f"{v} from {k}" for k, v in sorted(sourced.items()))
    print(f"  Loaded {total} deploy events ({src_str})", file=sys.stderr)
    return deploys_sorted

def get_deploy_cutoff(deploys, n=1):
    if not deploys:
        print("❌ No deployments.log found", file=sys.stderr); sys.exit(1)
    if n > len(deploys):
        print(f"❌ Requested deploy:{n} but only {len(deploys)} entries", file=sys.stderr); sys.exit(1)
    return deploys[-n]['dt']

def parse_since(since_str, deploys):
    s = since_str.strip().lower()
    m = re.fullmatch(r'deploy(?::(\d+))?', s)
    if m:
        n = int(m.group(1)) if m.group(1) else 1
        return get_deploy_cutoff(deploys, n)
    m = re.fullmatch(r'(\d+(?:\.\d+)?)\s*d(?:ays?)?', s)
    if m:
        return datetime.now() - timedelta(days=float(m.group(1)))
    m = re.fullmatch(r'(\d+(?:\.\d+)?)\s*h(?:ours?)?', s)
    if m:
        return datetime.now() - timedelta(hours=float(m.group(1)))
    for fmt in ('%Y-%m-%dT%H:%M', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(since_str.strip(), fmt)
        except ValueError:
            pass
    print(f"❌ Cannot parse --since: {since_str!r}", file=sys.stderr); sys.exit(1)


# ══════════════════════════════════════════════════════════════════
# DATA LOADING
# ══════════════════════════════════════════════════════════════════

def load_all_sessions(data_dir, cutoff=None, strategy_filter=None, env_filter=None):
    """
    Walk data_dir, find all session folders (YYYYMMDD_HHMMSS, optionally with a
    _prod / _stage / _shadow suffix), load preds CSVs.
    env_filter: None/'all' = every session; 'prod'/'stage'/'legacy' = that env only
                ('legacy' = old bare-named sessions with no suffix).
    Returns list of normalised trade dicts.
    """
    # Match bare AND suffixed session dirs: 20260616_205337  /  20260616_205337_stage
    session_pattern = re.compile(r'^(\d{8}_\d{6})(?:_(\w+))?$')
    all_trades = []

    session_dirs = []
    for d in os.listdir(data_dir):
        m = session_pattern.match(d)
        if not m or not os.path.isdir(os.path.join(data_dir, d)):
            continue
        env = (m.group(2) or 'legacy').lower()
        if env_filter and env_filter != 'all' and env != env_filter:
            continue
        session_dirs.append(os.path.join(data_dir, d))
    session_dirs = sorted(session_dirs)

    for session_dir in session_dirs:
        # Parse session datetime from the timestamp portion (ignore any _suffix)
        dname = os.path.basename(session_dir)
        m = session_pattern.match(dname)
        try:
            session_dt = datetime.strptime(m.group(1), '%Y%m%d_%H%M%S')
        except (ValueError, AttributeError):
            continue

        # Fast-skip: session entirely before cutoff
        if cutoff and session_dt < cutoff:
            # Could still contain later files; but for perf, skip sessions > 2 days before cutoff
            if cutoff - session_dt > timedelta(days=2):
                continue

        # Find preds CSVs in prod/, shadow/, or root
        # preds_b_ (_b variants) may live in shadow/ or alongside prod files
        search_dirs = [
            os.path.join(session_dir, 'prod'),
            os.path.join(session_dir, 'shadow'),
            session_dir,
        ]
        for sd in search_dirs:
            if not os.path.isdir(sd):
                continue
            for fpath in glob.glob(os.path.join(sd, 'preds_*.csv')):
                trades = load_preds_file(fpath, session_dt)
                for t in trades:
                    if cutoff and t['dt'] and t['dt'] < cutoff:
                        continue
                    if strategy_filter and t['strategy'] not in strategy_filter:
                        continue
                    all_trades.append(t)

    print(f"  Loaded {len(all_trades)} closed trades from {len(session_dirs)} sessions", file=sys.stderr)
    return all_trades


# ══════════════════════════════════════════════════════════════════
# ANALYSIS
# ══════════════════════════════════════════════════════════════════

def bucket_by_session(trades):
    """Group trades by (strategy, session_date_str)."""
    by_strat_session = defaultdict(lambda: defaultdict(list))
    for t in trades:
        dt = t['session_dt'] or t['dt']
        if dt is None:
            continue
        day = dt.strftime('%Y-%m-%d')
        by_strat_session[t['strategy']][day].append(t)
    return by_strat_session

def session_stats(trades):
    """Compute stats for a list of trades in one session."""
    closed = [t for t in trades if t['outcome'] in ('win', 'lose', 'flat')]
    if not closed:
        return None
    n       = len(closed)
    wins    = sum(1 for t in closed if t['outcome'] == 'win')
    losses  = sum(1 for t in closed if t['outcome'] == 'lose')
    decided = wins + losses
    wr      = wins / decided * 100 if decided > 0 else None
    nets    = [t['net_exit'] for t in closed if t['net_exit'] is not None]
    avg_net = sum(nets) / len(nets) if nets else None
    cum_net = sum(nets) if nets else 0

    exits = defaultdict(int)
    for t in closed:
        exits[t['reason']] += 1

    scores  = [t['score'] for t in closed if t['score'] is not None]
    confs   = [t['conf']  for t in closed if t['conf']  is not None]

    # Config snapshot from first trade's header
    cfg = closed[0].get('cfg', {})

    return {
        'n':        n,
        'wins':     wins,
        'losses':   losses,
        'wr':       round(wr, 1) if wr is not None else None,
        'avg_net':  round(avg_net, 4) if avg_net is not None else None,
        'cum_net':  round(cum_net, 4),
        'exits':    dict(exits),
        'score_avg': round(sum(scores)/len(scores), 1) if scores else None,
        'conf_avg':  round(sum(confs)/len(confs), 1)   if confs  else None,
        'cfg':      cfg,
    }

def build_timeline(trades):
    """Build per-strategy session timelines."""
    by_strat_session = bucket_by_session(trades)
    timeline = {}
    for strat, sessions in sorted(by_strat_session.items()):
        days = []
        for day in sorted(sessions.keys()):
            stats = session_stats(sessions[day])
            if stats:
                stats['day'] = day
                days.append(stats)
        if days:
            timeline[strat] = days
    return timeline

def overall_stats(trades):
    """Per-strategy aggregate stats."""
    by_strat = defaultdict(list)
    for t in trades:
        by_strat[t['strategy']].append(t)
    result = {}
    for strat, ts in sorted(by_strat.items()):
        s = session_stats(ts)
        if s:
            s['strategy'] = strat
            result[strat] = s
    return result


# ══════════════════════════════════════════════════════════════════
# HTML OUTPUT
# ══════════════════════════════════════════════════════════════════

def _js_array(values, none_as='null'):
    parts = []
    for v in values:
        if v is None:
            parts.append(none_as)
        elif isinstance(v, str):
            parts.append(f'"{v}"')
        else:
            parts.append(str(v))
    return '[' + ', '.join(parts) + ']'

def _exit_breakdown(exits, n):
    if not exits or n == 0:
        return '—'
    order = ['trail','sl','time','inertia','tp','rev','flat','unknown']
    colors = {
        'trail':   '#4ade80',
        'sl':      '#f87171',
        'time':    '#fbbf24',
        'inertia': '#60a5fa',
        'tp':      '#34d399',
        'rev':     '#a78bfa',
        'flat':    '#94a3b8',
        'unknown': '#64748b',
    }
    parts = []
    for k in order + [k for k in exits if k not in order]:
        cnt = exits.get(k, 0)
        if cnt == 0:
            continue
        pct = cnt / n * 100
        col = colors.get(k, '#888')
        parts.append(f'<span style="color:{col}">{k}:{cnt}({pct:.0f}%)</span>')
    return ' &nbsp; '.join(parts)

def _cfg_diff(cfg_a, cfg_b):
    """Return HTML showing what changed between two config dicts."""
    keys = set(list(cfg_a.keys()) + list(cfg_b.keys())) - {'strategy_name','_header_line'}
    diffs = []
    for k in sorted(keys):
        va, vb = cfg_a.get(k), cfg_b.get(k)
        if va != vb and va is not None and vb is not None:
            diffs.append(f'{k}: <span style="color:#f87171">{va}</span>→<span style="color:#4ade80">{vb}</span>')
    return ' &nbsp;|&nbsp; '.join(diffs) if diffs else ''

def format_html(timeline, overall, deploys, since_label, strategy_filter):
    """Generate self-contained HTML with Chart.js timeline charts."""
    strats = sorted(timeline.keys())
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Build deploy markers JS array
    deploy_markers_js = json.dumps([
        {'dt': d['dt'].strftime('%Y-%m-%d'), 'note': d['note'][:60]}
        for d in deploys
    ])

    # Build chart datasets per strategy
    chart_blocks = []
    for strat in strats:
        days_data = timeline[strat]
        labels     = [d['day']     for d in days_data]
        wr_data    = [d['wr']      for d in days_data]
        net_data   = [d['avg_net'] for d in days_data]
        n_data     = [d['n']       for d in days_data]
        trail_pct  = [round(d['exits'].get('trail',0)/d['n']*100) for d in days_data]
        sl_pct     = [round(d['exits'].get('sl',0)/d['n']*100)    for d in days_data]
        time_pct   = [round(d['exits'].get('time',0)/d['n']*100)  for d in days_data]
        inertia_pct= [round(d['exits'].get('inertia',0)/d['n']*100) for d in days_data]

        chart_blocks.append({
            'strat':       strat,
            'labels':      labels,
            'wr':          wr_data,
            'net':         net_data,
            'n':           n_data,
            'trail_pct':   trail_pct,
            'sl_pct':      sl_pct,
            'time_pct':    time_pct,
            'inertia_pct': inertia_pct,
            'days':        days_data,
        })

    # Summary table rows
    summary_rows = []
    for strat in strats:
        s = overall.get(strat, {})
        cfg_note = s.get('cfg', {}).get('strategy_name', '') or s.get('cfg', {}).get('notes', '')
        wr_col = f'<span style="color:{"#4ade80" if (s.get("wr") or 0) >= 50 else "#f87171"}">{s.get("wr","?"):.1f}%</span>' if s.get('wr') is not None else '?'
        net = s.get('avg_net', 0) or 0
        net_col = f'<span style="color:{"#4ade80" if net >= 0 else "#f87171"}">{net:+.4f}%</span>'
        exits_html = _exit_breakdown(s.get('exits', {}), s.get('n', 0))
        sessions_count = len(timeline.get(strat, []))
        summary_rows.append(
            f'<tr>'
            f'<td><a href="#{strat}" style="color:#7dd3fc;text-decoration:none">{strat}</a></td>'
            f'<td>{s.get("n","?")}</td>'
            f'<td>{sessions_count}</td>'
            f'<td>{wr_col}</td>'
            f'<td>{net_col}</td>'
            f'<td>{s.get("score_avg","?")}</td>'
            f'<td style="font-size:0.8em">{exits_html}</td>'
            f'</tr>'
        )

    # Per-strategy chart + table sections
    strat_sections = []
    for cb in chart_blocks:
        strat = cb['strat']
        days  = cb['days']

        # Config evolution table
        cfg_rows = []
        prev_cfg = {}
        for i, d in enumerate(days):
            cfg = d.get('cfg', {})
            diff_html = _cfg_diff(prev_cfg, cfg) if i > 0 and prev_cfg != cfg else ''
            key_params = []
            for k in ['sl_mult','inertia','max_window','min_vpin','min_conf','win_thr']:
                if k in cfg:
                    key_params.append(f'{k}={cfg[k]}')
            params_str = ' &nbsp; '.join(key_params)
            change_indicator = f' <span style="color:#fbbf24;font-size:0.75em">⚙ {diff_html}</span>' if diff_html else ''
            _wr_cell  = '?%</td>' if d['wr']      is None else f'<span>{d["wr"]:.1f}%</span></td>'
            _net_cell = '?%</td>' if d['avg_net'] is None else f'<span>{d["avg_net"]:+.4f}%</span></td>'
            _wr_col   = '#4ade80' if (d['wr']      or 0) >= 50 else '#f87171'
            _net_col  = '#4ade80' if (d['avg_net'] or 0) >= 0  else '#f87171'
            cfg_rows.append(
                f'<tr>'
                f'<td style="color:#94a3b8">{d["day"]}</td>'
                f'<td>{d["n"]}</td>'
                f'<td style="color:{_wr_col}">{_wr_cell}'
                f'<td style="color:{_net_col}">{_net_cell}'
                f'<td style="font-size:0.78em">{_exit_breakdown(d["exits"], d["n"])}</td>'
                f'<td style="font-size:0.75em;color:#94a3b8">{params_str}{change_indicator}</td>'
                f'</tr>'
            )

        cfg_table = f'''
        <table class="cfg-table">
          <thead><tr>
            <th>Session</th><th>Trades</th><th>WR%</th><th>Avg/T</th>
            <th>Exit Breakdown</th><th>Config at run-time</th>
          </tr></thead>
          <tbody>{"".join(cfg_rows)}</tbody>
        </table>''' if cfg_rows else ''

        _n_trades = overall.get(strat, {}).get('n', '?')
        strat_sections.append(f'''
  <section id="{strat}" class="strat-section">
    <h2 class="strat-header">
      <span class="strat-label">{strat}</span>
      <span class="strat-meta">{len(days)} sessions &nbsp;·&nbsp;
        {_n_trades} trades</span>
    </h2>
    <div class="chart-grid">
      <div class="chart-wrap">
        <div class="chart-title">Win Rate % per session</div>
        <canvas id="wr_{strat}" height="120"></canvas>
      </div>
      <div class="chart-wrap">
        <div class="chart-title">Avg net %/trade per session</div>
        <canvas id="net_{strat}" height="120"></canvas>
      </div>
      <div class="chart-wrap">
        <div class="chart-title">Exit distribution % per session</div>
        <canvas id="exit_{strat}" height="120"></canvas>
      </div>
    </div>
    {cfg_table}
  </section>''')

    # Build JS chart init — no annotation plugin, deploy markers via point colors + border
    chart_js_blocks = []
    for cb in chart_blocks:
        s = cb['strat']
        labs = json.dumps(cb['labels'])
        deploys_on = json.dumps([
            d['dt'].strftime('%Y-%m-%d') for d in deploys
            if cb['labels'] and cb['labels'][0] <= d['dt'].strftime('%Y-%m-%d') <= cb['labels'][-1]
        ]) if cb['labels'] else '[]'

        # Per-point colors for WR line: amber on deploy days
        wr_point_colors = json.dumps([
            '#fbbf24' if (cb['labels'][i] in [
                d['dt'].strftime('%Y-%m-%d') for d in deploys
            ]) else '#7dd3fc'
            for i in range(len(cb['labels']))
        ])
        wr_point_radius = json.dumps([
            7 if (cb['labels'][i] in [
                d['dt'].strftime('%Y-%m-%d') for d in deploys
            ]) else 4
            for i in range(len(cb['labels']))
        ])

        # Deploy note lookup for tooltip
        deploy_notes_js = json.dumps({
            d['dt'].strftime('%Y-%m-%d'): d['note'][:50]
            for d in deploys
        })

        chart_js_blocks.append(f'''
  // ── {s} ──
  (function() {{
    var labels = {labs};
    var deployDts = {deploys_on};
    var deployNotes = {deploy_notes_js};

    // Win Rate
    new Chart(document.getElementById('wr_{s}'), {{
      type: 'line',
      data: {{
        labels: labels,
        datasets: [{{
          label: 'WR%', data: {json.dumps(cb['wr'])},
          borderColor: '#7dd3fc', backgroundColor: 'rgba(125,211,252,0.08)',
          tension: 0.3,
          pointRadius: {wr_point_radius},
          pointHoverRadius: 8,
          pointBackgroundColor: {wr_point_colors},
          pointBorderColor: {wr_point_colors},
          fill: true, spanGaps: true,
        }}]
      }},
      options: {{
        responsive: true, maintainAspectRatio: true,
        plugins: {{
          legend: {{ display: false }},
          tooltip: {{ callbacks: {{
            label: function(ctx) {{
              var v = ctx.parsed.y;
              var note = deployNotes[ctx.label] || '';
              var s = v !== null ? v.toFixed(1)+'%' : 'N/A';
              return note ? [s, '⚙ '+note] : s;
            }}
          }} }},
        }},
        scales: {{
          y: {{ grid: {{ color: '#1e2a3a' }}, ticks: {{ color: '#64748b', callback: function(v) {{ return v+'%'; }} }},
                 suggestedMin: 0, suggestedMax: 100 }},
          x: {{ grid: {{ color: '#1e2a3a' }}, ticks: {{ color: '#64748b', maxRotation: 45 }} }}
        }}
      }}
    }});

    // Avg net/T
    var netData = {json.dumps(cb['net'])};
    new Chart(document.getElementById('net_{s}'), {{
      type: 'bar',
      data: {{
        labels: labels,
        datasets: [{{
          label: 'avg net %/T', data: netData,
          backgroundColor: netData.map(function(v, i) {{
            if (v === null) return '#334155';
            return deployDts.indexOf(labels[i]) >= 0
              ? (v >= 0 ? 'rgba(251,191,36,0.85)' : 'rgba(251,100,36,0.85)')
              : (v >= 0 ? 'rgba(74,222,128,0.7)' : 'rgba(248,113,113,0.7)');
          }}),
          borderRadius: 3,
        }}]
      }},
      options: {{
        responsive: true, maintainAspectRatio: true,
        plugins: {{
          legend: {{ display: false }},
          tooltip: {{ callbacks: {{
            label: function(ctx) {{
              var v = ctx.parsed.y;
              var note = deployNotes[ctx.label] || '';
              var s = v !== null ? v.toFixed(4)+'%' : 'N/A';
              return note ? [s, '⚙ '+note] : s;
            }}
          }} }},
        }},
        scales: {{
          y: {{ grid: {{ color: '#1e2a3a' }}, ticks: {{ color: '#64748b', callback: function(v) {{ return v.toFixed(3)+'%'; }} }} }},
          x: {{ grid: {{ color: '#1e2a3a' }}, ticks: {{ color: '#64748b', maxRotation: 45 }} }}
        }}
      }}
    }});

    // Exit distribution stacked bar
    new Chart(document.getElementById('exit_{s}'), {{
      type: 'bar',
      data: {{
        labels: labels,
        datasets: [
          {{ label: 'trail',   data: {json.dumps(cb['trail_pct'])},   backgroundColor: 'rgba(74,222,128,0.8)',  stack: 'exit' }},
          {{ label: 'sl',      data: {json.dumps(cb['sl_pct'])},      backgroundColor: 'rgba(248,113,113,0.8)', stack: 'exit' }},
          {{ label: 'time',    data: {json.dumps(cb['time_pct'])},    backgroundColor: 'rgba(251,191,36,0.8)',  stack: 'exit' }},
          {{ label: 'inertia', data: {json.dumps(cb['inertia_pct'])}, backgroundColor: 'rgba(96,165,250,0.8)',  stack: 'exit' }},
        ]
      }},
      options: {{
        responsive: true, maintainAspectRatio: true,
        plugins: {{
          legend: {{ position: 'bottom', labels: {{ color: '#94a3b8', boxWidth: 12, padding: 8 }} }},
        }},
        scales: {{
          y: {{ stacked: true, max: 100, grid: {{ color: '#1e2a3a' }},
                ticks: {{ color: '#64748b', callback: function(v) {{ return v+'%'; }} }} }},
          x: {{ stacked: true, grid: {{ color: '#1e2a3a' }},
                ticks: {{ color: '#64748b', maxRotation: 45 }} }}
        }}
      }}
    }});
  }})();''')

    since_note = f'<span class="meta-pill">since {since_label}</span>' if since_label else ''
    filter_note = f'<span class="meta-pill">strategy: {", ".join(strategy_filter)}</span>' if strategy_filter else ''

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PredictEngine — Performance Timeline</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;700&family=Syne:wght@400;700;800&display=swap');

  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

  :root {{
    --bg:      #050c17;
    --surface: #0a1628;
    --border:  #1a2744;
    --accent:  #3b82f6;
    --accent2: #06b6d4;
    --green:   #4ade80;
    --red:     #f87171;
    --amber:   #fbbf24;
    --text:    #e2e8f0;
    --dim:     #64748b;
    --dim2:    #94a3b8;
  }}

  html {{ scroll-behavior: smooth; }}

  body {{
    font-family: 'JetBrains Mono', monospace;
    background: var(--bg);
    color: var(--text);
    font-size: 13px;
    line-height: 1.6;
    overflow-x: hidden;
  }}

  /* — Scanline texture — */
  body::before {{
    content: '';
    position: fixed; inset: 0;
    background: repeating-linear-gradient(
      0deg, transparent, transparent 2px,
      rgba(0,0,0,0.03) 2px, rgba(0,0,0,0.03) 4px
    );
    pointer-events: none; z-index: 9999;
  }}

  /* — Header — */
  .site-header {{
    position: sticky; top: 0; z-index: 100;
    background: rgba(5,12,23,0.96);
    backdrop-filter: blur(12px);
    border-bottom: 1px solid var(--border);
    padding: 12px 28px;
    display: flex; align-items: center; gap: 16px;
    flex-wrap: wrap;
  }}

  .site-title {{
    font-family: 'Syne', sans-serif;
    font-size: 1.1rem; font-weight: 800;
    letter-spacing: 0.05em;
    background: linear-gradient(135deg, var(--accent2), var(--accent));
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
  }}

  .meta-pill {{
    font-size: 0.72rem; padding: 2px 10px;
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 99px; color: var(--dim2);
  }}

  /* — Nav sidebar — */
  .layout {{
    display: flex;
    min-height: calc(100vh - 50px);
  }}

  .sidebar {{
    width: 72px;
    position: sticky; top: 50px;
    height: calc(100vh - 50px);
    overflow-y: auto;
    background: var(--surface);
    border-right: 1px solid var(--border);
    padding: 16px 0;
    flex-shrink: 0;
    display: flex; flex-direction: column; gap: 4px; align-items: center;
  }}

  .sidebar a {{
    display: flex; align-items: center; justify-content: center;
    width: 48px; height: 36px;
    border-radius: 8px;
    font-family: 'Syne', sans-serif;
    font-weight: 700; font-size: 0.85rem;
    color: var(--dim); text-decoration: none;
    transition: all 0.15s;
  }}
  .sidebar a:hover {{ background: var(--border); color: var(--text); }}

  /* — Main content — */
  .main {{
    flex: 1; padding: 28px 32px; min-width: 0;
  }}

  /* — Summary table — */
  .summary-section {{ margin-bottom: 40px; }}

  .section-title {{
    font-family: 'Syne', sans-serif;
    font-size: 0.75rem; font-weight: 700;
    letter-spacing: 0.12em; text-transform: uppercase;
    color: var(--dim); margin-bottom: 14px;
  }}

  table {{ width: 100%; border-collapse: collapse; }}
  th, td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid var(--border); }}
  th {{ font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.08em;
        color: var(--dim); background: var(--surface); font-weight: 500; }}
  tr:hover td {{ background: rgba(59,130,246,0.04); }}

  /* — Strategy sections — */
  .strat-section {{
    margin-bottom: 56px;
    padding-top: 20px;
    border-top: 1px solid var(--border);
  }}

  .strat-header {{
    display: flex; align-items: baseline; gap: 14px;
    margin-bottom: 20px;
  }}

  .strat-label {{
    font-family: 'Syne', sans-serif;
    font-size: 1.6rem; font-weight: 800;
    background: linear-gradient(135deg, var(--accent2), var(--accent));
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
  }}

  .strat-meta {{ font-size: 0.8rem; color: var(--dim); }}

  /* — Charts — */
  .chart-grid {{
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 16px;
    margin-bottom: 20px;
  }}
  @media (max-width: 900px) {{ .chart-grid {{ grid-template-columns: 1fr; }} }}

  .chart-wrap {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 14px 16px;
  }}

  .chart-title {{
    font-size: 0.7rem; text-transform: uppercase;
    letter-spacing: 0.08em; color: var(--dim);
    margin-bottom: 10px;
  }}

  /* — Config table — */
  .cfg-table {{ font-size: 0.8em; margin-top: 8px; }}
  .cfg-table th {{ font-size: 0.68rem; }}
  .cfg-table td {{ padding: 6px 10px; }}

  /* — Deploy legend — */
  .deploy-legend {{
    display: flex; align-items: center; gap: 8px;
    margin-bottom: 20px; font-size: 0.75rem; color: var(--dim);
  }}
  .deploy-dot {{
    width: 10px; height: 10px; border-radius: 2px;
    background: rgba(251,191,36,0.5); border: 1px solid var(--amber);
  }}
</style>
</head>
<body>

<header class="site-header">
  <span class="site-title">⚡ PredictEngine</span>
  <span class="meta-pill">Performance Timeline</span>
  {since_note}
  {filter_note}
  <span class="meta-pill" style="margin-left:auto;color:var(--dim2)">{now_str}</span>
</header>

<div class="layout">
  <nav class="sidebar">
    <a href="#summary" title="Summary">∑</a>
    {"".join(f'<a href="#{s}" title="{s}">{s}</a>' for s in strats)}
  </nav>

  <main class="main">

    <!-- Summary -->
    <section id="summary" class="summary-section">
      <div class="section-title">Overall Summary</div>
      <div class="deploy-legend">
        <span class="deploy-dot"></span>
        <span>Yellow vertical lines = deploy events (config changes)</span>
      </div>
      <table>
        <thead><tr>
          <th>Strategy</th><th>Trades</th><th>Sessions</th>
          <th>WR%</th><th>Avg/T</th><th>Score@fire</th><th>Exit Breakdown</th>
        </tr></thead>
        <tbody>{"".join(summary_rows)}</tbody>
      </table>
    </section>

    <!-- Per-strategy sections -->
    {"".join(strat_sections)}

  </main>
</div>

<script>
{chr(10).join(chart_js_blocks)}
</script>
</body>
</html>'''

    return html


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════

def save_json(timeline, overall, deploys, out_path):
    """Save analysis as structured JSON for external processing."""
    def _serial(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, set):
            return list(obj)
        raise TypeError(f"Not serializable: {type(obj)}")

    out = {
        "generated_at": datetime.now().isoformat(),
        "summary": {},
        "strategies": {},
        "deploys": [{"dt": d["dt"].isoformat(), "kind": d["kind"], "note": d["note"]} for d in deploys],
    }

    for strat, s in sorted(overall.items()):
        out["summary"][strat] = {
            "trades":    s.get("n"),
            "wins":      s.get("wins"),
            "losses":    s.get("losses"),
            "wr_pct":    s.get("wr"),
            "avg_net":   s.get("avg_net"),
            "cum_net":   s.get("cum_net"),
            "score_avg": s.get("score_avg"),
            "conf_avg":  s.get("conf_avg"),
            "exits":     s.get("exits", {}),
        }

    for strat, days in sorted(timeline.items()):
        sessions = []
        for d in days:
            sessions.append({
                "day":       d["day"],
                "trades":    d["n"],
                "wins":      d["wins"],
                "losses":    d["losses"],
                "wr_pct":    d["wr"],
                "avg_net":   d["avg_net"],
                "cum_net":   d["cum_net"],
                "score_avg": d["score_avg"],
                "conf_avg":  d["conf_avg"],
                "exits":     d["exits"],
                "config":    {k: v for k, v in d.get("cfg", {}).items()
                              if k not in ("_header_line",)},
            })
        out["strategies"][strat] = sessions

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, default=_serial)
    print(f"  JSON: {out_path}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description='Strategy performance timeline analyzer')
    parser.add_argument('--data',     default=DATA_DIR, help='data_backup directory')
    parser.add_argument('--since',    help='Filter: deploy / deploy:2 / 7d / 3h / 2026-05-22')
    parser.add_argument('--strategy', help='Filter to specific strategy label(s), comma-separated, e.g. K,W,B')
    parser.add_argument('--out',      help='Output HTML file (default: auto-named)')
    parser.add_argument('--json',     action='store_true', help='Also write .json alongside HTML')
    parser.add_argument('--json-only',action='store_true', help='Write only JSON, skip HTML')
    args = parser.parse_args()

    deploys = load_deployments(data_dir=args.data)
    print(f"  Loaded {len(deploys)} deploy events", file=sys.stderr)

    cutoff = parse_since(args.since, deploys) if args.since else None
    if cutoff:
        print(f"  Cutoff: {cutoff.strftime('%Y-%m-%d %H:%M:%S')}", file=sys.stderr)

    # Support K, K_b, K_B — normalise to K_b form for _b variants
    if args.strategy:
        raw_labels = [l.strip() for l in args.strategy.split(',')]
        strategy_filter = set()
        for l in raw_labels:
            u = l.upper()
            if u.endswith('_B'):
                strategy_filter.add(u[:-2] + '_b')  # K_B → K_b
            else:
                strategy_filter.add(u)
    else:
        strategy_filter = None

    print("📊 Loading sessions...", file=sys.stderr)
    trades = load_all_sessions(args.data, cutoff=cutoff, strategy_filter=strategy_filter)

    if not trades:
        print("❌ No closed trades found. Check --data path and --since filter.", file=sys.stderr)
        sys.exit(1)

    print("📈 Building timeline...", file=sys.stderr)
    timeline = build_timeline(trades)
    overall  = overall_stats(trades)

    ts_str   = datetime.now().strftime('%Y%m%d_%H%M%S')
    strat_count = len(timeline)
    total_days  = sum(len(v) for v in timeline.values())

    # JSON output
    json_out = None
    if args.json or getattr(args, 'json_only', False):
        json_out = args.out.replace('.html', '.json') if args.out and args.out.endswith('.html') else f"analysis_perf_{ts_str}.json"
        save_json(timeline, overall, deploys, json_out)

    # HTML output
    if not getattr(args, 'json_only', False):
        since_label = cutoff.strftime('%Y-%m-%d %H:%M') if cutoff else None
        html = format_html(timeline, overall, deploys, since_label, strategy_filter)
        out_file = args.out or f"analysis_perf_{ts_str}.html"
        with open(out_file, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f"\n✅ {out_file}", file=sys.stderr)
        print(f"   {strat_count} strategies · {len(trades)} trades · {total_days} strategy-sessions", file=sys.stderr)
        if sys.platform == 'darwin':
            print(f"   open {out_file}", file=sys.stderr)
    else:
        print(f"\n✅ {json_out}", file=sys.stderr)
        print(f"   {strat_count} strategies · {len(trades)} trades · {total_days} strategy-sessions", file=sys.stderr)

if __name__ == '__main__':
    main()
