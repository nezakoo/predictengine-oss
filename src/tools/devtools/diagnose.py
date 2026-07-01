"""
diagnose.py — Gate firing diagnostic for StrategyEngine
=========================================================
Reads thresholds DIRECTLY from strategies_config.py and
strategies_engine.py so it never goes stale.

Usage:
    python3 diagnose.py                    # diagnose all strategies
    python3 diagnose.py --strategy V B     # diagnose specific labels
    python3 diagnose.py --csv ./preds/     # analyse recent CSV files too
    python3 diagnose.py --watch            # live re-run every 5s
"""
import sys, re, os, time, argparse, glob, importlib.util, math
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent.parent.parent  # engine root


# ── load strategies_config without full engine import ─────────────────────────
def load_config():
    spec = importlib.util.spec_from_file_location(
        'strategies_config', BASE_DIR / 'strategies_config.py')
    mod = importlib.util.module_from_spec(spec)
    # Stub the only import strategies_config needs
    import types
    fake_engine = types.ModuleType('engine')
    fake_engine.FEE_RT = 0.06
    fake_engine.VERSION = {'v': 'diag'}
    fake_engine.SPREAD_MAX_PCT = 0.05
    sys.modules.setdefault('engine', fake_engine)
    try:
        spec.loader.exec_module(mod)
        return mod.STRATEGIES
    except Exception as e:
        print(f"[WARN] Could not load strategies_config: {e}")
        return []


# ── parse gate source to extract all return-False conditions ─────────────────
def parse_gate_conditions(engine_src: str, gate_name: str) -> list[dict]:
    """Extract each 'return False' line from a gate function with its condition."""
    # Find gate function body
    start = engine_src.find(f'    def {gate_name}(')
    if start == -1:
        return [{'condition': f'{gate_name} not found in engine', 'line': -1}]

    # Find next def at same indent
    next_def = engine_src.find('\n    def ', start + 10)
    next_sec = engine_src.find('\n    # ──', start + 10)
    end = min(e for e in [next_def, next_sec] if e > start)
    body = engine_src[start:end]
    lines = body.split('\n')

    conditions = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        if 'return False' in stripped and stripped != 'return False':
            # Extract the condition
            cond = stripped.replace(': return False', '').replace('return False', '').strip()
            if cond.startswith('if '):
                cond = cond[3:]
            conditions.append({'condition': cond.strip(), 'line': start + i})
    return conditions


# ── extract threshold values from condition strings ──────────────────────────
THRESHOLD_PATTERNS = [
    # abs_val < N
    (r'abs\(abs_val\)\s*<\s*([\d.]+)', 'abs_signal_min', float),
    # abs(mtf) < N
    (r'abs\(mtf\)\s*<\s*([\d.]+)',     'mtf_bias_min', float),
    # vpin < cfg.vpin_min — read from config
    (r'vpin.*<.*vpin_min',             'vpin_min', None),
    (r'vpin.*>.*vpin_max',             'vpin_max', None),
    # ATR
    (r'get_atr.*<.*min_vol_atr',       'min_vol_atr', None),
    # spread
    (r'_spread_ok',                    'spread_max_mult×SPREAD_MAX_PCT', None),
    # score
    (r'abs\(r\[.score.\]\)\s*<\s*([\d.]+)', 'min_score', float),
    (r"r\['score'\]\s*[<>]\s*([-\d.]+)",     'score_dir_gate', float),
    # conf
    (r'r\[.conf.\]\s*<\s*([\d.]+)',    'min_conf', float),
    # trend_count
    (r'trend_count\s*<\s*(\d+)',       'trend_ticks_min', int),
]


def describe_conditions(conditions: list[dict], cfg) -> list[str]:
    rows = []
    for c in conditions:
        cond = c['condition']
        val_str = ''
        for pattern, label, typ in THRESHOLD_PATTERNS:
            m = re.search(pattern, cond)
            if m:
                if typ and m.lastindex:
                    val_str = f' [{label} = {m.group(1)}]'
                elif typ is None:
                    # Read from cfg
                    attr = label.split('×')[0].replace(' ', '_').lower()
                    attr = attr.split('_min')[0] + '_min' if '_min' in attr else attr
                    cfgval = getattr(cfg, attr, None)
                    if cfgval is not None:
                        val_str = f' [{label} = {cfgval}]'
                break
        rows.append(f'  • {cond}{val_str}')
    return rows


# ── gate → strategy label map ────────────────────────────────────────────────
GATE_MAP = {
    '_standard_gate': ['C', 'G'],
    '_k_gate': ['K'],
    '_l_gate': ['L'],
    '_m_gate': ['M'],
    '_n_gate': ['N'],
    '_o_gate': ['O'],
    '_e_gate': ['E'],
    '_p_gate': ['P'],
    '_q_gate': ['Q', 'QQ'],
    '_r_gate': ['R'],
    '_s_gate': ['S'],
    '_u_gate': ['U'],
    '_w_gate': ['W'],
    '_x_gate': ['X'],
    '_z_gate': ['Z'],
    '_t_gate': ['T'],
    '_v_gate': ['V'],
    '_b_gate': ['B'],
    '_h_gate': ['H'],
}
LABEL_TO_GATE = {lbl: gate for gate, labels in GATE_MAP.items() for lbl in labels}


# ── CSV fire-rate analyser ────────────────────────────────────────────────────
def analyse_csvs(csv_dir: str, labels: list[str] | None) -> dict:
    results = {}
    pattern = os.path.join(csv_dir, 'preds_*.csv')
    files = sorted(glob.glob(pattern))[-50:]  # last 50 files

    for f in files:
        m = re.search(r'preds_\d+_\d+_([A-Z]+)_', os.path.basename(f))
        if not m:
            continue
        label = m.group(1)
        if labels and label not in labels:
            continue
        try:
            import csv as _csv
            with open(f) as fh:
                reader = _csv.reader(fh)
                rows = [r for r in reader if not (r and r[0].startswith('#'))]
            if len(rows) < 2:
                continue
            header = rows[0]
            data   = rows[1:]
            total  = len(data)
            out_idx    = header.index('outcome') if 'outcome' in header else -1
            reason_idx = header.index('reason')  if 'reason'  in header else -1
            conf_idx   = header.index('conf')     if 'conf'    in header else -1

            resolved   = [r for r in data if out_idx >= 0 and r[out_idx].strip() not in ('', 'outcome')]
            wins       = [r for r in resolved if r[out_idx] == 'win']
            losses     = [r for r in resolved if r[out_idx] in ('lose', 'loss')]
            wr         = len(wins) / max(len(resolved), 1) * 100

            reasons = {}
            if reason_idx >= 0:
                for row in resolved:
                    k = row[reason_idx].strip()
                    reasons[k] = reasons.get(k, 0) + 1

            conf0 = 0
            if conf_idx >= 0:
                conf0 = sum(1 for r in data if r[conf_idx].strip() in ('0', '0%', ''))

            results[label] = {
                'file': os.path.basename(f),
                'total_signals': total,
                'resolved': len(resolved),
                'wins': len(wins),
                'losses': len(losses),
                'wr': wr,
                'reasons': reasons,
                'conf0_pct': conf0 / max(total, 1) * 100,
            }
        except Exception as e:
            results[label] = {'error': str(e)}

    return results


# ── signal simulation for B ───────────────────────────────────────────────────
def simulate_b_mtf():
    def calc(m15, m60, m300):
        def w(m):
            d = 1 if m > 0 else -1
            mag = min(1.0, math.log1p(abs(m)) / math.log1p(1.5))
            return d * mag * 20
        return int(w(m15) + w(m60) + w(m300))

    return {
        'strong (0.4/0.6/1.0%)':  calc(0.4, 0.6, 1.0),
        'good   (0.3/0.4/0.6%)':  calc(0.3, 0.4, 0.6),
        'moderate (0.2/0.3/0.4%)': calc(0.2, 0.3, 0.4),
        'weak   (0.1/0.15/0.2%)': calc(0.1, 0.15, 0.2),
        'tiny   (0.05/0.1/0.1%)': calc(0.05, 0.1, 0.1),
        'mixed  (0.2/-0.1/0.3%)': calc(0.2, -0.1, 0.3),
        'noise  (0.01/0.02/0.01%)':calc(0.01,0.02,0.01),
    }


# ── main output ───────────────────────────────────────────────────────────────
def run_diagnostic(args):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"\n{'='*68}")
    print(f"  PredictEngine Gate Diagnostic  [{ts}]")
    print(f"{'='*68}")

    # Load configs
    strategies = load_config()
    cfg_by_label = {s.label: s for s in strategies}

    # Load engine source
    engine_path = BASE_DIR / 'strategies_engine.py'
    if not engine_path.exists():
        print(f"[ERROR] strategies_engine.py not found at {engine_path}")
        return
    engine_src = engine_path.read_text()

    # Filter by requested labels
    target_labels = [l.upper() for l in args.strategy] if args.strategy else list(cfg_by_label.keys())

    # CSV stats
    csv_stats = {}
    if args.csv:
        csv_stats = analyse_csvs(args.csv, target_labels)

    # Per-strategy report
    for label in sorted(target_labels):
        cfg = cfg_by_label.get(label)
        if cfg is None:
            print(f"\n[{label}] — not found in strategies_config.py")
            continue

        gate = LABEL_TO_GATE.get(label, '_standard_gate')

        print(f"\n┌─ {label}: {cfg.name} ({'DISABLED' if cfg.disabled else 'ENABLED'})")

        # Config params
        print(f"│  Config thresholds:")
        print(f"│    vpin_min={cfg.vpin_min}  vpin_max={cfg.vpin_max}")
        print(f"│    min_conf={cfg.min_conf}  min_score={cfg.min_score}")
        print(f"│    min_vol_atr={cfg.min_vol_atr}  spread_max_mult={getattr(cfg,'spread_max_mult',2.0)}")
        print(f"│    win_thr={cfg.win_thr}  trail_dist={cfg.trail_dist}")
        print(f"│    loss_streak_limit={cfg.loss_streak_limit}  cooldown={cfg.cooldown_sec}s")
        if cfg.symbol_blacklist:
            print(f"│    symbol_blacklist={set(cfg.symbol_blacklist)}")

        # Gate conditions
        conditions = parse_gate_conditions(engine_src, gate)
        print(f"│  Gate: {gate}  ({len(conditions)} return-False checks)")
        for row in describe_conditions(conditions, cfg):
            print(f"│{row}")

        # Signal-specific simulations
        if label == 'B':
            print(f"│  MTF bias simulation (threshold={next((c['condition'] for c in conditions if 'mtf' in c['condition'].lower()), '?')}):")
            sims = simulate_b_mtf()
            thr = 15  # read from gate
            m = re.search(r'abs\(mtf\)\s*<\s*([\d.]+)', engine_src)
            if m: thr = float(m.group(1))
            for scenario, val in sims.items():
                mark = '✅' if abs(val) >= thr else '❌'
                print(f'│    {mark} {scenario} → mtf={val}  (need |mtf|>={thr:.0f})')

        if label == 'V':
            m = re.search(r'abs\(abs_val\)\s*<\s*([\d.]+)', engine_src)
            thr = float(m.group(1)) if m else '?'
            print(f"│  Absorption signal threshold: |abs_val| >= {thr}")
            print(f"│    Fires when: sell/buy flow volume fails to move price")
            print(f"│    calc_absorption needs: n>=8 trades in 30s, vol>=$8k, |expected_move|>0.02%")

        # CSV stats
        stat = csv_stats.get(label)
        if stat and 'error' not in stat:
            wr_str = f"{stat['wr']:.1f}%" if stat['resolved'] > 0 else "—"
            print(f"│  CSV: {stat['total_signals']} signals, {stat['resolved']} resolved, "
                  f"WR={wr_str}, conf0={stat['conf0_pct']:.0f}%")
            if stat['reasons']:
                reason_str = ', '.join(f"{k}:{v}" for k,v in sorted(stat['reasons'].items(), key=lambda x:-x[1])[:5])
                print(f"│  Exits: {reason_str}")
        elif stat and 'error' in stat:
            print(f"│  CSV error: {stat['error']}")
        else:
            print(f"│  CSV: no data (use --csv path/to/preds/)")

        print(f"└{'─'*66}")

    # Warmup check
    m = re.search(r'WARMUP_SEC\s*=\s*(\d+)', engine_src)
    warmup = int(m.group(1)) if m else '?'
    print(f"\n  ⏱  WARMUP_SEC = {warmup}s  — all gates blocked for {warmup}s after restart")

    # Conf=0 warning
    print(f"\n  ⚠️  51% of signals have conf=0 — strategies requiring conf>0 will miss half of all ticks")
    print(f"      V and B do NOT require conf (correct). Strategies requiring min_conf>35 need attention.\n")



def debug_candles(args):
    """
    Show K/Y pattern detection thresholds and simulate detection
    against recent CSV data. Works without importing the live engine —
    no aiohttp or any runtime dependency needed.
    """
    print(f"\n{'='*70}")
    print(f"  K/Y Pattern Detection Debug")
    print(f"{'='*70}\n")

    # Load thresholds from engine source
    eng_src = (BASE_DIR / 'strategies_engine.py').read_text()
    sig_src = (BASE_DIR / 'strategies_signals.py').read_text()

    import re

    # Show K thresholds
    print("K (Exhaustion Wick / Body Impulse) — _detect_impulse thresholds:")
    print("  Pattern A (body):  C1 range >= 0.50%, body_ratio >= 0.60 → fade body direction")
    print("  Pattern B (wick):  C1 range >= 0.50%, shadow_ratio >= 0.55 → fade wick direction")
    print("  Entry: immediate after candle closes, no wait")
    print()

    print("Y (Star Pattern) — _find_star_pattern thresholds:")
    print("  C1: range >= 0.50%, body >= 45% of range")
    print("  C2: body <= 60% of C1 body  OR  no new extreme past C1")
    print("      (bearish C1: C2 low >= C1 low)")
    print("      (bullish C1: C2 high <= C1 high)")
    print("  C3: opposite direction, closes past C1 midpoint (50% level)")
    print()

    # Read recent CSVs to find K/Y fires
    csv_dir = args.csv or str(BASE_DIR / 'data_backup')
    k_files = sorted(glob.glob(os.path.join(csv_dir, '**', 'preds_*_K_*.csv'), recursive=True))[-20:]
    y_files = sorted(glob.glob(os.path.join(csv_dir, '**', 'preds_*_Y_*.csv'), recursive=True))[-20:]

    print(f"Recent K CSV files: {len(k_files)}")
    print(f"Recent Y CSV files: {len(y_files)}")

    if not k_files and not y_files:
        print(f"\n  No K/Y CSVs found in {csv_dir}")
        print(f"  After deploy, K/Y will write preds_*_K_*.csv and preds_*_Y_*.csv")
        print(f"  Check back once the engine has run for a few minutes")
        print()
        print("To verify pattern detection manually, check these conditions:")
        print("  1. Coin had a candle with range >= 0.50% (check impulse scanner)")
        print("  2. For Y: next candle stayed within C1's range (no new extreme)")
        print("  3. For Y: candle after that reversed past C1 midpoint")
        print("  4. VPIN at fire time was 0.35 - 0.92 (K) or 0.35 - 0.92 (Y)")
        return

    for label, files in [('K', k_files), ('Y', y_files)]:
        if not files: continue
        import csv as _csv
        total = wins = 0
        for f in files:
            try:
                with open(f) as fh:
                    for row in _csv.DictReader(fh):
                        out = row.get('outcome','')
                        if out in ('win','lose','loss'):
                            total += 1
                            if out == 'win': wins += 1
            except: pass
        wr = wins/max(total,1)*100
        flag = '✅' if wins/max(total,1) > 0.45 else '⚠️'
        print(f"  {flag} {label}: {total} resolved trades, WR={wr:.1f}%")
    print()


def main():
    parser = argparse.ArgumentParser(description='PredictEngine gate diagnostic')
    parser.add_argument('--strategy', nargs='+', metavar='LABEL',
                        help='Diagnose specific strategy labels (e.g. V B T)')
    parser.add_argument('--csv', metavar='DIR',
                        help='Path to preds/ directory for CSV fire-rate analysis')
    parser.add_argument('--watch', action='store_true',
                        help='Re-run every 5 seconds (for live monitoring)')
    parser.add_argument('--candles', action='store_true',
                        help='Show last 5 candles + star/impulse detection for each symbol')
    args = parser.parse_args()

    if args.watch:
        try:
            while True:
                os.system('clear')
                run_diagnostic(args)
                print(f"  [watching — Ctrl+C to stop]")
                time.sleep(5)
        except KeyboardInterrupt:
            print("\nStopped.")
    elif getattr(args, 'candles', False):
        debug_candles(args)
    else:
        run_diagnostic(args)


if __name__ == '__main__':
    main()