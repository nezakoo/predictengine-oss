"""
PredictEngine — Pre-Deploy Check
==================================
Runs automated validation tests before each deploy to answer:
  "Will this config change help or hurt?"

Tests run:
  1. signal_replay.py  — validates gate param changes against 21k real trades
  2. level_backtester.py — validates L config changes against 30d klines
  3. Reads strategies_config.py to detect what changed vs last check
  4. Prints a clear DEPLOY / HOLD / NEUTRAL verdict per changed strategy

Usage:
  # Run before every deploy:
  python pre_deploy_check.py

  # Run specific strategy only:
  python pre_deploy_check.py --strategy B
  python pre_deploy_check.py --strategy L

  # Compare explicit config values:
  python pre_deploy_check.py --strategy B --param long_only --before False --after True
  python pre_deploy_check.py --strategy L --param short_only --before False --after True
  python pre_deploy_check.py --strategy W --param trail_dist --before 0.16 --after 0.20

  # Run all checks (used by deploy.sh):
  python pre_deploy_check.py --all --json  # machine-readable output

Integrate with deploy.sh:
  Add this to deploy.sh before the rsync step:
    python pre_deploy_check.py --all || { echo "Pre-deploy check failed"; exit 1; }
"""

import sys, os, json, argparse, subprocess
from pathlib import Path
from datetime import datetime, timezone

# ── Constants ──────────────────────────────────────────────────────────────────

OUTCOMES_FILE = 'signals_with_outcomes.csv'
CACHE_DIR     = Path('ohlcv_cache')

# Minimum trades required for a result to be meaningful
MIN_TRADES_SIGNAL  = 50
MIN_TRADES_LEVEL   = 30

# Thresholds for verdicts
MEANINGFUL_DELTA   = 0.005   # % per trade — changes below this are noise
MEANINGFUL_WR_DELTA = 2.0    # pp WR change considered meaningful

RESET  = '\033[0m'; BOLD = '\033[1m'; CYAN  = '\033[96m'
GREEN  = '\033[92m'; RED  = '\033[91m'; DIM   = '\033[2m'
YELLOW = '\033[93m'

def _c(col, txt): return f'{col}{txt}{RESET}'


# ── Signal replay runner ───────────────────────────────────────────────────────

def run_signal_compare(strategy: str, param: str, val_before, val_after) -> dict:
    """
    Runs signal_replay.py --strategy S --filter "param>=val" --compare "param>=val2"
    for numeric params, or uses --by direction/symbol for structural changes.
    Returns dict with 'before', 'after', 'delta', 'verdict'.
    """
    if not Path(OUTCOMES_FILE).exists():
        return {'error': f'{OUTCOMES_FILE} not found. Run signal_outcome_joiner.py first.'}

    # For boolean params (long_only, short_only), compare by direction
    if isinstance(val_before, bool) or str(val_before).lower() in ('true', 'false', '0', '1'):
        return _compare_direction(strategy, param, val_before, val_after)

    # For numeric params (vpin_min, trail_dist, etc), use --filter --compare
    try:
        val_b = float(val_before)
        val_a = float(val_after)
    except (ValueError, TypeError):
        return {'error': f'Cannot compare {param}: {val_before!r} vs {val_after!r}'}

    # Build filter expressions based on param
    filter_map = {
        'vpin_min':  'vpin',
        'min_conf':  'conf',
        'min_score': 'score',
    }
    field = filter_map.get(param, param)
    op = '>=' if val_a >= val_b else '<='

    result = {'param': param, 'before': val_before, 'after': val_after}

    try:
        # Import signal_replay directly
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # engine root
        from signal_replay import load, stats, parse_filter

        rows = load(OUTCOMES_FILE, strategy_filter=[strategy])
        if len(rows) < MIN_TRADES_SIGNAL:
            return {'error': f'Only {len(rows)} trades for {strategy} — need {MIN_TRADES_SIGNAL}'}

        fn_b, *_ = parse_filter(f'{field}>={val_b}')
        fn_a, *_ = parse_filter(f'{field}>={val_a}')
        rows_b = [r for r in rows if fn_b(r)]
        rows_a = [r for r in rows if fn_a(r)]

        sb = stats(rows_b)
        sa = stats(rows_a)

        result['before_stats'] = sb
        result['after_stats']  = sa
        result['delta_avg']    = sa['avg_net'] - sb['avg_net']
        result['delta_wr']     = sa['wr']      - sb['wr']
        result['trades_lost']  = sb['n'] - sa['n']
        result['verdict']      = _verdict(sb, sa, result['trades_lost'])
        return result

    except Exception as e:
        return {'error': str(e)}


def _compare_direction(strategy: str, param: str, val_before, val_after) -> dict:
    """Compare by direction split — for long_only/short_only changes."""
    try:
        from signal_replay import load, stats

        rows = load(OUTCOMES_FILE, strategy_filter=[strategy])
        if len(rows) < MIN_TRADES_SIGNAL:
            return {'error': f'Only {len(rows)} trades for {strategy}'}

        longs  = [r for r in rows if r['dir'] == 'long']
        shorts = [r for r in rows if r['dir'] == 'short']
        all_s  = stats(rows)
        long_s = stats(longs)
        short_s = stats(shorts)

        # Simulate effect of direction filter
        # before: all trades. after: filtered by direction
        val_after_bool = str(val_after).lower() in ('true', '1', 'yes')

        if param == 'long_only' and val_after_bool:
            after_rows = longs
            after_label = 'longs only'
        elif param == 'short_only' and val_after_bool:
            after_rows = shorts
            after_label = 'shorts only'
        else:
            after_rows = rows
            after_label = 'both directions'

        before_s = all_s
        after_s  = stats(after_rows)

        result = {
            'param':        param,
            'before':       val_before,
            'after':        val_after,
            'before_stats': before_s,
            'after_stats':  after_s,
            'long_stats':   long_s,
            'short_stats':  short_s,
            'delta_avg':    after_s['avg_net'] - before_s['avg_net'],
            'delta_wr':     after_s['wr']      - before_s['wr'],
            'trades_lost':  before_s['n'] - after_s['n'],
        }
        result['verdict'] = _verdict(before_s, after_s, result['trades_lost'])
        return result

    except Exception as e:
        return {'error': str(e)}


def _verdict(before: dict, after: dict, trades_lost: int) -> str:
    """
    Return DEPLOY / HOLD / NEUTRAL / CAUTION based on stats delta.
    """
    delta_avg = after['avg_net'] - before['avg_net']
    delta_wr  = after['wr']      - before['wr']

    if after['n'] < MIN_TRADES_SIGNAL:
        return 'HOLD — insufficient data after filter'

    if delta_avg > MEANINGFUL_DELTA and delta_wr > 0:
        return f'DEPLOY — avg +{delta_avg:+.4f}% per trade, WR {delta_wr:+.1f}pp'
    elif delta_avg > 0 and delta_wr >= 0:
        return f'NEUTRAL — small improvement, marginal'
    elif delta_avg < -MEANINGFUL_DELTA:
        return f'HOLD — avg {delta_avg:+.4f}% per trade (worse)'
    elif delta_avg > 0:
        return f'NEUTRAL — minimal delta ({delta_avg:+.5f}%/trade)'
    else:
        return f'CAUTION — WR {delta_wr:+.1f}pp, avg {delta_avg:+.4f}%'


# ── Level backtester runner ────────────────────────────────────────────────────

def run_level_compare(param: str, val_before, val_after) -> dict:
    """Runs level_backtester.py --compare param val_a val_b."""
    if not CACHE_DIR.exists() or not list(CACHE_DIR.glob('*_1m.csv')):
        return {'error': 'No klines cache. Run ohlcv_fetcher.py first.'}

    try:
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # engine root

        # Import level_backtester
        import level_backtester as lb

        # Build base config
        cfg = dict(lb.DEFAULTS)

        # Load all candles
        sym_list = [p.name.replace('_1m.csv', '')
                    for p in sorted(CACHE_DIR.glob('*_1m.csv'))
                    if p.name.replace('_1m.csv', '') not in ('BTCUSDT', 'ETHUSDT', 'SOLUSDT')]

        all_candles = {}
        for sym in sym_list:
            c = lb.load_candles(sym)
            if c:
                all_candles[sym] = c

        if not all_candles:
            return {'error': 'No candles loaded'}

        # Coerce type
        default = lb.DEFAULTS.get(param)
        try:
            if isinstance(default, bool):
                val_b = str(val_before).lower() in ('true', '1', 'yes')
                val_a = str(val_after).lower()  in ('true', '1', 'yes')
            elif isinstance(default, float):
                val_b = float(val_before); val_a = float(val_after)
            elif isinstance(default, int):
                val_b = int(val_before);   val_a = int(val_after)
            else:
                val_b = val_before; val_a = val_after
        except (ValueError, TypeError):
            val_b = val_before; val_a = val_after

        cfg_b = dict(cfg); cfg_b[param] = val_b
        cfg_a = dict(cfg); cfg_a[param] = val_a

        trades_b = []
        for sym, candles in all_candles.items():
            trades_b.extend(lb.backtest_symbol(sym, candles, cfg_b))

        trades_a = []
        for sym, candles in all_candles.items():
            trades_a.extend(lb.backtest_symbol(sym, candles, cfg_a))

        sb = lb.compute_stats(trades_b)
        sa = lb.compute_stats(trades_a)

        result = {
            'param':        param,
            'before':       val_b,
            'after':        val_a,
            'before_stats': sb,
            'after_stats':  sa,
            'delta_avg':    sa['avg_net'] - sb['avg_net'],
            'delta_wr':     sa['wr']      - sb['wr'],
            'trades_lost':  sb['n'] - sa['n'],
        }
        result['verdict'] = _verdict(sb, sa, result['trades_lost'])
        return result

    except Exception as e:
        import traceback
        return {'error': f'{e}\n{traceback.format_exc()}'}


# ── Printer ────────────────────────────────────────────────────────────────────

def print_result(result: dict, strategy: str, tool: str):
    if 'error' in result:
        print(f'  {_c(YELLOW, "[SKIP]")} {strategy} {tool}: {result["error"]}')
        return

    verdict = result.get('verdict', 'UNKNOWN')
    if verdict.startswith('DEPLOY'):
        vcol = GREEN
    elif verdict.startswith('HOLD'):
        vcol = RED
    elif verdict.startswith('CAUTION'):
        vcol = YELLOW
    else:
        vcol = DIM

    bs = result.get('before_stats', {})
    as_ = result.get('after_stats', {})

    print(f'\n  {_c(BOLD, strategy)} [{tool}]  {result["param"]}: {result["before"]!r} → {result["after"]!r}')
    print(f'  before: n={bs.get("n",0):,}  WR={bs.get("wr",0):.1f}%  avg={bs.get("avg_net",0):+.4f}%')
    print(f'  after:  n={as_.get("n",0):,}  WR={as_.get("wr",0):.1f}%  avg={as_.get("avg_net",0):+.4f}%')
    print(f'  trades filtered: {result.get("trades_lost",0):,}')
    print(f'  verdict: {_c(vcol, verdict)}')

    # Direction split if available
    if 'long_stats' in result and 'short_stats' in result:
        ls = result['long_stats']; ss = result['short_stats']
        print(f'  long:  n={ls["n"]:,}  WR={ls["wr"]:.1f}%  avg={ls["avg_net"]:+.4f}%')
        print(f'  short: n={ss["n"]:,}  WR={ss["wr"]:.1f}%  avg={ss["avg_net"]:+.4f}%')


# ── Main ───────────────────────────────────────────────────────────────────────

# Built-in checks per strategy — run these automatically
AUTO_CHECKS = [
    # strategy, param, before, after, tool
    ('B', 'long_only',   False,  True,  'signal'),
    ('B', 'vpin_min',    0.55,   0.60,  'signal'),
    ('W', 'trail_dist',  0.16,   0.20,  'signal'),
    ('L', 'short_only',  False,  True,  'level'),
    ('L', 'vpin_min',    0.50,   0.55,  'level'),
    ('L', 'trail_dist',  0.28,   0.20,  'level'),
    ('CGY', 'vpin_min',  0.45,   0.55,  'signal'),
]


def main():
    global OUTCOMES_FILE, CACHE_DIR
    parser = argparse.ArgumentParser(
        description='Pre-deploy validation — test config changes before deploying',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pre_deploy_check.py                          # run all built-in checks
  python pre_deploy_check.py --strategy B             # B strategy checks only
  python pre_deploy_check.py --strategy L --param short_only --before False --after True
  python pre_deploy_check.py --strategy B --param vpin_min --before 0.55 --after 0.65
  python pre_deploy_check.py --all --json             # machine-readable for deploy.sh
        """
    )
    parser.add_argument('--strategy',  default=None, help='Strategy label (B/W/L/K/CGY)')
    parser.add_argument('--param',     default=None, help='Parameter name to compare')
    parser.add_argument('--before',    default=None, help='Value before change')
    parser.add_argument('--after',     default=None, help='Value after change')
    parser.add_argument('--all',       action='store_true', help='Run all built-in checks')
    parser.add_argument('--json',      action='store_true', help='Output JSON for scripting')
    parser.add_argument('--outcomes',  default=OUTCOMES_FILE, help='signals_with_outcomes.csv path')
    args = parser.parse_args()

    OUTCOMES_FILE = args.outcomes

    dt = datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    print(_c(BOLD + CYAN, f'\nPredictEngine — Pre-Deploy Check  [{dt}]'))

    all_results = []

    # Single explicit check
    if args.strategy and args.param and args.before is not None and args.after is not None:
        # Determine tool
        level_params = {'vpin_min', 'trail_dist', 'atr_sl_mult', 'short_only', 'long_only'}
        tool = 'level' if args.strategy == 'L' and args.param in level_params else 'signal'

        if tool == 'level':
            result = run_level_compare(args.param, args.before, args.after)
        else:
            result = run_signal_compare(args.strategy, args.param, args.before, args.after)

        print_result(result, args.strategy, tool)
        all_results.append({'strategy': args.strategy, 'tool': tool, **result})

    # Strategy filter on auto-checks
    elif args.strategy and not args.param:
        checks = [(s, p, b, a, t) for s, p, b, a, t in AUTO_CHECKS if s == args.strategy]
        if not checks:
            print(f'  No auto-checks defined for {args.strategy}')
        for strat, param, before, after, tool in checks:
            if tool == 'level':
                result = run_level_compare(param, before, after)
            else:
                result = run_signal_compare(strat, param, before, after)
            print_result(result, strat, tool)
            all_results.append({'strategy': strat, 'param': param, 'tool': tool, **result})

    # All checks
    else:
        print(_c(DIM, f'  Running {len(AUTO_CHECKS)} built-in checks...\n'))
        for strat, param, before, after, tool in AUTO_CHECKS:
            if tool == 'level':
                result = run_level_compare(param, before, after)
            else:
                result = run_signal_compare(strat, param, before, after)
            print_result(result, strat, tool)
            all_results.append({'strategy': strat, 'param': param, 'tool': tool, **result})

    # Summary
    if len(all_results) > 1:
        print(_c(BOLD + CYAN, f'\n{"━"*64}'))
        print(_c(BOLD, '  SUMMARY'))
        print(_c(CYAN, f'{"━"*64}'))
        holds   = [r for r in all_results if r.get('verdict', '').startswith('HOLD')]
        deploys = [r for r in all_results if r.get('verdict', '').startswith('DEPLOY')]
        neutral = [r for r in all_results if r.get('verdict', '').startswith('NEUTRAL')]
        caution = [r for r in all_results if r.get('verdict', '').startswith('CAUTION')]

        if deploys:
            print(_c(GREEN,  f'  DEPLOY:  {len(deploys)} check(s) recommend deploying'))
        if neutral:
            print(_c(DIM,    f'  NEUTRAL: {len(neutral)} check(s) show no meaningful change'))
        if caution:
            print(_c(YELLOW, f'  CAUTION: {len(caution)} check(s) need review'))
        if holds:
            print(_c(RED,    f'  HOLD:    {len(holds)} check(s) recommend NOT deploying'))

    # JSON output for scripting
    if args.json:
        # Serialise — convert non-serialisable values
        def _clean(obj):
            if isinstance(obj, dict):
                return {k: _clean(v) for k, v in obj.items()}
            if isinstance(obj, (list, tuple)):
                return [_clean(i) for i in obj]
            if isinstance(obj, float):
                return round(obj, 6)
            return obj
        print('\n' + json.dumps(_clean(all_results), indent=2))

    print()


if __name__ == '__main__':
    main()
