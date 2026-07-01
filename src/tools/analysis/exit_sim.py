#!/usr/bin/env python3
"""
exit_sim.py — Exit-policy counterfactual for PredictEngine.

The SL-cut analysis showed entry gates can't separate SL losers from trail
winners (same signals, different price path). So the lever is the EXIT, not the
entry. This tool answers: how much of the SL bucket could a break-even lock
recover, and at what MFE trigger?

It reads the engine trade CSVs (preds_*.csv, which carry max_dp = MFE and
pct_exit = gross), re-fees to the true round-trip, then for each candidate
break-even trigger replaces losing trades that first reached MFE >= trigger with
a break-even scratch — the OPTIMISTIC ceiling of what a BE lock could do.

It also reports BE-lock HEALTH: B already runs a BE lock at 0.15% MFE, so if a
lot of losing trades reached MFE >= 0.15 and still lost, that lock isn't firing.

Usage:
  python3 exit_sim.py                                  # all strategies, true fee 0.093
  python3 exit_sim.py --strategy B L V --true-fee 0.093
  python3 exit_sim.py --strategy B --be-net -0.05     # conservative BE (slippage)

Caveat: with only MFE (not the full price path) this is an UPPER BOUND — it
assumes the BE lock never prematurely stops a trade that would have recovered to
a trail win. If even this ceiling stays red, a BE lock won't save the strategy.
"""
import os, sys, argparse
from collections import defaultdict

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
try:
    from analyze_perf import load_all_sessions
except Exception as e:
    print(f"[ERROR] could not import analyze_perf.load_all_sessions: {e}", file=sys.stderr)
    sys.exit(1)

RESET='\033[0m'; GREEN='\033[92m'; RED='\033[91m'; YELLOW='\033[93m'
CYAN='\033[96m'; BOLD='\033[1m'; DIM='\033[2m'
def _c(col, t): return f'{col}{t}{RESET}'
def _exp(v):    return _c(GREEN if v >= 0 else RED, f'{v:+.4f}%')

TRIGGERS = [0.05, 0.08, 0.10, 0.12, 0.15, 0.20, 0.25, 0.30]
BE_LOCK_LIVE = 0.15   # B's configured BE-lock MFE trigger — for the health check


def _parse_since(s):
    """Match analyze.sh vocabulary: deploy[:N], Nh, Nm, Nd, or an absolute date."""
    import re
    from datetime import datetime, timedelta
    s = s.strip()
    sl = s.lower()
    # deploy / deploy:N  → read the stage deploy log (fallback prod)
    m = re.fullmatch(r'deploy(?::(\d+))?', sl)
    if m:
        n = int(m.group(1)) if m.group(1) else 1
        for tag in ('stage', 'prod'):
            path = f'./data_backup/deployments_{tag}.log'
            deploys = []
            try:
                with open(path) as f:
                    for line in f:
                        line = line.strip()
                        if not line: continue
                        parts = [p.strip() for p in line.split('|', 2)]
                        try:
                            deploys.append(datetime.strptime(parts[0], '%Y%m%d_%H%M%S'))
                        except (ValueError, IndexError):
                            continue
            except FileNotFoundError:
                continue
            if deploys and n <= len(deploys):
                ts = deploys[-n]
                print(_c(DIM, f'  since {tag} deploy [{ts:%Y-%m-%d %H:%M:%S}]'))
                return ts
        print(_c(RED, '  [ERROR] --since deploy: no deployments_*.log found'), file=sys.stderr); sys.exit(1)
    # relative windows
    m = re.fullmatch(r'(\d+(?:\.\d+)?)\s*([hmdw])', sl)
    if m:
        val = float(m.group(1)); unit = m.group(2)
        delta = {'m': timedelta(minutes=val), 'h': timedelta(hours=val),
                 'd': timedelta(days=val),    'w': timedelta(weeks=val)}[unit]
        ts = datetime.now() - delta
        print(_c(DIM, f'  since {val}{unit} ago [{ts:%Y-%m-%d %H:%M:%S}]'))
        return ts
    # absolute
    for fmt in ('%Y-%m-%dT%H:%M:%S','%Y-%m-%d %H:%M:%S','%Y-%m-%d %H:%M','%Y-%m-%d'):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    print(_c(RED, f'  [ERROR] --since: use deploy, 4h, 2d, or YYYY-MM-DD'), file=sys.stderr); sys.exit(1)


def true_net(t, true_fee):
    """Net % at the true round-trip fee, computed from gross pct_exit when available."""
    if t.get('pct_exit') is not None:
        return t['pct_exit'] - true_fee
    # Fallback: net_exit was logged at the old fee; we can't know which, so skip.
    return None


def main():
    ap = argparse.ArgumentParser(description='Exit-policy counterfactual (break-even lock)')
    ap.add_argument('--data-dir', default='./data_backup', help='data_backup dir (default ./data_backup)')
    ap.add_argument('--strategy', nargs='+', default=None, help='filter strategies')
    ap.add_argument('--true-fee', type=float, default=0.093, help='true round-trip fee %% (default 0.093)')
    ap.add_argument('--be-net',   type=float, default=0.0, help='net %% assigned to a break-even exit (default 0.0)')
    ap.add_argument('--min-trades', type=int, default=50, help='min trades per strategy to show (default 50)')
    ap.add_argument('--since', default=None, metavar='WINDOW',
                    help='only trades at/after: deploy, 4h, 2d, or YYYY-MM-DD (isolate post-fix sessions)')
    ap.add_argument('--env', default='all', choices=['all', 'prod', 'stage', 'legacy'],
                    help='which sessions to read (default all; use stage to validate the stage deploy)')
    args = ap.parse_args()

    cutoff = None
    if args.since:
        cutoff = _parse_since(args.since)

    print(_c(BOLD+CYAN, '\nPredictEngine — Exit-policy counterfactual (break-even lock)'))
    print(_c(DIM, f'  data={args.data_dir}  true_fee={args.true_fee}%  be_net={args.be_net}%'))

    trades = load_all_sessions(args.data_dir, cutoff=cutoff,
                               strategy_filter=args.strategy, env_filter=args.env)
    if not trades:
        print(_c(RED, '  No trades loaded. Check --data-dir.'), file=sys.stderr); sys.exit(1)

    by_strat = defaultdict(list)
    skipped = 0
    for t in trades:
        tn = true_net(t, args.true_fee)
        if tn is None:
            skipped += 1; continue
        t['_tnet'] = tn
        by_strat[t['strategy']].append(t)
    print(_c(DIM, f'  loaded {sum(len(v) for v in by_strat.values()):,} trades '
                  f'({skipped:,} skipped: no gross pct_exit)'))

    strategies = args.strategy if args.strategy else sorted(by_strat)

    for label in strategies:
        rows = by_strat.get(label, [])
        if len(rows) < args.min_trades:
            continue

        n = len(rows)
        base_expect = sum(r['_tnet'] for r in rows) / n
        losers = [r for r in rows if r['_tnet'] < 0]
        n_loss = len(losers)

        def _reason(r): return (r.get('reason') or r.get('exit_reason') or '').lower()

        # Among losers that ARMED the lock (max_dp >= live trigger), how many leaked
        # to a full 'sl' exit vs were caught by 'be'. Leakage is the real failure rate;
        # a be-scratch (net slightly <0) is the lock WORKING, not failing.
        armed = [r for r in losers if r.get('max_dp') is not None and r['max_dp'] >= BE_LOCK_LIVE]
        leaked = [r for r in armed if _reason(r) == 'sl']
        caught = [r for r in armed if _reason(r) == 'be']
        leak_pct = (len(leaked) / len(armed) * 100) if armed else 0.0
        be_total = sum(1 for r in rows if _reason(r) == 'be')
        lc = RED if leak_pct >= 15 else (YELLOW if leak_pct >= 5 else GREEN)

        print(f"\n  {_c(BOLD, label)}  n={n}  baseline expect={_exp(base_expect)}  "
              f"losers={n_loss} ({n_loss/n*100:.0f}%)  be exits={be_total}")
        print(f"    {_c(lc, f'SL leakage: {leak_pct:.0f}% of armed losers (MFE≥{BE_LOCK_LIVE}%) took a full SL')}"
              + _c(DIM, f'  ({len(leaked)} leaked / {len(caught)} caught by be / {len(armed)} armed)'))
        print(f"    {_c(DIM, 'high leakage = lock not firing; ~0 with a healthy be count = working')}")
        print(f"    {'BE trig':>8} {'sim expect':>12} {'residual':>9} {'%losers':>8} {'Δexpect':>10}")
        for trig in TRIGGERS:
            new = []
            rescued = 0
            for r in rows:
                tn = r['_tnet']
                # Only count RESIDUAL rescue: armed losers the lock did NOT already
                # catch (i.e. not already a 'be' exit). Post-fix this should be small.
                if (tn < 0 and r.get('max_dp') is not None and r['max_dp'] >= trig
                        and _reason(r) != 'be'):
                    new.append(args.be_net); rescued += 1
                else:
                    new.append(tn)
            sim_expect = sum(new) / n
            d = sim_expect - base_expect
            pct_loss = (rescued / n_loss * 100) if n_loss else 0.0
            star = _c(GREEN, ' ✓green') if sim_expect >= 0 > base_expect else ''
            print(f"    {trig:>8.2f} {_exp(sim_expect):>12} {rescued:>9} "
                  f"{pct_loss:>7.0f}% {_c(GREEN if d>0 else DIM, f'{d:+.4f}%'):>10}{star}")

    print(_c(DIM, '\n  "residual" = armed losers the live be lock did NOT already catch.\n'
                  '  Small residual + healthy be count = the deployed lock is doing its job.\n'
                  '  Strategies can still be red — that is trade quality, not the exit.\n'))


if __name__ == '__main__':
    main()
