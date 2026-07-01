#!/usr/bin/env python3
"""
run_analysis.py — One command to pull all data and run full analysis pipeline.

Usage:
  python3 tools/analysis/run_analysis.py              # pull both servers + full analysis
  python3 tools/analysis/run_analysis.py --local       # use cached data, no server pull
  python3 tools/analysis/run_analysis.py --since 6h    # last 6 hours only
  python3 tools/analysis/run_analysis.py --sweep vpin  # also run gate sweep
  python3 tools/analysis/run_analysis.py --sweep all   # full gate sweep all strategies

Steps run:
  1. Pull prod logs  → data_backup/*_prod/
  2. Pull stage logs → data_backup/*_stage/
  3. signal_outcome_joiner  → signals_with_outcomes.csv
  4. signal_replay --sweep  → gate sweep table (if --sweep given)
  5. signal_replay --by direction → direction analysis
  6. Print demo/sim promotion recommendations
"""

import sys, os, subprocess, argparse, csv, re
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

HERE        = Path(__file__).parent           # tools/analysis/
ENGINE_ROOT = HERE.parent.parent              # engine root
BACKTEST    = HERE.parent / "backtest"
OUTCOMES    = HERE / "signals_with_outcomes.csv"

def run(cmd, label=""):
    if label:
        print(f"\n{'═'*60}", file=sys.stderr)
        print(f"  {label}", file=sys.stderr)
        print('═'*60, file=sys.stderr)
    result = subprocess.run(cmd, cwd=str(ENGINE_ROOT))
    return result.returncode == 0

def _parse_ts(s):
    for fmt in ('%Y%m%d_%H%M%S', '%Y%m%d_%H%M'):
        try: return datetime.strptime(s.strip(), fmt)
        except ValueError: pass
    return None

def analyze_signals_only(since_hours=10, data_backup="./data_backup"):
    """Read all local signals_combined.csv files and print per-strategy breakdown."""
    cutoff = datetime.utcnow() - timedelta(hours=since_hours)

    # Collect all signals CSVs from local sessions
    csvfiles = sorted(Path(data_backup).glob("*/logs/signals_combined.csv"))
    if not csvfiles:
        print("❌ No local signals_combined.csv found in data_backup/")
        return

    stats = defaultdict(lambda: {
        'fired': 0, 'blocked': 0, 'detected': 0, 'closed': 0,
        'block_reasons': defaultdict(int),
        'scores': [], 'vpins': [], 'confs': [],
        'coins': defaultdict(int),
        'closed_wins': 0, 'closed_losses': 0, 'closed_net': [],
    })

    total_rows = 0
    for csvf in csvfiles:
        try:
            with open(csvf, newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ts = _parse_ts(row.get('ts',''))
                    if ts and ts < cutoff:
                        continue
                    total_rows += 1
                    strat  = row.get('strategy','?')
                    event  = row.get('event','')
                    detail = row.get('detail','')
                    sym    = row.get('symbol','')
                    s      = stats[strat]

                    if event == 'fired' or event == 'fire_attempt':
                        s['fired'] += 1
                        for fld, arr in [('score', s['scores']), ('vpin', s['vpins']), ('conf', s['confs'])]:
                            v = row.get(fld,'').strip()
                            if v:
                                try: arr.append(float(v))
                                except ValueError: pass
                        if sym: s['coins'][sym] += 1
                    elif event == 'blocked':
                        s['blocked'] += 1
                        reason = re.split(r'[ _]', detail)[0] if detail else 'unknown'
                        # collapse mtf_bias variants
                        if 'mtf' in detail.lower(): reason = 'mtf_bias'
                        s['block_reasons'][reason] += 1
                    elif event == 'detected':
                        s['detected'] += 1
                    elif event == 'closed':
                        s['closed'] += 1
                        m = re.search(r'net=([+-]?\d+\.\d+)%', detail)
                        if m:
                            net = float(m.group(1))
                            s['closed_net'].append(net)
                            if net > 0: s['closed_wins'] += 1
                            else: s['closed_losses'] += 1
        except Exception as e:
            print(f"   ⚠️  {csvf}: {e}")

    if not total_rows:
        print(f"❌ No signal rows found since {cutoff.strftime('%Y-%m-%d %H:%M')} UTC")
        return

    print(f"\n📊 Signal analysis — last {since_hours}h  ({total_rows:,} rows across {len(csvfiles)} session(s))")
    print(f"   Cutoff: {cutoff.strftime('%Y-%m-%d %H:%M')} UTC\n")

    # Header
    print(f"{'Strat':<7} {'Fired':>6} {'Blocked':>8} {'Fire%':>6} {'AvgScore':>9} {'AvgVPIN':>8} {'AvgConf':>8}  {'Top block reasons':<40}  {'Top coins'}")
    print("─" * 130)

    def avg(lst): return f"{sum(lst)/len(lst):.1f}" if lst else "─"

    for strat in sorted(stats, key=lambda s: -stats[s]['fired']):
        s = stats[strat]
        total = s['fired'] + s['blocked']
        fire_pct = f"{100*s['fired']/total:.0f}%" if total else "─"
        top_reasons = sorted(s['block_reasons'].items(), key=lambda x: -x[1])[:3]
        reasons_str = "  ".join(f"{r}({n})" for r,n in top_reasons) if top_reasons else "─"
        top_coins   = sorted(s['coins'].items(), key=lambda x: -x[1])[:3]
        coins_str   = " ".join(c.replace('USDT','') for c,_ in top_coins) if top_coins else "─"
        print(f"{strat:<7} {s['fired']:>6} {s['blocked']:>8} {fire_pct:>6} "
              f"{avg(s['scores']):>9} {avg(s['vpins']):>8} {avg(s['confs']):>8}  "
              f"{reasons_str:<40}  {coins_str}")

    # Closed trades from signals (quick WR proxy)
    print(f"\n📊 Closed trades logged in signals (exit events):")
    print(f"{'Strat':<7} {'Closed':>7} {'Wins':>6} {'Losses':>7} {'WR%':>6} {'AvgNet%':>9}")
    print("─" * 50)
    for strat in sorted(stats, key=lambda s: -stats[s]['closed']):
        s = stats[strat]
        if s['closed'] == 0: continue
        wr = f"{100*s['closed_wins']/s['closed']:.0f}%" if s['closed'] else "─"
        an = avg(s['closed_net'])
        print(f"{strat:<7} {s['closed']:>7} {s['closed_wins']:>6} {s['closed_losses']:>7} {wr:>6} {an:>9}")


def main():
    parser = argparse.ArgumentParser(description="Full PredictEngine analysis pipeline")
    parser.add_argument("--local",  action="store_true", help="Skip server pull, use cached data")
    parser.add_argument("--since",  default=None, help="Time window: 2h / 6h / deploy")
    parser.add_argument("--sweep",  default=None, help="Gate sweep: vpin / all / <field>")
    parser.add_argument("--no-pull",action="store_true", help="Skip joiner, use existing signals_with_outcomes.csv")
    parser.add_argument("--signals-only", action="store_true", help="Analyze signals CSV only (no trade CSVs needed)")
    parser.add_argument("--since-hours",  type=float, default=10, help="Hours to look back for --signals-only (default: 10)")
    parser.add_argument("--prod-reconcile", action="store_true",
                        help="Prod-only run WITH Binance LIVE reconciliation (real-money P&L). "
                             "Read-only API (GET balance/income/userTrades) — no orders, no config change.")
    args = parser.parse_args()

    print("\n🔍 PredictEngine Full Analysis Pipeline")
    print("="*60)

    # ── Prod reconciliation (real money) ─────────────────────────────
    # Prod-only + Binance LIVE. Deliberately NOT --pull-all (which forces
    # no_binance and is why binance/reconciliation came back null), NOT
    # --stage (demo), NOT --json-only (so analyze_correlation runs and the
    # _corr.json real_pnl/reconciliation summary is produced).
    if args.prod_reconcile:
        cmd = [sys.executable, str(HERE / "analyze.sh"), "--live"]
        if args.since:
            cmd += ["--since", args.since]
        run(cmd, "Prod reconciliation — prod server + Binance LIVE (read-only)")
        return

    if args.signals_only:
        analyze_signals_only(since_hours=args.since_hours)
        return

    # ── Step 1: Pull from servers ────────────────────────────────────
    if not args.local:
        analyze_cmd = [sys.executable, str(HERE / "analyze.sh"), "--pull-all", "--json-only"]
        # if args.clean:
        #     analyze_cmd.append("--clean")
        if args.since:
            analyze_cmd += ["--since", args.since]
        run(analyze_cmd, "Step 1/3 — Pulling logs from prod + stage")
    else:
        print("\n⏭  Skipping server pull (--local)")

    # ── Step 2: Join signals to outcomes ────────────────────────────
    if not args.no_pull:
        run([sys.executable, str(HERE / "signal_outcome_joiner.py")],
            "Step 2/3 — Joining signals to outcomes")
    else:
        print(f"\n⏭  Skipping joiner (--no-pull), using {OUTCOMES.name}")

    if not OUTCOMES.exists():
        print(f"\n❌ {OUTCOMES} not found — cannot continue")
        sys.exit(1)

    # ── Step 3: Analysis ────────────────────────────────────────────
    print(f"\n{'═'*60}")
    print("  Step 3/3 — Signal replay analysis")
    print('═'*60)

    replay = [sys.executable, str(HERE / "signal_replay.py")]

    # Direction analysis — most important for demo/sim decisions
    print("\n📊 Win rate by direction (long vs short):")
    subprocess.run(replay + ["--by", "direction"], cwd=str(ENGINE_ROOT))

    # Exit reason breakdown
    print("\n📊 Win rate by exit reason:")
    subprocess.run(replay + ["--by", "exit_reason"], cwd=str(ENGINE_ROOT))

    # Gate sweep if requested
    if args.sweep:
        field = args.sweep
        print(f"\n📊 Gate sweep: {field}")
        subprocess.run(replay + ["--sweep", field], cwd=str(ENGINE_ROOT))

    # Per-strategy summary
    print("\n📊 Per-strategy summary:")
    subprocess.run(replay + ["--by", "strategy"], cwd=str(ENGINE_ROOT))

    # ── Recommendation ───────────────────────────────────────────────
    print(f"\n{'═'*60}")
    print("  Done. To get demo/sim recommendations:")
    print('═'*60)
    print("""
  Promote to DEMO (real demo orders) if:
    • WR ≥ 55% AND avg_net > 0 over 100+ trades

  Keep as SIM if:
    • WR 45-55% — needs more data or gate tuning
    • Fewer than 100 fired trades — insufficient sample

  Keep DISABLED if:
    • WR < 40% consistently across multiple gate configs
    • Confirmed structural issues (Z, C, G historically)

  Next: paste signal_replay output here for specific recommendations.
""", file=sys.stderr)

if __name__ == "__main__":
    main()
