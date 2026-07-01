"""
Phase 1 — Distribution Learner
================================
Reads your signals_combined.csv (2.8M rows) and produces sim_dists.json
containing per-strategy statistical distributions learned from real data.

Run ONCE locally against your full CSV:
  python sim_learn_dists.py --csv ./data_backup/*/logs/signals_combined.csv
  python sim_learn_dists.py --csv /path/to/signals_combined.csv

Output: sim_dists.json (consumed by synth_market_sim.py)
"""

import csv, json, math, argparse, glob, sys
from collections import defaultdict
from pathlib import Path


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
    lo, hi = int(i), min(int(i) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (i - lo)


def learn(csv_paths: list, max_rows: int = 0) -> dict:
    # Per-strategy accumulators
    strats = defaultdict(lambda: {
        'fired_scores':  [],
        'fired_vpins':   [],
        'fired_atrs':    [],
        'fired_confs':   [],
        'fired_prices':  [],
        'fired_dirs':    [],          # 'long'/'short' from detail field
        'fired_symbols': defaultdict(int),
        'fired_count':   0,
        'blocked_count': 0,
        'blocked_scores': [],
        'blocked_vpins':  [],
    })

    files = []
    for p in csv_paths:
        files.extend(glob.glob(p))
    if not files:
        print(f"[ERROR] No files matched: {csv_paths}", file=sys.stderr)
        sys.exit(1)

    total_rows = n_fired = n_blocked = 0
    print(f"Reading {len(files)} file(s)...", file=sys.stderr)

    for fpath in sorted(files):
        try:
            with open(fpath, newline='', encoding='utf-8') as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    total_rows += 1
                    if max_rows and total_rows > max_rows:
                        break

                    strat = row.get('strategy', '').strip()
                    event = row.get('event', '').strip()
                    if not strat or not event:
                        continue

                    d = strats[strat]

                    def _f(key, fallback=0.0):
                        v = row.get(key, '')
                        try: return float(v) if v.strip() else fallback
                        except: return fallback

                    vpin  = _f('vpin')
                    atr   = _f('atr')
                    score = _f('score')
                    conf  = _f('conf')
                    price = _f('price')
                    sym   = row.get('symbol', '').strip()
                    detail = row.get('detail', '').strip()

                    if event == 'fired':
                        n_fired += 1
                        d['fired_count'] += 1
                        if score != 0.0:  d['fired_scores'].append(score)
                        if vpin  != 0.0:  d['fired_vpins'].append(vpin)
                        if atr   != 0.0:  d['fired_atrs'].append(atr)
                        if conf  != 0.0:  d['fired_confs'].append(conf)
                        if price != 0.0:  d['fired_prices'].append(price)
                        if sym:           d['fired_symbols'][sym] += 1
                        # Parse direction from detail field
                        # detail format: "long entry=0.07 tp=..." or "short ..."
                        dl = detail.lower()
                        if dl.startswith('long'):   d['fired_dirs'].append('long')
                        elif dl.startswith('short'): d['fired_dirs'].append('short')

                    elif event == 'blocked':
                        n_blocked += 1
                        d['blocked_count'] += 1
                        if score != 0.0: d['blocked_scores'].append(score)
                        if vpin  != 0.0: d['blocked_vpins'].append(vpin)

        except Exception as exc:
            print(f"  [WARN] {fpath}: {exc}", file=sys.stderr)

    print(f"Parsed {total_rows:,} rows  fired={n_fired:,}  blocked={n_blocked:,}", file=sys.stderr)

    # Build output dist per strategy
    out = {}
    for strat, d in sorted(strats.items()):
        fc = d['fired_count']
        if fc == 0:
            continue

        scores = d['fired_scores']
        vpins  = d['fired_vpins']
        atrs   = d['fired_atrs']
        confs  = d['fired_confs']
        dirs   = d['fired_dirs']

        n_long  = dirs.count('long')
        n_short = dirs.count('short')
        long_pct = n_long / max(n_long + n_short, 1)

        # Top 10 symbols by fire count
        top_syms = sorted(d['fired_symbols'].items(), key=lambda x: -x[1])[:10]

        out[strat] = {
            'fired':         fc,
            'blocked':       d['blocked_count'],
            'd2f_pct':       round(fc / max(fc + d['blocked_count'], 1) * 100, 2),

            # Score distribution
            'score_mean':    round(_mean(scores), 3)   if scores else 0.0,
            'score_std':     round(_std(scores), 3)    if scores else 10.0,
            'score_p25':     round(_percentile(scores, 25), 3) if scores else 0.0,
            'score_p75':     round(_percentile(scores, 75), 3) if scores else 0.0,

            # VPIN distribution
            'vpin_mean':     round(_mean(vpins), 4)  if vpins else 0.55,
            'vpin_std':      round(_std(vpins), 4)   if vpins else 0.08,
            'vpin_p10':      round(_percentile(vpins, 10), 4) if vpins else 0.45,
            'vpin_p90':      round(_percentile(vpins, 90), 4) if vpins else 0.80,

            # ATR distribution
            'atr_mean':      round(_mean(atrs), 4)   if atrs else 0.35,
            'atr_std':       round(_std(atrs), 4)    if atrs else 0.10,

            # Conf distribution
            'conf_mean':     round(_mean(confs), 2)  if confs else 60.0,
            'conf_std':      round(_std(confs), 2)   if confs else 10.0,

            # Direction split
            'long_pct':      round(long_pct, 3),

            # Top symbols (used to pick realistic coin names)
            'top_symbols':   [s for s, _ in top_syms],
        }

        print(f"  {strat:6}  fired={fc:>6,}  "
              f"score={out[strat]['score_mean']:>+7.2f}±{out[strat]['score_std']:.2f}  "
              f"vpin={out[strat]['vpin_mean']:.3f}±{out[strat]['vpin_std']:.3f}  "
              f"long={long_pct:.0%}",
              file=sys.stderr)

    return out


def main():
    parser = argparse.ArgumentParser(description='Learn per-strategy distributions from signals CSV')
    parser.add_argument('--csv', nargs='+', required=True,
                        help='Path(s) to signals CSV file(s) or glob patterns')
    parser.add_argument('--out', default='sim_dists.json',
                        help='Output JSON path (default: sim_dists.json)')
    parser.add_argument('--max-rows', type=int, default=0,
                        help='Max rows to read (0 = all)')
    args = parser.parse_args()

    dists = learn(args.csv, max_rows=args.max_rows)

    out_path = Path(args.out)
    with open(out_path, 'w') as fh:
        json.dump(dists, fh, indent=2)

    print(f"\nWrote {len(dists)} strategy distributions → {out_path}", file=sys.stderr)
    print(f"Run: python synth_market_sim.py --dists {out_path}", file=sys.stderr)


if __name__ == '__main__':
    main()
