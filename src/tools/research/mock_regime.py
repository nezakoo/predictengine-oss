#!/usr/bin/env python3
"""
ITEM 11 (Kronos generation idea): Regime-aware mock sessions for gate testing.
Extends mock_data.py to generate parameterised sessions:
  python3 mock_regime.py --regime trending --out /tmp/mock_data/

Regimes:
  trending  — strong directional move (exposes K/Y fade quality)
  choppy    — high-frequency noise (exposes impulse threshold sensitivity)
  cascade   — liquidation cascade + reversal (tests K/CGY entry timing)
  ranging   — tight range, many S/R touches (tests L signal quality)
  low_natr  — low-volatility base (verifies floor filters work)
"""
import csv, json, random, math, time, os, argparse
from datetime import datetime, timezone

parser = argparse.ArgumentParser()
parser.add_argument('--regime', default='trending',
                    choices=['trending','choppy','cascade','ranging','low_natr'])
parser.add_argument('--out-dir', default='/tmp/mock_regime/')
parser.add_argument('--seed', type=int, default=42)
parser.add_argument('--candles', type=int, default=200)
args = parser.parse_args()

random.seed(args.seed)
os.makedirs(args.out_dir, exist_ok=True)

REGIME_PARAMS = {
    'trending': dict(
        drift=0.003, vol=0.008, autocorr=0.7,
        desc="Strong directional move — K/Y fades should mostly fail EMA21 filter"
    ),
    'choppy': dict(
        drift=0.0, vol=0.015, autocorr=-0.3,
        desc="High-freq noise — impulse threshold should block most entries"
    ),
    'cascade': dict(
        drift=-0.005, vol=0.020, autocorr=0.8,
        desc="Liquidation cascade then reversal — K should enter after cascade clears"
    ),
    'ranging': dict(
        drift=0.0, vol=0.005, autocorr=-0.6,
        desc="Tight range — L S/R levels should fire; K/Y should mostly stay out"
    ),
    'low_natr': dict(
        drift=0.001, vol=0.003, autocorr=0.2,
        desc="Low-volatility — NATR floor should restrict universe to almost nothing"
    ),
}

p = REGIME_PARAMS[args.regime]
print(f"Generating {args.regime} session: {p['desc']}")

# Generate synthetic price series
px = 1.0
returns = []
ret = 0.0
for i in range(args.candles):
    shock = random.gauss(0, p['vol'])
    ret = p['autocorr'] * ret + (1 - abs(p['autocorr'])) * shock + p['drift']
    px *= (1 + ret)
    returns.append((px, ret))

# Write preds CSV with synthetic trades
strategies = ['K','Y','W','B','E','C','G']
rows = []
now_ms = time.time() * 1000
for i, (px_val, ret) in enumerate(returns):
    ts_ms = now_ms - (args.candles - i) * 60_000
    ts_str = datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc).strftime('%Y%m%d_%H%M%S')
    if random.random() < 0.15:   # ~15% of candles generate a signal
        strat = random.choice(strategies)
        direction = 'long' if ret > 0 else 'short'
        # Simulate regime-appropriate outcome
        if args.regime == 'trending':
            win = direction == ('long' if p['drift'] > 0 else 'short')
        elif args.regime == 'choppy':
            win = random.random() < 0.45   # slightly below random
        elif args.regime == 'cascade':
            win = i > args.candles * 0.6 and direction == 'long'   # only late longs win
        else:
            win = random.random() < 0.52
        net = random.uniform(0.10, 0.45) if win else random.uniform(-0.50, -0.15)
        rows.append({
            'time': ts_str, 'strategy': strat, 'sym': f'TESTUSDT',
            'dir': direction, 'entry': round(px_val, 6),
            'out': 'win' if win else 'lose', 'pct': round(net, 4),
            'reason': 'trail' if win else 'sl',
            'dur': random.randint(20, 300),
            'conf': random.randint(40, 100), 'score': random.randint(-100, 100),
        })

# Write CSV
out_path = os.path.join(args.out_dir, f'preds_{args.regime}_{args.seed}.csv')
with open(out_path, 'w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)

print(f"Written {len(rows)} rows to {out_path}")
print(f"Expected behaviour for {args.regime}:")
print(f"  {p['desc']}")
print(f"  Run: python3 ~/predict-ai-manager/scripts/analyze_trades.py {args.out_dir}")
