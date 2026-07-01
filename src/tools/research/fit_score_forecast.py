#!/usr/bin/env python3
"""
ITEM 10 (Kronos autoregressive idea): Fit 1-tick score forecast weights.
Run offline against your historical CSV data:
  python3 fit_score_forecast.py --data-dir /home/ubuntu/engine/logs/

Outputs weights to score_forecast_weights.json — copy to engine dir.
Engine uses these to predict next tick's score from last 3 ticks.
"""
import json, csv, glob, sys, os, argparse
from collections import defaultdict

parser = argparse.ArgumentParser()
parser.add_argument('--data-dir', default='/home/ubuntu/engine/logs/')
parser.add_argument('--out', default='score_forecast_weights.json')
parser.add_argument('--min-samples', type=int, default=500)
args = parser.parse_args()

# Read all signals CSVs — look for score sequences per symbol
sequences = []   # list of (s[t-3], s[t-2], s[t-1], s[t])

csv_files = sorted(glob.glob(os.path.join(args.data_dir, 'signals_*.csv')))
if not csv_files:
    print(f"No signals CSV found in {args.data_dir}")
    sys.exit(1)

print(f"Reading {len(csv_files)} CSV files...")
per_sym = defaultdict(list)

for path in csv_files:
    try:
        with open(path) as f:
            for row in csv.DictReader(f):
                sc = row.get('score', '')
                sym = row.get('symbol', '')
                if sc and sym:
                    try:
                        per_sym[sym].append(float(sc))
                    except ValueError:
                        pass
    except Exception as e:
        print(f"  skip {path}: {e}")

print(f"Collected sequences for {len(per_sym)} symbols")

# Build (x, y) pairs: x = [s[t-3], s[t-2], s[t-1]], y = s[t]
X, Y = [], []
for sym, scores in per_sym.items():
    for i in range(3, len(scores)):
        X.append([scores[i-3], scores[i-2], scores[i-1]])
        Y.append(scores[i])

if len(X) < args.min_samples:
    print(f"Only {len(X)} samples — need at least {args.min_samples}. Exiting.")
    sys.exit(1)

print(f"Fitting on {len(X)} samples...")

# Simple OLS: fit w1, w2, w3 via normal equations
# [X^T X] [w] = [X^T y]
import math

n = len(X)
# Compute X^T X (3x3) and X^T y (3x1)
XTX = [[0.0]*3 for _ in range(3)]
XTy = [0.0]*3
for i in range(n):
    for j in range(3):
        XTy[j] += X[i][j] * Y[i]
        for k in range(3):
            XTX[j][k] += X[i][j] * X[i][k]

# Solve 3x3 system using Cramer or simple Gaussian elimination
def gauss(A, b):
    n = len(b)
    M = [row[:] + [b[i]] for i, row in enumerate(A)]
    for col in range(n):
        pivot = max(range(col, n), key=lambda r: abs(M[r][col]))
        M[col], M[pivot] = M[pivot], M[col]
        for row in range(col+1, n):
            if M[col][col] == 0: continue
            f = M[row][col] / M[col][col]
            M[row] = [M[row][k] - f * M[col][k] for k in range(n+1)]
    x = [0.0]*n
    for i in range(n-1, -1, -1):
        x[i] = M[i][n] / M[i][i]
        for j in range(i+1, n):
            x[i] -= M[i][j] * x[j] / M[i][i]
    return x

weights = gauss(XTX, XTy)
w1, w2, w3 = weights

# Validate: compute R^2 and direction accuracy
pred = [w1*x[0] + w2*x[1] + w3*x[2] for x in X]
ss_res = sum((Y[i] - pred[i])**2 for i in range(n))
ss_tot = sum((Y[i] - sum(Y)/n)**2 for i in range(n))
r2 = 1 - ss_res/ss_tot if ss_tot > 0 else 0

# Direction accuracy: does sign(pred) == sign(actual)?
dir_correct = sum(1 for i in range(n)
                  if pred[i] * Y[i] > 0 and abs(Y[i]) > 5)
dir_total   = sum(1 for i in range(n) if abs(Y[i]) > 5)
dir_acc = dir_correct / dir_total if dir_total > 0 else 0

print(f"Weights: w1={w1:.4f}  w2={w2:.4f}  w3={w3:.4f}")
print(f"R²={r2:.3f}  direction_accuracy={dir_acc:.1%} (on |score|>5 samples)")

result = {
    'w1': round(w1, 4), 'w2': round(w2, 4), 'w3': round(w3, 4),
    'r2': round(r2, 3), 'dir_acc': round(dir_acc, 3),
    'n_samples': n,
}
with open(args.out, 'w') as f:
    json.dump(result, f, indent=2)
print(f"Saved to {args.out}")
print("Deploy: copy score_forecast_weights.json to /home/ubuntu/engine/")
