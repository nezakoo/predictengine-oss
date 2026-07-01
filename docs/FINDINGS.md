# Findings — a rigorous negative result (case study)

This is the honest account of a ~3-month effort to find a retail systematic edge in
Binance crypto futures. The conclusion is negative, and that is the contribution.

## 1. Scalping / microstructure / momentum: no edge after honest fees
On ~1.5M matched trades across 25+ strategies, **every strategy with a meaningful sample
is net-negative** on average per-trade P&L after a 0.093% round-trip fee. Representative
per-trade expectancies cluster around −0.05% to −0.09%. Cumulative P&L is deeply negative
(individual strategies at −6,000% to −20,000% cumulative over their samples).

**Gate tuning does not rescue them.** Sweeping the entry gates (order-flow imbalance /
VPIN, confidence, score) across the full range moves average per-trade P&L by hundredths
of a percent and never crosses zero on any strategy with adequate n. Every "green" gate
setting is a tiny-n slice (tens to low-hundreds of trades out of hundreds of thousands).

**The small-sample mirage, repeatedly.** Live dashboards routinely showed strategies at
+2% with 80–100% win rates on 15–35 trades — sitting directly beside the *same* strategy's
large-sample variant at a clear loss. Sample size, not cleverness, explained every green
number. A +1%/day target is structurally impossible from a negative per-trade expectancy:
more trading = more fee bleed, not more profit.

**Why:** microstructure order-flow signals decay in minutes; retail cannot out-execute
HFT there. The profitable-looking exits (trailing/TP) and the losing exits (stop-loss) are
triggered by the *same* signals, so no entry gate can separate winners from losers.

Closed as strategy families (with evidence): scalping, market-making (adverse selection >
gross spread even at zero fees), maker-only execution (fill rate structurally too low).

## 2. Funding carry: a real edge, but thin and basis-bound
Delta-neutral cash-and-carry (short perp to collect funding, long spot to cancel price
risk) is the one **real, persistent, market-neutral** edge found. Deep replay over ~5.7
years with real basis showed positive out-of-sample APR that beats a null baseline and
clears honest fees — but only at longer holds, and in the mid-single-digit APR range
optimistically (assuming perfect hedge and maker fills).

**The catch, confirmed live.** In live paper trading on real funding + prices, the funding
line is real and steady — but **basis P&L dominates and does not wash to zero.** Over
multiple weeks it swung from strongly negative (basis −1.0 vs funding +0.6) to strongly
positive (basis +14 vs funding +3.8). A "market-neutral" strategy whose P&L is ~80% basis
is not neutral — it is a directional bet on the perp/spot spread that happens to pay
sometimes. On retail-accessible high-funding coins (often thin alts/memes), the hedge is
not clean enough to isolate the yield.

**Verdict:** real edge, too thin and too basis-bound to justify real capital at retail
scale. Only viable — if at all — at size ($10k+), on liquid coins, with maker execution,
where it competes with funded desks who compress the premium.

## 3. What actually transferred
The strategies are dead; the **measurement apparatus is the asset.** Honest fees, adequate
samples, OOS + null baselines, live-vs-sim reconciliation, and a verdict dashboard that
plots the durable component against the risky one. That apparatus is what allowed a
confident "no" — which is worth more than a dashboard that flatters you into risking money.

## Reproducing
See `backtest/` for the carry + momentum backtests (honest fees, OOS split, null baseline),
`measurement/` for the trade-outcome joiner and gate sweeps, and `carry/` for the live
paper engine that produced the basis-dominance finding. All numbers here came from these
tools on real Binance data.
