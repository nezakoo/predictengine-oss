# PredictEngine

**A rigorous framework for measuring whether a crypto trading strategy has a real edge — and the honest, negative result of applying it to a large family of retail strategies.**

Across ~1.5M matched trades and 25+ strategies (momentum, mean-reversion, order-flow /
microstructure, lag-arb, exhaustion), **every strategy with an adequate sample is
net-negative after honest fees.** The one genuinely real edge found — delta-neutral
funding carry — is thin, capital-gated, and dominated by basis risk on retail-accessible
coins. This repo is the apparatus that made it possible to know that *for certain* rather
than guess from a good-looking equity curve.

> The value here was never a profitable bot. It's the ability to tell a profitable
> strategy from a lucky one. Most apparent edge in retail crypto is small-sample noise or
> unpaid fees; this framework is built to strip both away.

## The methodology (why the result is trustworthy)
- **Honest fees.** A blended ~0.093% round-trip fee is applied to everything. If profit
  vanishes at honest fees, it was phantom.
- **Adequate samples.** A +2% / 100%-win panel on 30 trades is statistical zero. The same
  strategies flip green→red as n grows; we show this repeatedly.
- **Out-of-sample splits + shuffled-rank null baselines.** An edge must beat a randomized
  version of itself, out of sample, or it isn't one.
- **Live-paper vs backtest reconciliation.** Real fills, real basis, real slippage,
  measured against what the sim promised.

## Repository layout
```
src/
  engine/       real-time engine core: scanner, signal loop, logging, config
  strategies/   strategy definitions, per-env configs, weekly momentum
  execution/    Binance USDⓈ-M signing + persistent post-only (maker) executors,
                maker-fill simulator, market-maker paper model
  carry/        funding-carry: live PAPER engine + dashboard, backtests,
                cash-and-carry basis replay, cross-sectional momentum, data fetchers
  ops/          multi-strategy dashboard, Telegram monitor, position teardown
  tools/        research library
    analysis/   trade-outcome joiner, gate sweeps (VPIN/conf/score), fee analysis
    backtest/   synthetic sims, OHLCV walk-forward, distribution builders
    devtools/   deploy helpers, pre-deploy checks, import validation
    research/   score-forecast fitting, regime mocks
docs/           methodology notes, findings write-up, monitor setup
```

## Key findings (see `docs/FINDINGS.md`)
1. **Scalping / microstructure / momentum: no edge after honest fees.** Per-trade
   expectancy clusters at −0.05% to −0.09%; gate tuning never crosses zero at adequate n.
2. **Funding carry is real but thin and basis-bound.** Funding income is steady and clears
   fees, but on retail-accessible coins basis P&L dominates and doesn't wash to zero — a
   "market-neutral" book whose P&L is ~80% basis is a directional spread bet, not a yield.
3. **The transferable asset is the measurement apparatus**, not the strategies.

## Running the code
Modules use flat imports (historical layout). Use the helper, which sets `PYTHONPATH`:
```bash
./run.sh src/carry/carry_paper.py --report          # live paper carry snapshot
./run.sh src/tools/backtest/xsmom_backtest.py --help
```
Or export the path yourself (see `run.sh`). Python 3.11+.

## Configuration & safety
- Copy `.env.example` → `.env` (gitignored) and fill in. **No secrets are committed.**
- Use API keys with **withdrawals disabled** and **IP restrictions**.
- Execution defaults to **testnet + dry-run**; real orders require multiple explicit switches
  (`LIVE_MODE=true`, `SPOT_LIVE=true`, `--live`, `allow_prod`).
- Run `./check_secrets.sh` before every commit — it fails on any leaked key/IP/host.

## Not financial advice
This code places real orders when deliberately armed. The documented conclusion is that
these strategies **lose money at retail scale.** Nothing here is a recommendation to trade.
See `LICENSE`.
