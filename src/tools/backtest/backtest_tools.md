# PredictEngine — Backtest & Analysis Tools

All tools live in `tools/` and never deploy to prod/stage.

---

## Tool Map

```
tools/
  analysis/
    signal_outcome_joiner.py   ← Step 1: join signals to outcomes
    signal_replay.py           ← Step 2: gate sweep / parameter tuning
    engine_analyst.py          ← Deep per-strategy analysis
    analyze.sh                 ← Pull server logs + HTML report
    analyze_correlation.py     ← Cascade/conflict/correlation report

  backtest/
    ohlcv_fetcher.py           ← Fetch Binance klines to cache
    ohlcv_replay.py            ← Walk-forward replay on real candles
    dist_builder.py            ← Build real distributions from outcomes
    synth_market_sim.py        ← Synthetic market simulator
    synth_sim.py               ← Simpler synthetic simulator
    synth_test.py              ← Gate unit tests + regression suite
    sim_learn_dists.py         ← Learn signal distributions from CSVs
    ohlcv_cache/               ← Cached Binance klines (auto-populated)
```

---

## Workflow: Gate Tuning (most common task)

**Question:** "Should I raise B's vpin gate from 0.55 to 0.65?"
**Answer in ~30 seconds using your real trade history.**

### Step 1 — Join signals to outcomes
```bash
cd tools/analysis
python signal_outcome_joiner.py --backup-dir ../../data_backup
# Output: signals_with_outcomes.csv (~22k rows of fired trades with results)
```

### Step 2 — Sweep gate parameters
```bash
# VPIN sweep across all strategies
python signal_replay.py --sweep vpin

# Full sweep of all numeric gates
python signal_replay.py --sweep all

# Deep dive on one strategy
python signal_replay.py --strategy B --sweep vpin --by direction exit_reason

# Compare two specific configs
python signal_replay.py --strategy B --filter "vpin>=0.55" --compare "vpin>=0.65"

# WR by direction (find long_only/short_only candidates)
python signal_replay.py --by direction

# WR by hour (find active_hours_utc candidates)
python signal_replay.py --by hour
```

**Key principle:** This replays already-fired trades through hypothetical filters.
It answers "if we had required vpin≥0.65, what WR would those trades have had?"
It does NOT predict future WR — it measures historical signal quality.

---

## Workflow: Exit Parameter Testing

**Question:** "Should W's trail be 0.16% or 0.20%?"
**Use synth_market_sim — it needs real distributions first.**

### Step 1 — Build real distributions (once, then update periodically)
```bash
cd tools/backtest
python dist_builder.py
# Reads: tools/analysis/signals_with_outcomes.csv
# Output: sim_dists.json
```

### Step 2 — Run compare mode
```bash
# Compare two trail distances on identical price paths
python synth_market_sim.py --strategy W --ticks 200000 --compare trail_dist 0.16 0.20

# Compare inertia settings
python synth_market_sim.py --strategy W --ticks 200000 --compare inertia_sec 45 90

# Compare vpin gates
python synth_market_sim.py --strategy L --ticks 100000 --compare vpin_min 0.50 0.62

# Run all strategies, all regimes
python synth_market_sim.py --ticks 500000 --dists sim_dists.json
```

**Strategies that fire in synthetic state:** L, W, E, Q, WB
**Don't fire yet (complex gate conditions):** K, Y, CGY, B (rare)

---

## Workflow: Walk-Forward Backtest

**Question:** "Does B's long_only gate hold up on 30 days of real candles?"
**Use ohlcv_replay — the closest thing to real backtesting.**

### Step 1 — Fetch data
```bash
cd tools/backtest

# All default symbols, last 30 days (~6 min)
python ohlcv_fetcher.py --days 30

# Specific symbols only
python ohlcv_fetcher.py --symbols BTCUSDT ARBUSDT ONDOUSDT --days 30

# Check cache status
python ohlcv_fetcher.py --status
```

### Step 2 — Run replay
```bash
# All live strategies
python ohlcv_replay.py

# Specific strategy + date range
python ohlcv_replay.py --strategy B --from 2026-05-15 --to 2026-06-01

# Walk-forward (train 21 days, validate 9 days)
python ohlcv_replay.py --strategy B --walk-forward

# Note: ohlcv_replay is partially built — best for B and L validation
```

**Cache location:** `tools/backtest/ohlcv_cache/SYMBOL_1m.csv`
**Rate limits:** ~6 min for 50 symbols × 30 days. Cache is reused.

---

## Workflow: Regression Tests

**Run before every deploy to catch gate logic bugs.**

```bash
cd tools/backtest

# All test modes
python synth_test.py

# Unit tests only (fastest, catches gate logic bugs)
python synth_test.py --mode gate

# Synthetic replay only
python synth_test.py --mode synth

# CSV replay (uses real signals if present)
python synth_test.py --mode csv
```

---

## Workflow: Deep Strategy Analysis

```bash
cd tools/analysis

# Full analysis of all strategies from CSV data
python engine_analyst.py --backup-dir ../../data_backup

# Per-strategy breakdown with direction analysis
python engine_analyst.py --strategy B --direction

# Symbol-level analysis (find bad coins to blacklist)
python engine_analyst.py --strategy B --by symbol
```

---

## Data Flow

```
Stage demo trades
      ↓
data_backup/*_stage/prod/signals_combined.csv  (pulled by analyze.sh)
      ↓
signal_outcome_joiner.py
      ↓
tools/analysis/signals_with_outcomes.csv       (22k+ rows, grows over time)
      ↓
signal_replay.py ──── gate tuning decisions
dist_builder.py  ──→  sim_dists.json ──→  synth_market_sim.py ── exit tuning
engine_analyst.py ─── deep analysis
```

---

## Tool Status

| Tool | Status | Best For |
|---|---|---|
| `signal_outcome_joiner.py` | ✅ proven | Building the outcomes dataset |
| `signal_replay.py` | ✅ proven | Gate parameter sweeps (primary tuning tool) |
| `engine_analyst.py` | ✅ proven | Direction analysis, symbol blacklisting |
| `dist_builder.py` | ✅ works | Building real distributions for synth sim |
| `synth_market_sim.py` | ✅ works | Exit param testing (trail, SL, inertia) |
| `synth_test.py` | ✅ works | Pre-deploy regression checks |
| `ohlcv_fetcher.py` | ✅ proven | Fetching clean Binance klines |
| `ohlcv_replay.py` | ⚠️ partial | Walk-forward (B and L best supported) |
| `sim_learn_dists.py` | 🔵 legacy | Superseded by dist_builder.py |

---

## Key Insight: Why Stage Exists

Stage runs all 14 strategies with loose gates on demo account.
Every demo trade populates `signals_combined.csv`.
After a few sessions: re-run `signal_outcome_joiner.py` + `signal_replay.py`
and you have fresh gate-tuning data for every strategy, not just B.

**Target dataset:** 1,000+ fired trades per strategy → reliable sweep results.
B already has 3,000+ trades. C, G, Q, S, Y, WB, QQ need stage data to accumulate.
