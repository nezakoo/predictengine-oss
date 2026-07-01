"""
PredictEngine - strategies_config.py

TIGHTENING PASS — 2026-06-02 18:00 UTC
────────────────────────────────────────────────────────────────
PHASE CHANGE: testing → quality mode. Based on 81,406-trade / 1.5-month dataset.

REMOVED from engine entirely:
  T   — no signal implementation (classify_regime doesn't exist in signals file)
  R   — 17% WR, 93% rev exits, 41T confirmed no edge
  X   — 31.5% WR, rev dominant 49%, R:R=0.63, direction logic broken
  O   — 27.5% WR, SL dominant 55%, confirmed broken (was disabled)
  P   — 15% WR confirmed, no edge (was disabled)
  N   — 28.5% WR, equal tp/sl split, POC entries not predictive

RE-ENABLED with fix:
  E   — short direction was broken (321 shorts 35%WR vs 114 longs 52%WR) due to
        htf_trend filter bug: 'if htf_trend and ...' silently skips when EMA20
        fails to compute, letting counter-trend entries fire. Fixed in signals:
        'if not htf_trend or htf_trend != direction' (fail closed).
        Now long-only in practice (EMA20 trend filter enforces it correctly).

ROOT CAUSE FINDINGS:
  Z  conf=0 on 80% of v17 entries — inertia disabled during testing removed the
     only quality gate. Profitable variant: inertia_thr=0.10 gave +3.03%/T (153T).
     Fix: restore inertia_sec=50s, inertia_thr=0.10; raise min_conf 50→65;
     revert atr_sl_mult 1.80→0.90 and trail_dist 0.20→0.15.

  K  631/3317 v17 trades hit max_window=300s exactly (avg dur=300.1s) — all losers.
     K resolves at trail avg=130s, SL avg=93s. Anything open at 2.5min is dead.
     Fix: max_window 300→150s. Reset atr_sl_mult 1.90→1.20 (widening didn't help).
     Quality gate: min_conf 25→40, min_score 10→25.

TIGHTENING SUMMARY (quality-mode entry gates):
  C   inertia restored (44% inertia exits during testing = inertia was disabled)
  G   max_window 180→300 (19% time exits at low WR), atr_sl_mult 0.85→1.10
  K   max_window 300→150, atr_sl_mult 1.90→1.20, min_conf 25→40, min_score 10→25
  L   min_score 35→45, rev_min_hold 9999→120 (11% rev exits bleeding)
  Q   atr_sl_mult 1.90→1.50 (was widened too far), min_score 10→20
  W   min_conf/score 0/0→25/15, light inertia restored (inertia=7% of exits)
  Y   atr_sl_mult 1.05→1.30 (SL 36% of exits; widen to reduce shakeouts)
  B   atr_sl_mult 0.70→1.00 (SL 49% dominant), min_conf 45→55
  S   atr_sl_mult 0.65→0.90 (SL 52% dominant), min_score 15→20
  QQ  min_score 8→20 (quality gate)
  E   re-enabled — signal fix deployed; longs only via EMA20 trend filter

DISABLED 2026-06-03 (added to disabled=True):
  Z   debug_lag: 0/14413 signals profitable after fees; median div=0.11% vs fee=0.60%
  G   avg_score=-46 at fire; entering downtrends not exhausted spikes; -$40.2/7h
  L   228/1076 rev exits; near-random S/R entries; -$8.6/7h; no config fix possible

TIGHTENED 2026-06-03:
  K   max_window 150→90s (537/2494 time exits = 21.5%, still too many)
"""

import time, csv
from dataclasses import dataclass
from collections import deque, defaultdict
from datetime import datetime
from typing import Optional

import engine as E
from config import FEE_RT, VERSION, SPREAD_MAX_PCT

# ── Global coin blacklist ─────────────────────────────────────────
# Coins blocked from ALL strategies regardless of per-strategy blacklist.
# BTC/ETH/SOL: required for signal baselines (W decorrelation, score).
# STRKUSDT: thin tape, consistently high spread.
# Alpha/illiquid coins (HYPE, NIL, DYDX etc.) are handled automatically
# by the illiquid filter in _check_symbol() — no need to list them here.
GLOBAL_BLACKLIST = frozenset({'BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'STRKUSDT'})


# ══ CONFIG ════════════════════════════════════════════════════════
@dataclass
class StrategyConfig:
    name: str; label: str; color: str
    vpin_min: float = 0.45; min_conf: int = 65; min_score: float = 50.0
    vpin_max: float = 0.80
    max_score: float = 80.0
    min_accel: float = 0.0; min_vol_atr: float = 0.25
    use_spread_gate: bool = True; use_accel_gate: bool = True
    max_conflict: int = 0
    impulse_fade_mode: bool = False
    candle_level_mode: bool = False
    volume_wall_mode:  bool = False
    volume_profile_mode: bool = False
    consolidation_mode:  bool = False
    ema_cross_mode:       bool = False
    ema_trend_period:    int  = 21
    breakout_retest_mode: bool = False
    funding_fade_mode:    bool = False
    liq_cascade_mode:     bool = False
    oi_divergence_mode:   bool = False
    density_bounce_mode:  bool = False
    decorrelation_mode:   bool = False
    knife_catch_mode:     bool = False
    lag_monitor_mode:     bool = False
    regime_trend_mode:    bool = False
    funding_normalise_exit: bool = False
    absorption_reversal_mode: bool = False
    star_pattern_mode:        bool = False
    warmup_sec:              float = -1.0
    impulse_min_pct:         float = 0.50
    mtf_momentum_mode:        bool = False
    spoof_fade_mode:          bool = False
    parabolic_mode:           bool = False   # P: parabolic blowup short after rapid pump
    allow_alpha:              bool = False   # if True: trade alpha/illiquid coins too (B-test universe)
    cgy_mode:                 bool = False   # CGY: combined C+G+Y mean-reversion confirmation
    wb_mode:                  bool = False   # WB:  combined W+B momentum confirmation
    disabled:             bool = False
    # Direction filter — set to True to restrict strategy to one direction.
    # Based on signal_replay.py analysis: some strategies have strong direction asymmetry.
    # When long_only=True: blocks any 'short' fires. short_only=True: blocks 'long' fires.
    # Both False (default): strategy fires in both directions.
    long_only:            bool = False
    short_only:           bool = False
    # live_exec=True → strategy runs in simulation only, no real Binance orders.
    # Use for: unproven strategies, B-tests, strategies under observation.
    # live_exec=True (default) → real orders placed when LIVE_ENABLED=True.
    live_exec:            bool = True
    # exit params
    win_thr: float = 0.45; atr_tp_mult: float = 1.10; atr_sl_mult: float = 0.80
    trail_dist: float = 0.08; inertia_sec: float = 45.0; inertia_thr: float = 0.12
    rev_min_hold: float = 60.0; max_window: float = 300.0
    min_hold_any: float = 10.0; cooldown_sec: float = 300.0
    spread_max_mult: float = 2.0
    min_score_at_fire: float = 0.0
    inertia_floor_mode:   bool = False   # when True: inertia waits for dp>=FEE_RT before closing
    loss_streak_limit: int   = 0
    loss_streak_cd:    float = 600.0
    symbol_blacklist: frozenset = frozenset()
    active_hours_utc: tuple = ()
    sus_ticks:      int   = 5
    sus_score_thr:  float = 30.0
    oi_stack_filter: bool = False


STRATEGIES = [
    StrategyConfig(
        name='MTF Momentum', label='B', color='#80cbc4',
        vpin_min=0.55, vpin_max=0.88,
        max_conflict=2,
        min_conf=55, min_score=15.0,                     # was 45/10 — quality tighten
        min_vol_atr=0.25,
        use_accel_gate=False,
        win_thr=0.12,
        atr_tp_mult=999.0,
        atr_sl_mult=1.30,                                # 1.00→1.30: B_B conclusive (72.7%WR trail-dom vs 62.9%SL-dom)
        trail_dist=0.08,                                 # 0.08→0.12: best strategy this session, let winners run
        inertia_sec=9999.0, inertia_thr=0.001, rev_min_hold=9999.0,
        max_window=600.0, min_hold_any=25.0, cooldown_sec=180.0,  # 120→180: reduce B overtrading
        mtf_momentum_mode=True,
        loss_streak_limit=3, loss_streak_cd=600.0,
        # long_only: signal_replay.py analysis (3,053 trades, 2026-06-10):
        #   longs 1,377T: 59.5%WR, avg=+0.0047% (PROFITABLE)
        #   shorts 1,676T: 56.3%WR, avg=-0.0252% (loss driver)
        # MTF momentum fires during BTC uptrend — longs ride momentum, shorts fade it.
        # B-test short variant added to strategies_config_b.py for continued monitoring.
        long_only=True,
        symbol_blacklist=frozenset({
            '1000PEPEUSDT','BTCUSDT','DOTUSDT','ENAUSDT','ETHUSDT',
            'JTOUSDT','JUPUSDT','NEARUSDT','SOLUSDT','STRKUSDT','SUIUSDT',
            'TONUSDT','WLDUSDT',
            # Added 2026-06-10 — signal_replay.py: both-direction losers, n>=10, combined avg<-0.12%
            'WIFUSDT',   # n=11, combined avg=-0.31% (L:-0.41% S:-0.27%)
            'GALAUSDT',  # n=16, combined avg=-0.19% (L:-0.06% S:-0.29%)
            'TIAUSDT',   # n=32, combined avg=-0.16% (L:-0.07% S:-0.22%)
            'APTUSDT',   # n=16, combined avg=-0.13% (L:-0.06% S:-0.26%)
        }),
    ),
]

# Prod: only B fires live orders — no changes needed (live_exec=True already set)
