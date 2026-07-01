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
    mtf_bias_min: float = 20.0   # B MTF-bias entry gate; engine reads via getattr. prod default 20; stage B sets 8.
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
    ofi_mode:                 bool = False   # OF: order-flow imbalance (OBI + depth-flow)
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
    # shadow=True → strategy is a pure sim observer: it fires + logs trades for
    # data collection, but is COMPLETELY OUTSIDE the global position lock. It never
    # claims a symbol slot, never blocks other strategies (any direction), never
    # counts toward GLOBAL_MAX_OPEN_PER_SYM, never wins the live order, and never
    # triggers a real Binance close. Use for new/experimental strategies so they
    # don't interfere with the proven live book. (live_exec is ignored when True.)
    shadow:               bool = False
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

    # ── C: Sniper ────────────────────────────────────────────────
    StrategyConfig(
        name='Sniper (VPIN+score≥70)', label='C', color='#ffd740',
        warmup_sec=30.0,
        vpin_min=0.10, vpin_max=0.99,
        max_conflict=4,
        min_vol_atr=0.10, min_conf=20, min_score=10.0,
        win_thr=0.40, atr_tp_mult=1.30, trail_dist=0.06,
        inertia_sec=9999.0, inertia_thr=0.001,
        rev_min_hold=9999.0,
        cooldown_sec=60.0,
        min_score_at_fire=10.0,
        sus_ticks=2, sus_score_thr=10.0,
        loss_streak_limit=5, loss_streak_cd=120.0,
        symbol_blacklist=frozenset(),
        oi_stack_filter=False,
        active_hours_utc=(),
        live_exec=True,
        disabled=False,
    ),

    # ── G: Spike-Hunter ──────────────────────────────────────────
    StrategyConfig(
        name='Spike-Hunter (VPIN≥0.65+accel≥2×)', label='G', color='#ff7043',
        vpin_min=0.10, vpin_max=0.99,
        max_conflict=4,
        min_conf=20, min_score=10.0,
        warmup_sec=20.0,
        use_accel_gate=True, min_accel=1.2,
        win_thr=0.32,
        atr_tp_mult=0.85, atr_sl_mult=0.90,
        trail_dist=0.07,
        inertia_sec=9999.0, inertia_thr=0.001,
        rev_min_hold=9999.0,
        max_window=300.0,
        cooldown_sec=60.0,
        min_score_at_fire=10.0,
        sus_ticks=2, sus_score_thr=10.0,
        loss_streak_limit=5,
        symbol_blacklist=frozenset(),
        oi_stack_filter=False,
        active_hours_utc=(),
        live_exec=True,
        disabled=False,
    ),

    # ── K: Exhaustion Wick Fade ───────────────────────────────────
    StrategyConfig(
        name='Exhaustion Wick Fade', label='K', color='#ce93d8',
        vpin_min=0.05,
        min_conf=5, min_score=1.0,
        min_vol_atr=0.05,
        use_accel_gate=False, max_conflict=5,
        win_thr=0.10,
        atr_tp_mult=999.0,
        atr_sl_mult=1.90,
        trail_dist=0.18,
        inertia_sec=9999.0, inertia_thr=0.001,
        rev_min_hold=9999.0,
        max_window=300.0,
        min_hold_any=5.0, cooldown_sec=15.0,
        loss_streak_limit=10,
        impulse_fade_mode=True,
        symbol_blacklist=frozenset(),
        live_exec=True,
        disabled=False,
    ),

    # ── L: Candle S/R Level ───────────────────────────────────────
    StrategyConfig(
        name='Candle S/R Level', label='L', color='#80cbc4',
        vpin_min=0.20,
        min_conf=25, min_score=10.0,
        min_vol_atr=0.10,
        spread_max_mult=2.0,
        use_accel_gate=False, max_conflict=4,
        win_thr=0.40,
        atr_tp_mult=1.60, atr_sl_mult=2.00,
        trail_dist=0.28,
        inertia_sec=9999.0, inertia_thr=0.001,
        rev_min_hold=120.0,
        max_window=1200.0,
        min_hold_any=20.0, cooldown_sec=30.0,
        candle_level_mode=True,
        oi_stack_filter=False,
        loss_streak_limit=5, loss_streak_cd=300.0,
        symbol_blacklist=frozenset(),
        live_exec=True,
        disabled=False,
    ),

    # ── Q: Funding Rate Fade ──────────────────────────────────────
    StrategyConfig(
        name='Funding Rate Fade', label='Q', color='#b39ddb',
        vpin_min=0.10, min_conf=15, min_score=5.0,
        min_vol_atr=0.05,
        use_accel_gate=False, max_conflict=5,
        spread_max_mult=1.5,
        win_thr=0.30,
        atr_tp_mult=3.00,
        atr_sl_mult=1.50,
        trail_dist=0.15,
        inertia_sec=9999.0, inertia_thr=0.001, rev_min_hold=9999.0,
        max_window=7200.0,
        min_hold_any=10.0, cooldown_sec=30.0,
        funding_fade_mode=True,
        symbol_blacklist=frozenset(),
        loss_streak_limit=5, loss_streak_cd=1800.0,
        live_exec=True,
        disabled=False,
    ),

    # ── S: OI Divergence ─────────────────────────────────────────
    StrategyConfig(
        name='OI Divergence', label='S', color='#80cbc4',
        vpin_min=0.10, min_conf=15, min_score=5.0,
        min_vol_atr=0.08,
        use_accel_gate=False, max_conflict=5,
        win_thr=0.30,
        atr_tp_mult=1.50,
        atr_sl_mult=0.90,
        trail_dist=0.12,
        inertia_sec=9999.0, inertia_thr=0.001, rev_min_hold=9999.0,
        max_window=1200.0, min_hold_any=30.0, cooldown_sec=60.0,
        loss_streak_limit=5, loss_streak_cd=600.0,
        symbol_blacklist=frozenset(),
        live_exec=True,
        disabled=False,
    ),

    # ── E: EMA8/EMA21 Crossover ───────────────────────────────────
    StrategyConfig(
        name='EMA8/EMA21 Crossover (1m)', label='E', color='#ffb74d',
        vpin_min=0.20,
        min_conf=25, min_score=5.0,
        min_vol_atr=0.10,
        use_accel_gate=False, max_conflict=4,
        win_thr=0.12,
        atr_tp_mult=999.0,
        atr_sl_mult=1.00,
        trail_dist=0.08,
        inertia_sec=9999.0, inertia_thr=0.001, rev_min_hold=9999.0,
        max_window=600.0,
        min_hold_any=15.0, cooldown_sec=30.0,
        ema_cross_mode=True,
        loss_streak_limit=5, loss_streak_cd=600.0,
        symbol_blacklist=frozenset(),
        live_exec=True,
        disabled=False,
    ),

    # ── W: BTC Decorrelation ──────────────────────────────────────
    StrategyConfig(
        name='BTC Decorrelation', label='W', color='#4fc3f7',  # vpin≥0.75 → 51.8%WR +0.022%/T (in _b)
        vpin_min=0.05,
        min_conf=5, min_score=1.0,
        min_vol_atr=0.05,
        use_accel_gate=False, max_conflict=5,
        win_thr=0.05,
        atr_tp_mult=999.0, atr_sl_mult=2.00,
        trail_dist=0.16,
        inertia_sec=9999.0, inertia_thr=0.001, rev_min_hold=9999.0,
        max_window=600.0,
        min_hold_any=5.0, cooldown_sec=30.0,
        decorrelation_mode=True,
        loss_streak_limit=10, loss_streak_cd=60.0,
        symbol_blacklist=frozenset(),
        live_exec=True,
        disabled=False,
    ),

    # ── Z: Lag Arb — KEEP DISABLED (0/14413 profitable, structural impossibility) ──
    StrategyConfig(
        name='Lag Arb (cross-exchange)', label='Z', color='#26c6da',
        vpin_min=0.62, vpin_max=0.99,
        min_conf=50, min_score=0.0,  # score gate HURTS Z: score≥5 → WR drops 43.8%→37.3% (n=1453 confirmed)
        min_vol_atr=0.20,
        use_accel_gate=False, max_conflict=3,
        win_thr=0.20,
        atr_tp_mult=999.0, atr_sl_mult=0.90,
        trail_dist=0.15,
        inertia_sec=9999.0, inertia_thr=0.001, rev_min_hold=9999.0,
        max_window=120.0,
        min_hold_any=5.0, cooldown_sec=60.0,
        loss_streak_limit=3, loss_streak_cd=300.0,
        symbol_blacklist=frozenset({'BTCUSDT','ETHUSDT','SOLUSDT','STRKUSDT'}),
        live_exec=True,
        disabled=False,  # re-enabled on stage for data collection
    ),

    # ── Y: Star Pattern ───────────────────────────────────────────
    StrategyConfig(
        name='Star Pattern (3-candle fade)', label='Y', color='#ffcc02',
        vpin_min=0.20, vpin_max=0.99,
        min_conf=15, min_score=5.0,
        min_vol_atr=0.10,
        use_accel_gate=False, max_conflict=4,
        win_thr=0.20,
        atr_tp_mult=999.0, atr_sl_mult=2.00,  # widened 1.30→2.00: trail 65%WR, SL 0%WR dominating
        trail_dist=0.15,
        inertia_sec=9999.0, inertia_thr=0.001, rev_min_hold=9999.0,
        max_window=600.0, min_hold_any=20.0, cooldown_sec=60.0,
        loss_streak_limit=5, loss_streak_cd=600.0,
        symbol_blacklist=frozenset(),
        live_exec=True,
        disabled=False,
    ),

    # ── B: MTF Momentum ──────────────────────────────────────────
    StrategyConfig(
        name='MTF Momentum', label='B', color='#80cbc4',
        vpin_min=0.10, vpin_max=0.99,
        max_conflict=5,
        min_conf=10, min_score=1.0,
        min_vol_atr=0.05,
        mtf_bias_min=8.0,                                # keep stage B loose (8); engine default is 20 (prod)
        use_accel_gate=False,
        win_thr=0.08,
        atr_tp_mult=999.0,
        atr_sl_mult=1.30,
        trail_dist=0.08,
        inertia_sec=9999.0, inertia_thr=0.001, rev_min_hold=9999.0,
        max_window=600.0, min_hold_any=5.0, cooldown_sec=30.0,
        mtf_momentum_mode=True,
        loss_streak_limit=10, loss_streak_cd=60.0,
        symbol_blacklist=frozenset(),
        live_exec=True,
        disabled=False,
    ),

    # ── P: Parabolic Blowup Short ─────────────────────────────────
    StrategyConfig(
        name='Parabolic Blowup Short', label='P', color='#ff4757',
        vpin_min=0.10,
        min_conf=5, min_score=1.0,
        min_vol_atr=0.10,
        use_accel_gate=False, max_conflict=5,
        win_thr=0.10,
        atr_tp_mult=999.0,
        atr_sl_mult=1.50,
        trail_dist=0.20,
        inertia_sec=9999.0, inertia_thr=0.001,
        rev_min_hold=9999.0,
        max_window=600.0,
        min_hold_any=10.0,
        cooldown_sec=60.0,
        parabolic_mode=True,
        loss_streak_limit=10, loss_streak_cd=120.0,
        symbol_blacklist=frozenset(),
        live_exec=True,
        disabled=False,
    ),

    # ── CGY: Combined ─────────────────────────────────────────────
    StrategyConfig(
        name='CGY Combined (C+G+Y agree)', label='CGY', color='#ff9f43',
        vpin_min=0.20, vpin_max=0.99,
        min_conf=20, min_score=10.0,
        min_vol_atr=0.10,
        use_accel_gate=False, max_conflict=5,
        win_thr=0.15,
        atr_tp_mult=999.0,
        atr_sl_mult=1.20,
        trail_dist=0.12,
        inertia_sec=9999.0, inertia_thr=0.001,
        rev_min_hold=9999.0,
        max_window=300.0,
        min_hold_any=15.0, cooldown_sec=120.0,
        cgy_mode=True,
        loss_streak_limit=5, loss_streak_cd=300.0,
        symbol_blacklist=frozenset(),
        active_hours_utc=(),
        live_exec=True,
        disabled=False,
    ),

    # ── WB: Combined W+B ─────────────────────────────────────────
    StrategyConfig(
        name='WB Combined (W+B agree)', label='WB', color='#48dbfb',
        vpin_min=0.20, vpin_max=0.99,
        min_conf=15, min_score=5.0,
        min_vol_atr=0.10,
        use_accel_gate=False, max_conflict=5,
        win_thr=0.20,
        atr_tp_mult=999.0,
        atr_sl_mult=1.40,
        trail_dist=0.18,
        inertia_sec=9999.0, inertia_thr=0.001,
        rev_min_hold=9999.0,
        max_window=600.0,
        min_hold_any=15.0, cooldown_sec=60.0,
        wb_mode=True,
        loss_streak_limit=5, loss_streak_cd=300.0,
        symbol_blacklist=frozenset(),
        active_hours_utc=(),
        live_exec=True,
        disabled=False,
    ),

    # ── QQ: Full Funding Cycle ────────────────────────────────────
    StrategyConfig(
        name='Full Funding Cycle (8h)', label='QQ', color='#ce93d8',
        vpin_min=0.10, vpin_max=0.99,
        min_conf=15, min_score=5.0,
        min_vol_atr=0.05,
        use_accel_gate=False, use_spread_gate=True, spread_max_mult=1.5,
        max_conflict=5,
        win_thr=0.40,
        atr_tp_mult=4.00, atr_sl_mult=1.95,
        trail_dist=0.20,
        inertia_sec=9999.0, inertia_thr=0.001, rev_min_hold=9999.0,
        max_window=28800.0,
        min_hold_any=60.0, cooldown_sec=300.0,
        funding_fade_mode=True, funding_normalise_exit=True,
        active_hours_utc=((0,8),(8,16),(16,24)),
        symbol_blacklist=frozenset(),
        live_exec=True,
        disabled=False,
    ),

    # ══════════════════════════════════════════════════════════════
    # NEW (stage data-collection 2026-06-11): implemented-but-undeployed
    # gates wired into the STRATEGIES list. Gate + signal feed verified
    # live for each. Loose-ish gates to gather decided trades fast; tighten
    # once signal_outcome_joiner has ~1000T/strategy. live_exec=True = demo
    # execution on demo-fapi (no real money). DO NOT copy to prod as-is.
    # ══════════════════════════════════════════════════════════════

    # ── R: Liquidation Cascade Fade ───────────────────────────────
    # Rides the direction a cascade pushes (long-liq cascade → short, etc).
    # _r_gate enforces R_ENTRY_DELAY_SEC=20 anti-knife-catch delay; fire()
    # floors dyn_tp at 0.25% for liq_cascade_mode. Wide trail per the
    # "trail exits are the only profitable exit" finding.
    StrategyConfig(
        name='Liquidation Cascade Fade', label='R', color='#e74c3c',
        vpin_min=0.20, vpin_max=0.99,
        min_conf=10, min_score=1.0,
        min_vol_atr=0.10,
        use_accel_gate=False, max_conflict=5,
        win_thr=0.15,
        atr_tp_mult=999.0, atr_sl_mult=1.40,
        trail_dist=0.16,
        inertia_sec=9999.0, inertia_thr=0.001, rev_min_hold=9999.0,
        max_window=600.0, min_hold_any=10.0, cooldown_sec=60.0,
        liq_cascade_mode=True,
        loss_streak_limit=5, loss_streak_cd=300.0,
        symbol_blacklist=frozenset(),
        live_exec=False, shadow=True,
        disabled=False,
    ),

    # ── U: Density Bounce (multi-touch wall) ──────────────────────
    # Fires when price approaches a stable, multi-touch orderbook wall.
    # U_LONG_DISABLED (config.py) currently False — collecting both dirs.
    StrategyConfig(
        name='Density Bounce (multi-touch wall)', label='U', color='#1abc9c',
        vpin_min=0.20, vpin_max=0.99,
        min_conf=40, min_score=25.0,  # conf≥40→62%WR +0.021%/T; score≥25→55.7%WR +0.019%/T
        min_vol_atr=0.08,
        use_accel_gate=False, max_conflict=5,
        win_thr=0.20,
        atr_tp_mult=1.50, atr_sl_mult=1.20,
        trail_dist=0.12,
        inertia_sec=9999.0, inertia_thr=0.001, rev_min_hold=9999.0,
        max_window=900.0, min_hold_any=15.0, cooldown_sec=60.0,
        density_bounce_mode=True,
        loss_streak_limit=5, loss_streak_cd=300.0,
        symbol_blacklist=frozenset(),
        live_exec=True,  # promoted: conf≥40 → 62%WR, score≥25 → 55.7%WR (n=433)
        disabled=False,
    ),

    # ── V: Absorption Reversal ────────────────────────────────────
    # Large one-sided volume that fails to move price = absorption; fade
    # toward the absorbing side. abs signal from calc_absorption() each tick.
    StrategyConfig(
        name='Absorption Reversal', label='V', color='#9b59b6',
        vpin_min=0.20, vpin_max=0.99,
        min_conf=65, min_score=1.0,   # retuned 10→65: V_B (conf 65-75) hit 58.3%WR vs base 54.1%
        min_vol_atr=0.10,
        use_accel_gate=False, max_conflict=5,
        win_thr=0.15,
        atr_tp_mult=999.0, atr_sl_mult=1.30,
        trail_dist=0.12,
        inertia_sec=9999.0, inertia_thr=0.001, rev_min_hold=9999.0,
        max_window=600.0, min_hold_any=15.0, cooldown_sec=90.0,
        absorption_reversal_mode=True,
        loss_streak_limit=5, loss_streak_cd=300.0,
        symbol_blacklist=frozenset(),
        live_exec=True, shadow=False,   # PROMOTED 2026-06-12 (retuned conf=65)
        disabled=False,
    ),

    # ── T: Regime Trend Follower ──────────────────────────────────
    # LONGER-HORIZON: only fires after 12+ consecutive trend ticks
    # (_update_trend_state) and only WITH the trend. Wide trail + 30-min
    # window to ride persistence rather than scalp. This is the long-hold
    # diversifier vs the sub-30-min book.
    StrategyConfig(
        name='Regime Trend Follower', label='T', color='#3498db',
        vpin_min=0.25, vpin_max=0.99,
        min_conf=20, min_score=10.0,
        min_vol_atr=0.10,
        use_accel_gate=False, max_conflict=5,
        win_thr=0.25,
        atr_tp_mult=999.0, atr_sl_mult=1.60,
        trail_dist=0.22,
        inertia_sec=9999.0, inertia_thr=0.001, rev_min_hold=9999.0,
        max_window=1800.0, min_hold_any=30.0, cooldown_sec=120.0,
        regime_trend_mode=True,
        loss_streak_limit=5, loss_streak_cd=300.0,
        symbol_blacklist=frozenset(),
        live_exec=False, shadow=True,
        disabled=False,
    ),

    # ══════════════════════════════════════════════════════════════
    # NEW (stage 2026-06-11, batch 2): longer-hold forks + OFI gate.
    # ══════════════════════════════════════════════════════════════

    # ── BL: MTF Momentum (long-hold fork of B) ────────────────────
    # SAME entry signal as B (mtf_momentum_mode → _b_gate), tighter gates,
    # but 2h window + wide trail to let winners run. Clean A/B on holding
    # period: B scalps the move, BL rides it. Direct attack on fee drag —
    # one set of fees per 2h hold vs many per scalp.
    StrategyConfig(
        name='MTF Momentum (long-hold)', label='BL', color='#26a69a',
        vpin_min=0.20, vpin_max=0.99,
        max_conflict=5,
        min_conf=15, min_score=5.0,
        min_vol_atr=0.08,
        use_accel_gate=False,
        win_thr=0.20,
        atr_tp_mult=999.0,
        atr_sl_mult=1.60,
        trail_dist=0.28,
        inertia_sec=9999.0, inertia_thr=0.001, rev_min_hold=9999.0,
        max_window=7200.0, min_hold_any=60.0, cooldown_sec=180.0,
        mtf_momentum_mode=True,
        loss_streak_limit=5, loss_streak_cd=600.0,
        symbol_blacklist=frozenset(),
        live_exec=False, shadow=True,
        disabled=True,   # KILLED 2026-06-12: -36% net, 40.4%WR — inherits broken loose-gate MTF signal (B itself -50%)
    ),

    # ── CGYL: CGY Combined (long-hold fork of CGY) ────────────────
    StrategyConfig(
        name='CGY Combined (long-hold)', label='CGYL', color='#fbc531',
        vpin_min=0.30, vpin_max=0.99,
        min_conf=25, min_score=15.0,
        min_vol_atr=0.10,
        use_accel_gate=False, max_conflict=5,
        win_thr=0.25,
        atr_tp_mult=999.0,
        atr_sl_mult=1.50,
        trail_dist=0.28,
        inertia_sec=9999.0, inertia_thr=0.001, rev_min_hold=9999.0,
        max_window=7200.0, min_hold_any=60.0, cooldown_sec=180.0,
        cgy_mode=True,
        loss_streak_limit=5, loss_streak_cd=600.0,
        symbol_blacklist=frozenset(),
        live_exec=False, shadow=True,
        disabled=False,
    ),

    # ── OF: Order Flow Imbalance ──────────────────────────────────
    # Static depth-weighted OBI (calc_obi) confirmed by dynamic depth-flow
    # from book_history, + price micro-confirmation. Short-horizon by nature
    # → tight window/trail/cooldown. New family vs the existing book.
    StrategyConfig(
        name='Order Flow Imbalance', label='OF', color='#f39c12',
        vpin_min=0.20, vpin_max=0.99,
        min_conf=10, min_score=1.0,
        min_vol_atr=0.08,
        use_accel_gate=False, max_conflict=5,
        win_thr=0.12,
        atr_tp_mult=999.0, atr_sl_mult=1.20,
        trail_dist=0.10,
        inertia_sec=9999.0, inertia_thr=0.001, rev_min_hold=9999.0,
        max_window=300.0, min_hold_any=8.0, cooldown_sec=45.0,
        ofi_mode=True,
        loss_streak_limit=5, loss_streak_cd=300.0,
        symbol_blacklist=frozenset(),
        live_exec=True, shadow=False,   # PROMOTED 2026-06-12: 4022T, 52.6%WR, +0.0137%/T, +54.9% net
        disabled=False,
    ),
]
