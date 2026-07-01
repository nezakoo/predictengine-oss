"""
strategies_config_b_stage.py — Stage B-test variants
======================================================
Based on signal_replay analysis (2026-06-11, 24,022 matched trades):

Key findings:
  B     longs +0.018%/T, shorts -0.026%/T → long_only confirmed
        mtf_bias threshold was 20 (unreachable) → fixed to 8 in main
  Z     longs 60.4%WR +0.097%/T, shorts 37.2%WR -0.153%/T → long_only promising
        vpin_min=0.62 blocking 99% of signals → testing lower thresholds
  E     HTF trend filter blocking 99.97% of signals → testing without filter
  CGY   shorts 42.0%WR vs longs 39.7% → short_only + wider SL
  L     shorts 50.4%WR -0.016%/T vs longs 45.2%WR -0.055%/T → short_only
  QQ    shorts -0.009%/T nearly breakeven vs longs -0.082%/T → short_only
  K     SL dominant (1547/5343) → needs wider SL
  Q     trail exits 91.1%WR when they hit → widen SL to reach trail more
  Y     SL 0%WR at 46% of exits → widened atr_sl_mult 1.30→2.00 in main

All B-tests: live_exec=False (sim shadow) so they don't consume position slots.
Compare vs base strategy after 500+ trades per variant.
"""
from dataclasses import replace
from strategies_config import STRATEGIES, StrategyConfig

_map = {s.label: s for s in STRATEGIES}

def _tweak(label, **kw):
    base = _map[label]
    tag = '  '.join(f'{k}={v}' for k, v in kw.items())
    return replace(base, name=f"{label} [B: {tag[:60]}]", live_exec=False, **kw)

STRATEGIES_B = [

    # ── B: mtf threshold variants (threshold was 20 = unreachable, fixed to 8 in main)
    # Now testing direction split on top of the fix
    # Base data: longs +0.018%/T (n=1608), shorts -0.026%/T (n=1729)
    _tweak('B', long_only=True,  short_only=False),   # B_long: confirm long edge
    _tweak('B', long_only=False, short_only=True),    # B_short: monitor short direction

    # ── Z: lower VPIN gate (vpin=0.62 blocking 21k signals, avg VPIN at block=0.48)
    # Lag arb edge is price divergence speed, not order flow imbalance
    _tweak('Z', vpin_min=0.45),                               # Z_vpin45: lower floor
    _tweak('Z', vpin_min=0.35),                               # Z_vpin35: aggressive
    _tweak('Z', vpin_min=0.45, long_only=True),               # Z_vpin45_long: combined

    # ── Z: score gate CONFIRMED HARMFUL (62k trade dataset)
    # score≥0:  n=1113, WR=43.8%  ← baseline
    # score≥5:  n=217,  WR=37.3%  ← WR DROPS -6.5pp
    # score≥25: n=164,  WR=27.4%  ← WR DROPS -16.4pp
    # Direction edge confirmed: longs 51.8%WR +0.053%/T vs shorts -0.140%/T
    _tweak('Z', long_only=True),                              # Z_long: longs 51.8%WR +0.053%/T
    _tweak('Z', long_only=True, vpin_min=0.45),               # Z_long_vpin45: combined
    _tweak('Z', long_only=True, min_conf=70),                 # Z_long_conf70: conf≥70→49.1%WR

    # ── E: HTF trend filter blocking 99.97% of signals (3459 blocked vs 1 fired)
    # The filter `if not htf_trend or htf_trend != direction: continue` is too strict
    # Test: disable HTF filter entirely to see raw EMA crossover performance
    _tweak('E', ema_trend_period=0),                          # E_no_htf: bypass HTF filter
    _tweak('E', short_only=True,  ema_trend_period=0),        # E_short_no_htf
    _tweak('E', long_only=True,   ema_trend_period=0),        # E_long_no_htf

    # ── U: conf/score gate variants (conf≥40→62%WR confirmed) ─────
    # score≥40: n=40, WR=70%, avg=+0.079%/T ← very strong but few trades
    # score≥25: n=106, WR=55.7%, avg=+0.019%/T ← solid
    _tweak('U', min_conf=40, min_score=40),                   # U_tight: score≥40 → 70%WR
    _tweak('U', min_conf=30, min_score=20),                   # U_medium: relaxed gate

    # ── CGY: short_only + wider SL ───────────────────────────────────
    # shorts 42.0%WR vs longs 39.7%; trail exits 60.4%WR +0.114%/T
    # SL dominant (277 SL at 0% WR) → widen to let trail exits breathe
    _tweak('CGY', short_only=True),                           # CGY_short: direction edge
    _tweak('CGY', short_only=True, atr_sl_mult=1.80),         # CGY_short_wide: wider SL
    _tweak('CGY', short_only=True, atr_sl_mult=2.00),         # CGY_short_widest

    # ── L: short_only hypothesis ─────────────────────────────────────
    # shorts: 50.4%WR, -0.016%/T (near breakeven)
    # longs:  45.2%WR, -0.055%/T (losing)
    # SL dominant (571/1936) → widen SL to reach trail exits
    _tweak('L', short_only=True),                             # L_short: direction edge
    _tweak('L', short_only=True, atr_sl_mult=2.50),           # L_short_wide
    _tweak('L', short_only=True, atr_sl_mult=3.00),           # L_short_widest

    # ── K: wider SL to reduce SL dominance ──────────────────────────
    # Trail exits: 71.4%WR +0.140%/T — good when reached
    # 1547/5343 = 29% SL exits at 0% WR drag everything down
    _tweak('K', atr_sl_mult=2.50),                            # K_wide_sl
    _tweak('K', atr_sl_mult=3.00),                            # K_widest_sl

    # ── Q: wider SL to reach trail exits ─────────────────────────────
    # Trail exits: 91.1%WR +0.269%/T — excellent
    # 305/888 = 34% SL exits wiping gains
    _tweak('Q', atr_sl_mult=2.00),                            # Q_wide_sl
    _tweak('Q', atr_sl_mult=2.50),                            # Q_widest_sl

    # ── Q: score gate transforms it ──────────────────────────────────
    # score≥50: n=33, WR=54.5%, avg=+0.036%/T ← profitable!
    # score≥40: n=39, WR=48.7%, avg=+0.006%/T ← near breakeven
    _tweak('Q', min_score=40),                                # Q_score40: 48.7%WR
    _tweak('Q', min_score=50),                                # Q_score50: 54.5%WR +0.036%

    # ── QQ: score gate finding ────────────────────────────────────────
    # score≥50: n=34, WR=61.8%, avg=+0.001%/T ← near breakeven
    # score≥40: n=35, WR=60.0%, avg=-0.003%/T ← high WR
    _tweak('QQ', min_score=40),                               # QQ_score40: 60%WR
    _tweak('QQ', min_score=50),                               # QQ_score50: 61.8%WR

    # ── QQ: short_only hypothesis ────────────────────────────────────
    # shorts: 40.6%WR, -0.009%/T (nearly breakeven)
    # longs:  38.0%WR, -0.082%/T (losing)\
    _tweak('QQ', short_only=True),                            # QQ_short
    _tweak('QQ', short_only=True, atr_sl_mult=2.50),          # QQ_short_wide

    # ── V: conf gate finding ─────────────────────────────────────────
    # conf≥65: n=109, WR=53.2%, avg=-0.027%/T ← high WR, SL still hurting
    # conf≥75: n=107, WR=54.2%, avg=-0.024%/T
    # Hypothesis: wider SL + high conf = profitable
    _tweak('V', min_conf=65),                                 # V_conf65: 53.2%WR
    _tweak('V', min_conf=65, atr_sl_mult=2.50),               # V_conf65_widesl
    _tweak('V', min_conf=75, atr_sl_mult=2.50),               # V_conf75_widesl

    # ── L: conf gate finding ──────────────────────────────────────────
    # conf≥80: n=731, WR=51.4%, avg=-0.027%/T ← approaching breakeven
    # Rev exits killing it: 3612 rev at 28.3%WR → disable rev exits
    _tweak('L', min_conf=80),                                 # L_conf80: 51.4%WR
    _tweak('L', min_conf=80, rev_min_hold=9999.0),            # L_conf80_norev: disable rev
    _tweak('L', min_conf=55),                                 # L_conf55: 46.6%WR

    # ── W: tighter VPIN gate ─────────────────────────────────────────
    # Overall losing but trail+TP exits are positive
    # Hypothesis: tighter VPIN gate reduces low-quality entries
    _tweak('W', vpin_min=0.60),                               # W_tight_vpin
    _tweak('W', vpin_min=0.70),                               # W_tighter_vpin
    _tweak('W', vpin_min=0.75),                               # W_vpin75: 51.8%WR +0.022%/T (n=164)

    # ── E: direction split (with HTF filter still on, for comparison) ─
    # Only 165 trades — insufficient to decide, keep monitoring both
    _tweak('E', short_only=True),                             # E_short
    _tweak('E', long_only=True),                              # E_long

    # ── Y: SL widened in main (1.30→2.00); test even wider in _b ────
    # trail exits 65%WR +0.101%/T; SL 0%WR at 46% of exits
    _tweak('Y', atr_sl_mult=2.50),                            # Y_widest_sl: even wider

    # ── CGYL: re-tuned on 774k-row honest-fee sweep (2026-06-18) ─────
    # vpin70 was the lead candidate at +0.167%/T (n=149) but DEGRADED on 2.6x
    # more data → -0.008%/T (n=395). Dropped. Durable fee-clearers now:
    #   score>=40 → 47.1%WR, +0.045%/T (n=648)   ← primary, monotonic to score50 (+0.028)
    #   conf>=65  → 49.1%WR, +0.066%/T (n=322)   ← higher but NON-MONOTONIC (conf70 drops to +0.008); noise-prone
    # CGYL is the ONLY strategy with a positive pocket at real sample size after honest fees.
    _tweak('CGYL', min_score=40.0),                          # CGYL_score40: +0.045%/T (n=648) — primary candidate
    _tweak('CGYL', min_conf=65),                             # CGYL_conf65: +0.066%/T (n=322) — watch, likely noisy

    # ── Q: conf gate (LOW confidence — conf sparse above 50) ─────────
    # min_conf>=65 → 53.8%WR, +0.019%/T (n~186). Data-collection only.
    _tweak('Q', min_conf=65),                                 # Q_conf65

    # ── E: conf gate (small sample, full conf coverage) ──────────────
    # min_conf>=30 → 59%WR, +0.026%/T (n~285). E edge is short-side.
    _tweak('E', min_conf=30),                                 # E_conf30
]
