"""
PredictEngine — Synthetic Market Simulator
==========================================
Injects 8 market regime scenes into sym_state so real signal detectors return
valid payloads, then runs the full gate → fire → check_outcomes stack.

Step 1 (once): learn real distributions from your 2.8M signal CSV:
  python sim_learn_dists.py --csv ./data_backup/*/logs/signals_combined.csv
  # outputs sim_dists.json

Step 2: run the simulator:
  python synth_market_sim.py                              # all strategies, 50k ticks
  python synth_market_sim.py --ticks 1000000              # 1M ticks (~3 min)
  python synth_market_sim.py --strategy B W L             # specific strategies
  python synth_market_sim.py --regime mtf decorrelation   # specific regimes
  python synth_market_sim.py --seed 42                    # reproducible run
  python synth_market_sim.py --dists sim_dists.json       # use learned distributions

Compare mode — test a config change (identical price paths both sides):
  python synth_market_sim.py --strategy B --ticks 200000 --compare win_thr 0.30 0.45
  python synth_market_sim.py --strategy W --ticks 200000 --compare inertia_sec 45 90
  python synth_market_sim.py --strategy L --ticks 100000 --compare vpin_min 0.50 0.62
  python synth_market_sim.py --strategy E --ticks 100000 --compare min_score 15 25

Working strategies (gates fire under synthetic state):
  L   — S/R level bounce       catch ~6%    inject_level
  W   — BTC decorrelation      catch ~6%    inject_decorrelation
  E   — EMA crossover          catch ~17%   inject_ema
  B   — MTF momentum           catch ~0.2%  inject_mtf
  Q   — Funding extreme        fires on funding regime
  WB  — MTF + decorrelation    fires on both mtf and decorrelation regimes

Not yet firing (complex gate conditions — will fix separately):
  K   — Impulse fade   (EMA21 trend conflicts with fib zone in synthetic state)
  Y   — Star pattern   (same EMA21/price conflict)
  CGY — Composite      (needs K+Y+G votes simultaneously)
  Z   — Disabled in current config

Key output metrics:
  fires       — total entries fired
  catch_rate  — % of regime ticks where the strategy fired (gate selectivity)
  WR%         — win rate on random-walk exits (always ~0% — compare DELTA matters)
  exit mix    — trail/sl/tp/time/inertia distribution
"""

import sys, os, time, math, random, json, argparse, types, copy
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

# ── Mock setup (identical to synth_test.py) ──────────────────────────────────
_HERE = Path(__file__).parent
_ENGINE_ROOT = _HERE.parent.parent  # tools/backtest → engine root
if str(_ENGINE_ROOT) not in sys.path:
    sys.path.insert(0, str(_ENGINE_ROOT))

_aiohttp = types.ModuleType('aiohttp')
_aiohttp.ClientSession = object; _aiohttp.TCPConnector = object
_aiohttp.ClientTimeout = object; _aiohttp.ClientWSTimeout = object
class _WSMsgType:
    TEXT=1; BINARY=2; CLOSED=3; ERROR=4
_aiohttp.WSMsgType = _WSMsgType()
sys.modules['aiohttp'] = _aiohttp

import unittest.mock as _mock
sys.modules['requests'] = _mock.MagicMock()

_el = types.ModuleType('engine_logger')
for _fn in ['log_ws_event','log_scanner_change','log_signal','log_trade_open','log_trade_close']:
    setattr(_el, _fn, lambda *a, **kw: None)
sys.modules['engine_logger'] = _el

_lm = types.ModuleType('live_execution')
_lm.LIVE_ORDER_USDT = 10.0; _lm.LIVE_ENABLED = False
_lm.can_enter       = lambda **k: (True, 'sim_mode')
_lm.create_order    = lambda *a, **k: {'ok': False}
_lm.close_position  = lambda *a, **k: {}
sys.modules['live_execution'] = _lm

try:
    import config as CFG
    import engine as E
    from strategies_config import STRATEGIES, StrategyConfig
    import strategies_engine as SE
    import strategies_runtime as SR
    import strategies_signals as SS
    import engine_lag as EL
    from core_signals import calc_mtf_bias, calc_vpin
except ImportError as exc:
    print(f"[ERROR] Cannot import engine: {exc}")
    print("Run from the same directory as engine.py")
    sys.exit(1)

SR._slot_holders = {}
def _sim_release(sym, label): return True, False
SR._release_open = _sim_release


# ════════════════════════════════════════════════════════════════════════════
# FALLBACK DISTRIBUTIONS
# Used when sim_dists.json not present. Derived from analysis JSONs.
# ════════════════════════════════════════════════════════════════════════════

FALLBACK_DISTS: Dict[str, Dict] = {
    'B':   {'fired': 833,  'score_mean':  6.9,  'score_std': 25.0, 'vpin_mean': 0.701, 'vpin_std': 0.08, 'atr_mean': 0.38, 'atr_std': 0.10, 'conf_mean': 45.0, 'long_pct': 0.52},
    'W':   {'fired': 1204, 'score_mean': -1.6,  'score_std': 22.0, 'vpin_mean': 0.325, 'vpin_std': 0.10, 'atr_mean': 0.35, 'atr_std': 0.09, 'conf_mean': 47.0, 'long_pct': 0.50},
    'L':   {'fired': 412,  'score_mean': 14.2,  'score_std': 20.0, 'vpin_mean': 0.628, 'vpin_std': 0.09, 'atr_mean': 0.36, 'atr_std': 0.09, 'conf_mean': 64.6, 'long_pct': 0.55},
    'K':   {'fired': 35,   'score_mean': 26.5,  'score_std': 20.0, 'vpin_mean': 0.505, 'vpin_std': 0.10, 'atr_mean': 0.42, 'atr_std': 0.12, 'conf_mean': 72.7, 'long_pct': 0.48},
    'Y':   {'fired': 175,  'score_mean': -9.4,  'score_std': 22.0, 'vpin_mean': 0.521, 'vpin_std': 0.10, 'atr_mean': 0.38, 'atr_std': 0.10, 'conf_mean': 41.9, 'long_pct': 0.50},
    'CGY': {'fired': 214,  'score_mean': -4.1,  'score_std': 20.0, 'vpin_mean': 0.365, 'vpin_std': 0.09, 'atr_mean': 0.40, 'atr_std': 0.11, 'conf_mean': 70.8, 'long_pct': 0.50},
    'E':   {'fired': 36,   'score_mean': -6.3,  'score_std': 18.0, 'vpin_mean': 0.653, 'vpin_std': 0.10, 'atr_mean': 0.35, 'atr_std': 0.09, 'conf_mean': 59.2, 'long_pct': 0.50},
    'Q':   {'fired': 80,   'score_mean': -2.0,  'score_std': 18.0, 'vpin_mean': 0.580, 'vpin_std': 0.09, 'atr_mean': 0.36, 'atr_std': 0.10, 'conf_mean': 55.0, 'long_pct': 0.45},
    'Z':   {'fired': 40,   'score_mean':  5.0,  'score_std': 15.0, 'vpin_mean': 0.600, 'vpin_std': 0.08, 'atr_mean': 0.32, 'atr_std': 0.09, 'conf_mean': 60.0, 'long_pct': 0.50},
    'WB':  {'fired': 230,  'score_mean': 10.0,  'score_std': 20.0, 'vpin_mean': 0.670, 'vpin_std': 0.08, 'atr_mean': 0.37, 'atr_std': 0.09, 'conf_mean': 55.0, 'long_pct': 0.52},
}

# Which regimes each strategy is designed to trade
STRATEGY_REGIMES = {
    'B':   ['mtf'],
    'W':   ['decorrelation'],
    'WB':  ['mtf', 'decorrelation'],
    'L':   ['level'],
    'K':   ['impulse'],
    'C':   ['impulse'],
    'Y':   ['star'],
    'CGY': ['star'],  # CGY uses C+G+Y votes; star triggers Y vote
    'E':   ['ema'],
    'Q':   ['funding'],
    'QQ':  ['funding'],
    'Z':   ['lag'],
    'S':   ['level'],         # OI divergence — level-like state
}

ALL_REGIMES = ['impulse', 'level', 'decorrelation', 'mtf', 'funding', 'lag', 'ema', 'star']


# ════════════════════════════════════════════════════════════════════════════
# MOCK ENGINE
# ════════════════════════════════════════════════════════════════════════════

class MockEngine(SE.StrategyEngine):
    def __init__(self, cfg: StrategyConfig):
        super().__init__(cfg, log_prefix=None)
        self._start_ts = time.time() - 9999.0
    def _save_state(self): pass
    def _load_state(self): pass


def _make_engine(label: str, overrides: Optional[Dict] = None) -> Optional[MockEngine]:
    cfg = next((s for s in STRATEGIES if s.label == label), None)
    if cfg is None: return None
    if overrides:
        import dataclasses
        try: cfg = dataclasses.replace(cfg, **overrides)
        except Exception: pass
    return MockEngine(cfg)


# ════════════════════════════════════════════════════════════════════════════
# REGIME SCENE INJECTORS
# Each injects sym_state (and where needed global state) so that the REAL
# signal detector for the target strategy returns a valid non-None payload.
# ════════════════════════════════════════════════════════════════════════════

def _seed_base_state(sym: str, price: float, vpin: float, atr_pct: float,
                     now: float, rng: random.Random) -> None:
    """Inject minimal common state needed by all gates."""
    E.init_sym(sym)
    st = E.sym_state[sym]
    # CRITICAL: use real wall-clock time for ALL timestamps.
    # Signal detectors call time.time() internally, so injected history
    # must use the same clock or time windows will filter everything out.
    real_now    = time.time()
    real_now_ms = real_now * 1000

    st['price']      = price
    st['prev_price'] = price * (1 - 0.0001)
    st['regime']     = 'neutral'
    st['regime_conf'] = 0.3
    st['funding_rate'] = 0.0001
    st['oi']         = 60_000_000
    st['last_pred_ts'] = now - 600.0

    # Price history — 31 sparse ticks (1 per minute) spanning 30min.
    # Sparse keeps the deque(maxlen=600) mostly empty so regime injectors
    # can append their signal candle ticks without evicting the background.
    st['price_hist'].clear()
    for i in range(31):
        ts_i = real_now_ms - (31 - i) * 60_000
        px_i = price * (1 + rng.gauss(0, atr_pct / 1000))
        st['price_hist'].append((ts_i, px_i))
    st['price_hist'].append((real_now_ms, price))

    # VPIN buckets
    st['vpin_buckets'].clear()
    for _ in range(30): st['vpin_buckets'].append(vpin)
    st['vpin_acc'] = {'buy': 0.0, 'sell': 0.0, 'total': 0.0}

    # Trade tape
    st['trade_tape'].clear()
    for i in range(60):
        ts_i = real_now_ms - (60 - i) * 500
        st['trade_tape'].append((ts_i, price, 2000.0, rng.random() > 0.5))

    # CVD
    st['cvd'].clear()
    for i in range(200):
        ts_i = real_now_ms - (200 - i) * 500
        st['cvd'].append((ts_i, 3000.0, 2000.0))

    # OI
    st['oi_hist'].clear()
    for i in range(20):
        ts_i = real_now - (20 - i) * 60
        st['oi_hist'].append((ts_i, 60_000_000.0))

    # Funding
    st['funding_hist'].clear()
    for i in range(10):
        ts_i = real_now - (10 - i) * 3600
        st['funding_hist'].append((ts_i, 0.0001))

    # Wall hist
    st['wall_hist'].clear()
    for i in range(20):
        st['wall_hist'].append((real_now_ms - i * 500, 200_000.0, 200_000.0))

    # Order book
    spread = price * 0.0001
    st['bids']    = {f'{price - spread:.4f}': 5.0}
    st['asks']    = {f'{price + spread:.4f}': 5.0}
    st['bids_f']  = {price - spread: 5.0}
    st['asks_f']  = {price + spread: 5.0}
    st['best_bid'] = price - spread
    st['best_ask'] = price + spread

    # Klines - seed with flat candles (individual regimes will overwrite)
    base_candles = []
    for i in range(60):
        ts_i = real_now_ms - (60 - i) * 60_000
        px_i = price * (1 + rng.gauss(0, atr_pct / 500))
        # Set H/L to produce ATR ≈ atr_pct (Wilder's ATR reads kline ranges)
        half_rng = px_i * atr_pct / 200   # HL range = atr_pct per candle
        base_candles.append({
            'ts': ts_i, 'o': px_i - half_rng * 0.3,
            'h': px_i + half_rng * 0.7,
            'l': px_i - half_rng * 0.3,
            'c': px_i, 'v': 500_000.0
        })
    st['klines']['1m'] = base_candles
    st['klines_last_fetch']['1m'] = real_now

    # sig_hist
    st['sig_hist'].clear()
    for _ in range(10):
        st['sig_hist'].append({'score': 0, 'dir': 'long', 'conf': 50,
                               'n_agree': 2, 'n_avail': 5, 'strength': 40.0,
                               'sigs': {}, 'agree': [], 'conflict': []})


def _seed_price_hist_30min(sym: str, price: float, atr_pct: float, rng: random.Random) -> None:
    """
    Ensure price_hist spans 31 minutes so _build_candles produces 30+ one-minute
    buckets → EMA21 can be computed (needs >= 21).
    Uses sparse background (1 tick per minute = 31 ticks) so the deque(maxlen=600)
    isn't overwhelmed by signal ticks. Background is inserted BEFORE signal ticks.
    """
    st = E.sym_state.get(sym)
    if not st: return
    real_now_ms = time.time() * 1000
    existing = list(st['price_hist'])
    # Find oldest existing ts
    oldest_ts = min((ts for ts, _ in existing), default=real_now_ms)
    bg_start_ms = real_now_ms - 31 * 60_000   # 31 min ago
    if oldest_ts <= bg_start_ms:
        return   # already have enough history
    # Build sparse background: one tick per minute (31 ticks total)
    bg = []
    for i in range(31):
        ts_i = bg_start_ms + i * 60_000
        if ts_i >= oldest_ts: break
        px_i = price * (1 + rng.gauss(0, atr_pct / 1000))
        bg.append((ts_i, px_i))
    if not bg: return
    # Rebuild price_hist: sparse background + existing signal ticks
    # This stays well within maxlen=600
    st['price_hist'].clear()
    for entry in bg:
        st['price_hist'].append(entry)
    for entry in existing:
        st['price_hist'].append(entry)


def _fix_ema_trend(sym: str, fade_dir: str, rng: random.Random) -> None:
    """
    Tilt the price_hist background ticks (all except the last 3 signal entries)
    so that EMA21 falls on the correct side of st['price']:
      fade_dir='long'  → background trends down so EMA21 < current_price
      fade_dir='short' → background trends up   so EMA21 > current_price
    Does NOT modify st['price'] (would break fib-zone checks).
    """
    import strategies_engine as _SE
    _SE._ema21_cache.clear()
    st = E.sym_state.get(sym)
    if not st: return
    current_price = st['price']
    ph = list(st['price_hist'])
    if len(ph) < 5: return

    # Background = everything except last 3 signal ticks
    bg  = ph[:-3]
    sig = ph[-3:]
    if not bg: return

    n = len(bg)
    if fade_dir == 'long':
        # Trend background down gently: start 5% above price, end 2% below.
        # The EMA21 smoothed average of these will be ~1.5% above price on
        # early ticks but the last few background ticks (closest to now) will
        # be slightly below price, pulling EMA21 below current price.
        p_start, p_end = current_price * 1.050, current_price * 0.980
    else:
        p_start, p_end = current_price * 0.950, current_price * 1.020

    new_bg = []
    real_now_ms = time.time() * 1000
    cutoff_ms = real_now_ms - 3 * 60_000  # don't touch last 3 min (signal candle buckets)
    for i, (ts, _) in enumerate(bg):
        if ts >= cutoff_ms:
            # Keep signal-bucket-adjacent ticks as-is to avoid contamination
            new_bg.append((ts, _))
            continue
        frac  = i / max(n - 1, 1)
        px    = p_start + (p_end - p_start) * frac
        new_bg.append((ts, px))

    st['price_hist'].clear()
    for entry in new_bg:
        st['price_hist'].append(entry)
    for entry in sig:
        st['price_hist'].append(entry)

    _SE._ema21_cache.clear()


def _ensure_atr(sym: str, atr_pct: float, price: float) -> None:
    """
    Ensure get_atr(sym) returns approximately atr_pct by patching background
    kline H/L ranges. Called at the end of every regime injector.
    Leaves the last 3 candles untouched (those are the signal candles).
    """
    st = E.sym_state.get(sym)
    if not st: return
    klines = st.get('klines', {}).get('1m', [])
    if not klines: return
    # Set background candles (all but last 3) to have range ≈ atr_pct
    target_range = price * atr_pct / 100   # absolute range per candle
    for c in klines[:-3]:
        mid = (c.get('h', price) + c.get('l', price)) / 2
        if mid <= 0: mid = price
        c['h'] = mid + target_range * 0.65
        c['l'] = mid - target_range * 0.35


def _build_klines(price: float, n: int, bucket_ms: int, rng: random.Random,
                  atr_pct: float, now_ms: float) -> List[Dict]:
    """Generate n flat-ish candles ending at now_ms."""
    candles = []
    for i in range(n):
        ts_i = now_ms - (n - i) * bucket_ms
        o = price * (1 + rng.gauss(0, atr_pct / 800))
        c = price * (1 + rng.gauss(0, atr_pct / 800))
        h = max(o, c) * (1 + abs(rng.gauss(0, atr_pct / 1200)))
        l = min(o, c) * (1 - abs(rng.gauss(0, atr_pct / 1200)))
        candles.append({'ts': ts_i, 'o': o, 'h': h, 'l': l, 'c': c, 'v': 500_000.0})
    return candles


# ── Regime: IMPULSE (K, C) ───────────────────────────────────────────────────

def inject_impulse(sym: str, price: float, direction: str, atr_pct: float,
                   now: float, rng: random.Random) -> bool:
    """
    Inject a hammer/shooting_star matching exact _detect_impulse v5 criteria:
      hammer:        lower_ratio>=0.65, (c-l)/rng>=0.70, lower/upper>=3.0
      shooting_star: upper_ratio>=0.65, (h-c)/rng>=0.70, upper/lower>=3.0
    Background candles are small so EMA noise floor is easily cleared.
    """
    st = E.sym_state[sym]
    now_ms = time.time() * 1000
    t_now = time.time()

    # Small background candles for EMA noise floor
    small_rng_pct = atr_pct * 0.12
    candles = []
    for i in range(25):
        ts_i = now_ms - (27 - i) * 60_000
        px = price * (1 + rng.gauss(0, small_rng_pct / 400))
        half = px * small_rng_pct / 200
        candles.append({'ts': ts_i, 'o': px - half*0.3, 'c': px + half*0.3,
                        'h': px + half, 'l': px - half, 'v': 300_000.0})

    # Impulse candle: large enough to pass EMA noise floor (>2.5x avg)
    impulse_rng_pct = max(0.65, atr_pct * 3.0)
    total_rng = price * impulse_rng_pct / 100

    if direction == 'long':
        # HAMMER: lower_wick=76%, body=14%, upper_wick=10%
        # lower_ratio=0.76>=0.65 ✓  (c-l)/rng=0.90>=0.70 ✓  lower/upper=7.6>=3 ✓
        # CRITICAL: close must be at most 40% recovery from low so _in_fib_zone passes.
        # recovery_limit = low + rng*0.60 → we set close at low + rng*0.35 (safe margin)
        low       = price - total_rng * 0.90
        high      = price                        # high at reference price
        close_px  = low + total_rng * 0.35       # 35% recovery — below 60% limit ✓
        open_px   = low + total_rng * 0.22       # open below close (bullish body)
    else:
        # SHOOTING STAR: upper_wick=76%, body=14%, lower_wick=10%
        # upper_ratio=0.76>=0.65 ✓  (h-c)/rng=0.90>=0.70 ✓  upper/lower=7.6>=3 ✓
        # close must be at most 40% giveback from high
        high      = price + total_rng * 0.90
        low       = price                        # low at reference price
        close_px  = high - total_rng * 0.35      # 35% giveback from high ✓
        open_px   = high - total_rng * 0.22      # open above close (bearish body)

    c_imp = {'ts': now_ms - 60_000, 'o': open_px, 'c': close_px,
             'h': high, 'l': low, 'v': 5_000_000.0}
    # C2: quiet candle at close price
    c2 = {'ts': now_ms, 'o': close_px, 'c': close_px * (1 + rng.gauss(0, 0.0001)),
          'h': close_px * 1.0002, 'l': close_px * 0.9998, 'v': 200_000.0}
    candles.extend([c_imp, c2])

    st['klines']['1m'] = candles
    st['klines_last_fetch']['1m'] = t_now
    for bms, n in [(180_000, 15), (300_000, 12)]:
        key = f"{bms//60_000}m"
        bg  = _build_klines(price, n, bms, rng, small_rng_pct, now_ms - bms)
        bg.extend([{**c_imp, 'ts': now_ms - bms}, {**c2, 'ts': now_ms}])
        st['klines'][key] = bg
        st['klines_last_fetch'][key] = t_now

    # Seed price_hist so _build_candles reproduces the wick pattern.
    # _detect_impulse uses _build_candles (from price_hist), not st['klines'].
    # Inject enough ticks in the impulse candle bucket to create the wick shape.
    # Impulse candle bucket = 1 minute before now. Current bucket = now (excluded).
    imp_bucket_start = (int((now_ms - 90_000) // 60_000)) * 60_000
    # Seed 40 ticks inside the impulse bucket spanning [low, high]
    for i in range(40):
        frac = i / 39.0
        if direction == 'long':
            # Hammer: start high, drop to low, recover to close_px
            if frac < 0.3:   px_tick = high - (high - low) * (frac / 0.3)
            elif frac < 0.7: px_tick = low + rng.gauss(0, total_rng * 0.02)
            else:             px_tick = low + (close_px - low) * ((frac - 0.7) / 0.3)
        else:
            # Shooting star: start low, spike to high, close near low
            if frac < 0.3:   px_tick = low + (high - low) * (frac / 0.3)
            elif frac < 0.7: px_tick = high + rng.gauss(0, total_rng * 0.02)
            else:             px_tick = high - (high - close_px) * ((frac - 0.7) / 0.3)
        ts_tick = imp_bucket_start + int(frac * 59_000)
        st['price_hist'].append((ts_tick, max(low, min(high, px_tick))))
    # Background buckets: small range (noise floor)
    for i in range(22):
        ts_bg = imp_bucket_start - (22 - i) * 60_000
        for j in range(5):
            px_bg = price * (1 + rng.gauss(0, small_rng_pct / 400))
            st['price_hist'].append((ts_bg + j * 10_000, px_bg))
    st['price_hist'].append((now_ms, close_px))
    st['price'] = close_px
    _ensure_atr(sym, atr_pct, price)
    _fix_ema_trend(sym, direction, rng)  # tilt price_hist so EMA21 aligns with fade_dir
    # Align sig_hist with fade direction for _score_sustained(ticks=1) check
    st = E.sym_state[sym]
    st['sig_hist'].clear()
    for _ in range(5):
        st['sig_hist'].append({'score': 60.0 if direction == 'long' else -60.0,
                               'dir': direction, 'conf': 70, 'n_agree': 3,
                               'n_avail': 5, 'strength': 60.0, 'sigs': {}, 'agree': [], 'conflict': []})
    # Inject trade tape with large trades so calc_large_trade_ratio >= 0.03.
    # Need: some trades at >= 5x median. Mix: 50 small (1000) + 10 large (8000).
    # median=1000, threshold=5000, large_vol=80000, total=130000 → ltr=0.615 ✓
    real_now_ms_k = time.time() * 1000
    st['trade_tape'].clear()
    is_buy = direction == 'long'
    for i in range(50):
        ts_i = real_now_ms_k - (60 - i) * 800
        st['trade_tape'].append((ts_i, close_px, 1000.0, is_buy))
    for i in range(10):
        ts_i = real_now_ms_k - i * 300
        st['trade_tape'].append((ts_i, close_px, 8000.0, is_buy))
    return True


# ── Regime: LEVEL (L, S) ────────────────────────────────────────────────────

def inject_level(sym: str, price: float, direction: str, atr_pct: float,
                 now: float, rng: random.Random) -> bool:
    """
    Seed price_hist so _build_sr_levels finds >= SR_MIN_TOUCHES (7) at a level
    within SR_DIST_MAX_PCT (0.04%) of current price.

    Strategy: fill 30 one-minute buckets with price ticks. In 10 of those buckets
    inject a tick exactly AT level_price so the bucket's H or L == level_price,
    creating 10 touches in the clustering pass. Current price sits within
    SR_DIST_MAX_PCT of level_price so _find_level_signal returns non-None.
    """
    st = E.sym_state[sym]
    real_now_ms = time.time() * 1000
    SR_BAND_PCT    = 0.15   # touches within 0.15% are clustered
    SR_DIST_MAX_PCT = 0.04  # current price must be within 0.04% of level

    # Place level just within SR_DIST_MAX_PCT of price
    level_price = price * (1 + (SR_DIST_MAX_PCT * 0.4 / 100) *
                           (1 if direction == 'long' else -1))

    # Rebuild price_hist with 30 minutes of ticks, touching level in many buckets.
    # Each minute bucket = one 60_000ms window. _build_candles groups ticks by bucket.
    # We put 3 ticks per bucket: one near level (to set H or L), two near price (body).
    st['price_hist'].clear()
    bucket_ms = 60_000
    for i in range(30):
        bucket_start = real_now_ms - (30 - i) * bucket_ms
        # Regular body ticks
        px1 = price * (1 + rng.gauss(0, atr_pct / 1200))
        st['price_hist'].append((bucket_start + 10_000, px1))
        st['price_hist'].append((bucket_start + 40_000, price * (1 + rng.gauss(0, atr_pct / 1200))))
        # Every 3rd bucket: inject a level-touch tick
        if i % 3 == 0 and i < 29:
            if direction == 'long':
                touch_px = level_price * (1 - rng.uniform(0.0001, 0.0003))  # low touches support
            else:
                touch_px = level_price * (1 + rng.uniform(0.0001, 0.0003))  # high touches resistance
            st['price_hist'].append((bucket_start + 25_000, touch_px))

    # Final tick: current price near level (within SR_DIST_MAX_PCT)
    final_px = level_price * (1 + rng.gauss(0, 0.0001))
    st['price_hist'].append((real_now_ms - 1_000, final_px))
    st['price_hist'].append((real_now_ms, final_px))
    st['price'] = final_px

    # Reset SR cache so it rebuilds fresh
    if sym in SS._sr_levels:    del SS._sr_levels[sym]
    if sym in SS._sr_last_built: del SS._sr_last_built[sym]

    _ensure_atr(sym, atr_pct, price)
    return True


# ── Regime: DECORRELATION (W, WB) ───────────────────────────────────────────

def inject_decorrelation(sym: str, price: float, direction: str, atr_pct: float,
                          now: float, rng: random.Random) -> bool:
    """
    Inject BTC price_hist moving >= DECOR_BTC_MOVE_MIN (0.20%) in one direction,
    and alt price_hist moving >= DECOR_ALT_DIV_MIN (0.10%) in the OPPOSITE direction.
    """
    now_ms = time.time() * 1000
    DECOR_WINDOW_MS  = 600_000
    BTC_MOVE_MIN     = 0.20    # %
    ALT_DIV_MIN      = 0.10    # %

    btc_start_px  = 65000.0

    if direction == 'long':
        # BTC up, alt down → alt snaps back up
        btc_end_px  = btc_start_px * (1 + (BTC_MOVE_MIN + rng.uniform(0.05, 0.20)) / 100)
        alt_end_px  = price * (1 - (ALT_DIV_MIN + rng.uniform(0.02, 0.15)) / 100)
    else:
        # BTC down, alt up → alt snaps down
        btc_end_px  = btc_start_px * (1 - (BTC_MOVE_MIN + rng.uniform(0.05, 0.20)) / 100)
        alt_end_px  = price * (1 + (ALT_DIV_MIN + rng.uniform(0.02, 0.15)) / 100)

    # BTC history: linear move from start→end over last DECOR_WINDOW_MS
    E.btc_hist.clear()
    n_pts = 40
    for i in range(n_pts + 1):
        frac  = i / n_pts
        ts_i  = now_ms - DECOR_WINDOW_MS * (1 - frac)
        px_i  = btc_start_px + (btc_end_px - btc_start_px) * frac
        px_i += rng.gauss(0, 0.5)
        E.btc_hist.append((ts_i, px_i))

    # Alt history: opposite move
    st = E.sym_state[sym]
    alt_start_px = price
    st['price_hist'].clear()
    for i in range(n_pts + 1):
        frac  = i / n_pts
        ts_i  = now_ms - DECOR_WINDOW_MS * (1 - frac)
        px_i  = alt_start_px + (alt_end_px - alt_start_px) * frac
        px_i += rng.gauss(0, price * 0.0001)
        st['price_hist'].append((ts_i, px_i))

    st['price'] = alt_end_px
    st['price_hist'].append((now_ms, alt_end_px))
    _ensure_atr(sym, atr_pct, price)
    return True


# ── Regime: MTF MOMENTUM (B, WB) ────────────────────────────────────────────

def inject_mtf(sym: str, price: float, direction: str, atr_pct: float,
               now: float, rng: random.Random) -> bool:
    """
    Inject price_hist so that calc_mtf_bias returns a score > 15 (cfg threshold).
    Needs all three windows (15s, 60s, 300s) to agree directionally.
    Each weighted contribution needs to be meaningful.
    """
    st = E.sym_state[sym]
    now_ms = time.time() * 1000

    # Drift magnitude per window: enough that log1p(|move|)/log1p(1.5) is large
    # Need weighted(m15) + weighted(m60) + weighted(m300) > 15
    # Each max is 20, so we need combined > 15 out of max 60
    # Using ~0.8% move per window → log1p(0.8)/log1p(1.5) ≈ 0.62 → 12.4 per window
    drift_pct = 0.80 if direction == 'long' else -0.80

    st['price_hist'].clear()
    # 300s window (5 min)
    start_300 = price * (1 - drift_pct / 100)
    for i in range(300):
        ts_i = now_ms - (300 - i) * 1000
        frac = i / 299
        px_i = start_300 + (price - start_300) * frac + rng.gauss(0, price * 0.0001)
        st['price_hist'].append((ts_i, px_i))

    st['price_hist'].append((now_ms, price))
    st['price'] = price
    _ensure_atr(sym, atr_pct, price)
    return True


# ── Regime: FUNDING EXTREME (Q, QQ) ─────────────────────────────────────────

def inject_funding(sym: str, price: float, direction: str, atr_pct: float,
                   now: float, rng: random.Random) -> bool:
    """
    Inject funding_hist with >= FUNDING_TREND_MIN (3) consecutive extreme readings.
    FUNDING_LONG_THR = 0.0005 → short signal
    FUNDING_SHORT_THR = -0.0003 → long signal
    """
    st = E.sym_state[sym]

    if direction == 'short':
        # High positive funding → longs paying → fade with short
        base_rate = SS.FUNDING_LONG_THR * rng.uniform(1.1, 2.5)
    else:
        # High negative funding → shorts paying → fade with long
        base_rate = SS.FUNDING_SHORT_THR * rng.uniform(1.1, 2.5)

    st['funding_hist'].clear()
    # Inject 5 extreme readings to ensure 3+ consecutive pass
    for i in range(5):
        _rf = time.time()
        ts_i = _rf - (5 - i) * 3600
        rate = base_rate * rng.uniform(0.9, 1.1)
        st['funding_hist'].append((ts_i, rate))

    st['funding_rate'] = base_rate
    _ensure_atr(sym, atr_pct, price)
    return True


# ── Regime: LAG ARBITRAGE (Z, CGY) ──────────────────────────────────────────

def inject_lag(sym: str, price: float, direction: str, atr_pct: float,
               now: float, rng: random.Random) -> bool:
    """
    Directly injects _find_lag_signal result into E._pred_cache as a pre-computed
    payload. The live _find_lag_signal has inverted divergence checks (lines 72-75)
    that make it impossible to trigger synthetically — documented as a live engine
    discrepancy. For simulation purposes we inject the expected payload directly.
    """
    lag_ms  = rng.uniform(SS.LAG_MIN_MS * 2, SS.LAG_MAX_MS * 0.5)
    div_pct = rng.uniform(SS.LAG_DIVERGENCE_MIN * 2, SS.LAG_DIVERGENCE_MIN * 8)
    bnx_move = (SS.LAG_MOVE_THR + rng.uniform(0.05, 0.20)) * (1 if direction == 'short' else -1)
    ex_price = price * (1 - div_pct / 100) if direction == 'short' else price * (1 + div_pct / 100)

    # Store in sym_state so run_pred picks it up (strategies check _lag_best_ms etc.)
    E.init_sym(sym)
    st = E.sym_state[sym]
    st['_lag_signal'] = {
        'dir':         direction,
        'bnx_move':    round(bnx_move, 4),
        'bnx_px':      price,
        'lagging':     [{'exchange': 'bybit', 'lag_ms': round(lag_ms, 1),
                         'divergence_pct': round(div_pct if direction=='short' else -div_pct, 4),
                         'ex_price': ex_price}],
        'best_lag_ms': round(lag_ms, 1),
        'best_div_pct': round(div_pct, 4),
        'monitor_only': False,
    }
    _ensure_atr(sym, atr_pct, price)
    return True


# ── Regime: EMA CROSSOVER (E) ────────────────────────────────────────────────

def inject_ema(sym: str, price: float, direction: str, atr_pct: float,
               now: float, rng: random.Random) -> bool:
    """
    Build 1m klines with EMA5/EMA8 crossover in last 2 candles.
    - EMA5 crosses above EMA8 (bullish) or below (bearish)
    - Price must confirm: move > 0.08% in cross direction
    - EMA20 trend must agree (HTF filter)
    """
    st = E.sym_state[sym]
    now_ms = time.time() * 1000

    # Build 40 candles: first 35 trending, then cross
    candles = []
    pre_trend_bias = -0.001 if direction == 'long' else 0.001  # opposite before cross

    for i in range(35):
        ts_i = now_ms - (40 - i) * 60_000
        drift = pre_trend_bias * i
        px_i = price * (1 + drift + rng.gauss(0, atr_pct / 1000))
        candles.append({'ts': ts_i, 'o': px_i * 0.9999, 'h': px_i * 1.0004,
                        'l': px_i * 0.9996, 'c': px_i, 'v': 400_000.0})

    # Cross setup: 2 candles before cross (EMA5 just below/above EMA8)
    cross_base = price * (1 + pre_trend_bias * 35)
    epsilon = cross_base * 0.00005   # tiny spread so EMAs nearly equal

    if direction == 'long':
        # Previous candle: EMA5 < EMA8 (EMA5 still slightly below)
        c_prev = {'ts': now_ms - 2 * 60_000, 'o': cross_base - epsilon,
                  'h': cross_base, 'l': cross_base - epsilon * 3,
                  'c': cross_base - epsilon, 'v': 600_000.0}
        # Cross candle: big bullish move → EMA5 snaps above EMA8
        c_cross = {'ts': now_ms - 60_000,
                   'o': cross_base,
                   'h': price * 1.0015,
                   'l': cross_base * 0.9998,
                   'c': price * (1 + rng.uniform(0.0010, 0.0020)),   # strong close
                   'v': 2_000_000.0}
    else:
        c_prev = {'ts': now_ms - 2 * 60_000, 'o': cross_base + epsilon,
                  'h': cross_base + epsilon * 3, 'l': cross_base,
                  'c': cross_base + epsilon, 'v': 600_000.0}
        c_cross = {'ts': now_ms - 60_000,
                   'o': cross_base,
                   'h': cross_base * 1.0002,
                   'l': price * 0.9985,
                   'c': price * (1 - rng.uniform(0.0010, 0.0020)),
                   'v': 2_000_000.0}

    candles.extend([c_prev, c_cross])

    # Current candle (confirming move)
    conf_move = 0.0012 * (1 if direction == 'long' else -1)
    c_curr = {'ts': now_ms, 'o': c_cross['c'],
              'h': c_cross['c'] * (1 + abs(conf_move) * 0.5),
              'l': c_cross['c'] * (1 - abs(conf_move) * 0.3),
              'c': c_cross['c'] * (1 + conf_move), 'v': 800_000.0}
    candles.append(c_curr)

    # Clear EMA cache so fresh computation runs
    if sym in SS._ema_cache: del SS._ema_cache[sym]

    st['klines']['1m'] = candles
    st['klines_last_fetch']['1m'] = now
    st['price'] = c_curr['c']
    st['price_hist'].append((now_ms, c_curr['c']))
    _ensure_atr(sym, atr_pct, price)
    return True


# ── Regime: STAR PATTERN (Y, CGY) ───────────────────────────────────────────

def inject_star(sym: str, price: float, direction: str, atr_pct: float,
                now: float, rng: random.Random) -> bool:
    """
    Build 1m/3m/5m klines with morning_star (long) or evening_star (short).
    C1: large impulse >= 1.8x avg recent range, >= min_pct (0.50%)
    C2: small doji, body <= 40% of C1, no new extreme, C2 age < 1.5 buckets
    """
    st = E.sym_state[sym]
    now_ms = time.time() * 1000
    t_now  = time.time()
    min_pct = 1.5   # CGY requires min_pct=1.5; Y uses 0.50 but 1.5 passes both

    for bms in [60_000, 180_000, 300_000]:
        key = f"{bms//60_000}m"
        # 20 background candles with small range
        bg_rng_pct = atr_pct * 0.15
        candles = []
        for i in range(20):
            ts_i = now_ms - (22 - i) * bms
            px = price * (1 + rng.gauss(0, bg_rng_pct / 400))
            half = px * bg_rng_pct / 200
            candles.append({'ts': ts_i, 'o': px - half*0.3, 'c': px + half*0.3,
                            'h': px + half, 'l': px - half, 'v': 200_000.0})

        avg_rng = sum(c['h'] - c['l'] for c in candles[-8:]) / 8

        # C1: large candle, must be >= max(min_pct, 1.8x avg_rng as %)
        c1_rng = max(price * min_pct * 1.4 / 100, avg_rng * 2.2)
        c1_body = c1_rng * 0.65   # body = 65% of range (passes body/rng >= 0.20)

        # C1 timestamp: exactly 2 buckets ago (fresh)
        c1_ts = now_ms - 2 * bms

        if direction == 'long':
            # Morning star: C1 bearish (large drop)
            c1_open  = price + c1_rng * 0.15
            c1_close = c1_open - c1_body
            c1_high  = c1_open + c1_rng * 0.10
            c1_low   = c1_close - c1_rng * 0.25
            c1 = {'ts': c1_ts, 'o': c1_open, 'c': c1_close,
                  'h': c1_high, 'l': c1_low, 'v': 3_000_000.0}
            # C2: small doji near C1 low, no new low (c2_low >= c1_low)
            c2_mid   = c1_close - (c1_rng * 0.05)
            c2_body  = c1_body * 0.25
            c2 = {'ts': now_ms - bms, 'o': c2_mid - c2_body*0.3,
                  'c': c2_mid + c2_body*0.1,
                  'h': c2_mid + c2_body*0.5,
                  'l': max(c2_mid - c2_body*0.5, c1_low),   # no new low ✓
                  'v': 300_000.0}
        else:
            # Evening star: C1 bullish (large rise)
            c1_open  = price - c1_rng * 0.15
            c1_close = c1_open + c1_body
            c1_low   = c1_open - c1_rng * 0.10
            c1_high  = c1_close + c1_rng * 0.25
            c1 = {'ts': c1_ts, 'o': c1_open, 'c': c1_close,
                  'h': c1_high, 'l': c1_low, 'v': 3_000_000.0}
            c2_mid  = c1_close + (c1_rng * 0.05)
            c2_body = c1_body * 0.25
            c2 = {'ts': now_ms - bms, 'o': c2_mid + c2_body*0.3,
                  'c': c2_mid - c2_body*0.1,
                  'h': min(c2_mid + c2_body*0.5, c1_high),  # no new high ✓
                  'l': c2_mid - c2_body*0.5,
                  'v': 300_000.0}

        candles.extend([c1, c2])
        st['klines'][key] = candles
        st['klines_last_fetch'][key] = t_now

    # Seed price_hist so _build_candles also produces the star pattern.
    # _find_star_pattern calls _build_candles (reads price_hist), not _get_klines.
    # Inject ticks in C1 bucket (2 buckets ago) spanning the full C1 range,
    # and C2 bucket (1 bucket ago) as a small doji.
    real_now_ms3 = time.time() * 1000
    # C1 bucket: 2 minutes ago
    c1_start_ms = (int((real_now_ms3 - 2*60_000) // 60_000)) * 60_000
    # Background buckets: small ticks (EMA noise floor)
    for b in range(5):
        ts_b = c1_start_ms - (5 - b) * 60_000
        for j in range(4):
            px_b = price * (1 + rng.gauss(0, bg_rng_pct / 400))
            content_p = px_b * (1 + rng.gauss(0, bg_rng_pct / 800))
            st['price_hist'].append((ts_b + j * 12_000, content_p))
    # C1 ticks: span the full c1 range
    c1_ref = candles[-2]
    c1_h = c1_ref['h']; c1_l = c1_ref['l']
    c1_o = c1_ref['o']; c1_c = c1_ref['c']
    for j in range(20):
        frac = j / 19.0
        if direction == 'long':
            # Bearish candle: open → high briefly → drop to low → close near low
            if frac < 0.1:   px_t = c1_o + (c1_h - c1_o) * (frac / 0.1)
            elif frac < 0.5: px_t = c1_h - (c1_h - c1_l) * ((frac - 0.1) / 0.4)
            else:             px_t = c1_l + (c1_c - c1_l) * ((frac - 0.5) / 0.5)
        else:
            # Bullish candle: open → low briefly → spike to high → close near high
            if frac < 0.1:   px_t = c1_o - (c1_o - c1_l) * (frac / 0.1)
            elif frac < 0.5: px_t = c1_l + (c1_h - c1_l) * ((frac - 0.1) / 0.4)
            else:             px_t = c1_h - (c1_h - c1_c) * ((frac - 0.5) / 0.5)
        px_t = max(c1_l, min(c1_h, px_t))
        st['price_hist'].append((c1_start_ms + int(frac * 58_000), px_t))
    # C2 ticks: small range near c2['c']
    c2_ref = candles[-1]
    c2_bucket_start = c1_start_ms + 60_000
    for j in range(10):
        px_c2 = c2_ref['c'] * (1 + rng.gauss(0, 0.0002))
        st['price_hist'].append((c2_bucket_start + j * 4_000, px_c2))
    st['price_hist'].append((real_now_ms3, c2_ref['c']))
    st['price'] = c2_ref['c']
    _ensure_atr(sym, atr_pct, price)
    _fix_ema_trend(sym, direction, rng)  # tilt price_hist so EMA21 aligns with fade_dir
    # Boost trade tape acceleration so CGY G-vote passes (calc_trade_accel >= 1.2)
    real_now_ms4 = time.time() * 1000
    st['trade_tape'].clear()
    for i in range(80):
        ts_i = real_now_ms4 - (80 - i) * 120    # tight spacing = high accel
        is_buy = direction == 'long'
        st['trade_tape'].append((ts_i, price, 3000.0, is_buy))
    return True

# ── Regime dispatcher ────────────────────────────────────────────────────────

REGIME_INJECTORS = {
    'impulse':       inject_impulse,
    'level':         inject_level,
    'decorrelation': inject_decorrelation,
    'mtf':           inject_mtf,
    'funding':       inject_funding,
    'lag':           inject_lag,
    'ema':           inject_ema,
    'star':          inject_star,
}

# Strategies that natively fire on each regime (catch_rate denominator)
REGIME_TARGET_STRATEGIES = {
    'impulse':       ['K', 'C'],
    'level':         ['L', 'S'],
    'decorrelation': ['W', 'WB'],
    'mtf':           ['B', 'WB'],
    'funding':       ['Q', 'QQ'],
    'lag':           ['Z', 'CGY'],
    'ema':           ['E'],
    'star':          ['Y', 'CGY'],
}


# ════════════════════════════════════════════════════════════════════════════
# STATS AGGREGATOR
# ════════════════════════════════════════════════════════════════════════════

@dataclass
class StrategyRegimeStats:
    label:          str
    regime:         str
    regime_ticks:   int = 0    # ticks where this regime was injected
    fires:          int = 0
    closed:         int = 0
    wins:           int = 0
    losses:         int = 0
    cum_net:        float = 0.0
    exits:          Dict[str, int] = field(default_factory=dict)
    accounting_errors: List[str] = field(default_factory=list)

    @property
    def wr(self) -> float:
        return self.wins / max(self.closed, 1) * 100

    @property
    def avg_net(self) -> float:
        return self.cum_net / max(self.closed, 1)

    @property
    def catch_rate(self) -> float:
        """% of regime ticks where this strategy fired (gate selectivity)."""
        return self.fires / max(self.regime_ticks, 1) * 100

    @property
    def miss_rate(self) -> float:
        return 100.0 - self.catch_rate


# ════════════════════════════════════════════════════════════════════════════
# MAIN SIMULATION LOOP
# ════════════════════════════════════════════════════════════════════════════

COINS = [
    'INJUSDT', 'ENAUSDT', 'WLDUSDT', 'NEARUSDT', 'APTUSDT',
    'OPUSDT', 'HYPEUSDT', 'JTOUSDT', 'TIARUSDT', 'AVAXUSDT',
]


def run_simulation(
    strategy_labels:   List[str],
    regime_names:      List[str],
    n_ticks:           int,
    dists:             Dict,
    seed:              int = 0,
    cfg_overrides:     Optional[Dict] = None,
    verbose:           bool = True,
) -> Dict[str, Dict[str, StrategyRegimeStats]]:
    """
    Main loop. Returns stats[label][regime] → StrategyRegimeStats.
    """
    rng = random.Random(seed)

    # Build engines
    engines: Dict[str, MockEngine] = {}
    for label in strategy_labels:
        eng = _make_engine(label, cfg_overrides)
        if eng is not None:
            engines[label] = eng

    if not engines:
        print("[ERROR] No valid strategy engines built.", file=sys.stderr)
        return {}

    # Init stats grid
    stats: Dict[str, Dict[str, StrategyRegimeStats]] = {
        label: {regime: StrategyRegimeStats(label=label, regime=regime)
                for regime in regime_names}
        for label in engines
    }

    # Tick loop
    t0 = time.time()
    regime_cycle = regime_names * (n_ticks // len(regime_names) + 1)
    tick_dt = 1.0   # 1s per tick

    for tick_i in range(n_ticks):
        if verbose and tick_i % 10_000 == 0 and tick_i > 0:
            elapsed = time.time() - t0
            rate    = tick_i / elapsed
            remaining = (n_ticks - tick_i) / rate
            print(f"  tick {tick_i:>7,}/{n_ticks:,}  "
                  f"{rate:.0f} ticks/s  "
                  f"ETA {remaining:.0f}s",
                  file=sys.stderr)

        # Advance engine state
        E._tick_id += 1
        SE.advance_ema_tick()
        E._pred_cache.clear()
        E._gate_cache.clear()

        now     = 1_700_000_000.0 + tick_i * tick_dt
        regime  = regime_cycle[tick_i]
        coin    = COINS[tick_i % len(COINS)]

        # Sample from learned distributions (use first matching strategy for this regime)
        target_strats = REGIME_TARGET_STRATEGIES.get(regime, [])
        ref_label = next((l for l in target_strats if l in engines), strategy_labels[0])
        d = dists.get(ref_label, FALLBACK_DISTS.get(ref_label, {}))

        price = 1000.0 * rng.uniform(0.5, 2.0)
        vpin  = max(0.30, min(0.99, rng.gauss(
            d.get('vpin_mean', 0.55), d.get('vpin_std', 0.08))))
        atr   = max(0.15, min(1.50, rng.gauss(
            d.get('atr_mean', 0.35), d.get('atr_std', 0.10))))
        _all_long_only2  = all(getattr(eng.cfg, 'long_only',  False) for eng in engines.values())
        _all_short_only2 = all(getattr(eng.cfg, 'short_only', False) for eng in engines.values())
        if _all_long_only2:
            direction = 'long'
        elif _all_short_only2:
            direction = 'short'
        else:
            direction = 'long' if rng.random() < d.get('long_pct', 0.5) else 'short'

        # Inject base state
        _seed_base_state(coin, price, vpin, atr, now, rng)

        # Inject regime-specific state
        injector = REGIME_INJECTORS.get(regime)
        inject_ok = False
        if injector:
            try:
                inject_ok = injector(coin, price, direction, atr, now, rng)
            except Exception as exc:
                pass

        # Advance time mock
        _orig_time = time.time
        time.time = lambda _n=now: _n   # type: ignore

        # Per strategy: run pred → gate → fire → check_outcomes
        for label, eng in engines.items():
            st_reg = stats[label][regime]

            # Count this tick against regime ticks for target strategies
            if label in target_strats:
                st_reg.regime_ticks += 1

            try:
                r = E.run_pred(coin)
                # Seed score/conf from learned dist
                score = rng.gauss(d.get('score_mean', 10.0), d.get('score_std', 20.0))
                r['score']      = score
                r['dir']        = direction
                r['conf']       = int(max(20, min(100, rng.gauss(
                    d.get('conf_mean', 55.0), d.get('conf_std', 10.0)))))
                r['n_agree']    = rng.randint(2, 5)
                r['n_conflict'] = rng.randint(0, 1)
                r['strength']   = abs(score)
                E._pred_cache.clear()
                E._pred_cache[coin] = (E._tick_id, r)

                gate_ok = eng.gates_met(coin, r)
            except Exception:
                gate_ok = False

            if gate_ok:
                try:
                    open_before = sum(1 for p in eng.preds if p.get('out3') is None)
                    eng.fire(coin, r, force_sim=True)
                    open_after  = sum(1 for p in eng.preds if p.get('out3') is None)
                    if open_after > open_before:
                        st_reg.fires += 1
                except Exception:
                    pass

            try:
                eng.check_outcomes()
            except Exception:
                pass

        time.time = _orig_time

        # Collect closed trades from this tick
        for label, eng in engines.items():
            for regime_key, st_reg in stats[label].items():
                pass   # collected below after loop finishes

    # Final collection — iterate all closed preds
    time.time = lambda: 1_700_000_000.0 + n_ticks * tick_dt   # type: ignore
    for label, eng in engines.items():
        # We track per-regime stats by attributing closed trades proportionally.
        # In the loop we only increment fires per regime — WR/net come from
        # total closed preds across all regimes for simplicity.
        total_stats = stats[label]

        for p in eng.preds:
            if p.get('out3') is None: continue
            out    = p.get('out3', 'flat')
            pct    = p.get('pct3', 0.0) or 0.0
            reason = p.get('reason', 'unknown')
            net    = pct - CFG.FEE_RT

            # Attribute to 'all' regime bucket (sum)
            for regime_key in regime_names:
                st = total_stats[regime_key]
                if st.fires == 0: continue
                # Distribute proportionally by fire count
                break   # just attribute to first regime that fired — imperfect but fast

        # Simpler: just accumulate into a dedicated '_total' key
        # We'll add a total row in the reporter

    # Restore time
    import builtins
    time.time = _orig_time   # type: ignore

    return stats


# ════════════════════════════════════════════════════════════════════════════
# TRADE COLLECTOR — proper per-regime attribution via fire timestamps
# ════════════════════════════════════════════════════════════════════════════

@dataclass
class SimTotal:
    """Aggregate across all regimes for a strategy."""
    label:    str
    fires:    int = 0
    closed:   int = 0
    wins:     int = 0
    losses:   int = 0
    cum_net:  float = 0.0
    exits:    Dict[str, int] = field(default_factory=dict)
    regime_fires: Dict[str, int] = field(default_factory=dict)
    regime_ticks: Dict[str, int] = field(default_factory=dict)

    @property
    def wr(self): return self.wins / max(self.closed, 1) * 100
    @property
    def avg_net(self): return self.cum_net / max(self.closed, 1)


def run_full(
    strategy_labels: List[str],
    regime_names:    List[str],
    n_ticks:         int,
    dists:           Dict,
    seed:            int = 0,
    cfg_overrides:   Optional[Dict] = None,
    verbose:         bool = True,
) -> Dict[str, SimTotal]:
    """
    Streamlined loop with correct per-regime fire tracking.
    Returns SimTotal per label with regime_fires and regime_ticks dicts.
    """
    rng = random.Random(seed)

    engines: Dict[str, MockEngine] = {}
    for label in strategy_labels:
        eng = _make_engine(label, cfg_overrides)
        if eng is not None: engines[label] = eng

    # Patch _find_lag_signal to return from sym_state['_lag_signal'] when present.
    # The live function has inverted divergence checks that prevent synthetic triggering.
    _orig_lag_signal = SS._find_lag_signal
    def _mocked_lag_signal(sym):
        st = E.sym_state.get(sym, {})
        injected = st.get('_lag_signal')
        if injected is not None:
            return injected
        return _orig_lag_signal(sym)
    SS._find_lag_signal = _mocked_lag_signal
    import strategies_engine as _SE2
    _SE2._find_lag_signal = _mocked_lag_signal

    if not engines: return {}

    totals: Dict[str, SimTotal] = {
        label: SimTotal(
            label=label,
            regime_fires={r: 0 for r in regime_names},
            regime_ticks={r: 0 for r in regime_names},
        )
        for label in engines
    }

    regime_cycle = (regime_names * (n_ticks // max(len(regime_names), 1) + 1))[:n_ticks]
    tick_dt = 1.0
    base_ts = 1_700_000_000.0

    _orig_time = time.time

    t0_wall = time.time()
    for tick_i, regime in enumerate(regime_cycle):
        if verbose and tick_i % 50_000 == 0 and tick_i > 0:
            elapsed = _orig_time() - t0_wall
            rate    = tick_i / elapsed
            eta     = (n_ticks - tick_i) / max(rate, 1)
            print(f"  {tick_i:>8,}/{n_ticks:,}  {rate:,.0f} ticks/s  ETA {eta:.0f}s",
                  file=sys.stderr)

        E._tick_id += 1
        SE.advance_ema_tick()
        E._pred_cache.clear()
        E._gate_cache.clear()

        now   = base_ts + tick_i * tick_dt
        coin  = COINS[tick_i % len(COINS)]
        target_strats = REGIME_TARGET_STRATEGIES.get(regime, [])

        # Sample signal params from dist of target strategy
        ref_label = next((l for l in target_strats if l in engines), strategy_labels[0])
        d = dists.get(ref_label, FALLBACK_DISTS.get(ref_label, {}))

        price = 500.0 * rng.uniform(0.3, 4.0)
        vpin  = max(0.25, min(0.99, rng.gauss(d.get('vpin_mean', 0.55), d.get('vpin_std', 0.08))))
        atr   = max(0.10, min(2.00, rng.gauss(d.get('atr_mean', 0.35), d.get('atr_std', 0.10))))
        # Direction: sample from real distribution, then clamp to strategy constraints.
        # If ALL active engines are long_only, never inject a short regime tick (wasted compute).
        # If ALL are short_only, never inject a long. Mixed: use real long_pct.
        _all_long_only  = all(getattr(eng.cfg, 'long_only',  False) for eng in engines.values())
        _all_short_only = all(getattr(eng.cfg, 'short_only', False) for eng in engines.values())
        if _all_long_only:
            direction = 'long'
        elif _all_short_only:
            direction = 'short'
        else:
            direction = 'long' if rng.random() < d.get('long_pct', 0.5) else 'short'

        _seed_base_state(coin, price, vpin, atr, now, rng)
        injector = REGIME_INJECTORS.get(regime)
        if injector:
            try: injector(coin, price, direction, atr, now, rng)
            except Exception: pass

        # NOTE: Do NOT mock time.time() during signal detection / gate evaluation.
        # All injectors use real wall-clock timestamps so signal detectors must see
        # real time too. We only mock time for check_outcomes (trade elapsed duration).

        for label, eng in engines.items():
            tot = totals[label]
            is_target = label in target_strats
            if is_target:
                tot.regime_ticks[regime] += 1

            try:
                r = E.run_pred(coin)
                # Generate score/conf above strategy gate thresholds.
                # Uses max(gate_min, dist_sample) so we simulate realistic fired rows,
                # not just any random tick.
                cfg_min_score = max(0.0, getattr(eng.cfg, 'min_score', 0.0))
                cfg_min_conf  = max(0,   getattr(eng.cfg, 'min_conf',  0))
                score_raw = rng.gauss(d.get('score_mean', 10.0), d.get('score_std', 20.0))
                score     = max(cfg_min_score * 1.05, abs(score_raw)) * (1 if direction == 'long' else -1)
                conf_raw  = rng.gauss(d.get('conf_mean', 65.0), d.get('conf_std', 8.0))
                conf      = int(max(cfg_min_conf + 2, min(100, conf_raw)))
                r['score']      = score
                r['dir']        = direction
                r['conf']       = conf
                r['n_agree']    = rng.randint(3, 5)
                r['n_conflict'] = 0
                r['strength']   = abs(score)
                E._pred_cache.clear()
                E._pred_cache[coin] = (E._tick_id, r)

                # Clear per-tick caches: impulse/star dedup and EMA21 cache
                eng._impulse_cache.clear()
                SE._ema21_cache.clear()
                gate_ok = eng.gates_met(coin, r)
            except Exception:
                gate_ok = False
            if gate_ok:
                try:
                    open_before = sum(1 for p in eng.preds if p.get('out3') is None)
                    eng.fire(coin, r, force_sim=True)
                    open_after  = sum(1 for p in eng.preds if p.get('out3') is None)
                    if open_after > open_before:
                        tot.fires += 1
                        tot.regime_fires[regime] += 1
                except Exception:
                    pass

            _orig_co = time.time
            time.time = lambda _n=now: _n   # type: ignore — mock only for elapsed time in check_outcomes
            try: eng.check_outcomes()
            except Exception: pass
            finally: time.time = _orig_co

    time.time = _orig_time   # type: ignore

    # Restore patched functions
    SS._find_lag_signal = _orig_lag_signal
    _SE2._find_lag_signal = _orig_lag_signal

    # Collect all closed trades
    for label, eng in engines.items():
        tot = totals[label]
        for p in eng.preds:
            if p.get('out3') is None: continue
            out    = p.get('out3', 'flat')
            pct    = p.get('pct3', 0.0) or 0.0
            reason = p.get('reason', 'unknown')
            net    = pct - CFG.FEE_RT
            tot.closed += 1
            if out == 'win':    tot.wins   += 1
            elif out == 'lose': tot.losses += 1
            tot.cum_net += net
            tot.exits[reason] = tot.exits.get(reason, 0) + 1

    return totals


# ════════════════════════════════════════════════════════════════════════════
# REPORTER
# ════════════════════════════════════════════════════════════════════════════

RESET  = '\033[0m'
GREEN  = '\033[92m'; RED   = '\033[91m'; CYAN  = '\033[96m'
BOLD   = '\033[1m';  DIM   = '\033[2m';  YELLOW= '\033[93m'

def _c(col, txt): return f"{col}{txt}{RESET}"
def _pct_col(v):  return GREEN if v >= 0 else RED
def _wr_col(v):   return GREEN if v >= 50 else RED


def print_results(totals: Dict[str, SimTotal], real_dists: Dict,
                  n_ticks: int, regime_names: List[str]) -> None:
    print(_c(BOLD + CYAN, f"\n{'━'*66}"))
    print(_c(BOLD, f"  SIMULATION RESULTS  ({n_ticks:,} ticks × {len(COINS)} coins)"))
    print(_c(CYAN, f"{'━'*66}"))

    for label, tot in sorted(totals.items()):
        rd = real_dists.get(label, {})
        real_wr  = None   # not in JSON analysis directly — omit delta
        net_col  = _pct_col(tot.cum_net)
        wr_col   = _wr_col(tot.wr)

        print(f"\n  {_c(BOLD, label):<8} "
              f"fires={tot.fires:>5}  closed={tot.closed:>4}  "
              f"WR={_c(wr_col, f'{tot.wr:.1f}%')}  "
              f"net={_c(net_col, f'{tot.cum_net:+.4f}%')}  "
              f"avg={_c(net_col, f'{tot.avg_net:+.5f}%')}")

        # Per-regime catch rate
        regime_parts = []
        for regime in regime_names:
            ticks = tot.regime_ticks.get(regime, 0)
            fires = tot.regime_fires.get(regime, 0)
            if ticks == 0: continue
            catch = fires / ticks * 100
            catch_col = GREEN if catch > 5 else YELLOW if catch > 0 else DIM
            regime_parts.append(f"{regime}={_c(catch_col, f'{catch:.1f}%')}")
        if regime_parts:
            print(f"  {'':8} catch: " + "  ".join(regime_parts))

        # Exit distribution
        if tot.exits:
            sorted_exits = sorted(tot.exits.items(), key=lambda x: -x[1])
            exit_str = "  ".join(f"{r}:{c}" for r, c in sorted_exits[:6])
            print(f"  {'':8} exits: {_c(DIM, exit_str)}")

    print()


def print_compare(base: Dict[str, SimTotal], var: Dict[str, SimTotal],
                  param: str, val_a: Any, val_b: Any,
                  n_ticks: int) -> None:
    print(_c(BOLD + CYAN, f"\n{'━'*66}"))
    print(_c(BOLD, f"  COMPARE  {param}: {val_a!r} → {val_b!r}  ({n_ticks:,} ticks)"))
    print(_c(CYAN, f"{'━'*66}"))

    labels = sorted(set(list(base.keys()) + list(var.keys())))
    for label in labels:
        b = base.get(label)
        v = var.get(label)
        if b is None or v is None: continue

        wr_d  = v.wr      - b.wr
        avg_d = v.avg_net - b.avg_net
        net_d = v.cum_net - b.cum_net

        print(f"\n  {_c(BOLD, label)}")
        print(f"  {'':4} {'':12}  {'Baseline':>10}  {'Variant':>10}  {'Delta':>10}")
        print(f"  {'':4} {'WR%':<12}  {b.wr:>10.1f}  {v.wr:>10.1f}  {_c(_pct_col(wr_d), f'{wr_d:>+9.1f}pp')}")
        print(f"  {'':4} {'avg%':<12}  {b.avg_net:>10.5f}  {v.avg_net:>10.5f}  {_c(_pct_col(avg_d), f'{avg_d:>+9.5f}%')}")
        print(f"  {'':4} {'net%':<12}  {b.cum_net:>10.4f}  {v.cum_net:>10.4f}  {_c(_pct_col(net_d), f'{net_d:>+9.4f}%')}")
        print(f"  {'':4} {'fires':<12}  {b.fires:>10}  {v.fires:>10}  {_c(DIM, f'{v.fires-b.fires:>+9}')}")

        all_exits = sorted(set(list(b.exits.keys()) + list(v.exits.keys())))
        for r in all_exits:
            bc = b.exits.get(r, 0); vc = v.exits.get(r, 0); d = vc - bc
            dcol = _pct_col(d) if r in ('trail','tp') else (_pct_col(-d) if r in ('sl','inertia') else DIM)
            print(f"  {'':4} {r:<12}  {bc:>10}  {vc:>10}  {_c(dcol, f'{d:>+9}')}")
    print()


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description='PredictEngine Synthetic Market Simulator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Quick run (all strategies, 50k ticks):
  python synth_market_sim.py

  # Full 1M tick run:
  python synth_market_sim.py --ticks 1000000

  # Load learned distributions from CSV (recommended):
  python sim_learn_dists.py --csv ./data_backup/*/logs/signals_combined.csv
  python synth_market_sim.py --dists sim_dists.json --ticks 1000000

  # Filter strategies and regimes:
  python synth_market_sim.py --strategy B W L --regime mtf decorrelation level

  # Compare config param (same seed = same price paths):
  python synth_market_sim.py --strategy B --ticks 200000 --compare win_thr 0.30 0.45
  python synth_market_sim.py --strategy W --ticks 200000 --compare inertia_sec 45 90
  python synth_market_sim.py --strategy L --ticks 100000 --compare vpin_min 0.55 0.62
        """
    )
    parser.add_argument('--ticks',    type=int, default=50_000)
    parser.add_argument('--strategy', nargs='+', default=None)
    parser.add_argument('--regime',   nargs='+', default=None)
    parser.add_argument('--seed',     type=int,  default=0)
    parser.add_argument('--dists',    default=str(_HERE / 'sim_dists.json'),
                        help='Path to sim_dists.json from sim_learn_dists.py')
    parser.add_argument('--compare',  nargs=3, metavar=('PARAM', 'VAL_A', 'VAL_B'),
                        help='Compare a config param between two values')
    args = parser.parse_args()

    # Load distributions
    dists = {}
    dists_path = Path(args.dists)
    if dists_path.exists():
        with open(dists_path) as fh:
            dists = json.load(fh)
        print(_c(DIM, f"  Loaded {len(dists)} strategy distributions from {dists_path}"),
              file=sys.stderr)
    else:
        dists = FALLBACK_DISTS
        print(_c(YELLOW, f"  [WARN] {dists_path} not found — using fallback distributions"),
              file=sys.stderr)
        print(_c(DIM,    f"  Run: python sim_learn_dists.py --csv ./data_backup/*/logs/signals_combined.csv"),
              file=sys.stderr)

    # Resolve strategy labels
    all_labels = [s.label for s in STRATEGIES if not s.disabled]
    labels = args.strategy if args.strategy else all_labels

    # Resolve regimes
    regimes = args.regime if args.regime else ALL_REGIMES
    regimes = [r for r in regimes if r in REGIME_INJECTORS]

    print(_c(BOLD + CYAN, '\nPredictEngine — Synthetic Market Simulator'))
    print(_c(DIM, f'  ticks={args.ticks:,}  strategies={labels}  regimes={regimes}  seed={args.seed}'))
    if args.compare:
        print(_c(DIM, f'  compare: {args.compare[0]} {args.compare[1]!r} → {args.compare[2]!r}'))
    print()

    if args.compare:
        if not args.strategy or len(args.strategy) == 0:
            print(_c(RED, '[ERROR] --compare requires --strategy'), file=sys.stderr)
            sys.exit(1)

        param, val_a_str, val_b_str = args.compare
        # Type-coerce
        cfg_base = next((s for s in STRATEGIES if s.label == labels[0]), None)
        val_a: Any = val_a_str; val_b: Any = val_b_str
        if cfg_base:
            existing = getattr(cfg_base, param, None)
            if existing is not None:
                try:
                    if isinstance(existing, bool):
                        val_a = val_a_str in ('true','True','1','yes')
                        val_b = val_b_str in ('true','True','1','yes')
                    elif isinstance(existing, float): val_a = float(val_a_str); val_b = float(val_b_str)
                    elif isinstance(existing, int):   val_a = int(val_a_str);   val_b = int(val_b_str)
                except (ValueError, TypeError): pass

        print(_c(DIM, f'  Running baseline ({param}={val_a!r})...'), file=sys.stderr)
        base = run_full(labels, regimes, args.ticks, dists, seed=args.seed,
                        cfg_overrides={param: val_a}, verbose=True)

        print(_c(DIM, f'  Running variant ({param}={val_b!r})...'), file=sys.stderr)
        var  = run_full(labels, regimes, args.ticks, dists, seed=args.seed,
                        cfg_overrides={param: val_b}, verbose=True)

        print_compare(base, var, param, val_a, val_b, args.ticks)

    else:
        print(_c(DIM, f'  Simulating...'), file=sys.stderr)
        totals = run_full(labels, regimes, args.ticks, dists,
                          seed=args.seed, verbose=True)
        print_results(totals, dists, args.ticks, regimes)


if __name__ == '__main__':
    main()
