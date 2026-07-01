"""
PredictEngine — Realistic Synthetic Simulator
==============================================
Learns per-strategy distributions from your real analysis JSON files,
generates synthetic trades that match those distributions, then drives
them through the REAL exit engine (check_outcomes) tick-by-tick so that
changes to TP/SL/inertia/trail config are immediately measurable.

Usage:
  python synth_sim.py                             # simulate all strategies, 200 trades each
  python synth_sim.py --strategy B                # one strategy
  python synth_sim.py --strategy B --n 500        # 500 trades
  python synth_sim.py --compare inertia_sec 45 90 # compare two param values side-by-side
  python synth_sim.py --list                      # list known strategy distributions

The simulator:
  1. Loads per-strategy distributions (WR, avg exit pct, exit mix, avg TP/SL/dur).
     These are baked-in from your analysis JSONs and easily updatable.
  2. Generates a synthetic price walk per trade that reproduces the real exit
     distribution shape: win trades have realistic upward paths, loses have
     downward paths, with the correct trail/SL/TP/time/rev split.
  3. Feeds each price tick through StrategyEngine.check_outcomes() — the REAL
     exit logic — so any config change (inertia_sec, trail_dist, win_thr, etc.)
     is reflected in the simulation outcome immediately.
  4. Reports: WR, net P&L, exit reason distribution, avg duration, and a
     diff vs the baseline when using --compare.

Key principle: this is NOT a backtest. It's a sensitivity tester.
The generated price paths are calibrated to produce the observed real-world
exit distribution under current config. When you change config, you see how
the exit logic responds to the same price paths differently.

No network. No Binance. No file I/O. Fully deterministic with --seed.
"""

import sys, os, time, math, random, argparse, types
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import copy

# ── Same mock setup as synth_test.py ────────────────────────────────────────
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
_lm.create_order    = lambda *a, **k: {'ok': False, 'skipped': True}
_lm.close_position  = lambda *a, **k: {'realized_pnl': None, 'commission': None}
sys.modules['live_execution'] = _lm

try:
    import config as CFG
    import engine as E
    from strategies_config import STRATEGIES, StrategyConfig
    import strategies_engine as SE
    import strategies_runtime as SR
except ImportError as exc:
    print(f"[ERROR] Cannot import engine: {exc}")
    print("Run from the same directory as engine.py.")
    sys.exit(1)

SR._slot_holders = {}
def _sim_release(sym, label): return True, False
SR._release_open = _sim_release


# ════════════════════════════════════════════════════════════════════════════
# REAL STRATEGY DISTRIBUTIONS
# Extracted from analysis_20260606_065028, _112451, _122546, _010228 JSONs.
# Update these when you run fresh analysis. Format:
#   trades: total observed trades (weight for averaging)
#   wr:     observed win rate %
#   avg:    observed avg net P&L per trade %
#   tp:     typical dyn_tp range [min, max] %
#   sl:     typical dyn_sl range [min, max] %
#   dur:    typical trade duration range [min, max] seconds
#   exits:  exit reason weights (unnormalized — will be normalized)
#   score:  typical abs(score) at fire [mean, std]
#   vpin:   typical vpin at fire [mean, std]
# ════════════════════════════════════════════════════════════════════════════

STRATEGY_DISTS: Dict[str, Dict] = {
    'B': {
        'trades': 833,   # largest sample → most reliable
        'wr':     60.5,
        'avg':    0.0489,
        'tp':     [0.20, 0.55],
        'sl':     [0.14, 0.45],
        'dur':    [15,  180],
        'exits':  {'trail': 469, 'sl': 287, 'time': 55, 'tp': 6},
        'score':  [55.0, 18.0],
        'vpin':   [0.68, 0.08],
    },
    'W': {
        'trades': 1204,
        'wr':     48.7,
        'avg':    -0.0131,
        'tp':     [0.18, 0.45],
        'sl':     [0.12, 0.38],
        'dur':    [20,  250],
        'exits':  {'trail': 548, 'sl': 427, 'tp': 142, 'time': 59},
        'score':  [35.0, 20.0],
        'vpin':   [0.55, 0.10],
    },
    'L': {
        'trades': 412,
        'wr':     57.0,
        'avg':    0.0267,
        'tp':     [0.22, 0.50],
        'sl':     [0.14, 0.35],
        'dur':    [20,  200],
        'exits':  {'trail': 136, 'sl': 123, 'rev': 88, 'tp': 29, 'time': 9},
        'score':  [50.0, 22.0],
        'vpin':   [0.60, 0.10],
    },
    'Y': {
        'trades': 175,
        'wr':     36.4,
        'avg':    -0.0282,
        'tp':     [0.25, 0.70],
        'sl':     [0.15, 0.45],
        'dur':    [15,  200],
        'exits':  {'trail': 110, 'sl': 57, 'tp': 6, 'time': 2},
        'score':  [45.0, 20.0],
        'vpin':   [0.58, 0.10],
    },
    'CGY': {
        'trades': 214,
        'wr':     49.1,
        'avg':    -0.0196,
        'tp':     [0.25, 0.80],
        'sl':     [0.15, 0.50],
        'dur':    [20,  250],
        'exits':  {'trail': 100, 'sl': 71, 'time': 31, 'tp': 5},
        'score':  [52.0, 18.0],
        'vpin':   [0.62, 0.09],
    },
    'K': {
        'trades': 35,
        'wr':     42.9,
        'avg':    -0.1238,
        'tp':     [0.30, 0.90],
        'sl':     [0.18, 0.55],
        'dur':    [30,  150],
        'exits':  {'time': 23, 'sl': 12},
        'score':  [48.0, 22.0],
        'vpin':   [0.65, 0.10],
    },
    'E': {
        'trades': 36,
        'wr':     27.3,
        'avg':    -0.1511,
        'tp':     [0.22, 0.55],
        'sl':     [0.14, 0.40],
        'dur':    [20,  200],
        'exits':  {'sl': 16, 'trail': 13, 'time': 3, 'tp': 2},
        'score':  [42.0, 20.0],
        'vpin':   [0.56, 0.10],
    },
    'S': {
        'trades': 50,    # estimated
        'wr':     48.0,
        'avg':    -0.02,
        'tp':     [0.22, 0.55],
        'sl':     [0.14, 0.40],
        'dur':    [20,  200],
        'exits':  {'trail': 22, 'sl': 20, 'tp': 5, 'time': 3},
        'score':  [45.0, 20.0],
        'vpin':   [0.58, 0.10],
    },
    'Q': {
        'trades': 80,    # estimated
        'wr':     50.0,
        'avg':    0.01,
        'tp':     [0.25, 0.70],
        'sl':     [0.15, 0.45],
        'dur':    [30,  300],
        'exits':  {'trail': 35, 'sl': 30, 'tp': 10, 'time': 5},
        'score':  [40.0, 18.0],
        'vpin':   [0.60, 0.09],
    },
    'Z': {
        'trades': 40,    # estimated — mostly sim
        'wr':     45.0,
        'avg':    -0.03,
        'tp':     [0.15, 0.35],
        'sl':     [0.10, 0.25],
        'dur':    [5,   60],
        'exits':  {'trail': 18, 'sl': 16, 'tp': 4, 'time': 2},
        'score':  [38.0, 15.0],
        'vpin':   [0.62, 0.08],
    },
    'WB': {
        'trades': 230,
        'wr':     60.9,
        'avg':    0.0847,
        'tp':     [0.20, 0.55],
        'sl':     [0.13, 0.40],
        'dur':    [15,  200],
        'exits':  {'trail': 129, 'sl': 61, 'tp': 22, 'time': 13},
        'score':  [55.0, 17.0],
        'vpin':   [0.67, 0.08],
    },
}


# ════════════════════════════════════════════════════════════════════════════
# PRICE PATH GENERATOR
# Generates tick-by-tick price paths calibrated to produce realistic exit
# distributions. Each path is seeded with a target outcome (win/lose/time)
# drawn from the strategy's observed exit mix.
# ════════════════════════════════════════════════════════════════════════════

@dataclass
class TradePath:
    """A synthetic trade: entry conditions + price ticks."""
    sym:       str
    dir:       str          # 'long' or 'short'
    entry:     float
    dyn_tp:    float        # % TP
    dyn_sl:    float        # % SL
    score:     float
    vpin:      float
    atr:       float
    duration:  float        # target seconds
    prices:    List[float]  # tick prices at 100ms intervals
    target_exit: str        # what the calibrated path aims for


def _norm_weights(d: Dict[str, int]) -> Dict[str, float]:
    total = sum(d.values())
    return {k: v / total for k, v in d.items()}


class PathGenerator:
    """
    Generates calibrated price paths per trade.
    The path shape is controlled by target_exit to reproduce the real exit mix.
    """

    def __init__(self, seed: int = 0):
        self.rng = random.Random(seed)

    def _sample_exit(self, exits: Dict[str, int]) -> str:
        weights = _norm_weights(exits)
        r = self.rng.random()
        cumul = 0.0
        for reason, w in weights.items():
            cumul += w
            if r <= cumul:
                return reason
        return list(exits.keys())[-1]

    def generate(
        self,
        dist:     Dict,
        label:    str,
        n:        int,
        cfg:      StrategyConfig,
    ) -> List[TradePath]:
        """Generate n TradePath objects calibrated to dist."""
        paths = []
        for i in range(n):
            target = self._sample_exit(dist['exits'])
            path   = self._make_path(dist, cfg, target, i)
            paths.append(path)
        return paths

    def _make_path(
        self,
        dist:   Dict,
        cfg:    StrategyConfig,
        target: str,
        idx:    int,
    ) -> TradePath:
        rng = self.rng
        sym = 'BTCUSDT'

        # ── Entry conditions from distribution ──────────────────────────────
        entry  = 50000.0 + rng.uniform(-5000, 5000)
        # Use cfg's actual ATR-scaled TP/SL — don't use dist estimates for these
        # because they're already baked into what the exit engine will compute.
        atr    = rng.uniform(0.28, 0.60)
        # _calc_tp_sl replication: use cfg.atr_tp_mult / atr_sl_mult
        tp_mult = min(cfg.atr_tp_mult, 2.0)   # cap 999 sentinel to something finite
        sl_mult = cfg.atr_sl_mult
        dyn_tp  = max(0.18, min(1.50, atr * tp_mult))
        dyn_sl  = max(0.12, min(0.60, atr * sl_mult))
        # For trail-dominant strategies (atr_tp_mult=999), TP is irrelevant —
        # trail will close. Use a sentinel that check_outcomes won't hit.
        if cfg.atr_tp_mult > 10:
            dyn_tp = 2.0   # effectively unreachable; trail will fire instead

        score  = abs(rng.gauss(*dist['score']))
        vpin   = max(0.30, min(0.99, rng.gauss(*dist['vpin'])))
        trade_dir = 'long' if rng.random() > 0.5 else 'short'

        # ── Target duration ──────────────────────────────────────────────────
        win_thr    = cfg.win_thr
        trail_dist = cfg.trail_dist
        max_win    = cfg.max_window
        min_hold   = cfg.min_hold_any
        inertia    = min(cfg.inertia_sec, max_win)   # cap infinite inertia
        # Use 1s tick resolution for the simulator (not LOOP_MS=100ms).
        # This makes each trade ~max_window ticks at most (600 ticks for a 600s trade)
        # instead of 6000, while preserving all elapsed-time logic correctly.
        tick_dt    = 1.0

        # All durations must exceed min_hold_any so the engine's hold gate passes.
        # Trail paths need: rise phase + plateau + retrace, all within max_window.
        min_trail_dur = max(min_hold * 2.5, min(180.0, max_win * 0.5))
        max_trail_dur = max_win * 0.80  # leave 20% headroom for retrace to complete

        if target == 'time':
            duration = max_win * rng.uniform(0.97, 1.03)
        elif target in ('sl', 'inertia'):
            duration = rng.uniform(min_hold * 1.5, min(inertia * 1.2, max_win * 0.55))
        elif target == 'trail':
            duration = rng.uniform(min_trail_dur, max_trail_dur)
        elif target == 'tp':
            duration = rng.uniform(min_hold * 2.0, max_win * 0.45)
        elif target == 'rev':
            duration = rng.uniform(max(min_hold * 2.0, getattr(cfg, 'rev_min_hold', 60.0)),
                                   max_win * 0.55)
        else:
            duration = rng.uniform(min_hold * 1.5, max_win * 0.65)

        n_ticks = max(10, min(int(duration / tick_dt), int(max_win / tick_dt)))

        # ── Price path calibrated to target exit ─────────────────────────────
        prices = self._path_for_target(
            target, n_ticks, entry, dyn_tp, dyn_sl, cfg, rng, atr, tick_dt
        )

        return TradePath(
            sym=sym, dir=trade_dir, entry=entry,
            dyn_tp=dyn_tp, dyn_sl=dyn_sl,
            score=score, vpin=vpin, atr=atr,
            duration=duration, prices=prices,
            target_exit=target,
        )

    def _path_for_target(
        self,
        target:  str,
        n:       int,
        entry:   float,
        tp:      float,
        sl:      float,
        cfg:     StrategyConfig,
        rng:     random.Random,
        atr:     float = 0.35,
        tick_dt: float = 0.1,
    ) -> List[float]:
        """
        Generate a price series (in raw price units) whose %-from-entry
        trajectory is shaped to naturally trigger the target exit condition.
        We work in dp% space (% from entry) then convert back to price.
        """
        def dp_to_price(dp_pct: float) -> float:
            return entry * (1 + dp_pct / 100)

        prices: List[float] = []
        # Noise per tick: proportional to ATR, scaled to 100ms ticks
        # ATR is % per bar (1m) → per 100ms tick it's ~ATR/600
        noise_vol = atr / 600.0 * 2.5   # slight amplification for realism

        win_thr    = cfg.win_thr
        trail_dist = cfg.trail_dist

        if target == 'tp':
            # Steady drift to TP
            target_dp = tp * rng.uniform(1.01, 1.10)
            for i in range(n):
                frac  = i / max(n - 1, 1)
                dp    = target_dp * frac + rng.gauss(0, noise_vol * 0.4)
                prices.append(dp_to_price(dp))

        elif target == 'trail':
            # Must rise past win_thr AFTER min_hold_any ticks, then retrace by trail_dist.
            # Important: check_outcomes may widen trail_dist by 1.5x if snap30 > hold_thr.
            # Use the widened value (trail_dist * 1.5) to guarantee the retrace triggers.
            # The widened trail fires if dp <= max_dp - (trail_dist * 1.5).
            effective_trail = min(cfg.trail_dist * 1.5, 0.25)   # mirrors the widening logic
            min_hold_ticks = max(1, int(cfg.min_hold_any / tick_dt))
            # Peak must complete early enough that the retrace fits within n_ticks.
            # Use first 40-55% of ticks for the rise, leaving the rest for retrace.
            peak_at = rng.randint(max(min_hold_ticks, n // 3), max(min_hold_ticks + 1, int(n * 0.50)))
            # Peak must be above win_thr and deep enough that retrace still closes above zero
            peak_dp = rng.uniform(cfg.win_thr * 1.4, max(cfg.win_thr * 3.0, effective_trail * 3.5))
            # Retrace: must go below peak_dp - effective_trail to guarantee a close
            retrace_target = peak_dp - effective_trail * rng.uniform(1.10, 1.50)
            # Ensure retrace ends in positive territory (it's a win, not a loss)
            retrace_dp = max(retrace_target, 0.0)
            for i in range(n):
                if i <= peak_at:
                    frac = i / max(peak_at, 1)
                    dp   = peak_dp * frac
                else:
                    frac = (i - peak_at) / max(n - peak_at, 1)
                    dp   = peak_dp - (peak_dp - retrace_dp) * frac
                prices.append(dp_to_price(dp + rng.gauss(0, noise_vol)))

        elif target == 'sl':
            # Drift to -SL cleanly, but only after min_hold_any has elapsed.
            min_hold_ticks = max(1, int(cfg.min_hold_any / tick_dt))
            breach_at = rng.randint(min_hold_ticks, min(min_hold_ticks * 3, n - 1))
            target_dp = -sl * rng.uniform(1.02, 1.25)
            for i in range(n):
                frac = min(i / max(breach_at, 1), 1.0)
                dp   = target_dp * frac + rng.gauss(0, noise_vol * 0.4)
                prices.append(dp_to_price(dp))

        elif target == 'inertia':
            # Stays slightly negative, never crosses SL or win_thr
            for i in range(n):
                dp = rng.gauss(-sl * 0.20, noise_vol * 0.6)
                prices.append(dp_to_price(dp))

        elif target == 'time':
            # Oscillates inside (-sl*0.7, +win_thr*0.7) — never triggers exits
            band = min(sl * 0.65, win_thr * 0.65)
            for i in range(n):
                dp = math.sin(i * 0.25) * band * rng.uniform(0.5, 1.0) + rng.gauss(0, noise_vol * 0.3)
                # Clamp to safe band so SL/TP never fire
                dp = max(-sl * 0.70, min(win_thr * 0.70, dp))
                prices.append(dp_to_price(dp))

        elif target == 'rev':
            # Rises modestly, then sells off so sig_reversed() triggers
            peak_at = rng.randint(n // 4, n // 2)
            peak_dp = rng.uniform(0.04, win_thr * 0.55)
            for i in range(n):
                if i <= peak_at:
                    frac = i / max(peak_at, 1)
                    dp   = peak_dp * frac
                else:
                    frac = (i - peak_at) / max(n - peak_at, 1)
                    dp   = peak_dp - (peak_dp + sl * 0.45) * frac
                prices.append(dp_to_price(dp + rng.gauss(0, noise_vol)))

        else:
            # Generic flat-ish
            for _ in range(n):
                prices.append(dp_to_price(rng.gauss(0, noise_vol)))

        return prices


# ════════════════════════════════════════════════════════════════════════════
# MOCK STRATEGY ENGINE (same as synth_test.py)
# ════════════════════════════════════════════════════════════════════════════

class MockEngine(SE.StrategyEngine):
    def __init__(self, cfg: StrategyConfig):
        super().__init__(cfg, log_prefix=None)
        self._start_ts = time.time() - 9999.0
    def _save_state(self): pass
    def _load_state(self): pass


def _make_engine(label: str) -> Optional[MockEngine]:
    cfg = next((s for s in STRATEGIES if s.label == label), None)
    return MockEngine(cfg) if cfg else None


# ════════════════════════════════════════════════════════════════════════════
# TRADE SIMULATOR
# For each TradePath: inject a pred dict directly (bypassing fire/gate),
# then drive price tick-by-tick through check_outcomes() until closed.
# ════════════════════════════════════════════════════════════════════════════

def _inject_sym_state(sym: str, price: float, vpin: float, atr_pct: float) -> None:
    """Minimal sym_state for check_outcomes to read."""
    E.init_sym(sym)
    st = E.sym_state[sym]
    st['price'] = price
    st['prev_price'] = price * 0.9999
    now_ms = time.time() * 1000
    # Price hist for ATR
    if len(st['price_hist']) < 5:
        for i in range(5):
            st['price_hist'].append((now_ms - (5-i)*60000,
                                     price * (1 + atr_pct/100 * math.sin(i))))
    # VPIN
    st['vpin_buckets'].clear()
    for _ in range(20):
        st['vpin_buckets'].append(vpin)
    # Trade tape (for sig_reversed/sig_still_valid)
    st['trade_tape'].clear()
    for i in range(20):
        st['trade_tape'].append((now_ms - i*500, price, 2000.0, True))
    st['cvd'].clear()
    for i in range(50):
        st['cvd'].append((now_ms - i*500, 3000.0, 1000.0))
    st['oi'] = 60_000_000
    st['regime'] = 'neutral'
    st['funding_rate'] = 0.0001


def _make_pred(path: TradePath, label: str, now: float) -> Dict:
    """Construct a pred dict matching what fire() would produce."""
    return dict(
        id=random.randint(1000, 9999),
        ts=now,
        sym=path.sym,
        dir=path.dir,
        conf=70,
        score=path.score,
        n_agree=3, n_avail=5,
        entry=path.entry,
        dyn_tp=path.dyn_tp,
        dyn_sl=path.dyn_sl,
        out3=None, pct3=None,
        max_dp=-999, min_dp=999,
        snap30=None, snap60=None, snap1=None,
        tp_touches=0,
        be_activated=False, be_activated_at=None,
        exit_price=None,
        atr_entry=round(path.atr, 4),
        vpin_entry=round(path.vpin, 3),
        spread_entry=0.01,
        reason=None, dur=None,
        tp_extended=False, be_locked=False,
        _trail_widened=False,
        _strategy_label=label,
        _wall_dir=None, _vp_poc=None,
        _range_top=None, _range_bot=None,
        _funding_rate=None, _cascade_usd30=None,
        _oi_type=None, _density_level=None,
        _decor_divergence=None, _knife_wick=None,
        _lag_best_ms=None, _lag_best_div=None,
        _lag_exchanges=None,
        _inertia_floor=False,
        _live_ok=False,
    )


@dataclass
class SimResult:
    label:    str
    n:        int
    wins:     int = 0
    losses:   int = 0
    flats:    int = 0
    cum_net:  float = 0.0
    exits:    Dict[str, int] = field(default_factory=dict)
    durations: List[float]   = field(default_factory=list)
    pnls:      List[float]   = field(default_factory=list)

    @property
    def wr(self) -> float:
        closed = self.wins + self.losses + self.flats
        return self.wins / max(closed, 1) * 100

    @property
    def avg(self) -> float:
        return self.cum_net / max(self.n, 1)

    @property
    def closed(self) -> int:
        return self.wins + self.losses + self.flats


def simulate_strategy(
    label:  str,
    n:      int = 200,
    seed:   int = 0,
    cfg_overrides: Optional[Dict] = None,
) -> Optional[SimResult]:
    """
    Run n synthetic trades for strategy `label` through the real exit engine.

    cfg_overrides: dict of StrategyConfig field → value overrides.
    E.g. {'inertia_sec': 90.0, 'trail_dist': 0.10}
    """
    dist = STRATEGY_DISTS.get(label)
    if dist is None:
        return None

    cfg = next((s for s in STRATEGIES if s.label == label), None)
    if cfg is None:
        return None

    # Apply overrides by shallow-copying and patching
    if cfg_overrides:
        import dataclasses
        cfg = dataclasses.replace(cfg, **cfg_overrides)

    eng   = MockEngine(cfg)
    gen   = PathGenerator(seed=seed)
    paths = gen.generate(dist, label, n, cfg)
    res   = SimResult(label=label, n=n)

    tick_dt  = 1.0          # 1s resolution — preserves all elapsed-time logic
    sym      = 'BTCUSDT'

    E.init_sym(sym)
    SR._slot_holders = {}

    for path in paths:
        # Use a fixed fake base time so elapsed works correctly
        # pred['ts'] stays fixed; we advance time.time() via the tick counter
        trade_start = 1_700_000_000.0  # fixed epoch — irrelevant, only elapsed matters
        pred = _make_pred(path, label, trade_start)

        # Inject pred directly (bypass gate/fire)
        if not isinstance(eng.preds, deque):
            eng.preds = deque(eng.preds, maxlen=200)
        eng.preds.appendleft(pred)
        eng._open_syms.add(sym)
        eng.hist_total += 1

        # Drive price tick-by-tick
        closed = False
        for tick_i, price in enumerate(path.prices):
            now = trade_start + (tick_i + 1) * tick_dt

            # Update sym_state price
            st = E.sym_state[sym]
            st['prev_price'] = st['price']
            st['price']      = price
            st['price_hist'].append((now * 1000, price))

            # Run real exit logic with patched time
            _orig_time = time.time
            time.time = lambda _n=now: _n   # type: ignore
            try:
                eng.check_outcomes()
            finally:
                time.time = _orig_time

            if pred.get('out3') is not None:
                closed = True
                break

        # Force close any still-open trade as 'time' exit
        if not closed and pred.get('out3') is None:
            now = trade_start + len(path.prices) * tick_dt
            st  = E.sym_state[sym]
            dp_raw = (st['price'] - pred['entry']) / pred['entry'] * 100
            dp = dp_raw if pred['dir'] == 'long' else -dp_raw
            _orig_time = time.time
            time.time = lambda _n=now: _n   # type: ignore
            try:
                eng._close(pred, dp, 'time')
            finally:
                time.time = _orig_time

        # Collect result
        out    = pred.get('out3')
        pct    = pred.get('pct3', 0.0) or 0.0
        reason = pred.get('reason', 'unknown')
        dur    = pred.get('dur', 0.0) or 0.0
        net    = pct - CFG.FEE_RT

        if out == 'win':    res.wins   += 1
        elif out == 'lose': res.losses += 1
        else:               res.flats  += 1
        res.cum_net += net
        res.exits[reason] = res.exits.get(reason, 0) + 1
        res.durations.append(dur)
        res.pnls.append(net)

        # Clean up pred from engine for next trade
        eng._open_syms.discard(sym)
        if eng.preds and eng.preds[0] is pred:
            eng.preds[0]['out3'] = pred.get('out3', 'time')

    return res


# ════════════════════════════════════════════════════════════════════════════
# COMPARISON RUNNER
# ════════════════════════════════════════════════════════════════════════════

@dataclass
class CompareResult:
    label:    str
    param:    str
    baseline: SimResult
    variant:  SimResult
    val_a:    object   # baseline value
    val_b:    object   # variant value


def compare_param(
    label:  str,
    param:  str,
    val_a:  object,
    val_b:  object,
    n:      int = 300,
    seed:   int = 0,
) -> Optional[CompareResult]:
    """
    Run the same synthetic trades (same seed) under two different param values.
    Uses identical price paths so differences are purely due to the config change.
    """
    # Try to coerce to the right type
    cfg_base = next((s for s in STRATEGIES if s.label == label), None)
    if cfg_base is None:
        return None
    existing = getattr(cfg_base, param, None)
    if existing is not None:
        try:
            if isinstance(existing, float): val_a = float(val_a); val_b = float(val_b)
            elif isinstance(existing, int):  val_a = int(val_a);   val_b = int(val_b)
            elif isinstance(existing, bool): val_a = val_a in ('true','True','1'); val_b = val_b in ('true','True','1')
        except (ValueError, TypeError):
            pass

    base = simulate_strategy(label, n=n, seed=seed, cfg_overrides={param: val_a})
    var  = simulate_strategy(label, n=n, seed=seed, cfg_overrides={param: val_b})
    if base is None or var is None:
        return None
    return CompareResult(label=label, param=param,
                         baseline=base, variant=var,
                         val_a=val_a, val_b=val_b)


# ════════════════════════════════════════════════════════════════════════════
# REPORT
# ════════════════════════════════════════════════════════════════════════════

RESET  = '\033[0m'
GREEN  = '\033[92m'; RED    = '\033[91m'; CYAN   = '\033[96m'
BOLD   = '\033[1m';  DIM    = '\033[2m';  YELLOW = '\033[93m'
MAGENTA= '\033[95m'

def _c(color, text): return f"{color}{text}{RESET}"

def _pct_color(v: float) -> str:
    return GREEN if v >= 0 else RED

def _wr_color(wr: float) -> str:
    return GREEN if wr >= 50 else RED

def _bar(v: float, total: int, width: int = 20) -> str:
    filled = int(v / max(total, 1) * width)
    return '█' * filled + '░' * (width - filled)


def print_sim_result(res: SimResult, real_dist: Optional[Dict] = None) -> None:
    real_wr  = real_dist['wr']  if real_dist else None
    real_avg = real_dist['avg'] if real_dist else None

    wr_col  = _wr_color(res.wr)
    net_col = _pct_color(res.cum_net)

    wr_delta  = f" ({res.wr - real_wr:+.1f}pp vs real)"   if real_wr  is not None else ''
    avg_delta = f" ({res.avg - real_avg:+.4f}pp vs real)" if real_avg is not None else ''

    print(f"\n  {_c(BOLD, res.label):<8} "
          f"n={res.closed}/{res.n}  "
          f"WR={_c(wr_col, f'{res.wr:.1f}%')}{_c(DIM, wr_delta)}  "
          f"net={_c(net_col, f'{res.cum_net:+.4f}%')}  "
          f"avg={_c(net_col, f'{res.avg:+.5f}%')}{_c(DIM, avg_delta)}")

    if res.durations:
        avg_dur = sum(res.durations) / len(res.durations)
        print(f"  {'':8} avg_dur={avg_dur:.0f}s  "
              f"pnl_std={_std(res.pnls):.4f}%")

    total = res.closed
    sorted_exits = sorted(res.exits.items(), key=lambda x: -x[1])
    exit_parts = []
    for reason, cnt in sorted_exits:
        bar = _bar(cnt, total, width=10)
        pct = cnt / max(total, 1) * 100
        exit_parts.append(f"{reason}:{cnt}({pct:.0f}%) {_c(DIM, bar)}")
    print(f"  {'':8} " + '  '.join(exit_parts))


def print_compare_result(comp: CompareResult) -> None:
    b, v = comp.baseline, comp.variant
    wr_d   = v.wr   - b.wr
    avg_d  = v.avg  - b.avg
    net_d  = v.cum_net - b.cum_net

    print(_c(BOLD + CYAN, f"\n  ─── Compare {comp.label}.{comp.param}: {comp.val_a!r} → {comp.val_b!r} ───"))
    print(f"  {'':6} {'Baseline':>12}  {'Variant':>12}  {'Delta':>10}")
    print(f"  {'':6} {_c(DIM, str(comp.val_a)):>12}  {_c(DIM, str(comp.val_b)):>12}")
    print(f"  {'WR%':<6} {b.wr:>12.1f}  {v.wr:>12.1f}  {_c(_pct_color(wr_d), f'{wr_d:>+9.1f}pp')}")
    print(f"  {'avg%':<6} {b.avg:>12.5f}  {v.avg:>12.5f}  {_c(_pct_color(avg_d), f'{avg_d:>+9.5f}%')}")
    print(f"  {'net%':<6} {b.cum_net:>12.4f}  {v.cum_net:>12.4f}  {_c(_pct_color(net_d), f'{net_d:>+9.4f}%')}")

    # Exit reason diffs
    all_reasons = sorted(set(list(b.exits.keys()) + list(v.exits.keys())))
    print(f"  {'':6} {'':>12}  {'':>12}")
    print(f"  exits:")
    for r in all_reasons:
        bc = b.exits.get(r, 0); vc = v.exits.get(r, 0)
        d  = vc - bc
        dcol = _pct_color(d) if r in ('trail','tp') else (_pct_color(-d) if r in ('sl','inertia','time') else DIM)
        print(f"    {r:<10} base={bc:>4}  var={vc:>4}  Δ={_c(dcol, f'{d:>+4}')}")


def _std(vals: List[float]) -> float:
    if len(vals) < 2: return 0.0
    m = sum(vals) / len(vals)
    return math.sqrt(sum((v-m)**2 for v in vals) / (len(vals)-1))


def print_header() -> None:
    print(_c(BOLD + CYAN, '\n' + '═'*62))
    print(_c(BOLD, '  PredictEngine — Realistic Synthetic Simulator'))
    print(_c(DIM,  '  Distributions learned from analysis_20260606–07 JSONs'))
    print(_c(CYAN, '═'*62))


def print_legend() -> None:
    print(_c(DIM, '\n  ▸ WR delta vs real: how simulation WR compares to observed'))
    print(_c(DIM,  '  ▸ avg delta vs real: avg per-trade net P&L comparison'))
    print(_c(DIM,  '  ▸ exits: per-reason count + fill bar (longer = more common)\n'))


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description='PredictEngine Realistic Synthetic Simulator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python synth_sim.py                              # all strategies, 200 trades
  python synth_sim.py --strategy B                 # B only
  python synth_sim.py --strategy B --n 500         # B, 500 trades
  python synth_sim.py --strategy B --n 300 --compare inertia_sec 45 90
  python synth_sim.py --strategy W --n 300 --compare trail_dist 0.08 0.12
  python synth_sim.py --strategy K --n 200 --compare max_window 90 150
  python synth_sim.py --strategy B --n 300 --compare win_thr 0.35 0.45
  python synth_sim.py --list
        """
    )
    parser.add_argument('--strategy', default=None,
                        help='Strategy label to simulate (default: all known)')
    parser.add_argument('--n', type=int, default=200,
                        help='Number of synthetic trades per strategy (default: 200)')
    parser.add_argument('--seed', type=int, default=0,
                        help='Random seed (same seed = same price paths)')
    parser.add_argument('--compare', nargs=3, metavar=('PARAM', 'VAL_A', 'VAL_B'),
                        help='Compare a config param between two values (requires --strategy)')
    parser.add_argument('--list', action='store_true',
                        help='List known strategy distributions and exit')
    args = parser.parse_args()

    if args.list:
        print(_c(BOLD + CYAN, '\nKnown strategy distributions:'))
        print(f"  {'Label':<8} {'Trades':>8} {'WR%':>6} {'avg%':>8}  exits")
        print(f"  {'─'*7}  {'─'*8} {'─'*6} {'─'*8}  {'─'*35}")
        for lbl, d in sorted(STRATEGY_DISTS.items()):
            exits_str = ', '.join(f"{k}:{v}" for k,v in sorted(d['exits'].items(), key=lambda x:-x[1])[:4])
            print(f"  {lbl:<8} {d['trades']:>8} {d['wr']:>6.1f} {d['avg']:>+8.4f}  {exits_str}")
        print()
        return

    print_header()
    print(_c(DIM, f'  n={args.n}  seed={args.seed}'
             + (f'  strategy={args.strategy}' if args.strategy else '  strategy=all')
             + (f'  compare={args.compare}' if args.compare else '')))
    print_legend()

    labels = [args.strategy] if args.strategy else sorted(STRATEGY_DISTS.keys())

    # ── Compare mode ─────────────────────────────────────────────────────────
    if args.compare:
        if not args.strategy:
            print(_c(RED, '  --compare requires --strategy'))
            sys.exit(1)
        param, val_a, val_b = args.compare
        print(_c(BOLD + CYAN, f'━'*62))
        print(_c(BOLD, f'  COMPARISON  {args.strategy}.{param}: {val_a!r} → {val_b!r}'))
        print(_c(CYAN, f'━'*62))
        comp = compare_param(args.strategy, param, val_a, val_b, n=args.n, seed=args.seed)
        if comp is None:
            print(_c(RED, f'  Strategy {args.strategy!r} or param {param!r} not found.'))
            sys.exit(1)
        print_compare_result(comp)
        print()
        return

    # ── Standard simulation mode ──────────────────────────────────────────────
    print(_c(BOLD + CYAN, f'━'*62))
    print(_c(BOLD, f'  SIMULATION RESULTS  (n={args.n} per strategy)'))
    print(_c(CYAN, f'━'*62))

    total_wr_weighted = total_w = 0.0
    for label in labels:
        dist = STRATEGY_DISTS.get(label)
        if dist is None:
            print(f"  {_c(YELLOW, label)}: no distribution data (add to STRATEGY_DISTS)")
            continue
        result = simulate_strategy(label, n=args.n, seed=args.seed)
        if result is None:
            print(f"  {_c(YELLOW, label)}: strategy config not found in STRATEGIES")
            continue
        print_sim_result(result, real_dist=dist)
        total_wr_weighted += result.wr * result.closed
        total_w += result.closed

    if total_w > 0 and len(labels) > 1:
        overall_wr = total_wr_weighted / total_w
        print(_c(BOLD + CYAN, f'\n{"─"*62}'))
        wr_col = _c(GREEN if overall_wr >= 50 else RED, f'{overall_wr:.1f}%')
        print(f'  Overall weighted WR: {wr_col}  ({int(total_w)} trades)')

    print()


if __name__ == '__main__':
    main()
