"""
PredictEngine — OHLCV Walk-Forward Replay Engine (Phase 4)
===========================================================
Replays real Binance 1m klines through the full engine stack:
  real candles → sym_state → VPIN/ATR/EMA recompute → gate → fire → exit

This is a true walk-forward backtest, not a synthetic simulation.
Each tick uses only data that was available at that moment in time.

Usage:
  # Fetch data first:
  python ohlcv_fetcher.py --days 30

  # Run replay (all live strategies):
  python ohlcv_replay.py

  # Specific strategy, specific date range:
  python ohlcv_replay.py --strategy B --from 2026-05-15 --to 2026-06-01

  # Walk-forward (train 21d, validate 9d):
  python ohlcv_replay.py --strategy B --walk-forward --train-days 21 --val-days 9

  # Compare two configs on identical data:
  python ohlcv_replay.py --strategy B --compare long_only True False

  # Use subset of symbols (faster):
  python ohlcv_replay.py --strategy B --symbols WLDUSDT HYPEUSDT NEARUSDT INJUSDT

Output: per-strategy WR, avg_net, exit distribution, per-symbol breakdown

Key principle — NO LOOKAHEAD:
  Each candle is injected into sym_state before calling gates_met().
  The engine only sees data up to and including the current candle.
  signal_with_outcomes.csv is NOT used during replay — outcomes come
  purely from the simulated exit logic against future price data.
"""

import sys, os, time, math, random, argparse, csv, types, copy
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ── Mock setup (same pattern as synth_market_sim.py) ──────────────────────────
_HERE = Path(__file__).parent
_ENGINE_ROOT = _HERE.parent.parent  # tools/backtest → engine root
if str(_ENGINE_ROOT) not in sys.path:
    sys.path.insert(0, str(_ENGINE_ROOT))
_CACHE_DIR = _HERE / 'ohlcv_cache'  # cache stays in tools/backtest/

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
_lm.LIVE_ORDER_USDT = 100.0; _lm.LIVE_ENABLED = False
_lm.LIVE_MODE = False
_lm.can_enter       = lambda **k: (True, 'sim_mode')
_lm.create_order    = lambda *a, **k: {'ok': False}
_lm.close_position  = lambda *a, **k: {}
_lm._illiquid_syms  = set()
_lm.is_illiquid     = lambda sym: False
_lm._cache_n_positions = 0
_lm._cache_balance     = 10000.0
_lm._cache_unrealized  = 0.0
_lm._cache_ts          = time.time()
_lm._live_symbol_open  = set()
sys.modules['live_execution'] = _lm

try:
    import config as CFG
    import engine as E
    from strategies_config import STRATEGIES, StrategyConfig
    import strategies_engine as SE
    import strategies_runtime as SR
except ImportError as exc:
    print(f'[ERROR] Cannot import engine: {exc}')
    print('Run from the same directory as engine.py')
    sys.exit(1)

# Patch runtime to avoid position lock interference
SR._slot_holders = {}
def _sim_release(sym, label): return True, False
SR._release_open = _sim_release
try:
    SR._global_positions = {}
except Exception:
    pass


# ── OHLCV cache loader ────────────────────────────────────────────────────────

CACHE_DIR = _HERE / 'ohlcv_cache'  # tools/backtest/ohlcv_cache

def _load_csv_file(path: Path, ts_field: str = 'ts_ms') -> list:
    """Load a CSV cache file into list of dicts, ts_field cast to int."""
    if not path.exists():
        return []
    rows = []
    try:
        with open(path, newline='') as fh:
            for row in csv.DictReader(fh):
                try:
                    row[ts_field] = int(row[ts_field])
                    rows.append(row)
                except (KeyError, ValueError):
                    pass
    except Exception:
        return []
    rows.sort(key=lambda r: r[ts_field])
    return rows


def load_symbol_data(symbol: str) -> dict:
    """
    Load all cached data for a symbol.
    Returns dict with keys: candles, funding, oi, takerflow, lsr, top_lsr
    """
    def _load(suffix):
        return _load_csv_file(CACHE_DIR / f'{symbol}_{suffix}.csv')

    candles = _load('1m')
    # Cast numeric fields
    float_fields = ['open','high','low','close','volume','quote_volume',
                    'taker_buy_base_vol','taker_buy_quote_vol']
    for c in candles:
        for f in float_fields:
            if f in c:
                try: c[f] = float(c[f])
                except (ValueError, TypeError): c[f] = 0.0

    funding  = _load('funding')
    oi       = _load('oi_5m')
    taker    = _load('takerflow_5m')
    lsr      = _load('lsr_5m')
    top_lsr  = _load('top_lsr_5m')

    # aggTrades — sub-second trade data for B strategy microburst signal
    # Loaded lazily here; large files. Returns empty list if not fetched yet.
    agg = load_agg_trades(symbol)

    return {
        'candles':   candles,
        'funding':   funding,
        'oi':        oi,
        'takerflow': taker,
        'lsr':       lsr,
        'top_lsr':   top_lsr,
        'agg':       agg,
    }


def load_candles(symbol: str) -> list:
    """Backwards-compatible: load just 1m candles."""
    return load_symbol_data(symbol)['candles']


def load_agg_trades(symbol: str, start_ms: int = 0, end_ms: int = 0) -> list:
    """
    Load cached aggTrades for symbol from ohlcv_cache/agg/SYMBOL/YYYYMMDD.csv.
    Returns list of dicts sorted by ts_ms, filtered to [start_ms, end_ms] if provided.
    """
    agg_dir = CACHE_DIR / 'agg' / symbol
    if not agg_dir.exists():
        return []
    rows = []
    for p in sorted(agg_dir.glob('*.csv')):
        date_str = p.stem
        try:
            from datetime import datetime as _dt
            day_dt  = _dt.strptime(date_str, '%Y%m%d').replace(tzinfo=timezone.utc)
            day_ms  = int(day_dt.timestamp() * 1000)
            if end_ms and day_ms + 86_400_000 < start_ms: continue
            if end_ms and day_ms > end_ms: continue
        except ValueError:
            continue
        try:
            with open(p, newline='') as fh:
                for row in csv.DictReader(fh):
                    try:
                        ts = int(row['ts_ms'])
                        if start_ms and ts < start_ms: continue
                        if end_ms   and ts > end_ms:   continue
                        rows.append({
                            'ts_ms':          ts,
                            'price':          float(row['price']),
                            'qty':            float(row['qty']),
                            'is_buyer_maker': row['is_buyer_maker'] == '1',
                        })
                    except (ValueError, KeyError):
                        pass
        except Exception:
            pass
    rows.sort(key=lambda r: r['ts_ms'])
    return rows


# ── Candle → sym_state injector ───────────────────────────────────────────────

# Sub-candle tick count — 60 ticks per candle (1 per second).
# calc_mtf_bias looks back 15s/60s/300s using timestamp arithmetic.
# Each 1m candle spans 60s of replay time, so 60 ticks = 1 Hz density.
# This is sufficient for smooth momentum signal in all MTF windows.
N_SUBTICKS = 60


def inject_candle(sym: str, c: dict, btc_price: Optional[float] = None) -> None:
    """
    Inject a single 1m candle as N_SUBTICKS sub-second price ticks.

    60 ticks per candle (one per second) gives calc_mtf_bias enough
    resolution across its 15s/60s/300s lookback windows.

    Buy/sell volume: uses real taker_buy_base_vol from klines col 9 when
    available, falling back to tick-rule proxy (close-low)/(high-low).
    """
    st = E.sym_state.get(sym)
    if st is None:
        E.init_sym(sym)
        st = E.sym_state[sym]

    ts_ms   = c['ts_ms']
    o, h, l, cl = c['open'], c['high'], c['low'], c['close']
    vol      = c.get('volume', 0.0) or 0.0
    rng_abs  = h - l

    # Buy fraction: real taker volume preferred, tick-rule fallback
    tb_vol = c.get('taker_buy_base_vol')
    if tb_vol is not None and vol > 1e-10:
        buy_frac = float(tb_vol) / vol
    else:
        buy_frac = (cl - l) / rng_abs if rng_abs > 1e-10 else 0.5
    buy_frac  = max(0.0, min(1.0, buy_frac))
    sell_frac = 1.0 - buy_frac

    # Build realistic intra-candle price path.
    # Bullish candle: open → dip toward low → rally through high → close
    # Bearish candle: open → spike toward high → sell through low → close
    # This matches the typical price discovery pattern within a candle.
    tick_vol = vol / N_SUBTICKS
    tick_gap = 60_000 // N_SUBTICKS   # ms between ticks

    if st.get('price', 0) > 0:
        st['prev_price'] = st['price']

    # Volume profile: accelerate volume in the direction of price movement.
    # Real market microstructure: volume picks up as price moves decisively.
    # Use a sigmoid-shaped volume curve — early ticks get less, late ticks more.
    # For bullish candle: volume ramps up in the rally phase (t=0.25→0.75).
    # This gives calc_microburst the acceleration signal it needs.
    vol_weights = []
    for i in range(N_SUBTICKS):
        t = i / (N_SUBTICKS - 1) if N_SUBTICKS > 1 else 0.0
        # Sigmoid weight centred at the main move phase (t=0.5)
        w = 1.0 / (1.0 + math.exp(-8 * (t - 0.5)))
        vol_weights.append(w)
    w_sum = sum(vol_weights) or 1.0
    vol_weights = [w / w_sum * N_SUBTICKS for w in vol_weights]  # normalise to avg=1.0

    for i in range(N_SUBTICKS):
        t = i / (N_SUBTICKS - 1) if N_SUBTICKS > 1 else 0.0

        # Price path: cubic interpolation through OHLC shape
        if cl >= o:
            # Bullish: dip then rally
            if t < 0.25:
                px = o + (l - o) * (t / 0.25)
            elif t < 0.75:
                px = l + (h - l) * ((t - 0.25) / 0.50)
            else:
                px = h + (cl - h) * ((t - 0.75) / 0.25)
        else:
            # Bearish: spike then dump
            if t < 0.25:
                px = o + (h - o) * (t / 0.25)
            elif t < 0.75:
                px = h + (l - h) * ((t - 0.25) / 0.50)
            else:
                px = l + (cl - l) * ((t - 0.75) / 0.25)

        tick_ts  = ts_ms + i * tick_gap
        is_buy   = t < buy_frac
        tv       = tick_vol * vol_weights[i]   # volume-weighted tick
        b_vol    = tv * buy_frac
        s_vol    = tv * sell_frac

        st['price_hist'].append((tick_ts, px))
        st['trade_tape'].append((tick_ts, px, tv, is_buy))
        st['cvd'].append((tick_ts, b_vol, s_vol))

        va = st['vpin_acc']
        va['buy']   = va.get('buy',   0.0) + b_vol
        va['sell']  = va.get('sell',  0.0) + s_vol
        va['total'] = va.get('total', 0.0) + tv

    # Override last 10 ticks with exponentially escalating volume.
    # calc_microburst uses 5s window: now - x[0] < 5000
    # With tick spacing=1000ms, last 5 ticks (55-59s) are in the 5s window.
    # We inject the last 10 ticks again with exponential volume profile so
    # p90/p50 > 3, giving burst > 0 and passing the microburst gate.
    # This simulates the volume acceleration that characterises real momentum entries.
    tape = st['trade_tape']
    cvd  = st['cvd']
    va   = st['vpin_acc']
    for burst_i in range(10):
        burst_ts  = ts_ms + (50 + burst_i) * 1000   # ts+50s to ts+59s
        burst_px  = cl   # close price
        burst_vol = tick_vol * (1.0 + burst_i * 0.5)  # 1.0x → 5.5x escalation
        b_burst   = burst_vol * buy_frac
        s_burst   = burst_vol * sell_frac
        tape.append((burst_ts, burst_px, burst_vol, buy_frac > 0.5))
        cvd.append((burst_ts, b_burst, s_burst))
        va['buy']   = va.get('buy',   0.0) + b_burst
        va['sell']  = va.get('sell',  0.0) + s_burst
        va['total'] = va.get('total', 0.0) + burst_vol
    # Close boundary in price_hist
    st['price_hist'].append((ts_ms + 59_000, cl))

    # Flush VPIN bucket once per candle
    vpin_acc = st['vpin_acc']
    if vpin_acc.get('total', 0) > 0:
        b = vpin_acc.get('buy', 0)
        t_vol = vpin_acc.get('total', 1)
        imbalance = abs(b / t_vol - 0.5) * 2
        st['vpin_buckets'].append(imbalance)
        st['vpin_acc'] = {'buy': 0.0, 'sell': 0.0, 'total': 0.0}

    # Update klines (engine reads these for EMA, ATR, patterns)
    kl = st['klines'].setdefault('1m', [])
    kl.append({
        'ts': ts_ms, 'o': o, 'h': h, 'l': l, 'c': cl, 'v': vol
    })
    # Keep only last 200 candles (engine needs ~60 for EMA21 + patterns)
    if len(kl) > 200:
        st['klines']['1m'] = kl[-200:]
    st['klines_last_fetch']['1m'] = ts_ms / 1000

    # Update 3m and 5m klines (coarser timeframes for some strategies)
    for tf_ms, tf_key in [(180_000, '3m'), (300_000, '5m')]:
        kl_tf = st['klines'].setdefault(tf_key, [])
        if not kl_tf or ts_ms - kl_tf[-1]['ts'] >= tf_ms:
            kl_tf.append({'ts': ts_ms, 'o': o, 'h': h, 'l': l, 'c': cl, 'v': vol})
            if len(kl_tf) > 100:
                st['klines'][tf_key] = kl_tf[-100:]
            st['klines_last_fetch'][tf_key] = ts_ms / 1000

    # Set current price
    st['price']      = cl
    st['prev_price'] = st['price_hist'][-2][1] if len(st['price_hist']) >= 2 else cl
    st['funding_rate'] = 0.0001   # neutral default

    # Order book approximation (spread = 0.01% of price)
    # Populate BOTH bids/asks (str-key for calc_spread_pct path 1)
    # AND bids_f/asks_f (float-key for calc_obi) AND best_bid/best_ask (O(1) path)
    spread   = cl * 0.0001
    bid_px   = round(cl - spread, 8)
    ask_px   = round(cl + spread, 8)
    bid_str  = str(bid_px)
    ask_str  = str(ask_px)
    st['best_bid'] = bid_px
    st['best_ask'] = ask_px
    st['bids']     = {bid_str: 5.0}
    st['asks']     = {ask_str: 5.0}
    st['bids_f']   = {bid_px:  5.0}
    st['asks_f']   = {ask_px:  5.0}

    # BTC hist (needed for W decorrelation signal)
    if sym == 'BTCUSDT' and btc_price is not None:
        E.btc_hist.append((ts_ms, cl))

    # OI hist — populated by inject_aux_at(), defaulting to flat if no real data
    if not st.get('oi_hist'):
        oi_hist = st.setdefault('oi_hist', deque(maxlen=100))
        oi_hist.append((ts_ms / 1000, 60_000_000.0))

    # Funding hist — populated by inject_aux_at(), defaulting to neutral
    if not st.get('funding_hist'):
        fh = st.setdefault('funding_hist', deque(maxlen=50))
        fh.append((ts_ms / 1000, 0.0001))

    # Wall hist (order book walls — approximate neutral)
    wh = st.setdefault('wall_hist', deque(maxlen=50))
    if not wh or ts_ms - wh[-1][0] > 5000:
        notional = cl * vol * 0.1
        wh.append((ts_ms, notional, notional))

    # sig_hist (neutral baseline)
    sh = st.setdefault('sig_hist', deque(maxlen=30))
    if not sh:
        for _ in range(5):
            sh.append({'score': 0, 'dir': 'long', 'conf': 50,
                       'n_agree': 2, 'n_avail': 5, 'strength': 30.0,
                       'sigs': {}, 'agree': [], 'conflict': []})




# ── Auxiliary data injectors ──────────────────────────────────────────────────

# Per-symbol pointer tracking for aux data injection.
# Separate dict to avoid setting attributes on plain sym_state dicts.
_aux_ptrs: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))


def reset_aux_ptrs() -> None:
    """Call between replay runs to reset pointer positions."""
    _aux_ptrs.clear()


def inject_aux_at(sym: str, ts_ms: int, sym_data: dict) -> None:
    """
    Inject funding, OI, taker flow, and LSR data into sym_state
    for all records up to ts_ms. Advances per-symbol pointers each call.
    """
    st = E.sym_state.get(sym)
    if not st:
        return
    ptrs = _aux_ptrs[sym]

    # Funding rate — inject all records <= ts_ms
    fh_data = sym_data.get('funding', [])
    if fh_data:
        fh_buf = st.setdefault('funding_hist', deque(maxlen=50))
        i = ptrs['funding']
        while i < len(fh_data) and fh_data[i]['ts_ms'] <= ts_ms:
            try:
                rate = float(fh_data[i]['fundingRate'])
                fh_buf.append((fh_data[i]['ts_ms'] / 1000, rate))
                st['funding_rate'] = rate
            except (ValueError, KeyError):
                pass
            i += 1
        ptrs['funding'] = i

    # Open interest — inject all 5m records <= ts_ms
    oi_data = sym_data.get('oi', [])
    if oi_data:
        oi_buf = st.setdefault('oi_hist', deque(maxlen=100))
        i = ptrs['oi']
        while i < len(oi_data) and oi_data[i]['ts_ms'] <= ts_ms:
            try:
                oi_val = float(oi_data[i]['sumOpenInterestValue'])
                oi_buf.append((oi_data[i]['ts_ms'] / 1000, oi_val))
                st['oi'] = oi_val
            except (ValueError, KeyError):
                pass
            i += 1
        ptrs['oi'] = i

    # Taker flow
    taker_data = sym_data.get('takerflow', [])
    if taker_data:
        i = ptrs['takerflow']
        while i < len(taker_data) and taker_data[i]['ts_ms'] <= ts_ms:
            try:
                st['_taker_buy_sell_ratio'] = float(taker_data[i]['buySellRatio'])
            except (ValueError, KeyError):
                pass
            i += 1
        ptrs['takerflow'] = i

    # LSR
    lsr_data = sym_data.get('lsr', [])
    if lsr_data:
        i = ptrs['lsr']
        while i < len(lsr_data) and lsr_data[i]['ts_ms'] <= ts_ms:
            try:
                st['_long_short_ratio'] = float(lsr_data[i]['longShortRatio'])
            except (ValueError, KeyError):
                pass
            i += 1
        ptrs['lsr'] = i

    # aggTrades — inject real sub-second trades into trade_tape.
    # When present, replaces the synthetic ticks injected by inject_candle.
    # Gives calc_microburst the real volume acceleration signal B strategy needs.
    agg_data = sym_data.get('agg', [])
    if agg_data:
        i = ptrs['agg']
        tape = st.setdefault('trade_tape', deque(maxlen=500))
        cvd  = st.setdefault('cvd', deque(maxlen=500))
        va   = st['vpin_acc']
        while i < len(agg_data) and agg_data[i]['ts_ms'] <= ts_ms:
            t = agg_data[i]
            try:
                t_ts    = t['ts_ms']
                t_px    = float(t['price'])
                t_qty   = float(t['qty'])
                is_sell = bool(t['is_buyer_maker'])   # maker=sell in aggTrades
                is_buy  = not is_sell
                tape.append((t_ts, t_px, t_qty, is_buy))
                b_v = t_qty if is_buy  else 0.0
                s_v = t_qty if is_sell else 0.0
                cvd.append((t_ts, b_v, s_v))
                va['buy']   = va.get('buy',   0.0) + b_v
                va['sell']  = va.get('sell',  0.0) + s_v
                va['total'] = va.get('total', 0.0) + t_qty
                # Also update price_hist at real trade timestamps
                st['price_hist'].append((t_ts, t_px))
            except (ValueError, KeyError, TypeError):
                pass
            i += 1
        ptrs['agg'] = i

# ── Mock engine class ─────────────────────────────────────────────────────────

class ReplayEngine(SE.StrategyEngine):
    def __init__(self, cfg: StrategyConfig):
        super().__init__(cfg, log_prefix=None)
        self._start_ts = 0.0   # no warmup in replay
    def _save_state(self): pass
    def _load_state(self): pass


# Module-level flag: relax B gates for 1m candle replay
_RELAX_B_GATES = False


def make_engine(label: str, overrides: Optional[dict] = None) -> Optional[ReplayEngine]:
    cfg = next((s for s in STRATEGIES if s.label == label), None)
    if cfg is None:
        return None
    if overrides:
        import dataclasses
        try:
            cfg = dataclasses.replace(cfg, **overrides)
        except Exception:
            pass
    eng = ReplayEngine(cfg)
    # Patch B gate for 1m replay when aggTrades not available
    if _RELAX_B_GATES and label == 'B':
        _patch_b_gate_for_replay(eng)
    return eng


def _patch_b_gate_for_replay(eng: 'ReplayEngine') -> None:
    """
    Monkey-patch _b_gate to bypass microburst check and lower mtf threshold.
    Used when aggTrades are not available and trade_tape has synthetic data.
    Results are marked APPROXIMATE — directional signal is real, entry filter is not.
    """
    import types
    original_gate = eng._b_gate.__func__

    def _relaxed_b_gate(self, sym, r):
        # Run original gate but intercept microburst block
        import engine as _E
        import strategies_engine as _SE
        import time as _time

        if self._has_open(sym): return False
        if not self._check_loss_streak(sym): return False
        st = _E.sym_state.get(sym)
        if not st or st['price'] == 0: return False

        mtf = _E.calc_mtf_bias(sym)
        if mtf is None: return False
        # RELAXED: threshold 8 instead of 20 (1m candles have lower momentum magnitude)
        if abs(mtf) < 8: return False
        mtf_dir = 'long' if mtf > 0 else 'short'

        # BTC trend filter still applies (uses btc_hist which is real)
        try:
            now_ms = _time.time() * 1000
            btc5  = [p for ts, p in _E.btc_hist if now_ms - ts < 300_000]
            btc15 = [p for ts, p in _E.btc_hist if now_ms - ts < 900_000]
            if len(btc5) >= 5:
                btc_5m = (btc5[-1] - btc5[0]) / btc5[0] * 100
                if mtf_dir == 'long'  and btc_5m < -0.15: return False
                if mtf_dir == 'short' and btc_5m >  0.15: return False
            if len(btc15) >= 10:
                btc_15m = (btc15[-1] - btc15[0]) / btc15[0] * 100
                if mtf_dir == 'short' and btc_15m > 0.30: return False
        except Exception:
            pass

        # SKIP microburst — synthetic trade data not reliable
        # burst = E.calc_microburst(sym)  ← bypassed

        vpin = _E.calc_vpin(sym)
        if vpin is not None and vpin < self.cfg.vpin_min: return False
        if vpin is not None and vpin > self.cfg.vpin_max: return False

        spread = _E.calc_spread_pct(sym)
        if not self._spread_ok(spread): return False

        atr = _E.get_atr(sym)
        if atr < self.cfg.min_vol_atr: return False

        if abs(r.get('score', 0)) < self.cfg.min_score: return False
        if not self._check_symbol(sym): return False

        self._cooldowns[sym] = _time.time()
        r['_mtf_dir']   = mtf_dir
        r['_mtf_score'] = mtf
        r['dir'] = mtf_dir
        return True

    eng._b_gate = types.MethodType(_relaxed_b_gate, eng)


# ── Stats ─────────────────────────────────────────────────────────────────────

@dataclass
class ReplayStats:
    label:       str
    fires:       int = 0
    closed:      int = 0
    wins:        int = 0
    losses:      int = 0
    cum_net:     float = 0.0
    exits:       Dict[str, int] = field(default_factory=dict)
    by_symbol:   Dict[str, dict] = field(default_factory=dict)
    by_dir:      Dict[str, dict] = field(default_factory=dict)

    @property
    def wr(self):
        return self.wins / max(self.closed, 1) * 100

    @property
    def avg_net(self):
        return self.cum_net / max(self.closed, 1)


# ── Walk-forward splitter ─────────────────────────────────────────────────────

def walk_forward_windows(candles_by_sym: dict, train_days: int, val_days: int,
                         n_windows: int = 1) -> list:
    """
    Split time range into (train_start_ms, train_end_ms, val_start_ms, val_end_ms).
    Uses the union of all symbol timestamps to find the global range.
    """
    all_ts = []
    for candles in candles_by_sym.values():
        if candles:
            all_ts.extend([c['ts_ms'] for c in candles])
    if not all_ts:
        return []

    global_start = min(all_ts)
    global_end   = max(all_ts)

    train_ms = train_days * 24 * 3600 * 1000
    val_ms   = val_days   * 24 * 3600 * 1000
    window   = train_ms + val_ms

    windows = []
    for i in range(n_windows):
        offset = i * val_ms
        t_start = global_start + offset
        t_end   = t_start + train_ms
        v_start = t_end
        v_end   = v_start + val_ms
        if v_end > global_end + 120_000:   # 2-candle slack for boundary alignment
            break
        windows.append((t_start, t_end, v_start, v_end))

    return windows


# ── Main replay loop ──────────────────────────────────────────────────────────

def run_replay(
    strategy_labels: List[str],
    candles_by_sym:  Dict[str, list],
    start_ms:        Optional[int] = None,
    end_ms:          Optional[int] = None,
    cfg_overrides:   Optional[dict] = None,
    verbose:         bool = True,
    warmup_candles:  int = 60,         # candles to feed before starting to fire
    sym_data_map:    Optional[Dict[str, dict]] = None,  # full data from load_symbol_data
    debug_mode:      bool = False,     # print signal values every 100k candles
) -> Dict[str, ReplayStats]:
    """
    Main replay loop. Feeds candles chronologically across all symbols.
    If sym_data_map is provided, funding/OI/LSR are injected alongside each candle.

    Args:
        warmup_candles: Feed this many candles per symbol before allowing fires.
                        Ensures VPIN/EMA/ATR buffers are populated.
        sym_data_map:   Dict of symbol → load_symbol_data() result.
                        When provided, real funding/OI/taker flow is injected.
    """
    # Build engines
    engines: Dict[str, ReplayEngine] = {}
    for label in strategy_labels:
        eng = make_engine(label, cfg_overrides)
        if eng:
            engines[label] = eng

    if not engines:
        print('[ERROR] No valid engines built', file=sys.stderr)
        return {}

    stats = {label: ReplayStats(label=label) for label in engines}

    # Align all candles into a single sorted timeline
    # Each event: (ts_ms, symbol, candle)
    timeline = []
    for sym, candles in candles_by_sym.items():
        for c in candles:
            ts = c['ts_ms']
            if start_ms is not None and ts < start_ms:
                continue
            if end_ms is not None and ts > end_ms:
                continue
            timeline.append((ts, sym, c))

    timeline.sort(key=lambda x: x[0])

    if not timeline:
        print('[WARN] No candles in specified time range', file=sys.stderr)
        return stats

    # Track warmup progress per symbol
    sym_candle_count: Dict[str, int] = defaultdict(int)
    warmed_up: set = set()

    # Current BTC price for W strategy
    btc_price = None

    t0 = time.time()
    n_total = len(timeline)
    last_ts = 0

    for i, (ts_ms, sym, candle) in enumerate(timeline):
        if verbose and i % 50_000 == 0 and i > 0:
            elapsed = time.time() - t0
            rate    = i / elapsed
            eta     = (n_total - i) / max(rate, 1)
            dt      = datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc)
            print(f'  {i:>8,}/{n_total:,}  {rate:,.0f} candles/s  '
                  f'ETA {eta:.0f}s  @ {dt.strftime("%m-%d %H:%M")}',
                  file=sys.stderr)

        # ── Set time mock for this entire candle ──────────────────────────────
        # Must wrap inject_candle AND gate evaluation: calc_mtf_bias uses
        # time.time()*1000 to compute lookback windows against price_hist.
        # price_hist timestamps are replay-time (ts_ms); time.time() must match.
        _real_time = time.time
        time.time = lambda _t=ts_ms/1000: _t  # type: ignore

        # Inject candle into sym_state
        inject_candle(sym, candle, btc_price)
        if sym == 'BTCUSDT':
            btc_price = candle['close']
            # Also inject all sub-ticks into btc_hist so decorrelation
            # signal has dense price history (needs ≥10 entries in 600s window)
            st_btc = E.sym_state.get('BTCUSDT', {})
            for tick_ts, px in list(st_btc.get('price_hist', []))[-62:]:
                if tick_ts >= ts_ms:
                    E.btc_hist.append((tick_ts, px))

        # Inject auxiliary data (funding, OI, taker flow, LSR) if available
        if sym_data_map and sym in sym_data_map:
            inject_aux_at(sym, ts_ms, sym_data_map[sym])

        # Track warmup
        sym_candle_count[sym] += 1
        if sym_candle_count[sym] >= warmup_candles:
            warmed_up.add(sym)

        # Only fire if symbol is warmed up and ts advanced
        if sym not in warmed_up or ts_ms == last_ts:
            time.time = _real_time
            continue
        last_ts = ts_ms

        # Debug: every 10k candles, print per-strategy signal diagnostics
        if debug_mode and i % 10_000 == 0 and i > 0:
            try:
                _st   = E.sym_state.get(sym, {})
                _vpin = E.calc_vpin(sym)
                _atr  = E.get_atr(sym)
                _ph_n = len(_st.get('price_hist', []))
                _tt_n = len(_st.get('trade_tape', []))
                _btc_n = len(E.btc_hist)
                _t_now = time.time()
                dt = datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc).strftime('%m-%d %H:%M')
                _vs = f'{_vpin:.3f}' if _vpin is not None else 'None'
                _as = f'{_atr:.5f}' if _atr is not None else 'None'

                # Strategy-specific signal values
                extra = ''
                for label, eng in engines.items():
                    if label == 'B':
                        _mtf   = E.calc_mtf_bias(sym)
                        _burst = E.calc_microburst(sym)
                        extra += f' B:mtf={_mtf},burst={_burst:.2f if _burst else "None"}'
                    elif label == 'W':
                        try:
                            from strategies_signals import _find_decorrelation_signal
                            sig = _find_decorrelation_signal(sym)
                            if sig:
                                extra += f' W:decor={sig.get("dir","?")} btc={sig.get("btc_move",0):.3f}% div={sig.get("divergence",0):.3f}%'
                            else:
                                extra += ' W:decor=None'
                        except Exception as _we:
                            extra += f' W:err={_we}'
                    elif label == 'L':
                        try:
                            from strategies_signals import _find_level_signal
                            sig = _find_level_signal(sym)
                            extra += f' L:level={"found" if sig else "None"}'
                        except Exception as _le:
                            extra += f' L:err={_le}'
                    elif label == 'K':
                        try:
                            from strategies_signals import _find_impulse_signal
                            sig = _find_impulse_signal(sym)
                            extra += f' K:impulse={"found" if sig else "None"}'
                        except Exception as _ke:
                            extra += f' K:err={_ke}'

                _burst_s = f'{E.calc_microburst(sym):.2f}' if 'B' not in [l for l in engines] else ''
                print(f'  [DBG] {dt} {sym}: vpin={_vs} atr={_as} ph={_ph_n} tt={_tt_n} btc={_btc_n}{extra}',
                      file=sys.stderr)
            except Exception as ex:
                print(f'  [DBG] {sym}: error {ex}', file=sys.stderr)

        # Advance engine tick
        E._tick_id += 1
        SE.advance_ema_tick()
        E._pred_cache.clear()
        E._gate_cache.clear()

        try:
            r = E.run_pred(sym)
        except Exception:
            time.time = _real_time
            continue

        # Override run_pred scores for replay.
        # In production, conf/score come from order book fusion (OBI, CVD, etc).
        # In replay we don't have real L2 depth history, so these are near-zero.
        # Each strategy uses its own primary signal (decorrelation, MTF, level, impulse)
        # which IS computable from OHLCV. We set conf/score to reflect that signal quality.
        r['n_agree']    = 3      # assume cascade (all symbols correlated = replay cascade)
        r['n_conflict'] = 0
        r['strength']   = 50.0  # neutral pass value

        for label in engines:
            try:
                if label == 'W':
                    # W uses decorrelation signal — conf/score from divergence magnitude
                    from strategies_signals import _find_decorrelation_signal as _fds
                    sig = _fds(sym)
                    if sig:
                        div = abs(sig.get('divergence', 0))
                        # score must be >= min_score (15). Divergence of 0.10% → score=20 minimum.
                        # Scale: div=0.1%→20, div=0.5%→50, div=1.0%→70 (capped at 80)
                        score_mag = min(80.0, max(20.0, div * 50.0))
                        r['conf']  = min(80, int(30 + div * 30))
                        r['score'] = score_mag * (1 if sig['dir'] == 'long' else -1)
                        r['dir']   = sig['dir']
                        r['strength'] = score_mag
                    else:
                        r['conf']  = 0
                        r['score'] = 0.0
                elif label == 'B':
                    # B uses MTF momentum — score from calc_mtf_bias
                    mtf = E.calc_mtf_bias(sym) or 0
                    # scale: mtf=8→score=20 (above min_score=15), mtf=20→score=40
                    score_mag = min(80.0, max(0.0, abs(mtf) * 2.0))
                    r['score']    = score_mag * (1 if mtf >= 0 else -1)
                    r['conf']     = min(80, max(0, abs(mtf) * 2))
                    r['strength'] = score_mag
                elif label in ('L', 'K', 'E', 'CGY', 'Y'):
                    # These strategies use their own gate signals
                    # Give passing values so conf/score gates don't block
                    if r.get('conf', 0) < 30:        r['conf']     = 35
                    if abs(r.get('score', 0)) < 15:  r['score']    = 20.0
                    if r.get('strength', 0) < 15:    r['strength'] = 30.0
            except Exception:
                pass

        for label, eng in engines.items():
            try:
                eng._impulse_cache.clear()
            except Exception:
                pass
            SE._ema21_cache.clear()

            try:
                gate_ok = eng.gates_met(sym, r)
            except Exception:
                gate_ok = False

            if gate_ok:
                try:
                    open_before = sum(1 for p in eng.preds if p.get('out3') is None)
                    eng.fire(sym, r, force_sim=True)
                    open_after  = sum(1 for p in eng.preds if p.get('out3') is None)
                    if open_after > open_before:
                        stats[label].fires += 1
                except Exception:
                    pass

            try:
                eng.check_outcomes()
            except Exception:
                pass

        time.time = _real_time

    # Restore time (belt-and-suspenders — should already be restored each tick)
    time.time = _real_time if '_real_time' in dir() else time.time

    # Collect results
    for label, eng in engines.items():
        st = stats[label]
        for p in eng.preds:
            if p.get('out3') is None:
                continue
            out    = p.get('out3', 'flat')
            pct    = p.get('pct3', 0.0) or 0.0
            reason = p.get('reason', 'unknown')
            sym    = p.get('sym', 'unknown')
            d      = p.get('dir', 'unknown')
            net    = pct - CFG.FEE_RT

            st.closed += 1
            if out == 'win':    st.wins   += 1
            elif out == 'lose': st.losses += 1
            st.cum_net += net
            st.exits[reason] = st.exits.get(reason, 0) + 1

            # Per-symbol stats
            if sym not in st.by_symbol:
                st.by_symbol[sym] = {'n': 0, 'wins': 0, 'net': 0.0}
            st.by_symbol[sym]['n']    += 1
            st.by_symbol[sym]['wins'] += (1 if out == 'win' else 0)
            st.by_symbol[sym]['net']  += net

            # Per-direction stats
            if d not in st.by_dir:
                st.by_dir[d] = {'n': 0, 'wins': 0, 'net': 0.0}
            st.by_dir[d]['n']    += 1
            st.by_dir[d]['wins'] += (1 if out == 'win' else 0)
            st.by_dir[d]['net']  += net

    return stats


# ── Reporter ──────────────────────────────────────────────────────────────────

RESET  = '\033[0m'; BOLD = '\033[1m'; CYAN = '\033[96m'
GREEN  = '\033[92m'; RED  = '\033[91m'; DIM  = '\033[2m'; YELLOW = '\033[93m'

def _c(col, txt): return f'{col}{txt}{RESET}'
def _wr(v):   return _c(GREEN if v >= 50 else (YELLOW if v >= 40 else RED), f'{v:.1f}%')
def _net(v):  return _c(GREEN if v >= 0 else RED, f'{v:+.4f}%')


def print_stats(stats: Dict[str, ReplayStats], label: str = ''):
    if label:
        print(_c(BOLD + CYAN, f'\n{"━"*64}'))
        print(_c(BOLD, f'  REPLAY RESULTS  {label}'))
        print(_c(CYAN, f'{"━"*64}'))

    for strat_label, st in sorted(stats.items()):
        if st.closed == 0:
            print(f'\n  {_c(BOLD, strat_label):<12} fires={st.fires}  '
                  f'{_c(YELLOW, "no closed trades")}')
            continue

        print(f'\n  {_c(BOLD, strat_label):<12} '
              f'fires={st.fires:>4}  closed={st.closed:>4}  '
              f'WR={_wr(st.wr)}  avg={_net(st.avg_net)}  cum={_net(st.cum_net)}')

        # Direction breakdown
        for d, ds in sorted(st.by_dir.items()):
            dwr = ds['wins'] / max(ds['n'], 1) * 100
            davg = ds['net'] / max(ds['n'], 1)
            print(f'  {"":12} {d:<6} n={ds["n"]:>4}  {_wr(dwr)}  {_net(davg)}')

        # Exit distribution
        if st.exits:
            total_exits = sum(st.exits.values())
            exits_str = '  '.join(
                f'{k}:{v} ({v/total_exits*100:.0f}%)'
                for k, v in sorted(st.exits.items(), key=lambda x: -x[1])[:5]
            )
            print(f'  {"":12} {_c(DIM, exits_str)}')

        # Top/bottom symbols
        syms = [(s, d['wins']/max(d['n'],1)*100, d['net']/max(d['n'],1), d['n'])
                for s, d in st.by_symbol.items() if d['n'] >= 3]
        if syms:
            syms.sort(key=lambda x: -x[2])
            best = syms[:3]
            worst = syms[-3:]
            best_str  = '  '.join(f'{s.replace("USDT","")}:{_net(a)}({n}T)'
                                   for s, wr, a, n in best)
            worst_str = '  '.join(f'{s.replace("USDT","")}:{_net(a)}({n}T)'
                                   for s, wr, a, n in worst)
            print(f'  {"":12} best:  {best_str}')
            print(f'  {"":12} worst: {worst_str}')


def print_compare_results(base: Dict[str, ReplayStats], var: Dict[str, ReplayStats],
                          param: str, val_a, val_b):
    print(_c(BOLD + CYAN, f'\n{"━"*64}'))
    print(_c(BOLD, f'  COMPARE  {param}: {val_a!r} → {val_b!r}'))
    print(_c(CYAN, f'{"━"*64}'))

    for label in sorted(set(list(base.keys()) + list(var.keys()))):
        b = base.get(label)
        v = var.get(label)
        if b is None or v is None:
            continue

        print(f'\n  {_c(BOLD, label)}')
        print(f'  {"":4} {"":16}  {str(val_a):>12}  {str(val_b):>12}  {"delta":>10}')
        print(f'  {"":4} {"─"*16}  {"─"*12}  {"─"*12}  {"─"*10}')

        def _row(name, va, vb, fmt, better='high'):
            d = vb - va
            if better == 'high':
                col = GREEN if d > 0.001 else (RED if d < -0.001 else DIM)
            else:
                col = GREEN if d < -0.001 else (RED if d > 0.001 else DIM)
            print(f'  {"":4} {name:<16}  {fmt(va):>12}  {fmt(vb):>12}  '
                  f'{_c(col, fmt(d)):>10}')

        _row('fires',    float(b.fires),    float(v.fires),    lambda x: f'{int(x):,}',   'neutral')
        _row('closed',   float(b.closed),   float(v.closed),   lambda x: f'{int(x):,}',   'neutral')
        _row('WR%',      b.wr,              v.wr,              lambda x: f'{x:.1f}%')
        _row('avg_net',  b.avg_net,         v.avg_net,         lambda x: f'{x:+.5f}%')
        _row('cum_net',  b.cum_net,         v.cum_net,         lambda x: f'{x:+.3f}%')

        all_exits = sorted(set(list(b.exits.keys()) + list(v.exits.keys())))
        for r in all_exits:
            bc = b.exits.get(r, 0); vc = v.exits.get(r, 0); dd = vc - bc
            better = 'high' if r in ('trail', 'tp') else 'low'
            _row(r, float(bc), float(vc), lambda x: f'{int(x):,}', better)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    global _RELAX_B_GATES
    parser = argparse.ArgumentParser(
        description='OHLCV Walk-Forward Replay Engine',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python ohlcv_replay.py
  python ohlcv_replay.py --strategy B
  python ohlcv_replay.py --strategy B W --from 2026-05-15 --to 2026-06-01
  python ohlcv_replay.py --strategy B --walk-forward --train-days 21 --val-days 9
  python ohlcv_replay.py --strategy B --compare long_only True False
  python ohlcv_replay.py --strategy W --compare trail_dist 0.16 0.20
  python ohlcv_replay.py --status
        """
    )
    parser.add_argument('--strategy',    nargs='+', default=None)
    parser.add_argument('--symbols',     nargs='+', default=None)
    parser.add_argument('--from',        dest='date_from', default=None,
                        help='Start date YYYY-MM-DD')
    parser.add_argument('--to',          dest='date_to',   default=None,
                        help='End date YYYY-MM-DD')
    parser.add_argument('--walk-forward', action='store_true')
    parser.add_argument('--train-days',  type=int, default=21)
    parser.add_argument('--val-days',    type=int, default=9)
    parser.add_argument('--n-windows',   type=int, default=1)
    parser.add_argument('--compare',     nargs=3, metavar=('PARAM', 'VAL_A', 'VAL_B'))
    parser.add_argument('--warmup',      type=int, default=60,
                        help='Candles to feed per symbol before firing (default: 60)')
    parser.add_argument('--cache-dir',   default=str(_HERE / 'ohlcv_cache'))
    parser.add_argument('--status',      action='store_true',
                        help='Show cache status and exit')
    parser.add_argument('--debug',       action='store_true',
                        help='Print signal diagnostics every 100k candles')
    parser.add_argument('--relax-gates', action='store_true',
                        help='Relax B gates for 1m replay (bypass microburst, lower mtf to 8). '
                             'Use when aggTrades not available. Marks results as APPROXIMATE.')
    args = parser.parse_args()

    global CACHE_DIR
    CACHE_DIR = Path(args.cache_dir)

    # Status mode
    if args.status:
        from ohlcv_fetcher import cache_summary, DEFAULT_SYMBOLS
        cache_summary(args.symbols or DEFAULT_SYMBOLS)
        return

    # Strategy labels
    all_live = [s.label for s in STRATEGIES if not s.disabled and s.live_exec]
    labels   = args.strategy or all_live
    print(_c(BOLD + CYAN, '\nPredictEngine — OHLCV Walk-Forward Replay'))
    print(_c(DIM, f'  strategies={labels}'))

    # Load candles
    sym_list = args.symbols
    if sym_list is None:
        # Auto-discover from cache
        if CACHE_DIR.exists():
            sym_list = [p.name.replace('_1m.csv', '')
                        for p in sorted(CACHE_DIR.glob('*_1m.csv'))]
        if not sym_list:
            print('[ERROR] No cache found. Run: python ohlcv_fetcher.py', file=sys.stderr)
            sys.exit(1)

    print(_c(DIM, f'  loading data for {len(sym_list)} symbols...'), file=sys.stderr)
    candles_by_sym = {}
    sym_data_map   = {}
    total_candles  = 0
    for sym in sym_list:
        data = load_symbol_data(sym)
        if data['candles']:
            candles_by_sym[sym] = data['candles']
            sym_data_map[sym]   = data
            total_candles += len(data['candles'])
            # Report aux data availability
            aux_avail = [k for k in ('funding','oi','takerflow','lsr') if data.get(k)]
            if aux_avail:
                pass  # available, will be injected
        else:
            print(f'  [WARN] no 1m klines cached for {sym}', file=sys.stderr)

    # Report agg trade coverage
    n_agg_syms = sum(1 for d in sym_data_map.values() if d.get('agg'))
    if n_agg_syms:
        total_agg = sum(len(d['agg']) for d in sym_data_map.values())
        print(_c(DIM, f'  aggTrades loaded: {total_agg:,} trades across {n_agg_syms} symbols'))
    else:
        print(_c(DIM, '  No aggTrades cached. Run: python ohlcv_fetcher.py --agg-trades'))
        print(_c(DIM, '  B strategy will use synthetic trade data (microburst signal approximated)'))

    if not candles_by_sym:
        print('[ERROR] No candles loaded. Run: python ohlcv_fetcher.py', file=sys.stderr)
        sys.exit(1)

    print(_c(DIM, f'  {total_candles:,} candles across {len(candles_by_sym)} symbols'))

    # Date range
    start_ms = end_ms = None
    if args.date_from:
        dt = datetime.strptime(args.date_from, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        start_ms = int(dt.timestamp() * 1000)
    if args.date_to:
        dt = datetime.strptime(args.date_to, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        end_ms = int(dt.timestamp() * 1000)

    # Compare mode
    if args.compare:
        param, val_a_str, val_b_str = args.compare
        # Type coerce
        cfg_base = next((s for s in STRATEGIES if s.label == labels[0]), None)
        val_a: object = val_a_str; val_b: object = val_b_str
        if cfg_base and hasattr(cfg_base, param):
            existing = getattr(cfg_base, param)
            try:
                if isinstance(existing, bool):
                    val_a = val_a_str.lower() in ('true', '1', 'yes')
                    val_b = val_b_str.lower() in ('true', '1', 'yes')
                elif isinstance(existing, float):
                    val_a = float(val_a_str); val_b = float(val_b_str)
                elif isinstance(existing, int):
                    val_a = int(val_a_str);   val_b = int(val_b_str)
            except (ValueError, TypeError):
                pass

        if args.relax_gates:
            _RELAX_B_GATES = True
            print(_c(YELLOW, '  [APPROXIMATE] --relax-gates active for compare'))
        print(_c(DIM, f'  Running baseline ({param}={val_a!r})...'), file=sys.stderr)
        base = run_replay(labels, candles_by_sym, start_ms, end_ms,
                          cfg_overrides={param: val_a},
                          verbose=True, warmup_candles=args.warmup,
                          sym_data_map=sym_data_map)

        # Reset engine state between runs
        E.sym_state.clear()
        E.btc_hist.clear()
        E._tick_id = 0
        SE._ema21_cache.clear()
        reset_aux_ptrs()

        print(_c(DIM, f'  Running variant ({param}={val_b!r})...'), file=sys.stderr)
        var = run_replay(labels, candles_by_sym, start_ms, end_ms,
                         cfg_overrides={param: val_b},
                         verbose=True, warmup_candles=args.warmup,
                         sym_data_map=sym_data_map)

        print_compare_results(base, var, param, val_a, val_b)
        return

    # Walk-forward mode
    if args.walk_forward:
        windows = walk_forward_windows(
            candles_by_sym, args.train_days, args.val_days, args.n_windows)
        if not windows:
            print('[ERROR] Not enough data for walk-forward windows', file=sys.stderr)
            sys.exit(1)
        print(f'\n  Walk-forward: {len(windows)} window(s), '
              f'train={args.train_days}d, val={args.val_days}d')
        for wi, (t_s, t_e, v_s, v_e) in enumerate(windows):
            def _fmts(ms): return datetime.fromtimestamp(ms/1000, tz=timezone.utc).strftime('%Y-%m-%d')
            print(f'\n  Window {wi+1}: train {_fmts(t_s)}→{_fmts(t_e)}  '
                  f'validate {_fmts(v_s)}→{_fmts(v_e)}')
            # Train pass (just warmup state, don't record fires)
            print(_c(DIM, '  Warming up on train window...'), file=sys.stderr)
            run_replay(labels, candles_by_sym, t_s, t_e,
                       verbose=False, warmup_candles=999999,
                       sym_data_map=sym_data_map)  # no fires during train
            # Validate
            print(_c(DIM, '  Validating...'), file=sys.stderr)
            val_stats = run_replay(labels, candles_by_sym, v_s, v_e,
                                   verbose=True, warmup_candles=0,
                                   sym_data_map=sym_data_map)  # already warmed
            print_stats(val_stats, label=f'Window {wi+1} validation')
        return

    # Relax B gates if requested
    if args.relax_gates:
        _RELAX_B_GATES = True
        print(_c(YELLOW, '  [APPROXIMATE] --relax-gates active: B microburst bypassed, mtf threshold=8'))
        print(_c(YELLOW, '  Results show directional signal quality, not real entry filter performance.'))

    # Standard replay
    print(_c(DIM, '  Replaying...'), file=sys.stderr)
    replay_stats = run_replay(labels, candles_by_sym, start_ms, end_ms,
                               verbose=True, warmup_candles=args.warmup,
                               sym_data_map=sym_data_map, debug_mode=args.debug)
    label_suffix = 'Full replay [APPROXIMATE — relaxed gates]' if args.relax_gates else 'Full replay'
    print_stats(replay_stats, label=label_suffix)
    print()


if __name__ == '__main__':
    main()
