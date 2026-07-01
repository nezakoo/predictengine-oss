"""
PredictEngine - engine.py | v13  (Binance USDT-M Futures)
Core: shared state, data handlers, prediction/gate/exit logic, async I/O.

Signals are in signals.py - import from there to tune them in isolation.

Exchange migration Bybit → Binance:
  WebSocket  : wss://fstream.binance.com/stream  (combined streams)
  REST ticker: GET /fapi/v1/ticker/price + /fapi/v1/openInterest
  REST book  : GET /fapi/v1/depth
  REST liq   : GET /fapi/v1/forceOrders  (Binance liquidation endpoint)

  Binance stream names (lowercase symbol):
    <sym>@depth20@100ms   → order book (top 20 levels, 100ms updates)
    <sym>@aggTrade        → aggregated trades
    <sym>@markPrice@1s    → mark price + funding (carries lastPrice)
    <sym>@forceOrder      → liquidation orders (real-time)

  Binance message schemas differ from Bybit — all handlers rewritten.
  Everything above ws_task / rest_task / liq_poll_task is UNCHANGED.

Thread safety: all deque reads use list() snapshot before iteration.
"""

import asyncio, json, time, csv
from collections import deque
from datetime import datetime
import os, sys, traceback, requests as _requests
# WebSocket connections use aiohttp (already a dependency for REST calls).
# websockets library had persistent Python 3.14 incompatibilities in its
# legacy protocol layer; aiohttp.ClientSession.ws_connect is stable on 3.14.
_WS_LEGACY = False   # kept for any external code that checks this flag

class _AiohttpWsConnect:
    """
    Drop-in async context manager replacing websockets.connect / ws_connect.
    Uses aiohttp under the hood — no websockets library needed at all.
    Accepts the same kwargs that callers historically passed (ping_interval,
    ping_timeout, open_timeout, max_size) and silently maps them to aiohttp.
    """
    def __init__(self, url, *, open_timeout=15, ping_interval=None,
                 ping_timeout=None, max_size=2**22, **_kw):
        self._url      = url
        self._timeout  = open_timeout
        self._max_size = max_size
        self._session  = None
        self._ws       = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        self._ws = await self._session.ws_connect(
            self._url,
            heartbeat=None,
            max_msg_size=self._max_size,
            timeout=aiohttp.ClientWSTimeout(ws_close=5.0),
        )
        return self._ws

    async def __aexit__(self, *_):
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()

def ws_connect(url, **kwargs):
    """Module-level ws_connect used by engine_lag (E.ws_connect)."""
    return _AiohttpWsConnect(url, **kwargs)

# ── WS exception helpers ───────────────────────────────────────────
# Python 3.13/3.14 + websockets ≥14 raises ConnectionClosedError with
# "keepalive ping timeout; no close frame received" even when
# ping_interval=None is set, due to internal library behaviour changes.
# Treat these as silent reconnects — they are NOT crashes.
_WS_BENIGN_MSGS = (
    'keepalive ping timeout',
    'no close frame received',
    'keepalive_ping',
    'assert waiter is None',
    'ConnectionClosedError',
)

def _is_benign_ws_error(exc: BaseException) -> bool:
    """Return True for noisy-but-harmless WS keepalive errors."""
    msg = str(exc)
    return any(p in msg for p in _WS_BENIGN_MSGS)

import aiohttp

from config import (
    WS_URL, API_URL, LIQUID_WHITELIST,
    LOOP_MS, PRED_COOLDOWN, FEE_RT,
    MIN_CONF, MIN_SCORE, MIN_VOL_ATR, TRADE_MIN, OBI_THR,
    W_OBI, W_CVD, W_LIQ, W_ABS, W_SPOOF,
    WIN_THR, ATR_TP_MULT, ATR_SL_MULT, TRAIL_DIST,
    SIG_HOLD_SCORE, MIN_HOLD_ANY, REVERSAL_SCORE,
    REV_MIN_HOLD, MAX_WINDOW,
    INERTIA_SEC, INERTIA_THR,
    VPIN_BUCKET_VOL, VPIN_MIN, VPIN_HIGH,
    KYLE_LAM_GATE,
    SPREAD_MAX_PCT, ACCEL_MIN,
    VERSION, LOG_FILE,
    SCANNER_TOP_N, SCANNER_MIN_NATR, SCANNER_MIN_VOL_USD,
)
from engine_logger import log_ws_event, log_scanner_change

# -- Signal accuracy tracking (adaptive weights) -------------------
signal_stats = {
    'obi':   {'win': 1, 'lose': 1},
    'cvd':   {'win': 1, 'lose': 1},
    'liq':   {'win': 1, 'lose': 1},
    'abs':   {'win': 1, 'lose': 1},
    'spoof': {'win': 1, 'lose': 1},
}

# ══ SHARED STATE ══════════════════════════════════════════════════
sym_state    = {}
btc_hist     = deque(maxlen=1000)
preds        = deque(maxlen=200)
hist_win     = 0
hist_lose    = 0
hist_total   = 0
running      = True
ws_status    = 'connecting…'
ACTIVE_COINS = []

# -- Wire signals module to our shared state -----------------------
import core_signals as _sig

# ══ TELEGRAM CRASH NOTIFICATIONS ══════════════════════════════════
def _tg_send(text: str):
    """Fire-and-forget Telegram message (blocking, called on crash)."""
    token   = os.getenv("TG_BOT_TOKEN", "")
    chat_id = os.getenv("TG_CHAT_ID", "")
    if not token or not chat_id:
        return
    prefix = os.getenv("TELEGRAM_PREFIX", "")
    if prefix:
        text = f"{prefix} {text}"
    try:
        _requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text,
                  "parse_mode": "HTML", "disable_notification": False},
            timeout=10,
        )
    except Exception:
        pass  # never let the notifier itself crash the process


def _tg_excepthook(exc_type, exc_value, exc_tb):
    """Catches any unhandled Python exception → sends traceback to Telegram."""
    tb_str = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
    _tg_send(
        f"🔴 <b>ENGINE CRASH</b>\n"
        f"<pre>{tb_str[-3500:]}</pre>"
    )
    sys.__excepthook__(exc_type, exc_value, exc_tb)  # still log to stderr/journald

sys.excepthook = _tg_excepthook


def _tg_async_exception_handler(loop, context):
    """Catches unhandled asyncio task exceptions → sends to Telegram."""
    msg = context.get("message", "async exception")
    exc = context.get("exception")
    if exc:
        tb_str = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        _tg_send(
            f"⚠️ <b>Async Exception</b>\n"
            f"{msg}\n"
            f"<pre>{tb_str[-2500:]}</pre>"
        )
    loop.default_exception_handler(context)
# ══════════════════════════════════════════════════════════════════
_sig.sym_state = sym_state
_sig.btc_hist  = btc_hist

# Re-export signal functions so callers can do `import engine as E; E.calc_vpin()`
from core_signals import (
    get_atr,
    detect_regime,
    calc_obi,
    calc_spoofing,
    calc_cvd,
    calc_cvd_divergence,
    calc_liq,
    calc_absorption,
    calc_vpin,
    calc_kyle_lambda,
    calc_depth_imbalance,
    calc_large_trade_ratio,
    calc_vwap_deviation,
    calc_oi_velocity,
    calc_funding_momentum,
    calc_liq_pressure,
    calc_spread_pct,
    calc_trade_accel,
    calc_microburst,
    calc_mtf_bias,
    calc_btc_lead,
    prediction_quality,
)


# ══ STATE INIT ════════════════════════════════════════════════════
def init_sym(sym):
    if sym not in sym_state:
        sym_state[sym] = dict(
            price=0.0, prev_price=0.0,
            price_hist=deque(maxlen=600),
            bids={}, asks={},
            cvd=deque(maxlen=6000),
            liqs=deque(maxlen=1000),
            seen_liq=set(),
            oi=0.0, last_pred_ts=0.0,
            sig_hist=deque(maxlen=20),
            trade_tape=deque(maxlen=500),
            vpin_acc={'buy': 0.0, 'sell': 0.0, 'total': 0.0},
            vpin_buckets=deque(maxlen=50),
            regime='neutral',
            regime_conf=0.0,
            book_history=deque(maxlen=100),
            # klines[tf] = list of {ts,o,h,l,c,v} dicts — fetched by rest_task
            # tf keys: '1m', '15m', '1d'
            klines={},
            klines_last_fetch={},
            # Q: Funding Rate Fade — rate from @markPrice@1s field 'r'
            funding_rate=0.0,
            funding_hist=deque(maxlen=50),
            # R: Liquidation Cascade — longer retention for 60s windows
            liq_cascade_hist=deque(maxlen=5000),
            # S: OI Divergence — (ts, oi_usd) polled by rest_task
            oi_hist=deque(maxlen=200),
            # M: wall_hist rolling 10s window of (ts_ms, bid_wall_usd, ask_wall_usd)
            # recorded by on_book(); used by _wall_stable() in strategies.py
            wall_hist=deque(maxlen=200),
        )


# ══ FORMAT ════════════════════════════════════════════════════════
def fp(p):
    if   p >= 10000: return f'{p:,.1f}'
    elif p >= 1000:  return f'{p:,.2f}'
    elif p >= 1:     return f'{p:.4f}'
    else:            return f'{p:.8f}'


# ══ ADAPTIVE WEIGHT ═══════════════════════════════════════════════
def adaptive_weight(sig):
    s  = signal_stats[sig]
    wr = s['win'] / (s['win'] + s['lose'])
    return 0.5 + wr

# ══ SIGNAL FUSION ═════════════════════════════════════════════════
# Cache: store last run_pred result per symbol so terminal draw() and
# pred_loop() don't recompute it twice per 100ms tick.
_pred_cache: dict = {}   # sym → (tick_id, result)
_tick_id    = 0          # incremented once per pred_loop iteration

# ── Event-driven fast-path handlers ───────────────────────────────
# Registered by strategies_engine after import. Called directly from
# on_ticker() and _update_lag_price() — bypasses the 100ms pred_loop
# for latency-sensitive strategies (Z, G).
#
# _z_fast_handler(sym) → called on every Binance price tick AND on
#   every lag exchange price update. Checks Z gate inline, sub-1ms
#   from message receipt to order submission.
#
# _check_outcomes_handler() → called on every Binance price tick so
#   exits (trail, SL, TP) react within 1 message latency (~5ms)
#   instead of waiting up to 100ms for the next pred_loop tick.
_z_fast_handler:        object = None
_check_outcomes_handler: object = None
_cg_fast_handler:       object = None   # C+G fast-path


def register_z_handler(fn):
    """Called by strategies_engine to register Z's fast-path gate."""
    global _z_fast_handler
    _z_fast_handler = fn


def register_check_outcomes_handler(fn):
    """Called by strategies_engine to register the fast-path exit checker."""
    global _check_outcomes_handler
    _check_outcomes_handler = fn


def register_cg_handler(fn):
    """Called by strategies_engine to register C+G fast-path gate."""
    global _cg_fast_handler
    _cg_fast_handler = fn


def run_pred(sym):
    """Score all available signals and fuse into a single directional score.
    Returns cached result if called a second time within the same tick."""
    cached = _pred_cache.get(sym)
    if cached is not None and cached[0] == _tick_id:
        return cached[1]
    sigs = {
        'obi':   calc_obi(sym),
        'cvd':   calc_cvd_divergence(sym),
        'liq':   calc_liq(sym),
        'abs':   calc_absorption(sym),
        'spoof': calc_spoofing(sym),
    }
    st = sym_state.get(sym)
    if not st:
        return dict(score=0.0, dir='long', conf=0, sigs=sigs,
                    agree=[], conflict=[], n_agree=0, n_conflict=0,
                    n_avail=0, strength=0.0)

    regime = st.get('regime', 'neutral')

    if regime in ('trend_up', 'trend_down'):
        weights = {'obi': W_OBI*0.8, 'cvd': W_CVD*1.6, 'liq': W_LIQ*0.7, 'abs': W_ABS*0.8, 'spoof': W_SPOOF}
    elif regime == 'breakout':
        weights = {'obi': W_OBI*1.8, 'cvd': W_CVD*1.5, 'liq': W_LIQ*1.2, 'abs': W_ABS*0.5, 'spoof': W_SPOOF}
    elif regime == 'cascade':
        weights = {'obi': W_OBI*0.7, 'cvd': W_CVD*2.0, 'liq': W_LIQ*2.5, 'abs': W_ABS*0.6, 'spoof': W_SPOOF}
    elif regime == 'chop':
        weights = {'obi': W_OBI*0.5, 'cvd': W_CVD*0.5, 'liq': W_LIQ*0.5, 'abs': W_ABS*1.8, 'spoof': W_SPOOF}
    else:
        weights = {'obi': W_OBI, 'cvd': W_CVD, 'liq': W_LIQ, 'abs': W_ABS, 'spoof': W_SPOOF}

    w_sum = w_tot = 0.0
    for k, v in sigs.items():
        if v is not None:
            w = weights[k] * adaptive_weight(k)
            w_sum += v * w
            w_tot += w
    score = w_sum / w_tot if w_tot > 0 else 0.0

    btc_lead = calc_btc_lead(sym)
    if btc_lead is not None:
        if   btc_lead >  0.25: score += 12
        elif btc_lead < -0.25: score -= 12

    # ITEM 1: depth imbalance — multi-level book skew
    depth_imb = calc_depth_imbalance(sym, levels=5)
    if depth_imb is not None:
        score += depth_imb * 0.15   # scaled contribution (max ±15 pts)

    direction = 'long' if score > 0 else 'short'
    avail     = [k for k, v in sigs.items() if v is not None]
    agree     = [k for k in avail if (sigs[k] > 15  if direction == 'long' else sigs[k] < -15)]
    conflict  = [k for k in avail if (sigs[k] < -20 if direction == 'long' else sigs[k] > 20)]
    raw_conf  = round(len(agree) / max(len(avail), 1) * 100) if avail else 0
    c_pen     = round(len(conflict) / max(len(avail), 1) * 40)
    conf      = max(0, raw_conf - c_pen)
    strength  = sum(abs(sigs[k]) for k in agree) / max(len(agree), 1) if agree else 0.0
    result = dict(
        score=score, dir=direction, conf=conf,
        sigs=sigs, agree=agree, conflict=conflict,
        n_agree=len(agree), n_conflict=len(conflict),
        n_avail=len(avail), strength=strength,
    )
    _pred_cache[sym] = (_tick_id, result)
    return result


# ══ GATES ═════════════════════════════════════════════════════════
# FIX #1: _gate_cache stores computed gate results keyed by (sym, tick_id).
# gates_met() and gate_count() both call _compute_gates() which writes the
# cache on first call per tick; the second caller gets the cached value at O(1).
# Previously every gate call re-ran calc_vpin / calc_kyle_lambda /
# calc_spread_pct / calc_trade_accel / get_atr — all O(n) deque scans —
# doubling the signal computation budget per 100ms tick per coin.
_gate_cache: dict = {}   # sym → (tick_id, ok, checks)


def _compute_gates(sym, r):
    """Core gate logic — single source of truth for gates_met & gate_count."""
    st = sym_state.get(sym)
    if not st: return False, []
    has_oi = st.get('oi', 0) >= 5e6 or sym in LIQUID_WHITELIST
    if not has_oi: return False, []

    hist      = list(st['sig_hist'])
    sustained = False
    if len(hist) >= 5:
        last5     = hist[-5:]
        d         = last5[-1]['dir']
        sustained = all(h['dir'] == d and h['n_agree'] >= 2 and abs(h['score']) > 35 for h in last5)
    atr = get_atr(sym)

    vpin      = calc_vpin(sym)
    vpin_ok   = vpin is not None and vpin >= VPIN_MIN

    lam       = calc_kyle_lambda(sym)
    lam_ok    = (lam is None) or (not KYLE_LAM_GATE) or (lam > 0)

    spread    = calc_spread_pct(sym)
    spread_ok = (spread is None) or (SPREAD_MAX_PCT is None) or (spread <= SPREAD_MAX_PCT)

    accel     = calc_trade_accel(sym)
    accel_ok  = (accel is None) or (accel >= ACCEL_MIN)

    checks = [
        ('conf',    r['conf']       >= MIN_CONF),
        ('score',   abs(r['score']) >= MIN_SCORE),
        ('sigs',    r['n_agree']    >= 2),
        ('noconfl', r['n_conflict'] == 0),
        ('str',     r['strength']   >= 40),
        ('sustain', sustained),
        ('vol',     atr             >= MIN_VOL_ATR),
        ('cd',      (time.time() - st['last_pred_ts']) > PRED_COOLDOWN),
        ('vpin',    vpin_ok),
        ('kyle',    lam_ok),
        ('spread',  spread_ok),
        ('accel',   accel_ok),
    ]
    ok = all(v for _, v in checks)
    return ok, checks


def gates_met(sym, r):
    cached = _gate_cache.get(sym)
    if cached is not None and cached[0] == _tick_id:
        return cached[1], cached[2]
    ok, checks = _compute_gates(sym, r)
    _gate_cache[sym] = (_tick_id, ok, checks)
    return ok, checks


def gate_count(sym, r):
    """Return 'N/12' gate-pass string. Reuses cached gate result when available."""
    cached = _gate_cache.get(sym)
    if cached is not None and cached[0] == _tick_id:
        checks = cached[2]
    else:
        _, checks = gates_met(sym, r)
    if not checks: return '0/12'
    return f"{sum(v for _, v in checks)}/12"


# ══ DYNAMIC TP/SL ═════════════════════════════════════════════════
def calc_dynamic_tp_sl(sym, score, strength):
    atr = max(get_atr(sym), 0.08)  # v16: raised from 0.05; below 0.08% ATR = no edge after fees
    tp  = max(0.18, min(0.80, atr * ATR_TP_MULT))
    sl  = max(0.12, min(0.60, atr * ATR_SL_MULT))  # v16: raised cap 0.35→0.60 to match strategies_engine
    if strength >= 70 and abs(score) >= 70:
        tp = min(0.80, tp * 1.25)
    if strength < 50 or abs(score) < 55:
        tp = max(0.16, tp * 0.80)
        sl = max(0.12, sl * 0.85)
    return round(tp, 4), round(sl, 4)


def sig_still_valid(sym, original_dir):
    obi  = calc_obi(sym)
    cvd  = calc_cvd_divergence(sym)
    vals = [v for v in [obi, cvd] if v is not None]
    if not vals: return False
    strong = [v for v in vals if (v > SIG_HOLD_SCORE if original_dir == 'long' else v < -SIG_HOLD_SCORE)]
    return len(strong) >= max(1, len(vals) - 1)


def sig_reversed(sym, original_dir):
    avail = [v for v in [calc_obi(sym), calc_cvd_divergence(sym)] if v is not None]
    if not avail: return False
    opp = [v for v in avail if (v < -REVERSAL_SCORE if original_dir == 'long' else v > REVERSAL_SCORE)]
    return len(opp) >= max(1, len(avail) - 1)


# ══ EXIT ENGINE ════════════════════════════════════════════════════
def _close(p, dp, reason):
    global hist_win, hist_lose
    if   reason == 'tp':      result = 'win'
    elif reason == 'sl':      result = 'lose'
    elif reason == 'trail':   result = 'win' if (dp - FEE_RT) > 0 else 'flat'
    elif reason == 'inertia': result = 'lose'
    else:
        result = ('win'  if dp >=  WIN_THR else
                  'lose' if dp <= -WIN_THR else 'flat')
    p['out3']   = result
    p['pct3']   = dp
    p['reason'] = reason
    p['dur']    = time.time() - p['ts']
    if result == 'win':    hist_win  += 1
    elif result == 'lose': hist_lose += 1
    for sig in p.get('agree', []):
        if result == 'win':    signal_stats[sig]['win']  += 1
        elif result == 'lose': signal_stats[sig]['lose'] += 1
    log_outcome(p)


def check_outcomes():
    now = time.time()
    for p in list(preds):
        if p['out3'] is not None: continue
        st = sym_state.get(p['sym'])
        if not st or not p['entry'] or st['price'] == 0: continue
        elapsed = now - p['ts']
        raw     = (st['price'] - p['entry']) / p['entry'] * 100
        dp      = raw if p['dir'] == 'long' else -raw
        if elapsed >= 60 and p['snap1'] is None: p['snap1'] = dp
        p['max_dp'] = max(p.get('max_dp', -999), dp)
        tp = p['dyn_tp']; sl = p['dyn_sl']

        # 1. Hard SL
        if dp <= -sl:
            _close(p, dp, 'sl'); continue
        # 2. Minimum hold
        if elapsed < MIN_HOLD_ANY: continue
        # 3. Inertia kill - only cut if actively going wrong direction
        if elapsed >= INERTIA_SEC and dp < 0 and p['max_dp'] < INERTIA_THR:
            _close(p, dp, 'inertia'); continue
        # 4. Trailing stop (once past WIN_THR)
        if p['max_dp'] >= WIN_THR and dp <= p['max_dp'] - TRAIL_DIST:
            _close(p, dp, 'trail'); continue
        # 5. Breakeven lock (halfway to TP)
        if dp >= tp * 0.5 and not p.get('be_locked', False):
            p['dyn_sl'] = max(0.03, sl * 0.25); p['be_locked'] = True
        # 6. Take profit (with signal-hold extension)
        if dp >= tp:
            if sig_still_valid(p['sym'], p['dir']) and not p.get('tp_extended', False):
                atr = get_atr(p['sym'])
                p['dyn_tp'] = min(0.80, tp + atr * 0.4); p['tp_extended'] = True
            else:
                _close(p, dp, 'tp')
            continue
        # 7. Signal reversal
        if elapsed >= REV_MIN_HOLD and sig_reversed(p['sym'], p['dir']):
            _close(p, dp, 'rev'); continue
        # 8. Time fallback
        if elapsed >= MAX_WINDOW:
            _close(p, dp, 'time')


def fire_pred(sym, r):
    global hist_total
    st = sym_state[sym]
    st['last_pred_ts'] = time.time()
    st['sig_hist'].clear()
    dyn_tp, dyn_sl = calc_dynamic_tp_sl(sym, r['score'], r['strength'])
    p = dict(
        id=hist_total + 1, ts=time.time(), sym=sym,
        dir=r['dir'], conf=r['conf'], score=r['score'],
        n_agree=r['n_agree'], n_avail=r['n_avail'],
        entry=st['price'],
        dyn_tp=dyn_tp, dyn_sl=dyn_sl,
        out3=None, pct3=None, snap1=None,
        max_dp=-999, reason=None, dur=None,
        tp_extended=False, be_locked=False,
    )
    preds.appendleft(p)
    hist_total += 1
    log_pred(p)


# ══ LOGGING ═══════════════════════════════════════════════════════
def init_log():
    with open(LOG_FILE, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['# VERSION'] + [f"{k}={v}" for k, v in VERSION.items()])
        w.writerow(['time', 'sym', 'dir', 'conf', 'score', 'sigs', 'n_avail',
                    'entry', 'dyn_tp', 'dyn_sl', 'vpin', 'kyle_lam', 'spread_pct', 'accel',
                    'pct_exit', 'outcome', 'net_exit', 'reason', 'dur_sec', 'version'])


def log_pred(p):
    sigs_s = f"{p['n_agree']}/{p['n_avail']}"
    sym    = p['sym']
    vpin   = calc_vpin(sym)
    lam    = calc_kyle_lambda(sym)
    spread = calc_spread_pct(sym)
    accel  = calc_trade_accel(sym)
    with open(LOG_FILE, 'a', newline='') as f:
        csv.writer(f).writerow([
            datetime.fromtimestamp(p['ts']).strftime('%H:%M:%S'),
            sym, p['dir'], p['conf'], round(p['score']),
            sigs_s, p['n_avail'], f"{p['entry']:.6f}",
            f"{p['dyn_tp']:.4f}", f"{p['dyn_sl']:.4f}",
            f"{vpin:.3f}"   if vpin   is not None else '',
            f"{lam:.2e}"    if lam    is not None else '',
            f"{spread:.4f}" if spread is not None else '',
            f"{accel:.2f}"  if accel  is not None else '',
            '', '', '', '', '', VERSION['v'],
        ])


def log_outcome(p):
    sigs_s = f"{p['n_agree']}/{p['n_avail']}"
    n3     = (p['pct3'] - FEE_RT) if p['pct3'] is not None else None
    sym    = p['sym']
    vpin   = calc_vpin(sym)
    lam    = calc_kyle_lambda(sym)
    spread = calc_spread_pct(sym)
    accel  = calc_trade_accel(sym)
    with open(LOG_FILE, 'a', newline='') as f:
        csv.writer(f).writerow([
            'OUT_' + datetime.fromtimestamp(p['ts']).strftime('%H:%M:%S'),
            sym, p['dir'], p['conf'], round(p['score']),
            sigs_s, p['n_avail'], f"{p['entry']:.6f}",
            f"{p['dyn_tp']:.4f}", f"{p['dyn_sl']:.4f}",
            f"{vpin:.3f}"   if vpin   is not None else '',
            f"{lam:.2e}"    if lam    is not None else '',
            f"{spread:.4f}" if spread is not None else '',
            f"{accel:.2f}"  if accel  is not None else '',
            f"{p['pct3']:.4f}"  if p['pct3'] is not None else '',
            p['out3'] or '',
            f"{n3:.4f}"         if n3 is not None else '',
            p.get('reason', ''),
            f"{p['dur']:.1f}"   if p.get('dur') else '',
            VERSION['v'],
        ])


# ══ WS DATA HANDLERS (Binance) ════════════════════════════════════
#
# Binance depth20 snapshot (no delta updates — full snapshot every 100ms):
# { "lastUpdateId": ..., "bids": [["price","qty"],...], "asks": [...] }
#
def on_book(sym, data):
    st = sym_state.get(sym)
    if not st: return
    # depth20 WS uses 'b'/'a'; REST depth uses 'bids'/'asks'
    raw_bids = data.get('b') or data.get('bids', [])
    raw_asks = data.get('a') or data.get('asks', [])

    # Maintain two parallel dicts:
    #   bids / asks  — string-keyed (kept for any external code that reads them)
    #   bids_f / asks_f — float-keyed (used by calc_obi for zero-cost sorting)
    # Also cache best_bid / best_ask for O(1) spread calculation.
    st['bids'].clear(); st['asks'].clear()
    bids_f: dict = {}; asks_f: dict = {}
    best_bid = 0.0; best_ask = float('inf')

    for px_s, sz in raw_bids:
        s = float(sz)
        if s > 0:
            st['bids'][px_s] = s
            px_f = float(px_s)
            bids_f[px_f] = s
            if px_f > best_bid: best_bid = px_f

    for px_s, sz in raw_asks:
        s = float(sz)
        if s > 0:
            st['asks'][px_s] = s
            px_f = float(px_s)
            asks_f[px_f] = s
            if px_f < best_ask: best_ask = px_f

    st['bids_f'] = bids_f
    st['asks_f'] = asks_f
    st['best_bid'] = best_bid if best_bid > 0    else None
    st['best_ask'] = best_ask if best_ask < float('inf') else None

    now_ms = time.time() * 1000
    st['book_history'].append((
        now_ms,
        sum(bids_f.values()),
        sum(asks_f.values()),
    ))

    # Record near-price wall USD for _wall_stable() in strategies.py.
    cur_px = st['price']
    if cur_px > 0:
        _band = 0.001
        bw = sum(px_f * sz for px_f, sz in bids_f.items()
                 if 0 <= (cur_px - px_f) / cur_px <= _band)
        aw = sum(px_f * sz for px_f, sz in asks_f.items()
                 if 0 <= (px_f - cur_px) / cur_px <= _band)
        st['wall_hist'].append((now_ms, bw, aw))


# Binance aggTrade:
# { "e":"aggTrade","T":ts_ms,"p":"price","q":"qty","m":bool_maker_side }
# m=True  → buyer is maker → aggressive SELL (price going down)
# m=False → seller is maker → aggressive BUY  (price going up)
#
def on_trade(sym, data):
    st = sym_state.get(sym)
    if not st: return
    now    = time.time() * 1000
    px     = float(data['p'])
    sz     = float(data['q'])
    val    = px * sz
    # m=True means the buyer is the maker, so the aggressor is the SELLER
    is_buy = not data['m']

    st['cvd'].append((now, val if is_buy else 0, 0 if is_buy else val))
    st['trade_tape'].append((now, px, val, is_buy))

    acc = st['vpin_acc']
    acc['total'] += val
    if is_buy: acc['buy']  += val
    else:      acc['sell'] += val
    if acc['total'] >= VPIN_BUCKET_VOL:
        imbalance = abs(acc['buy'] - acc['sell']) / acc['total']
        st['vpin_buckets'].append(imbalance)
        acc['buy'] = acc['sell'] = acc['total'] = 0.0

    if val >= TRADE_MIN:
        st['liqs'].append((now, not is_buy, val))


# Binance markPrice stream (@markPrice@1s):
# { "e":"markPriceUpdate","T":ts_ms,"s":"BTCUSDT","p":"markPrice","i":"indexPrice" }
# We use markPrice as the price feed — it's manipulation-resistant.
#
def on_ticker(sym, data):
    st = sym_state.get(sym)
    if not st: return
    px_str = data.get('p') or data.get('c')   # 'p'=markPrice, 'c'=close (fallback)
    if px_str:
        px = float(px_str)
        if px > 0:
            if st['price'] > 0: st['prev_price'] = st['price']
            st['price'] = px
            now = time.time() * 1000
            st['price_hist'].append((now, px))
            if sym == 'BTCUSDT': btc_hist.append((now, px))

            # ── FAST-PATH: fire immediately on price tick ─────────
            # check_outcomes: exits react within 1 WS message (~5ms)
            # instead of waiting up to 100ms for next pred_loop tick.
            if _check_outcomes_handler is not None:
                try:
                    _check_outcomes_handler()
                except Exception:
                    pass
            # Z fast-path: detect lag divergence inline on Binance tick
            if _z_fast_handler is not None:
                try:
                    _z_fast_handler(sym)
                except Exception:
                    pass
            # C/G fast-path: sniper level touch + spike acceleration
            if _cg_fast_handler is not None:
                try:
                    _cg_fast_handler(sym)
                except Exception:
                    pass
            # ──────────────────────────────────────────────────────

    # Capture funding rate — field 'r' in markPriceUpdate stream
    # Value is the 8h rate as a decimal string e.g. "0.0001" = 0.01%
    fr_str = data.get('r')
    if fr_str:
        try:
            fr = float(fr_str)
            st['funding_rate'] = fr
            st['funding_hist'].append((time.time(), fr))
        except (ValueError, TypeError):
            pass


def _liq_key_ts(key: str) -> float:
    """Extract the timestamp (ms) from a seen_liq key of the form '{ts}_{px}_{sz}'."""
    try:
        return float(key.split('_', 1)[0])
    except (ValueError, IndexError):
        return 0.0


# Binance forceOrder (liquidation) stream:
# { "e":"forceOrder","E":ts_ms,"o":{"s":"BTCUSDT","S":"SELL","q":"qty","p":"price","T":ts_ms} }
# S="SELL" means a LONG position was liquidated (long squeezed)
# S="BUY"  means a SHORT position was liquidated (short squeezed)
#
def on_liq(sym, data):
    st = sym_state.get(sym)
    if not st: return
    o       = data.get('o', data)   # unwrap nested order if present
    side    = o.get('S', '')
    px_s    = o.get('p', '0')
    sz_s    = o.get('q', '0') or o.get('l', '0')
    ts      = float(o.get('T', time.time() * 1000))
    try:
        px  = float(px_s); sz = float(sz_s); val = px * sz
    except (ValueError, TypeError):
        return
    if val <= 0: return
    # SELL = long liq; BUY = short liq
    # is_long_liq=True → long positions blown out → bearish pressure
    is_long_liq = (side == 'SELL')
    key = f"{ts}_{px_s}_{sz_s}"
    if key in st['seen_liq']: return
    st['seen_liq'].add(key)
    # FIX #4: set(list(set)[-1000:]) is O(n) and gives arbitrary elements
    # (sets are unordered so [-1000:] doesn't keep the "newest" 1000 keys).
    # Instead evict by age: keys are f"{ts_ms}_..." so compare the ts prefix.
    # This eviction runs rarely (only when >2000 keys) and is still O(n) but
    # now actually removes the OLDEST entries rather than arbitrary ones.
    if len(st['seen_liq']) > 2000:
        cutoff = (time.time() - 120) * 1000   # drop keys older than 2 minutes
        st['seen_liq'] = {k for k in st['seen_liq']
                          if _liq_key_ts(k) >= cutoff}
    st['liqs'].append((ts, is_long_liq, val))
    st['liq_cascade_hist'].append((ts, is_long_liq, val))


# ══ ASYNC TASKS (Binance) ══════════════════════════════════════════
#
# Binance now requires routed WebSocket paths:
#   /public  → depth, aggTrade (high-frequency order book + trades)
#   /market  → markPrice, forceOrder (price feed + liquidations)
#
# A single unrouted /stream connection silently drops /market streams,
# which is why markPrice and forceOrder never arrived.
# We run two concurrent connections, one per endpoint.
#
_ws_public_live  = False
_ws_market_live  = False

def _dispatch(stream, data):
    """Route a decoded Binance combined-stream message to the right handler."""
    if not stream or not data:
        return
    event   = data.get('e', '')
    sym_raw = stream.split('@')[0].upper()
    sym     = sym_raw if sym_raw else data.get('s', '')
    if not sym:
        return
    if '@depth' in stream:
        on_book(sym, data)
    elif event == 'aggTrade':
        on_trade(sym, data)
    elif event == 'markPriceUpdate':
        on_ticker(sym, data)
    elif event == 'forceOrder':
        on_liq(sym, data)


async def _ws_loop(url, label):
    """Generic reconnecting WS loop using aiohttp. Calls _dispatch on every message."""
    global ws_status, _ws_public_live, _ws_market_live
    backoff = 1.0
    while running:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    url,
                    heartbeat=None,        # Binance manages its own keepalive
                    receive_timeout=60.0,  # reconnect if silent for 60s
                    max_msg_size=2**23,
                    timeout=aiohttp.ClientWSTimeout(ws_receive=60.0, ws_close=5.0),
                ) as ws:
                    if label == 'public':  _ws_public_live = True
                    else:                  _ws_market_live = True
                    ws_status = 'live · public+market' if (_ws_public_live and _ws_market_live) else f'live · {label}'
                    log_ws_event('connect', label)
                    backoff = 1.0
                    async for msg in ws:
                        if not running:
                            return
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            raw = msg.data
                        elif msg.type == aiohttp.WSMsgType.BINARY:
                            raw = msg.data.decode('utf-8', errors='replace')
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        else:
                            continue
                        try:
                            m = json.loads(raw)
                        except Exception:
                            continue
                        _dispatch(m.get('stream', ''), m.get('data', m))
        except Exception as e:
            if label == 'public':  _ws_public_live = False
            else:                  _ws_market_live = False
            if _is_benign_ws_error(e):
                ws_status = f'reconnecting-{label}'
            else:
                ws_status = f'reconnecting-{label} ({type(e).__name__})'
                log_ws_event('disconnect', label, str(e))
            await asyncio.sleep(min(backoff, 30.0))
            backoff = min(backoff * 2, 30.0)


async def ws_task(coins):
    """
    Two parallel WebSocket connections to Binance USDT-M Futures.

    Routed URL format (required since 2026):
      /public/stream?streams=   → depth only (high-frequency public)
      /market/stream?streams=   → aggTrade, markPrice, forceOrder

    Binance hard limit: 200 streams per combined stream connection.
    With N coins: public = N×1 stream, market = N×3 streams.
    Safe max: 60 coins (60×3=180 market streams, well under 200).
    If coins exceed the safe limit, market streams are split across
    multiple WS connections automatically.
    """
    BINANCE_STREAM_LIMIT = 190  # hard limit 200, leave 10 buffer

    pub_streams = []
    mkt_streams = []
    for s in coins:
        sl = s.lower()
        pub_streams.append(f'{sl}@depth20@100ms')
        mkt_streams += [f'{sl}@aggTrade', f'{sl}@markPrice@1s', f'{sl}@forceOrder']
    if 'BTCUSDT' not in coins:
        mkt_streams.append('btcusdt@markPrice@1s')

    base = "wss://fstream.binance.com"

    # Split public streams if over limit (unlikely — only 1 stream per coin)
    pub_tasks = []
    for i in range(0, len(pub_streams), BINANCE_STREAM_LIMIT):
        chunk = pub_streams[i:i+BINANCE_STREAM_LIMIT]
        url = f"{base}/public/stream?streams={'/'.join(chunk)}"
        pub_tasks.append(_ws_loop(url, f'public{i//BINANCE_STREAM_LIMIT or ""}'))

    # Split market streams across multiple connections if needed
    mkt_tasks = []
    for i in range(0, len(mkt_streams), BINANCE_STREAM_LIMIT):
        chunk = mkt_streams[i:i+BINANCE_STREAM_LIMIT]
        url = f"{base}/market/stream?streams={'/'.join(chunk)}"
        label = 'market' if i == 0 else f'market{i//BINANCE_STREAM_LIMIT}'
        mkt_tasks.append(_ws_loop(url, label))

    if len(mkt_tasks) > 1:
        log.warning(f"[ws] {len(mkt_streams)} market streams split across {len(mkt_tasks)} connections")

    await asyncio.gather(*pub_tasks, *mkt_tasks)


async def rest_task(coins):
    """
    Periodic REST polling for price + open interest seed data.

    FIX #3: symbols are now fetched concurrently via asyncio.gather + semaphore
    instead of serially (one await per symbol).  With 50 coins the old code
    could block up to 50 × 5s = 250s worst-case; concurrently it completes in
    ~1 round-trip time regardless of coin count.

    REST_SEM_LIMIT caps simultaneous open connections to Binance to avoid
    triggering rate-limiting (Binance futures REST: 1200 req/min per IP).
    At 50 coins × 3 endpoint types = ~150 reqs per round; 20 concurrent is
    well within limits.

    Binance endpoints used:
      /fapi/v1/ticker/24hr?symbol=    → price + 24h stats (single call)
      /fapi/v1/openInterest?symbol=   → open interest in base coin
      /fapi/v1/klines                 → OHLCV bars (throttled per timeframe)
    """
    REST_SEM_LIMIT = 20
    all_syms = list(set(coins + ['BTCUSDT']))

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=REST_SEM_LIMIT + 5)
    ) as sess:
        sem = asyncio.Semaphore(REST_SEM_LIMIT)

        async def _fetch_sym(sym):
            async with sem:
                # -- Price via 24hr ticker --
                try:
                    async with sess.get(
                        f'{API_URL}/fapi/v1/ticker/24hr',
                        params={'symbol': sym},
                        timeout=aiohttp.ClientTimeout(total=5)
                    ) as r:
                        j = await r.json()
                        px = float(j.get('lastPrice', 0) or 0)
                        init_sym(sym)
                        if px > 0:
                            if sym_state[sym]['price'] == 0:
                                sym_state[sym]['price'] = px
                            now = time.time() * 1000
                            if len(sym_state[sym]['price_hist']) < 3:
                                # Seed with 24h high/low for real ATR from first tick.
                                # Without this, 3 identical prices → ATR=0 → all gates block.
                                hi = float(j.get('highPrice', px) or px)
                                lo = float(j.get('lowPrice',  px) or px)
                                sym_state[sym]['price_hist'].append((now - 180000, lo))
                                sym_state[sym]['price_hist'].append((now - 90000,  hi))
                                sym_state[sym]['price_hist'].append((now,          px))
                            if sym == 'BTCUSDT' and len(btc_hist) < 3:
                                hi = float(j.get('highPrice', px) or px)
                                lo = float(j.get('lowPrice',  px) or px)
                                btc_hist.append((now - 180000, lo))
                                btc_hist.append((now - 90000,  hi))
                                btc_hist.append((now,          px))
                except Exception:
                    pass

                # -- Open Interest --
                try:
                    async with sess.get(
                        f'{API_URL}/fapi/v1/openInterest',
                        params={'symbol': sym},
                        timeout=aiohttp.ClientTimeout(total=5)
                    ) as r:
                        j  = await r.json()
                        oi_base = float(j.get('openInterest', 0) or 0)
                        st      = sym_state.get(sym)
                        if st and oi_base > 0 and st['price'] > 0:
                            st['oi'] = oi_base * st['price']
                            st['oi_hist'].append((time.time(), st['oi']))
                except Exception:
                    pass

                # -- Klines (throttled: 1m/60s, 15m/5min, 1d/30min) --
                st    = sym_state.get(sym)
                now_t = time.time()
                if st:
                    for tf, interval_s, limit in [('1m', 60, 100), ('15m', 300, 96), ('1d', 1800, 30)]:
                        if now_t - st['klines_last_fetch'].get(tf, 0) < interval_s:
                            continue
                        try:
                            async with sess.get(
                                f'{API_URL}/fapi/v1/klines',
                                params={'symbol': sym, 'interval': tf, 'limit': limit},
                                timeout=aiohttp.ClientTimeout(total=8)
                            ) as r:
                                rows = await r.json()
                                if isinstance(rows, list):
                                    st['klines'][tf] = [
                                        {'ts': k[0], 'o': float(k[1]), 'h': float(k[2]),
                                         'l': float(k[3]), 'c': float(k[4]), 'v': float(k[5])}
                                        for k in rows[:-1]  # exclude last (unclosed)
                                    ]
                                    st['klines_last_fetch'][tf] = now_t
                        except Exception:
                            pass

        while running:
            await asyncio.gather(*[_fetch_sym(sym) for sym in all_syms])
            await asyncio.sleep(30)


async def pred_loop(coins):
    global _tick_id
    while running:
        t0 = time.time()
        _tick_id += 1          # invalidate run_pred cache for this tick
        check_outcomes()
        for sym in coins:
            st = sym_state.get(sym)
            if not st or st['price'] == 0: continue
            regime, regime_conf = detect_regime(sym)
            st['regime']      = regime
            st['regime_conf'] = regime_conf
            r = run_pred(sym)
            st['sig_hist'].append(r)
            ok, _ = gates_met(sym, r)
            if ok: fire_pred(sym, r)
        await asyncio.sleep(max(0, LOOP_MS / 1000 - (time.time() - t0)))


def start_engine(coins):
    """Entry point for GUI background thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.set_exception_handler(_tg_async_exception_handler)
    _tg_send(f"\U0001f7e2 <b>Engine started</b>\nCoins: {len(coins)}  |  v{VERSION}")
    try:
        loop.run_until_complete(asyncio.gather(
            ws_task(coins), rest_task(coins),
            pred_loop(coins),
        ))
    except Exception:
        tb_str = traceback.format_exc()
        _tg_send(f"\U0001f534 <b>Engine stopped (exception)</b>\n<pre>{tb_str[-3000:]}</pre>")
        raise
    finally:
        _tg_send("\U0001f7e1 <b>Engine loop exited</b> (shutdown or crash)")


def setup(coins):
    """Call once at startup."""
    global ACTIVE_COINS
    ACTIVE_COINS = coins
    init_log()
    for sym in set(coins + ['BTCUSDT']):
        init_sym(sym)


# ── Re-export scanner and lag tasks for backwards compatibility ──────────────
# predict_engine.py can import coin_scanner_task and lag_ws_task from engine
# without changing any existing imports.
from engine_scanner import fetch_top_coins, coin_scanner_task          # noqa: F401,E402
from engine_lag import (                                                # noqa: F401,E402
    exchange_prices, LAG_EXCHANGES, get_lag_snapshot,
    lag_ws_task, _update_lag_price,
)
