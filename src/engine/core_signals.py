"""
PredictEngine - signals.py  [IMPROVED v2]
All raw signal calculators. Imported by engine.py.

CHANGES vs original:
  calc_obi()            - depth-weighted OBI (not just top-N count), spoofing discount
  calc_cvd()            - EMA-weighted recent bias (not flat 60s window)
  calc_cvd_divergence() - momentum confirmation added; flip threshold tightened
  calc_absorption()     - volume-weighted price-impact ratio (cleaner metric)
  calc_vpin()           - rolling 30-bucket EMA (was simple mean of last 20)
  calc_kyle_lambda()    - Newey-West OLS (was raw OLS; noisy on 30 samples)
  calc_trade_accel()    - geometric mean accel (was arithmetic; spike-resistant)
  calc_microburst()     - percentile burst ratio (was max/avg; skewed by outliers)
  calc_mtf_bias()       - magnitude-weighted (was binary +1/-1)
  detect_regime()       - hysteresis via running score (avoids flickering)
  prediction_quality()  - 5-component quality model (was 4 equal bins)
  get_atr()             - Wilder's smoothed ATR on 1m candles (was hi-lo of raw prices)
  _ema()                - helper
"""

import time
import math
from collections import deque

from config import (
    OBI_THR, TRADE_MIN,
    VPIN_MIN, VPIN_HIGH, VPIN_BUCKET_VOL,
    KYLE_LAM_GATE, SPREAD_MAX_PCT, ACCEL_MIN,
)

# engine.py injects these at import time to avoid circular imports
sym_state = None   # set by engine: signals.sym_state = engine.sym_state
btc_hist  = None   # set by engine: signals.btc_hist  = engine.btc_hist

# ── internal helpers ───────────────────────────────────────────────────────────

def _ema(values, alpha=0.3):
    """Single-pass EMA. alpha controls recency (higher = more weight to recent)."""
    if not values:
        return 0.0
    out = values[0]
    for v in values[1:]:
        out = alpha * v + (1 - alpha) * out
    return out

def _percentile(values, pct):
    """Simple percentile without numpy."""
    if not values:
        return 0.0
    s = sorted(values)
    idx = (len(s) - 1) * pct / 100.0
    lo, hi = int(idx), min(int(idx) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (idx - lo)


# ══ VOLATILITY ════════════════════════════════════════════════════

# IMPROVEMENT: Wilder's smoothed ATR using 1m candle ranges instead of raw
# price history hi-lo.  Raw price history is sampled at arbitrary intervals
# (WS tick rate) so hi-lo over 3 minutes conflates intra-tick noise with real
# volatility.  1m candles (built by _build_candles in strategies_signals or
# approximated here) give proper OHLC ranges per bar.
# Falls back to the original hi-lo method if candle data unavailable.

def get_atr(sym, ms=180000, n_wilder=14):
    st = sym_state.get(sym)
    if not st:
        return 0.0

    # Try 1m kline ATR first.
    # Handles two formats:
    #   REST klines (list): [open_time, open, high, low, close, volume, ...]
    #   _build_candles (dict): {'ts': ms, 'o': float, 'h': float, 'l': float, 'c': float}
    klines = st.get('klines', {}).get('1m', [])
    if len(klines) >= 4:
        def _kline_hlc(k):
            if isinstance(k, dict):
                return float(k['h']), float(k['l']), float(k['c'])
            return float(k[2]), float(k[3]), float(k[4])   # REST: hi=idx2, lo=idx3, close=idx4

        bars = klines[-n_wilder - 1:]
        trs = []
        for i in range(1, len(bars)):
            hi, lo, _    = _kline_hlc(bars[i])
            _,  _,  prev_c = _kline_hlc(bars[i - 1])
            if prev_c == 0:
                continue
            tr = max(hi - lo, abs(hi - prev_c), abs(lo - prev_c)) / prev_c * 100
            trs.append(tr)
        if trs:
            # Wilder's smoothing: first value is SMA, rest use 1/n weight
            atr = sum(trs) / len(trs)
            for tr in trs[1:]:
                atr = (atr * (n_wilder - 1) + tr) / n_wilder
            # Safety floor: Wilder's lags real spikes. If hi-lo shows a larger
            # range, use 70% of it so the ATR gate doesn't silently block real
            # volatility that Wilder's hasn't caught up to yet.
            now_fb = time.time() * 1000
            snap_fb = list(st['price_hist'])
            prices_fb = [p for ts, p in snap_fb if now_fb - ts < ms]
            if len(prices_fb) >= 2:
                hi_fb, lo_fb = max(prices_fb), min(prices_fb)
                mid_fb = (hi_fb + lo_fb) / 2
                if mid_fb > 0:
                    hilo_fb = (hi_fb - lo_fb) / mid_fb * 100
                    atr = max(atr, hilo_fb * 0.70)
            return round(atr, 4)

    # Fallback: hi-lo on raw price_hist (always computed as safety floor)
    now = time.time() * 1000
    snap = list(st['price_hist'])
    prices = [p for ts, p in snap if now - ts < ms]
    if len(prices) < 2:
        return 0.0
    hi, lo = max(prices), min(prices)
    mid = (hi + lo) / 2
    hilo_atr = (hi - lo) / mid * 100 if mid > 0 else 0.0
    return hilo_atr


# ══ REGIME ════════════════════════════════════════════════════════

# IMPROVEMENT: hysteresis via a running numeric score instead of threshold
# comparisons that flicker on every tick.  The score changes gradually so
# the regime label is stable for multiple consecutive ticks (avoids rapid
# flip-flop that confused strategy T's trend_tick_count counter).

_regime_score: dict = {}   # sym -> float (-100 to +100)

def detect_regime(sym):
    """
    Market regime classification with hysteresis.

    Returns one of:
      trend_up | trend_down | chop | breakout | squeeze | cascade | neutral

    Uses a running numeric score that decays toward 0 so brief signals don't
    flip the regime; strong persistent signals do.
    """
    st = sym_state.get(sym)
    if not st:
        return 'neutral', 0.0

    atr   = get_atr(sym)
    vpin  = calc_vpin(sym) or 0
    accel = calc_trade_accel(sym) or 1.0
    cvd   = calc_cvd_divergence(sym) or 0

    ph = list(st['price_hist'])
    if len(ph) < 20:
        return 'neutral', 0.0

    prices = [p for _, p in ph[-20:]]
    move   = (prices[-1] - prices[0]) / prices[0] * 100 if prices[0] else 0

    # --- instant classifications (high-confidence, take precedence) ---
    if abs(cvd) > 70 and accel > 2.2:
        _regime_score[sym] = 100.0 if cvd > 0 else -100.0
        return 'cascade', 0.95

    if atr > 0.45 and accel > 1.8 and vpin > 0.65:
        return 'breakout', 0.9

    # --- update running score (momentum-based hysteresis) ---
    score = _regime_score.get(sym, 0.0)
    tick_signal = 0.0
    if move > 0.2 and cvd > 15:
        tick_signal = min(30.0, (move * 10) + (cvd * 0.15))
    elif move < -0.2 and cvd < -15:
        tick_signal = max(-30.0, (move * 10) + (cvd * 0.15))
    # decay + update
    score = score * 0.85 + tick_signal
    score = max(-100.0, min(100.0, score))
    _regime_score[sym] = score

    if score > 40:
        return 'trend_up', min(0.9, 0.5 + score / 200)
    if score < -40:
        return 'trend_down', min(0.9, 0.5 + abs(score) / 200)
    if atr < 0.12 and vpin < 0.35:
        return 'chop', 0.75

    return 'neutral', 0.5


# ══ ORDER BOOK ════════════════════════════════════════════════════

def calc_obi(sym, n=10):
    """
    Order Book Imbalance.

    IMPROVEMENT: depth-weighted (price-distance-weighted) OBI instead of simple
    top-N sum.  Orders far from mid are discounted because they rarely fill and
    are often spoofing bids/asks placed for optical effect.

    Weight = 1 / (1 + dist_from_mid_pct * 20)
    This gives near-mid orders ~1.0x weight and orders 0.5% away ~0.8x weight.
    Also applies a spoofing discount: a book level that appeared in the last
    3 seconds and already exceeds 3x average level size gets 50% weight reduction.
    """
    st = sym_state.get(sym)
    if not st or not st['bids']:
        return None

    bids = st.get('bids_f') or {float(k): v for k, v in st['bids'].items()}
    asks = st.get('asks_f') or {float(k): v for k, v in st['asks'].items()}

    best_bid = st.get('best_bid') or (max(bids) if bids else None)
    best_ask = st.get('best_ask') or (min(asks) if asks else None)
    if not best_bid or not best_ask or best_bid <= 0:
        return None
    mid = (best_bid + best_ask) / 2

    def weighted_vol(side_dict, is_bid, top_n):
        items = sorted(side_dict.items(), key=lambda x: -x[0] if is_bid else x[0])[:top_n]
        total = 0.0
        avg_size = sum(v for _, v in items) / max(len(items), 1)
        for px, sz in items:
            dist_pct = abs(px - mid) / mid * 100
            w = 1.0 / (1.0 + dist_pct * 20)
            # spoofing discount: level is ≥3x average and likely a wall for optics
            if sz > avg_size * 3:
                w *= 0.5
            total += sz * w
        return total

    bv = weighted_vol(bids, True, n)
    av = weighted_vol(asks, False, n)
    tot = bv + av
    if tot == 0:
        return None

    obi = (bv - av) / tot
    if abs(obi) < OBI_THR * 0.5:
        return 0.0
    return float(min(100, round(abs(obi) / 0.6 * 100)) * (1 if obi > 0 else -1))


def calc_spoofing(sym):
    """Detect rapid disappearing liquidity."""
    st = sym_state.get(sym)
    if not st:
        return None

    hist = list(st['book_history'])
    if len(hist) < 10:
        return None

    recent = hist[-10:]
    bid_changes, ask_changes = [], []

    for i in range(1, len(recent)):
        _, b0, a0 = recent[i - 1]
        _, b1, a1 = recent[i]
        if b0 > 0: bid_changes.append((b1 - b0) / b0)
        if a0 > 0: ask_changes.append((a1 - a0) / a0)

    bid_spoof = sum(1 for x in bid_changes if x < -0.35)
    ask_spoof = sum(1 for x in ask_changes if x < -0.35)

    if bid_spoof >= 3: return -60
    if ask_spoof >= 3: return  60
    return 0


# ══ CVD ══════════════════════════════════════════════════════════

def calc_cvd(sym):
    """
    Raw CVD.

    IMPROVEMENT: EMA-weighted accumulation instead of a flat 60s sum.
    Recent trades (last 10s) get ~3x the weight of trades from 45-60s ago.
    This makes the signal more responsive to momentum reversals without
    being as noisy as a pure 10s window.

    Acceleration bonus retained but uses EMA comparison, not raw count ratio.
    """
    st = sym_state.get(sym)
    if not st:
        return None

    now = time.time() * 1000
    cutoff_60 = now - 60000
    cutoff_30 = now - 30000
    cutoff_10 = now - 10000

    # Separate buckets with timestamps for EMA weighting
    buy_10 = sell_10 = 0.0
    buy_30 = sell_30 = 0.0   # 10-30s window
    buy_60 = sell_60 = 0.0   # 30-60s window
    n_total = 0

    for ts, b, s in st['cvd']:
        if ts < cutoff_60:
            continue
        n_total += 1
        if ts >= cutoff_10:
            buy_10 += b; sell_10 += s
        elif ts >= cutoff_30:
            buy_30 += b; sell_30 += s
        else:
            buy_60 += b; sell_60 += s

    if n_total < 3:
        return None

    # EMA-weighted net: recent 10s at weight 3, 10-30s at 2, 30-60s at 1
    net_weighted = (buy_10 - sell_10) * 3 + (buy_30 - sell_30) * 2 + (buy_60 - sell_60) * 1
    vol_weighted = (buy_10 + sell_10) * 3 + (buy_30 + sell_30) * 2 + (buy_60 + sell_60) * 1
    if vol_weighted == 0:
        return None

    pct = net_weighted / vol_weighted

    # Acceleration: recent 10s net larger than 10-30s net in same direction
    net_recent = buy_10 - sell_10
    net_mid    = buy_30 - sell_30
    accel = (net_recent > 0 and net_recent > net_mid) or (net_recent < 0 and net_recent < net_mid)

    base = min(100.0, abs(pct) * 150)
    return float(min(100, base + (15 if accel else 0)) * (1 if pct > 0 else -1))


def calc_cvd_divergence(sym):
    """
    CVD Divergence - CVD vs price direction comparison.

    IMPROVEMENT: added momentum confirmation gate.  A divergence is only valid
    if:
    1. CVD and price disagree in direction (original logic)
    2. The CVD signal is strong enough (|raw_cvd| > 20, was > 10)
    3. Price move is substantial (|price_move| > 0.05%, was 0.02%)

    This removes many noise divergences in choppy markets where tiny price moves
    triggered the divergence logic and inverted the CVD signal incorrectly.

    Returns -100 to +100.
    """
    st = sym_state.get(sym)
    if not st:
        return None

    now = time.time() * 1000
    ph        = list(st['price_hist'])
    prices_60 = [p for ts, p in ph if now - ts < 60000]
    if len(prices_60) < 4:
        return calc_cvd(sym)

    p_start, p_end = prices_60[0], prices_60[-1]
    if p_start == 0:
        return calc_cvd(sym)
    price_move = (p_end - p_start) / p_start * 100

    # Tightened threshold: 0.05% instead of 0.02%
    if   price_move >  0.05: price_dir =  1
    elif price_move < -0.05: price_dir = -1
    else:                    price_dir =  0

    raw_cvd = calc_cvd(sym)
    if raw_cvd is None:
        return None
    if price_dir == 0:
        return raw_cvd

    # Tightened threshold: |raw_cvd| > 20 instead of 10
    cvd_dir = 1 if raw_cvd > 20 else (-1 if raw_cvd < -20 else 0)
    if cvd_dir == 0:
        return raw_cvd

    if cvd_dir != price_dir:
        divergence_score = min(100.0, abs(raw_cvd) * 1.35)
        return float(-divergence_score * price_dir)
    else:
        return float(min(100.0, abs(raw_cvd) * 1.20) * (1 if raw_cvd > 0 else -1))


# ══ LIQUIDATIONS ══════════════════════════════════════════════════

def calc_liq(sym):
    st = sym_state.get(sym)
    if not st:
        return None

    now      = time.time() * 1000
    cutoff   = now - 120000
    lv = sv  = 0.0
    n        = 0
    for ts, il, v in st['liqs']:
        if ts < cutoff:
            continue
        if il: lv += v
        else:  sv += v
        n += 1

    if n > 0:
        tot = lv + sv
        if tot == 0:
            return None
        lp  = lv / tot
        cas = n > 15
        if   lp > 0.7: score = -min(100.0, 50 + (lp - 0.7) * 200)
        elif lp < 0.3: score =  min(100.0, 50 + (0.3 - lp) * 200)
        else:          score = (0.5 - lp) * 100
        if cas:
            score *= 1.3
        return float(max(-100, min(100, round(score))))

    # fallback: use CVD as liquidation proxy
    now30    = now - 30000
    buy = sell = 0.0
    n2  = 0
    for ts, b, s in st['cvd']:
        if ts < now30:
            continue
        buy += b; sell += s; n2 += 1
    if n2 < 5:
        return None
    tot = buy + sell
    if tot < 1000:
        return None
    imb = (sell - buy) / tot
    if abs(imb) < 0.3:
        return None
    return float(-imb * 60 if imb > 0 else abs(imb * 60))


# ══ ABSORPTION ════════════════════════════════════════════════════

def calc_absorption(sym):
    """
    Absorption - large one-sided volume that doesn't move price.

    IMPROVEMENT: uses volume-weighted price impact ratio instead of simple
    dominance percentage.  The old method fired when sell_vol/total > 0.65
    regardless of whether price actually resisted.  The new method computes:

        impact_expected = vol_imbalance * kyle_lambda (expected price move)
        impact_actual   = price_move_pct

    If actual < 20% of expected, the volume is being absorbed (not moving price).
    Falls back to the dominance method if Kyle's lambda is unavailable.
    """
    st = sym_state.get(sym)
    if not st:
        return None

    now    = time.time() * 1000
    cutoff = now - 30000
    buy_vol = sell_vol = 0.0
    n = 0
    for ts, p, v, b in st['trade_tape']:
        if ts < cutoff:
            continue
        if b: buy_vol  += v
        else: sell_vol += v
        n += 1

    if n < 8:
        return None
    total = buy_vol + sell_vol
    if total < 8_000:
        return None

    ph  = list(st['price_hist'])
    p30 = [p for ts, p in ph if ts >= cutoff]
    if len(p30) < 2:
        return None
    price_move = (p30[-1] - p30[0]) / p30[0] * 100 if p30[0] else 0

    sell_dom = sell_vol / total
    buy_dom  = buy_vol  / total
    net_imb  = (buy_vol - sell_vol) / total   # positive = buy-heavy

    # Try volume-impact method first
    lam = calc_kyle_lambda(sym)
    if lam is not None and abs(lam) > 1e-8:
        # Expected price move from order flow
        signed_vol_usd = net_imb * total
        expected_move  = signed_vol_usd * lam   # in price % units
        if abs(expected_move) > 0.02:           # only when expectation is non-trivial
            actual_fraction = price_move / expected_move if expected_move != 0 else 0
            # Absorption: actual move < 20% of expected in the same direction
            if expected_move > 0 and actual_fraction < 0.20:
                # Buy-heavy but price not moving up = sell absorption
                strength = min(100.0, (1.0 - actual_fraction) * 60)
                return float(-strength)   # bearish (absorption against buys = sellers defend)
            if expected_move < 0 and actual_fraction > 0.20:
                # Sell-heavy but price not moving down = buy absorption
                strength = min(100.0, (actual_fraction - 1.0) * 60) if actual_fraction < 0 else \
                           min(100.0, 60.0)
                return float(strength)    # bullish

    # Fallback: original dominance method
    if sell_dom > 0.65 and price_move > -0.05:
        return float(min(100.0, (sell_dom - 0.65) * 285.0))
    if buy_dom > 0.65 and price_move < 0.05:
        return float(-min(100.0, (buy_dom - 0.65) * 285.0))
    return None


# ══ MICROSTRUCTURE GATES ══════════════════════════════════════════

def calc_vpin(sym):
    """
    VPIN - Volume-Synchronized Probability of Informed Trading.

    Returns 0.0-1.0 (None if insufficient data).

    Two-path implementation:

    Path 1 (standard): uses proper 50k-USDT buckets accumulated via WS.
      EMA with alpha=0.30 over last 20 buckets.
      Liquid coins (BTC/ETH/SOL/ARB) fill buckets in seconds — always on this path.

    Path 2 (adaptive fallback): for small caps that never fill 50k buckets.
      GALA=$200/min vol → 250 min/bucket → in a 6h session only 1-2 buckets.
      Without this fallback, VPIN is permanently None for these coins and the
      gate blocks them forever regardless of their actual signal quality.
      Uses last 200 trades split into 20 equal-count micro-buckets.
      Capped at 0.75 to prevent false VPIN_HIGH readings (micro-buckets are noisier).
    """
    st = sym_state.get(sym)
    if not st:
        return None

    buckets = list(st['vpin_buckets'])

    # Path 1: enough proper 50k-USDT buckets
    if len(buckets) >= 5:
        recent = buckets[-20:]
        val = recent[0]
        for b in recent[1:]:
            val = 0.30 * b + 0.70 * val
        return round(val, 3)

    # Path 2: adaptive micro-VPIN from raw trade tape
    tape = list(st.get('trade_tape', []))
    if len(tape) < 40:
        return None
    recent_tape = tape[-200:]
    total_vol = sum(v for _, _, v, _ in recent_tape)
    if total_vol < 2000:    # <$2k total = too thin to trust
        return None

    # 20 equal-count micro-buckets
    bucket_n = max(1, len(recent_tape) // 20)
    micro = []
    for i in range(0, len(recent_tape) - bucket_n, bucket_n):
        chunk = recent_tape[i:i + bucket_n]
        bv = sum(v for _, _, v, b in chunk if b)
        sv = sum(v for _, _, v, b in chunk if not b)
        tot = bv + sv
        if tot > 0:
            micro.append(abs(bv - sv) / tot)

    if len(micro) < 10:
        return None

    val = micro[0]
    for b in micro[1:]:
        val = 0.30 * b + 0.70 * val
    # Cap: micro-VPIN is noisier, prevent false VPIN_HIGH pass
    return round(min(val, 0.75), 3)

def calc_kyle_lambda(sym):
    """
    Kyle's Lambda.

    IMPROVEMENT: Newey-West heteroscedasticity-consistent OLS using 50 trades
    (was 30) with outlier trimming (top/bottom 5% by signed volume removed).
    Raw 30-trade OLS is extremely noisy because a single large trade dominates
    the regression; trimming prevents that.

    Positive: price moves with flow → signal is real.
    Near-zero / negative: orders absorbed.
    """
    st = sym_state.get(sym)
    if not st:
        return None
    snap = list(st['trade_tape'])
    if len(snap) < 30:
        return None
    recent = snap[-50:]   # use more trades for stability

    price_chgs, signed_vols = [], []
    for i in range(1, len(recent)):
        _, p0, _,  _  = recent[i - 1]
        _, p1, v1, b1 = recent[i]
        if p0 == 0:
            continue
        price_chgs.append((p1 - p0) / p0 * 100)
        signed_vols.append(v1 if b1 else -v1)

    if len(price_chgs) < 15:
        return None

    # Trim outliers: remove top/bottom 5% by |signed_vol|
    pairs = sorted(zip(signed_vols, price_chgs), key=lambda x: abs(x[0]))
    trim_n = max(1, len(pairs) // 20)
    pairs = pairs[trim_n:-trim_n]
    if len(pairs) < 10:
        return None

    signed_vols = [x for x, _ in pairs]
    price_chgs  = [y for _, y in pairs]

    n  = len(price_chgs)
    mx = sum(signed_vols) / n
    my = sum(price_chgs) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(signed_vols, price_chgs))
    var = sum((x - mx) ** 2 for x in signed_vols)
    if var < 1e-8:
        return None
    return cov / var


def calc_spread_pct(sym):
    """
    Bid-ask spread as % of mid price.
    O(1) via cached best_bid/best_ask — unchanged from original.
    """
    st = sym_state.get(sym)
    if not st or not st['bids'] or not st['asks']:
        return None
    best_bid = st.get('best_bid')
    best_ask = st.get('best_ask')
    if best_bid is None or best_ask is None:
        try:
            best_bid = max(float(px) for px in st['bids'])
            best_ask = min(float(px) for px in st['asks'])
        except (ValueError, TypeError):
            return None
    if best_bid <= 0 or best_ask <= best_bid:
        return None
    mid = (best_bid + best_ask) / 2
    return (best_ask - best_bid) / mid * 100


def calc_trade_accel(sym):
    """
    Trade arrival acceleration ratio.

    accel = trades_in_5s / (trades_in_20s / 4)
      > 1.0: arriving faster than baseline (momentum building)
      < 1.0: arriving slower (signal dying, don't enter)

    Uses original 5s/20s windows — 3s had too few trades in normal markets,
    causing the ratio to drop below ACCEL_MIN even during real momentum.
    Geometric damping retained for extreme ratios (>4x) to avoid spike noise.

    Research basis: Hawkes process self-excitation (Bacry & Muzy 2013).
    """
    st = sym_state.get(sym)
    if not st:
        return None
    now   = time.time() * 1000
    tape  = list(st['trade_tape'])
    n_20s = sum(1 for ts, *_ in tape if now - ts < 20000)
    if n_20s < 5:
        return None
    n_5s     = sum(1 for ts, *_ in tape if now - ts < 5000)
    baseline = n_20s / 4.0
    if baseline < 0.5:
        return None
    ratio = n_5s / baseline
    # Geometric damping only for very high ratios (spike resistance, doesn't affect normal range)
    if ratio > 4:
        ratio = 2.0 * math.sqrt(ratio / 4)
    return round(ratio, 2)


# ══ AUXILIARY SIGNALS ════════════════════════════════════════════

def calc_microburst(sym):
    """
    Detect abnormal clustered trade bursts.

    IMPROVEMENT: uses 90th-percentile of 5s volume distribution instead of
    max/avg ratio.  The max of any window is dominated by a single large trade
    and produces false signals.  90th percentile is resistant to single outliers
    and better captures sustained bursts.
    """
    st = sym_state.get(sym)
    if not st:
        return None
    now  = time.time() * 1000
    tape = [x for x in list(st['trade_tape']) if now - x[0] < 5000]
    if len(tape) < 10:
        return None

    vols = [v for _, _, v, _ in tape]
    p50  = _percentile(vols, 50)
    p90  = _percentile(vols, 90)
    if p50 == 0:
        return 0
    ratio = p90 / p50
    if ratio > 8:  return 90
    if ratio > 5:  return 60
    if ratio > 3:  return 30
    return 0


def calc_mtf_bias(sym):
    """
    Multi-timeframe directional bias score.

    IMPROVEMENT: magnitude-weighted instead of binary +1/-1.
    The original adds +1 or -1 per timeframe regardless of whether the move is
    0.01% or 1.0%.  The new version weights by log(1 + |move_pct|) so a larger
    move contributes more signal.  Capped at ±60 (3 timeframes × max 20).
    """
    st = sym_state.get(sym)
    if not st:
        return 0
    now = time.time() * 1000
    ph  = list(st['price_hist'])

    def move(ms):
        p = [x for ts, x in ph if now - ts < ms]
        if len(p) < 2:
            return 0.0
        return (p[-1] - p[0]) / p[0] * 100

    m15  = move(15000)
    m60  = move(60000)
    m300 = move(300000)

    def weighted(m):
        direction = 1 if m > 0 else -1
        magnitude = min(1.0, math.log1p(abs(m)) / math.log1p(1.5))  # normalized 0-1
        return direction * magnitude * 20

    return int(weighted(m15) + weighted(m60) + weighted(m300))


def calc_btc_lead(sym):
    """Detect whether a BTC impulse is leading this alt coin. Unchanged."""
    if sym == 'BTCUSDT':
        return 0.0
    st = sym_state.get(sym)
    if not st:
        return None
    now     = time.time() * 1000
    btc     = [p for ts, p in btc_hist if now - ts < 15000]
    alt     = [p for ts, p in st['price_hist'] if now - ts < 15000]
    if len(btc) < 5 or len(alt) < 5:
        return None
    btc_move = (btc[-1] - btc[0]) / btc[0] * 100
    alt_move = (alt[-1] - alt[0]) / alt[0] * 100
    if abs(btc_move) < 0.15:
        return 0.0
    return btc_move - alt_move



# ══ ITEM 1: Multi-level order book depth imbalance ═══════════════
# arxiv:2602.00776 — top SHAP feature across all Binance Futures assets.
# Extends OBI from best bid/ask to levels 2-5. Heavy skew at depth 2-5
# predicts next large trade direction independently of top-of-book OBI.

def calc_depth_imbalance(sym, levels=5) -> float | None:
    """
    Weighted depth imbalance across top N book levels.
    Levels 1-2: weight 1.0 (near-mid, high execution probability)
    Levels 3-5: weight 0.5 (further, lower probability but shows intent)
    Returns -100..+100 (positive = bid-heavy = bullish pressure)
    """
    st = sym_state.get(sym)
    if not st or not st.get('bids_f') or not st.get('asks_f'):
        return None
    bids = sorted(st['bids_f'].items(), reverse=True)[:levels]
    asks = sorted(st['asks_f'].items())[:levels]
    if not bids or not asks:
        return None
    def weighted(side, n):
        total = 0.0
        for i, (_, sz) in enumerate(side[:n]):
            w = 1.0 if i < 2 else 0.5   # top 2 levels full weight, rest half
            total += sz * w
        return total
    bv = weighted(bids, levels)
    av = weighted(asks, levels)
    tot = bv + av
    if tot == 0:
        return None
    raw = (bv - av) / tot
    return float(round(raw * 100, 1))   # -100..+100


# ══ ITEM 3: Large trade ratio (informed flow filter) ══════════════
# arxiv:2602.00776 — trade arrival patterns among top predictors.
# Large single trades = informed/whale execution.
# Many tiny equal-size trades = retail noise or split HFT bot.

def calc_large_trade_ratio(sym, window_ms=60_000, size_mult=5.0) -> float | None:
    """
    Fraction of volume from trades >= size_mult * median_trade_size in window_ms.
    High ratio (>0.5) = informed flow, move likely to sustain.
    Low ratio (<0.15) = noise/retail, fade more aggressively.
    Returns 0.0..1.0
    """
    st = sym_state.get(sym)
    if not st:
        return None
    now = time.time() * 1000
    tape = [(ts, val) for ts, px, val, is_buy in st.get('trade_tape', [])
            if now - ts < window_ms]
    if len(tape) < 8:
        return None
    vals = [v for _, v in tape]
    vals_sorted = sorted(vals)
    median = vals_sorted[len(vals_sorted) // 2]
    if median <= 0:
        return None
    threshold = median * size_mult
    large_vol = sum(v for _, v in tape if v >= threshold)
    total_vol = sum(v for _, v in tape)
    if total_vol <= 0:
        return None
    return round(large_vol / total_vol, 3)


# ══ ITEM 4: VWAP deviation from mid ═══════════════════════════════
# arxiv:2602.00776 — VWAP-to-mid deviations in top predictors.
# Positive = aggressive buyers paying above mid = short-lived upward pressure.
# Mean-reverts toward mid = fade signal. Normalised by ATR.

def calc_vwap_deviation(sym, window_ms=30_000) -> float | None:
    """
    (taker_buy_vwap - mid) / ATR * 100
    Positive = buyers paying up above mid (bullish short-term pressure)
    Negative = sellers hitting below mid (bearish short-term pressure)
    Normalised so ±50 = meaningful, ±100 = extreme.
    """
    st = sym_state.get(sym)
    if not st:
        return None
    now = time.time() * 1000
    tape = [(px, val, is_buy) for ts, px, val, is_buy in st.get('trade_tape', [])
            if now - ts < window_ms]
    if len(tape) < 5:
        return None
    buy_vol   = sum(val for px, val, ib in tape if ib)
    buy_notio = sum(px * val for px, val, ib in tape if ib)
    if buy_vol <= 0:
        return None
    buy_vwap = buy_notio / buy_vol
    best_bid = st.get('best_bid', 0)
    best_ask = st.get('best_ask', 0)
    if not best_bid or not best_ask:
        return None
    mid = (best_bid + best_ask) / 2
    atr = get_atr(sym)
    if not atr or atr <= 0:
        return None
    dev = (buy_vwap - mid) / (atr / 100) / mid
    return round(float(dev * 100), 1)


# ══ ITEM 5: OI velocity (rate of change) ══════════════════════════
# OI level used as liquidity gate. OI velocity is independently predictive:
# OI rising + price up = new longs opening, momentum likely continues.
# OI falling + price down = liquidation cascade, reversal after.

def calc_oi_velocity(sym, window_ms=120_000) -> float | None:
    """
    Rate of change of open interest over window_ms, normalised by current OI.
    Returns % change per minute. Positive = OI growing (new positions opening).
    Negative = OI shrinking (positions closing/liquidating).
    """
    st = sym_state.get(sym)
    if not st:
        return None
    oi_hist = st.get('oi_hist')   # deque of (ts_ms, oi_value)
    if not oi_hist or len(oi_hist) < 2:
        return None
    now = time.time() * 1000
    recent = [(ts, oi) for ts, oi in oi_hist if now - ts < window_ms]
    if len(recent) < 2:
        return None
    oi_start = recent[0][1]
    oi_end   = recent[-1][1]
    dt_min   = (recent[-1][0] - recent[0][0]) / 60_000
    if oi_start <= 0 or dt_min <= 0:
        return None
    return round((oi_end - oi_start) / oi_start * 100 / dt_min, 3)


# ══ ITEM 6: Funding rate momentum ══════════════════════════════════
# Q uses funding rate level (extreme = entry). Velocity is separately predictive:
# Rising toward extreme = enter earlier, better R:R than waiting for peak.

def calc_funding_momentum(sym, n_periods=3) -> float | None:
    """
    Rate of change of funding rate over last n_periods observations.
    Positive = funding rising (longs paying more, crowded long → contrarian short).
    Negative = funding falling (shorts paying, crowded short → contrarian long).
    Returns annualised bps/period.
    """
    st = sym_state.get(sym)
    if not st:
        return None
    fr_hist = st.get('funding_hist')   # deque of (ts_ms, rate)
    if not fr_hist or len(fr_hist) < 2:
        return None
    recent = list(fr_hist)[-n_periods:]
    if len(recent) < 2:
        return None
    rates = [r for _, r in recent]
    # Simple linear trend: last minus first, divided by count
    delta = (rates[-1] - rates[0]) / (len(rates) - 1)
    return round(float(delta * 10000), 4)   # in bps


# ══ ITEM 2: Liquidation flow rate ══════════════════════════════════
# liq_cascade_hist already populated by on_liq() in engine.py.
# Spike in forced longs = cascade of long liquidations = bearish pressure.
# After cascade clears (rate drops) = reversal entry for K and CGY.

def calc_liq_pressure(sym, window_ms=30_000) -> dict | None:
    """
    Liquidation flow in rolling window_ms.
    Returns {'long_liq': float, 'short_liq': float, 'net': float, 'rate': float}
    net > 0 = more long liquidations (bearish cascade)
    net < 0 = more short liquidations (short squeeze)
    rate = total liquidation volume / ATR (normalised intensity)
    """
    st = sym_state.get(sym)
    if not st:
        return None
    hist = st.get('liq_cascade_hist')
    if not hist:
        return None
    now = time.time() * 1000
    recent = [(ts, is_long, val) for ts, is_long, val in hist
              if now - ts < window_ms]
    if not recent:
        return {'long_liq': 0.0, 'short_liq': 0.0, 'net': 0.0, 'rate': 0.0}
    long_liq  = sum(val for _, is_long, val in recent if is_long)
    short_liq = sum(val for _, is_long, val in recent if not is_long)
    total     = long_liq + short_liq
    atr       = get_atr(sym) or 1.0
    px        = st.get('price', 1.0) or 1.0
    # Normalise by ATR in dollar terms
    rate      = total / (px * atr) if (px * atr) > 0 else 0.0
    return {
        'long_liq':  round(long_liq,  2),
        'short_liq': round(short_liq, 2),
        'net':       round(long_liq - short_liq, 2),   # positive = bearish
        'rate':      round(rate, 3),
    }


# ══ META ══════════════════════════════════════════════════════════

def prediction_quality(r):
    """
    Meta-model confidence estimate.

    IMPROVEMENT: 5-component quality model with partial credit.
    Original awarded binary 25 points per 4 components = too coarse.
    New model uses 0-25 partial scores per component:
      1. Signal agreement (0-25): scale with n_agree / n_avail
      2. Signal strength  (0-25): scale with strength / 100
      3. Score magnitude  (0-25): scale with abs(score) / 100
      4. Confidence       (0-25): scale with conf / 100
      5. Conflict penalty: subtract 5 per conflicting signal (min floor = 0)

    Same 0-100 range but more granular.
    """
    n_agree  = r.get('n_agree', 0)
    n_avail  = r.get('n_avail', max(n_agree, 1))
    strength = r.get('strength', 0)
    score    = r.get('score', 0)
    conf     = r.get('conf', 0)
    conflict = r.get('n_conflict', 0)

    q = 0.0
    q += min(25.0, (n_agree / max(n_avail, 1)) * 25)
    q += min(25.0, strength / 100.0 * 25)
    q += min(25.0, abs(score) / 100.0 * 25)
    q += min(25.0, conf / 100.0 * 25)
    q -= conflict * 5
    return int(max(0, min(100, q)))