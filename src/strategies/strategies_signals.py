"""
PredictEngine - strategies_signals.py
All signal detection functions used by StrategyEngine gates.
Imported by strategies_engine.py.

One section per strategy letter:
  _build_candles / _get_klines  — shared candle builder
  K — impulse detection + fib zone
  L — candle S/R levels
  M — volume wall
  N — volume profile (POC/VAH/VAL)
  O — consolidation range
  P — breakout + retest state machine
  Q — funding rate fade
  R — liquidation cascade
  S — OI divergence
  U — density bounce
  W — BTC decorrelation
  X — knife catch
  Z — cross-exchange lag
"""
import time
from collections import defaultdict
from typing import Optional
import engine as E
from config import SPREAD_MAX_PCT
# ══ CANDLE BUILDER ════════════════════════════════════════════════

def _ema_k(vals: list, period: int) -> float:
    """Fast single-value EMA for impulse noise-floor check."""
    k = 2 / (period + 1); e = vals[0]
    for v in vals[1:]: e = v * k + e * (1 - k)
    return e

def _build_candles(sym, lookback_ms=600_000, bucket_ms=60_000):
    """Build closed 1-min candles from price_hist."""
    st = E.sym_state.get(sym)
    if not st: return []
    now  = time.time() * 1000
    hist = [(ts, px) for ts, px in list(st['price_hist']) if now - ts < lookback_ms]
    if len(hist) < 10: return []
    candles = {}
    for ts_ms, px in hist:
        b = int(ts_ms // bucket_ms) * bucket_ms
        if b not in candles:
            candles[b] = {'ts': b, 'o': px, 'h': px, 'l': px, 'c': px}
        else:
            candles[b]['h'] = max(candles[b]['h'], px)
            candles[b]['l'] = min(candles[b]['l'], px)
            candles[b]['c'] = px
    cur = int(now // bucket_ms) * bucket_ms
    return sorted([c for b, c in candles.items() if b < cur], key=lambda x: x['ts'])


def _get_klines(sym, interval):
    """Return REST klines from sym_state, fall back to price_hist candles for 1m."""
    st = E.sym_state.get(sym)
    if not st: return []
    kl = st['klines'].get(interval, [])
    if kl: return kl
    if interval == '1m': return _build_candles(sym, lookback_ms=3_600_000)
    return []


# ══ K: IMPULSE DETECTION ══════════════════════════════════════════

def _detect_impulse(sym, impulse_min_pct=0.50, timeframes=(60_000, 180_000, 300_000)):
    """
    K signal redesign v5 2026-06-04 — WICK-ONLY exhaustion detection.

    ROOT CAUSE of K -$3,187 all-time (66,739 trades) identified:
    Original v4 accepted BODY IMPULSE candles (body >= 60% of range) which are
    TRENDING candles. Fading a trending candle means entering against momentum.
    Body candles continued in their direction ~58% of the time → SL hit.
    conf=24 all-time was the signal: K was entering on weak composite signals.

    v5 CHANGES — wick exhaustion only:
    1. WICK-ONLY: body impulse patterns removed entirely. Only hammer/shooting_star.
    2. STRICTER WICK: shadow must be >= 65% of range (was 55%) — cleaner setup.
    3. CLOSE QUALITY: close must be in top 30% for hammer, bottom 30% for star.
       A hammer closing at midpoint is not an exhaustion signal.
    4. WICK DOMINANCE: lower_wick/upper_wick >= 3.0 (hammer) — pure wick, not doji.
    5. QUALITY SORT: picks highest wick_ratio, not largest size.
       A clean 1.5% wick beats a messy 3% body candle.
    """
    candidates = []

    for bucket_ms in timeframes:
        lookback = max(bucket_ms * 15, 600_000)
        candles = _build_candles(sym, lookback_ms=lookback, bucket_ms=bucket_ms)
        if len(candles) < 2: continue
        tf = f"{bucket_ms//60_000}m"

        for i in range(len(candles)-1, max(len(candles)-3, 0), -1):
            c = candles[i]
            if c['l'] == 0 or c['h'] == c['l']: continue

            total_rng = c['h'] - c['l']
            rng_pct   = total_rng / c['l'] * 100
            if rng_pct < impulse_min_pct: continue

            # Dual-EMA noise floor (item 9): candle must stand out from recent baseline
            recent_k = candles[max(0, i-22):i]
            if len(recent_k) >= 8:
                rng_k = [x['h']-x['l'] for x in recent_k]
                n5k = _ema_k(rng_k, 5); t20k = _ema_k(rng_k, 20)
                if total_rng < max(n5k*2.5, t20k*1.2):
                    continue

            body_top    = max(c['o'], c['c'])
            body_bottom = min(c['o'], c['c'])
            lower_wick  = body_bottom - c['l']
            upper_wick  = c['h'] - body_top
            lower_ratio = lower_wick / total_rng
            upper_ratio = upper_wick / total_rng

            # HAMMER: large lower wick, close near top, dominant lower shadow
            if (lower_ratio >= 0.65
                    and (c['c'] - c['l']) / total_rng >= 0.70
                    and upper_wick > 0 and lower_wick / upper_wick >= 3.0):
                candidates.append({
                    'pattern': 'hammer', 'fade_dir': 'long', 'impulse_dir': 'short',
                    'size_pct': rng_pct, 'body_ratio': (body_top-body_bottom)/total_rng,
                    'wick_ratio': lower_wick / total_rng,
                    'lower_ratio': lower_ratio, 'upper_ratio': upper_ratio,
                    'high': c['h'], 'low': c['l'], 'close': c['c'],
                    'ts_ms': c['ts'], 'timeframe': tf,
                })

            # SHOOTING STAR: large upper wick, close near bottom, dominant upper shadow
            elif (upper_ratio >= 0.65
                    and (c['h'] - c['c']) / total_rng >= 0.70
                    and lower_wick > 0 and upper_wick / lower_wick >= 3.0):
                candidates.append({
                    'pattern': 'shooting_star', 'fade_dir': 'short', 'impulse_dir': 'long',
                    'size_pct': rng_pct, 'body_ratio': (body_top-body_bottom)/total_rng,
                    'wick_ratio': upper_wick / total_rng,
                    'lower_ratio': lower_ratio, 'upper_ratio': upper_ratio,
                    'high': c['h'], 'low': c['l'], 'close': c['c'],
                    'ts_ms': c['ts'], 'timeframe': tf,
                })
            # Body impulse patterns intentionally removed — see docstring above.

    if not candidates:
        return None
    # Sort by wick quality (ratio), not size — clean small wicks > messy large candles
    return max(candidates, key=lambda x: x.get('wick_ratio', 0))

def _in_fib_zone(sym, imp, fib_min=0.20, fib_max=0.85):
    """
    With chill-candle approach, the 'fib zone' check is replaced by:
    simply verifying current price is still near the impulse area
    (hasn't reversed sharply before we enter).
    Entry is valid as long as price hasn't moved > 50% back toward impulse origin.
    """
    st = E.sym_state.get(sym)
    if not st or not st['price']: return False
    px = st['price']
    rng = imp['high'] - imp['low']
    if rng == 0: return False

    # For a DOWN impulse (fade = long): price should still be in lower half
    # i.e. price hasn't already recovered more than 60% of the drop
    if imp['impulse_dir'] == 'short':
        # Drop: from high to low. Recovery threshold = low + 60% of range
        recovery_limit = imp['low'] + rng * 0.60
        return px <= recovery_limit  # still near the lows — valid long entry

    # For an UP impulse (fade = short): price shouldn't have given back >60%
    if imp['impulse_dir'] == 'long':
        giveback_limit = imp['high'] - rng * 0.60
        return px >= giveback_limit  # still near the highs — valid short entry

    return False


# ── Y: Morning/Evening Star (3-candle confirmed impulse fade) ────────────────
def _find_star_pattern(sym, min_pct: float = 0.50, timeframes: tuple = (60_000, 180_000, 300_000)):
    """
    Detect Morning Star setup (→ long) or Evening Star setup (→ short).
    Checks 1m, 3m, 5m candles.

    ENTRY LOGIC: Enter at the CLOSE of C2. C3 is the trade itself.
      C1: Large impulse candle (the move we're fading)
      C2: Exhaustion/pause candle (just closed) — we enter here
      C3: NOT checked — that's the candle we're riding

    Per-TF thresholds:
      1m: min_pct (1.5%), 3m: 0.8x (1.2%), 5m: 0.67x (1.0%)
    """
    matches = []

    tf_min_pct = {60_000: min_pct, 180_000: min_pct * 0.8, 300_000: min_pct * 0.67}

    for bucket_ms in timeframes:
        lookback = max(bucket_ms * 15, 600_000)
        candles = _build_candles(sym, lookback_ms=lookback, bucket_ms=bucket_ms)
        if len(candles) < 2: continue
        tf = f"{bucket_ms//60_000}m"
        effective_min = tf_min_pct.get(bucket_ms, min_pct)

        # C1 = second-to-last completed candle
        # C2 = last completed candle (just closed) — we enter at its close
        c1 = candles[-2]
        c2 = candles[-1]

        # C2 recency: must have closed within 1.5 candles ago
        # On 5m TF, a C2 that closed 8 min ago is stale — pattern has moved on
        now_ms = time.time() * 1000
        c2_age_ms = now_ms - (c2['ts'] + bucket_ms)  # age since C2 closed
        if c2_age_ms > bucket_ms * 1.5: continue  # stale C2 — skip

        if c1['l'] == 0 or c1['h'] == c1['l']: continue
        c1_rng     = c1['h'] - c1['l']
        c1_rng_pct = c1_rng / c1['l'] * 100
        if c1_rng_pct < effective_min: continue

        # C1 body must be meaningful (not a pure doji)
        c1_body = abs(c1['c'] - c1['o'])
        if c1_body / c1_rng < 0.20: continue

        # C1 must stand out from recent candle history — relative impulse strength
        # True exhaustion candles are 2x+ bigger than the recent average range
        # Prevents triggering on small preliminary candles before the real move
        recent = candles[max(0, len(candles)-8):-2]  # 8 candles before C1
        if len(recent) >= 4:
            avg_rng = sum(c['h'] - c['l'] for c in recent) / len(recent)
            if avg_rng > 0 and c1_rng < avg_rng * 1.8:
                continue  # C1 not significantly larger than recent — skip

        # C2: exhaustion/pause — small body OR didn't push beyond C1
        c2_body = abs(c2['c'] - c2['o'])
        c2_body_ok = (c1_body > 0 and c2_body <= c1_body * 0.40)

        if c1['c'] < c1['o']:   # C1 bearish → morning star setup → enter LONG
            c2_no_new_extreme = (c2['l'] >= c1['l'])
            if not (c2_body_ok or c2_no_new_extreme): continue
            # C2 should not make a new low AND ideally stalls near C1 low
            matches.append({
                'pattern':     'morning_star',
                'fade_dir':    'long',
                'impulse_dir': 'short',
                'c1_size_pct': round(c1_rng_pct, 3),
                'c2_body_pct': round(c2_body / max(c1_body, 1e-9) * 100, 1),
                'c2_no_new':   c2_no_new_extreme,
                'high':        c1['h'],
                'low':         c1['l'],
                'ts_ms':       c2['ts'],   # entry at C2 close
                'timeframe':   tf,
            })

        elif c1['c'] > c1['o']: # C1 bullish → evening star setup → enter SHORT
            c2_no_new_extreme = (c2['h'] <= c1['h'])
            if not (c2_body_ok or c2_no_new_extreme): continue
            matches.append({
                'pattern':     'evening_star',
                'fade_dir':    'short',
                'impulse_dir': 'long',
                'c1_size_pct': round(c1_rng_pct, 3),
                'c2_body_pct': round(c2_body / max(c1_body, 1e-9) * 100, 1),
                'c2_no_new':   c2_no_new_extreme,
                'high':        c1['h'],
                'low':         c1['l'],
                'ts_ms':       c2['ts'],   # entry at C2 close
                'timeframe':   tf,
            })

    if not matches:
        return None
    return max(matches, key=lambda m: m['c1_size_pct'])


# ── S/R level constants ──────────────────────────────────────────
SR_MIN_CANDLES  = 30
# 2026-06-05: raised SR quality thresholds after L WR 77%→46.7% collapse.
# Micro-levels (10m lookback, 5 touches) fire on noise in trending conditions.
# Genuine structure needs longer history and more historical tests.
SR_DIST_MAX_PCT = 0.04   # 0.06%→0.04%: only fire very close to level (less approach noise)
SR_BAND_PCT     = 0.15   # clustering band: levels within 0.15% merged
SR_MIN_TOUCHES  = 7      # 5→7: require more historical tests before qualifying as S/R

# ── Module-level caches ──────────────────────────────────────────
_sr_levels:     dict = {}  # sym → list of S/R level dicts
_sr_last_built: dict = {}  # sym → timestamp of last build

def _build_sr_levels(sym):
    now = time.time()
    if now - _sr_last_built.get(sym, 0) < 60: return
    _sr_last_built[sym] = now
    
    st = E.sym_state.get(sym)
    if not st: return
    
    # 1-hour lookback for TPO profile
    lookback_ms = 3_600_000 
    hist = [(ts, px) for ts, px in list(st.get('price_hist', [])) if (now * 1000) - ts < lookback_ms]
    
    if len(hist) < 100: 
        _sr_levels[sym] = []
        return
        
    # Dynamic bucket sizing: ~0.10% of the current price
    px_first = hist[0][1]
    bucket_size = px_first * 0.0010 
    if bucket_size == 0: return
    
    # Build Time-Price Opportunity (TPO) Profile
    profile = defaultdict(int)
    for ts, px in hist:
        if px == 0: continue
        b = round(px / bucket_size) * bucket_size
        profile[b] += 1
        
    # Find Peaks (Points of Control)
    levels = []
    # Sort buckets to find local maxima
    sorted_buckets = sorted(profile.items(), key=lambda x: x[0])
    
    for i in range(1, len(sorted_buckets) - 1):
        prev_b, prev_c = sorted_buckets[i-1]
        curr_b, curr_c = sorted_buckets[i]
        next_b, next_c = sorted_buckets[i+1]
        
        # If this bucket has more time spent than neighbors, it's a structural node
        if curr_c > prev_c and curr_c > next_c and curr_c >= (len(hist) * 0.05):
            levels.append({'price': curr_b, 'touches': curr_c, 'kind': 'both'})
            
    _sr_levels[sym] = sorted(levels, key=lambda x: -x['touches'])


def _find_level_signal(sym) -> Optional[dict]:
    _build_sr_levels(sym)
    st = E.sym_state.get(sym)
    if not st or not st['price']: return None
    px = st['price']; candles = _build_candles(sym)
    if not candles: return None
    last_c = candles[-1]
    for lv in _sr_levels.get(sym, []):
        dist = abs(px - lv['price']) / lv['price'] * 100
        if dist <= SR_DIST_MAX_PCT:  # was hardcoded 0.06 → now uses SR_DIST_MAX_PCT (0.04)
            if lv['kind'] in ('support', 'both') and px >= lv['price']:
                return {'dir': 'long',  'type': 'bounce', 'level': lv}
            if lv['kind'] in ('resistance', 'both') and px <= lv['price']:
                return {'dir': 'short', 'type': 'bounce', 'level': lv}
        close_pct = (last_c['c'] - lv['price']) / lv['price'] * 100
        if lv['kind'] in ('resistance', 'both') and close_pct > 0.08:
            # Break signal: require candle close meaningfully through level (not just tickling it)
            # and check VPIN to avoid firing on low-flow false breakouts
            vpin = E.calc_vpin(sym)
            if vpin is not None and vpin < 0.50: continue  # low flow = likely false break
            return {'dir': 'long',  'type': 'break', 'level': lv}
        if lv['kind'] in ('support', 'both') and close_pct < -0.08:
            vpin = E.calc_vpin(sym)
            if vpin is not None and vpin < 0.50: continue
            return {'dir': 'short', 'type': 'break', 'level': lv}
    return None


# ══ M: VOLUME WALL ════════════════════════════════════════════════

WALL_MIN_USD  = 600_000     # was $1.5M — too rare; $600k still a meaningful wall
WALL_BAND_PCT = 0.0010      # was 0.0005 — slightly wider detection window near price
WALL_PRESENCE_WINDOW_MS = 10_000   # 10s rolling
WALL_PRESENCE_MIN_RATIO = 0.60     # must be present in 60% of snapshots


def _find_volume_wall(sym) -> Optional[dict]:
    st = E.sym_state.get(sym)
    if not st or not st['price'] or not st['bids'] or not st['asks']: return None
    px = st['price']
    bid_w = sum(float(ps)*sz for ps,sz in st['bids'].items()
                if 0 <= (px-float(ps))/px <= WALL_BAND_PCT)
    ask_w = sum(float(ps)*sz for ps,sz in st['asks'].items()
                if 0 <= (float(ps)-px)/px <= WALL_BAND_PCT)
    if bid_w >= WALL_MIN_USD: return {'dir': 'long',  'wall_usd': bid_w, 'side': 'bid'}
    if ask_w >= WALL_MIN_USD: return {'dir': 'short', 'wall_usd': ask_w, 'side': 'ask'}
    return None


def _wall_stable(sym, original_dir) -> bool:
    """
    Check if wall has been consistently present in the last 10 seconds.
    Uses wall_hist snapshots recorded by on_book() in engine.py.
    Prevents false exits from momentary order book refreshes.
    """
    st = E.sym_state.get(sym)
    if not st: return False
    now = time.time() * 1000
    recent = [(ts, bw, aw) for ts, bw, aw in list(st['wall_hist'])
              if now - ts < WALL_PRESENCE_WINDOW_MS]
    if len(recent) < 5: return True   # not enough data yet — assume present
    if original_dir == 'long':
        present = sum(1 for _, bw, _ in recent if bw >= WALL_MIN_USD)
    else:
        present = sum(1 for _, _, aw in recent if aw >= WALL_MIN_USD)
    return present / len(recent) >= WALL_PRESENCE_MIN_RATIO


# ══ N: VOLUME PROFILE ═════════════════════════════════════════════

VP_BUCKET_COUNT = 50
VP_VALUE_AREA   = 0.70
VP_TOUCH_BAND   = 0.25   # was 0.15 — too narrow, only 1 trade fired all session
_vp_cache:      dict = {}
VP_CACHE_TTL    = 300


def _build_volume_profile(sym) -> Optional[dict]:
    now = time.time()
    cached = _vp_cache.get(sym)
    if cached and now - cached['ts'] < VP_CACHE_TTL: return cached
    st = E.sym_state.get(sym)
    if not st: return None
    klines_15m = _get_klines(sym, '15m')
    klines_1d  = _get_klines(sym, '1d')
    all_bars   = klines_15m + klines_1d * 3
    if len(all_bars) < 10: return None
    hi = max(b['h'] for b in all_bars); lo = min(b['l'] for b in all_bars)
    if hi <= lo or lo == 0: return None
    bsz     = (hi - lo) / VP_BUCKET_COUNT
    buckets = [0.0] * VP_BUCKET_COUNT
    for b in all_bars:
        bi = max(0, int((b['l']-lo)/bsz)); bj = min(VP_BUCKET_COUNT-1, int((b['h']-lo)/bsz))
        span = bj-bi+1; vps = b['v']/span
        for i in range(bi, bj+1): buckets[i] += vps
    total = sum(buckets)
    if total == 0: return None
    poc_idx = buckets.index(max(buckets)); poc = lo + (poc_idx+0.5)*bsz
    va_vol = buckets[poc_idx]; li = hi_i = poc_idx
    while va_vol/total < VP_VALUE_AREA:
        ln = buckets[li-1]  if li > 0                  else 0
        hn = buckets[hi_i+1] if hi_i < VP_BUCKET_COUNT-1 else 0
        if ln >= hn and li > 0:   li   -= 1; va_vol += ln
        elif hi_i < VP_BUCKET_COUNT-1: hi_i += 1; va_vol += hn
        else: break
    result = {'poc': poc, 'vah': lo+(hi_i+1)*bsz, 'val': lo+li*bsz,
              'ts': now, 'hi': hi, 'lo': lo}
    _vp_cache[sym] = result
    return result


def _find_vp_signal(sym) -> Optional[dict]:
    vp = _build_volume_profile(sym)
    if not vp: return None
    st = E.sym_state.get(sym)
    if not st or not st['price']: return None
    px = st['price']
    if 0 <= (px-vp['val'])/vp['val']*100 <= VP_TOUCH_BAND:
        return {'dir': 'long',  'type': 'val_bounce', **vp}
    if 0 <= (vp['vah']-px)/vp['vah']*100 <= VP_TOUCH_BAND:
        return {'dir': 'short', 'type': 'vah_bounce', **vp}
    return None


# ══ O: CONSOLIDATION RANGE ════════════════════════════════════════

RANGE_MIN_PCT      = 0.20    # was 0.25
RANGE_MAX_PCT      = 3.50
RANGE_MIN_BARS     = 8       # was 12 — 8 candles still meaningful; 12 rarely satisfied
RANGE_TOUCH_BAND   = 0.10    # was 0.07 — wider band helps detect boundary touches
BREAKOUT_VOL_MULT  = 2.0
_range_cache:  dict = {}
RANGE_CACHE_TTL    = 120


def _detect_consolidation(sym) -> Optional[dict]:
    now = time.time()
    cached = _range_cache.get(sym)
    if cached and now - cached.get('ts',0) < RANGE_CACHE_TTL:
        return cached if cached.get('confirmed') else None
    st = E.sym_state.get(sym)
    if not st: return None
    candles = _get_klines(sym, '1m') or _build_candles(sym, lookback_ms=3_600_000)
    if len(candles) < RANGE_MIN_BARS:
        _range_cache[sym] = {'confirmed': False, 'ts': now}; return None
    recent = candles[-40:]
    hi = max(c['h'] for c in recent); lo = min(c['l'] for c in recent)
    if lo == 0: _range_cache[sym] = {'confirmed': False, 'ts': now}; return None
    width = (hi-lo)/lo*100
    if not (RANGE_MIN_PCT <= width <= RANGE_MAX_PCT):
        _range_cache[sym] = {'confirmed': False, 'ts': now}; return None
    
    # STABILITY CHECK: recent candles must be clustered near midpoint, not bouncing edges
    # If price is currently near an edge, it's a breakout attempt, not stable range
    mid = (hi + lo) / 2
    recent_h = max(c['h'] for c in recent[-10:])  # last 10 candles
    recent_l = min(c['l'] for c in recent[-10:])
    recent_width = (recent_h - recent_l) / lo * 100
    if recent_width > width * 0.85:  # was 0.7 — too tight, valid ranges naturally oscillate widely
        _range_cache[sym] = {'confirmed': False, 'ts': now}; return None
    
    inside = sum(1 for c in recent if c['h'] <= hi*1.001 and c['l'] >= lo*0.999)
    if inside < RANGE_MIN_BARS:
        _range_cache[sym] = {'confirmed': False, 'ts': now}; return None
    touches_top = sum(1 for c in recent if abs(c['h']-hi)/hi*100 <= 0.20)
    touches_bot = sum(1 for c in recent if abs(c['l']-lo)/lo*100 <= 0.20)
    if touches_top < 2 or touches_bot < 2:
        _range_cache[sym] = {'confirmed': False, 'ts': now}; return None
    avg_vol = sum(c.get('v',0) for c in recent) / max(len(recent),1)
    result  = {'confirmed': True, 'ts': now, 'top': hi, 'bot': lo,
               'width_pct': width, 'avg_vol': avg_vol,
               'touches_top': touches_top, 'touches_bot': touches_bot}
    _range_cache[sym] = result
    return result


def _find_range_signal(sym) -> Optional[dict]:
    rng = _detect_consolidation(sym)
    if not rng: return None
    st = E.sym_state.get(sym)
    if not st or not st['price']: return None
    px = st['price']
    # Volume fakeout filter
    klines_1m = _get_klines(sym, '1m')
    if klines_1m and rng.get('avg_vol',0) > 0:
        if klines_1m[-1].get('v',0) > rng['avg_vol'] * BREAKOUT_VOL_MULT:
            return None
    dist_top = (rng['top']-px)/rng['top']*100
    dist_bot = (px-rng['bot'])/rng['bot']*100
    if 0 <= dist_bot <= RANGE_TOUCH_BAND:
        # Require rejection: wick must have touched the level AND close above it
        if klines_1m and len(klines_1m) >= 1:
            last_c   = klines_1m[-1]
            wick_low = last_c.get('l', px)
            close    = last_c.get('c', px)
            if close <= rng['bot'] * 1.001:
                return None  # breakdown — close at/below bottom
            if wick_low > rng['bot'] * 1.002:
                return None  # wick never reached the level — premature signal
        tp = (rng['top']-px)/px*100*0.85
        return {'dir':'long',  'type':'range_bounce', 'top':rng['top'], 'bot':rng['bot'], 'tp_pct':tp}
    if 0 <= dist_top <= RANGE_TOUCH_BAND:
        # Require rejection: wick must have touched the level AND close below it
        if klines_1m and len(klines_1m) >= 1:
            last_c   = klines_1m[-1]
            wick_hi  = last_c.get('h', px)
            close    = last_c.get('c', px)
            if close >= rng['top'] * 0.999:
                return None  # breakout — close at/above top
            if wick_hi < rng['top'] * 0.998:
                return None  # wick never reached the level — premature signal
        tp = (px-rng['bot'])/px*100*0.85
        return {'dir':'short', 'type':'range_bounce', 'top':rng['top'], 'bot':rng['bot'], 'tp_pct':tp}
    return None


# ══ P: BREAKOUT + RETEST ══════════════════════════════════════════

_bo_state: dict = {}
BO_VOLUME_CONFIRM = 1.5
BO_RETEST_BAND    = 0.15
BO_RETEST_TIMEOUT = 900


def _update_breakout_state(sym) -> Optional[dict]:
    st = E.sym_state.get(sym)
    if not st or not st['price']: return None
    px = st['price']; now = time.time()
    state = _bo_state.get(sym, {'state': 'IDLE'})
    s     = state['state']
    klines_1m = _get_klines(sym, '1m')
    if len(klines_1m) < RANGE_MIN_BARS: return None

    if s in ('IDLE','RANGING'):
        rng = _detect_consolidation(sym)
        if rng:
            _bo_state[sym] = {**state, 'state':'RANGING',
                               'top':rng['top'],'bot':rng['bot'],'avg_vol':rng['avg_vol']}
            if len(klines_1m) >= 2:
                last    = klines_1m[-2]; avg_vol = rng['avg_vol']
                vol_ok  = avg_vol==0 or last.get('v',0) > avg_vol*BO_VOLUME_CONFIRM
                broke_up   = last['c'] > rng['top']*1.0008
                broke_down = last['c'] < rng['bot']*0.9992
                if vol_ok and broke_up:
                    _bo_state[sym] = {'state':'BROKEN','direction':'long',
                        'level':rng['top'],'broken_ts':now,
                        'top':rng['top'],'bot':rng['bot'],'avg_vol':avg_vol}
                elif vol_ok and broke_down:
                    _bo_state[sym] = {'state':'BROKEN','direction':'short',
                        'level':rng['bot'],'broken_ts':now,
                        'top':rng['top'],'bot':rng['bot'],'avg_vol':avg_vol}
        else:
            _bo_state[sym] = {'state':'IDLE'}
        return None

    if s == 'BROKEN':
        if now - state.get('broken_ts',now) > BO_RETEST_TIMEOUT:
            _bo_state[sym] = {'state':'IDLE'}; return None
        if abs(px-state['level'])/state['level']*100 <= BO_RETEST_BAND:
            _bo_state[sym] = {**state, 'state':'RETEST', 'retest_ts':now}
        return None

    if s == 'RETEST':
        if now - state.get('retest_ts',now) > 120:
            _bo_state[sym] = {'state':'IDLE'}; return None
        level = state['level']; direction = state['direction']
        # Require candle CLOSE confirmation — not just price touch
        # For long: retest touched level AND last candle closed ABOVE it (rejection wick)
        # For short: retest touched level AND last candle closed BELOW it
        klines = _get_klines(sym, '1m') or []
        last_close = klines[-1].get('c', px) if klines else px
        if direction=='long'  and px > level*1.0005 and last_close > level*1.0003:
            _bo_state[sym] = {'state':'IDLE'}
            return {'dir':'long',  'type':'retest_bounce','level':level,
                    'top':state['top'],'bot':state['bot']}
        if direction=='short' and px < level*0.9995 and last_close < level*0.9997:
            _bo_state[sym] = {'state':'IDLE'}
            return {'dir':'short', 'type':'retest_bounce','level':level,
                    'top':state['top'],'bot':state['bot']}
        if direction=='long'  and px < level*0.9990: _bo_state[sym] = {'state':'IDLE'}
        if direction=='short' and px > level*1.0010: _bo_state[sym] = {'state':'IDLE'}
    return None


# ══ Q: FUNDING RATE FADE ══════════════════════════════════════════
#
# Binance publishes the next funding rate every second in @markPrice@1s.
# Rates are 8h settlement rates expressed as decimals:
#   0.0001 = 0.01% per 8h (normal / neutral)
#   0.0005 = 0.05% per 8h (elevated long bias — fade short)
#  -0.0003 = -0.03% per 8h (elevated short bias — fade long)
#
# Logic: extreme positive rate → market is overcrowded long → fade short.
#        extreme negative rate → market overcrowded short → fade long.
#
# We also check trend: if rate has been extreme for ≥3 consecutive readings
# (≥3 seconds), the signal is more reliable than a single spike.

FUNDING_LONG_THR  =  0.0005   # +0.05%/8h → fade SHORT (longs paying a lot)
FUNDING_SHORT_THR = -0.0003   # -0.03%/8h → fade LONG  (shorts paying a lot)
FUNDING_TREND_MIN =  3        # min consecutive extreme readings to confirm


def _find_funding_signal(sym) -> Optional[dict]:
    """
    Return funding fade signal if rate is extreme and sustained.
    Returns {'dir': 'long'|'short', 'rate': float, 'sustained': int} or None.
    """
    st = E.sym_state.get(sym)
    if not st: return None
    rate = st.get('funding_rate', 0.0)
    hist = list(st.get('funding_hist', []))

    # Need at least a few readings for trend confirmation
    if len(hist) < FUNDING_TREND_MIN: return None

    recent_rates = [r for _, r in hist[-FUNDING_TREND_MIN:]]

    if rate >= FUNDING_LONG_THR:
        # All recent readings must also be elevated (not a transient spike)
        if all(r >= FUNDING_LONG_THR * 0.7 for r in recent_rates):
            return {'dir': 'short', 'rate': rate, 'sustained': len(recent_rates)}

    if rate <= FUNDING_SHORT_THR:
        if all(r <= FUNDING_SHORT_THR * 0.7 for r in recent_rates):
            return {'dir': 'long', 'rate': rate, 'sustained': len(recent_rates)}

    return None


# ══ R: LIQUIDATION CASCADE ════════════════════════════════════════
#
# A cascade is: large one-directional liquidations accumulating rapidly,
# AND the rate is accelerating (last 10s > prior 20s average).
#
# long_liqs  = LONG positions being liquidated → price being forced DOWN
#            → cascade direction is SHORT (ride the selling)
# short_liqs = SHORT positions being liquidated → price being forced UP
#            → cascade direction is LONG (ride the buying)
#
# Note: this is the OPPOSITE of K (impulse fade). K fades a price spike;
# R rides a liquidation-driven forced move. The edge: cascades don't stop
# until the liq queue is exhausted, which typically takes 60–120s.

CASCADE_USD_30S     = 500_000    # was $1M — lowered to catch smaller/earlier cascades
CASCADE_USD_10S     = 150_000    # was $300k — proportional reduction
CASCADE_ACCEL_RATIO = 1.5        # recent rate must be 1.5× the base rate


def _find_cascade_signal(sym) -> Optional[dict]:
    """
    Detect an accelerating liquidation cascade.
    Returns {'dir': 'long'|'short', 'usd_30s': float, 'usd_10s': float} or None.
    """
    st = E.sym_state.get(sym)
    if not st: return None
    now = time.time() * 1000
    hist = list(st.get('liq_cascade_hist', []))
    if len(hist) < 5: return None

    recent_30s = [(il, v) for ts, il, v in hist if now - ts < 30_000]
    recent_10s = [(il, v) for ts, il, v in hist if now - ts < 10_000]
    if not recent_30s: return None

    long_30s  = sum(v for il, v in recent_30s if il)
    short_30s = sum(v for il, v in recent_30s if not il)
    long_10s  = sum(v for il, v in recent_10s if il)
    short_10s = sum(v for il, v in recent_10s if not il)

    # Check for a long-liq cascade (market selling, ride short)
    if long_30s >= CASCADE_USD_30S:
        base_rate = (long_30s - long_10s) / 20_000 * 10_000   # per 10s equivalent
        if base_rate > 0 and long_10s / max(base_rate, 1) >= CASCADE_ACCEL_RATIO:
            return {'dir': 'short', 'usd_30s': long_30s, 'usd_10s': long_10s}
        # Even without acceleration, accept very large cascades
        if long_30s >= CASCADE_USD_30S * 3:
            return {'dir': 'short', 'usd_30s': long_30s, 'usd_10s': long_10s}

    # Check for a short-liq cascade (market buying, ride long)
    if short_30s >= CASCADE_USD_30S:
        base_rate = (short_30s - short_10s) / 20_000 * 10_000
        if base_rate > 0 and short_10s / max(base_rate, 1) >= CASCADE_ACCEL_RATIO:
            return {'dir': 'long', 'usd_30s': short_30s, 'usd_10s': short_10s}
        if short_30s >= CASCADE_USD_30S * 3:
            return {'dir': 'long', 'usd_30s': short_30s, 'usd_10s': short_10s}

    return None


# ══ S: OI DIVERGENCE ══════════════════════════════════════════════
#
# Open Interest = total open contracts. When price moves but OI moves
# opposite, it signals position closing, not new positioning:
#
#   Price ↑ + OI ↓ → longs taking profit / closing → exhaustion → fade short
#   Price ↓ + OI ↓ → shorts covering / closing → short squeeze potential → fade long
#   Price ↑ + OI ↑ → new longs entering → continuation (no signal from us)
#   Price ↓ + OI ↑ → new shorts entering → continuation (no signal from us)
#
# Requires ≥10 OI samples (polled every ~30s = ~5 minutes of history).
# Price comparison uses same window as OI window.

OI_DIV_MIN_SAMPLES  = 10      # min OI history points needed
OI_DIV_PRICE_PCT    = 0.35    # was 0.20 — weak divergences entered too early
OI_DIV_OI_PCT       = 0.50    # was 0.30 — need stronger OI contra-move
OI_DIV_WINDOW_S     = 300     # look back 5 min for both price and OI


def _find_oi_divergence(sym) -> Optional[dict]:
    """
    Detect OI diverging from price direction.
    Returns {'dir': 'long'|'short', 'price_chg': float, 'oi_chg': float} or None.
    """
    st = E.sym_state.get(sym)
    if not st or not st['price']: return None

    oi_hist = list(st.get('oi_hist', []))
    if len(oi_hist) < OI_DIV_MIN_SAMPLES: return None

    now = time.time()
    window_oi = [(ts, oi) for ts, oi in oi_hist if now - ts < OI_DIV_WINDOW_S]
    if len(window_oi) < 5: return None

    oi_start, oi_end = window_oi[0][1], window_oi[-1][1]
    if oi_start == 0: return None
    oi_chg_pct = (oi_end - oi_start) / oi_start * 100

    # Price change over same window
    ph = list(st['price_hist'])
    now_ms = now * 1000
    window_px = [p for ts, p in ph if now_ms - ts < OI_DIV_WINDOW_S * 1000]
    if len(window_px) < 5: return None
    px_start, px_end = window_px[0], window_px[-1]
    if px_start == 0: return None
    price_chg_pct = (px_end - px_start) / px_start * 100

    # price_up + OI_down = longs closing (momentum fading) → SHORT ✅
    if price_chg_pct >= OI_DIV_PRICE_PCT and oi_chg_pct <= -OI_DIV_OI_PCT:
        return {'dir': 'short', 'price_chg': price_chg_pct, 'oi_chg': oi_chg_pct,
                'type': 'price_up_oi_down'}

    # price_down + OI_down = LONG liquidation cascade → price continues DOWN → skip
    # (was incorrectly labeled 'shorts covering' — actually longs being liquidated)

    # price_down + OI_UP = new longs entering on the dip = real buying → LONG ✅
    if price_chg_pct <= -OI_DIV_PRICE_PCT and oi_chg_pct >= OI_DIV_OI_PCT:
        return {'dir': 'long', 'price_chg': price_chg_pct, 'oi_chg': oi_chg_pct,
                'type': 'price_down_oi_up'}

    return None




# ══ U: DENSITY BOUNCE ═════════════════════════════════════════════
#
# A "density" is a large order cluster that price has approached and bounced
# from multiple times. This is fundamentally different from M (which just
# checks if a big wall exists right now). The multi-touch requirement confirms
# the level is actively defended, not a transient large order.
#
# Algorithm:
#   1. Find current bid/ask wall ≥ DENSITY_MIN_USD within DENSITY_BAND_PCT
#   2. Check price_hist for ≥ DENSITY_MIN_TOUCHES approaches within last 30 min
#      (approach = price came within 0.15% of the wall level)
#   3. Require wall has been present ≥ DENSITY_STABLE_SEC before entry
#   4. Price must be APPROACHING (moving toward the wall in last 5s)
#
# Exit: same as M — wall_stable() check every tick.

DENSITY_MIN_USD      = 800_000    # was $2M — too rare; $800k still meaningful institutional wall
DENSITY_BAND_PCT     = 0.0010     # was 0.0008 — slightly wider detection band
DENSITY_TOUCH_BAND   = 0.0020     # was 0.0015 — wider approach detection
DENSITY_MIN_TOUCHES  = 1          # was 2 — first touch already shows defended level
DENSITY_STABLE_SEC   = 15         # was 30s — wall only needs 15s of stability
DENSITY_LOOKBACK_MS  = 1_800_000  # 30 min lookback


def _find_density_signal(sym) -> Optional[dict]:
    """
    Find a multi-touch confirmed wall and return signal if price is approaching.
    Returns {'dir': 'long'|'short', 'wall_usd': float, 'level': float, 'touches': int} or None.
    """
    st = E.sym_state.get(sym)
    if not st or not st['price'] or not st['bids'] or not st['asks']:
        return None
    px = st['price']

    # Find the dominant wall in the near-price band
    bid_w = sum(float(ps)*sz for ps, sz in st['bids'].items()
                if 0 <= (px - float(ps))/px <= DENSITY_BAND_PCT)
    ask_w = sum(float(ps)*sz for ps, sz in st['asks'].items()
                if 0 <= (float(ps) - px)/px <= DENSITY_BAND_PCT)

    if bid_w >= DENSITY_MIN_USD:
        wall_dir, wall_usd = 'long', bid_w
        # Best bid wall price = highest bid in band
        wall_px = max((float(ps) for ps in st['bids']
                       if 0 <= (px - float(ps))/px <= DENSITY_BAND_PCT), default=0)
    elif ask_w >= DENSITY_MIN_USD:
        wall_dir, wall_usd = 'short', ask_w
        wall_px = min((float(ps) for ps in st['asks']
                       if 0 <= (float(ps) - px)/px <= DENSITY_BAND_PCT), default=0)
    else:
        return None

    if wall_px == 0: return None

    # Check wall has been stable (present in wall_hist for ≥ DENSITY_STABLE_SEC)
    now_ms = time.time() * 1000
    wh = [(ts, bw, aw) for ts, bw, aw in list(st.get('wall_hist', []))
          if now_ms - ts < DENSITY_STABLE_SEC * 1000]
    if len(wh) < 5: return None   # not enough history yet
    if wall_dir == 'long':
        stable_ratio = sum(1 for _, bw, _ in wh if bw >= DENSITY_MIN_USD) / len(wh)
    else:
        stable_ratio = sum(1 for _, _, aw in wh if aw >= DENSITY_MIN_USD) / len(wh)
    if stable_ratio < 0.70: return None   # wall flickering, not stable
    # Count prior approaches to this price level in the last 30 min
    ph = list(st['price_hist'])
    touches = sum(1 for ts, p in ph
                  if now_ms - ts < DENSITY_LOOKBACK_MS
                  and abs(p - wall_px) / wall_px <= DENSITY_TOUCH_BAND)
    if touches < DENSITY_MIN_TOUCHES: return None
    # Price must be APPROACHING the wall (moving toward it in last 5s)
    px5 = [p for ts, p in ph if now_ms - ts < 5000]
    if len(px5) < 3: return None
    move = px5[-1] - px5[0]
    if wall_dir == 'long'  and move > 0: return None   # moving away from bid wall
    if wall_dir == 'short' and move < 0: return None   # moving away from ask wall

    return {'dir': wall_dir, 'wall_usd': wall_usd, 'level': wall_px, 'touches': touches}


# ══ W: BTC DECORRELATION ══════════════════════════════════════════
#
# Most alts correlate tightly with BTC on short timeframes (2–10 min).
# When BTC makes a clear directional move but an alt diverges (moves opposite
# or stays flat), the alt is temporarily out of sync. The divergence resolves
# by the alt snapping back toward BTC's direction.
#
# Signal:
#   BTC 10min move ≥ +0.20% AND alt 10min move ≤ -0.10% → long alt (snap up)
#   BTC 10min move ≤ -0.20% AND alt 10min move ≥ +0.10% → short alt (snap down)
#   BTC 10min move ≥ +0.20% AND alt move in range [-0.10%, +0.10%] → long alt (flat divergence)
#   BTC 10min move ≤ -0.20% AND alt move in range [-0.10%, +0.10%] → short alt (flat divergence)
#
# FIX (zero fires in 20:22 session): DECOR_BTC_MOVE_MIN was 0.30%.
#   Session had 195 trades vs 471 previously — lower volatility.
#   BTC likely did not move 0.30% in any single 10-min window.
#   Reducing to 0.20% captures the same decorrelation pattern at lower vol.
#   The alt divergence requirement (DECOR_ALT_DIV_MIN = 0.10%) ensures we
#   don't fire when alt is just following BTC at a smaller magnitude.
#
#   Also added flat-correlation case: if alt moves <0.10% while BTC moves
#   0.20%+ in either direction, the alt is lagging (not diverging).
#   This is the most common decorrelation pattern in low-vol sessions.
#
# Veto conditions (unchanged):
#   - Alt has its own strong news/catalyst (OI spike, liq cascade active)
#   - BTC move < DECOR_BTC_MOVE_MIN (noise)
#
# Uses btc_hist from engine.py and per-sym price_hist.

DECOR_BTC_MOVE_MIN   = 0.20   # was 0.30 — too strict; missed entire low-vol session
DECOR_ALT_DIV_MIN    = 0.10   # was 0.15 — lowered to match new BTC threshold
DECOR_FLAT_THR       = 0.10   # alt move within ±this% while BTC moves = flat divergence
DECOR_WINDOW_MS      = 600_000  # 10 min lookback (unchanged)


def _find_decorrelation_signal(sym) -> Optional[dict]:
    """
    Detect BTC/alt divergence and return snap-back direction.
    Returns {'dir': 'long'|'short', 'btc_move': float, 'alt_move': float,
             'divergence': float, 'case': 'opposite'|'flat'} or None.
    """
    if sym == 'BTCUSDT': return None
    st = E.sym_state.get(sym)
    if not st or not st['price']: return None

    now_ms = time.time() * 1000  # type: ignore
    btc_h  = list(E.btc_hist)
    alt_h  = list(st['price_hist'])

    btc_w = [p for ts, p in btc_h if now_ms - ts < DECOR_WINDOW_MS]
    alt_w = [p for ts, p in alt_h if now_ms - ts < DECOR_WINDOW_MS]

    if len(btc_w) < 10 or len(alt_w) < 10: return None

    btc_move = (btc_w[-1] - btc_w[0]) / btc_w[0] * 100
    alt_move = (alt_w[-1] - alt_w[0]) / alt_w[0] * 100

    if abs(btc_move) < DECOR_BTC_MOVE_MIN: return None

    # ONLY fire on true opposite divergence — flat case removed.
    # snap30=-0.061 across W session proved flat entries open in wrong direction.
    # Alt must move OPPOSITE to BTC by at least DECOR_ALT_DIV_MIN to qualify.

    # Case 1: BTC up, alt moving opposite → long alt (snap up)
    if btc_move >= DECOR_BTC_MOVE_MIN:
        if alt_move <= -DECOR_ALT_DIV_MIN:
            return {'dir': 'long', 'btc_move': btc_move, 'alt_move': alt_move,
                    'divergence': btc_move - alt_move, 'case': 'opposite'}

    # Case 2: BTC down, alt moving opposite → short alt (snap down)
    if btc_move <= -DECOR_BTC_MOVE_MIN:
        if alt_move >= DECOR_ALT_DIV_MIN:
            return {'dir': 'short', 'btc_move': btc_move, 'alt_move': alt_move,
                    'divergence': alt_move - btc_move, 'case': 'opposite'}

    return None


# ══ X: KNIFE CATCH (НОЖИ) ═════════════════════════════════════════
#
# A "knife" (нож) is a single 1m candle with a long wick that reverses.
# Pattern:
#   Downside knife: candle LOW is ≥1.5% below the OPEN, but candle CLOSE
#                   is within 0.4% of the OPEN → wick was a stop-hunt → long
#   Upside knife:   candle HIGH is ≥1.5% above the OPEN, but CLOSE is within
#                   0.4% of OPEN → wick was a stop-hunt → short
#
# Key filters (from trader notes):
#   - "Only in sharp impulse only" — the candle must be isolated, not in a trend
#     (prior 3 candles should not all be going the same direction as the wick)
#   - Tape must not be aggressive in wick direction AFTER the candle closed
#     (if it is, it's a cascade not a stop-hunt)
#   - The wick must be the LARGEST candle in the last 5 candles by range
#     (confirms it's abnormal, not just volatility)
#
# Entry: on the NEXT tick after the knife candle closes.
# Exit: short hold (60–120s), trail from entry.

KNIFE_WICK_MIN_PCT   = 0.80   # was 1.50 — 1.5% wick almost never occurs; 0.80% still meaningful stop-hunt
KNIFE_CLOSE_MAX_PCT  = 0.60   # was 0.40 — allow slightly less perfect close-back
KNIFE_IS_BIGGEST     = True   # must be biggest range candle in last 5
KNIFE_LOOKBACK_CANDLES = 5    # comparison window


def _find_knife_signal(sym) -> Optional[dict]:
    """
    Detect a completed knife (stop-hunt wick) candle.
    Returns {'dir': 'long'|'short', 'wick_pct': float, 'close_pct': float} or None.
    """
    candles = _build_candles(sym)
    if len(candles) < KNIFE_LOOKBACK_CANDLES + 1: return None

    # The last CLOSED candle (current candle is still forming)
    knife = candles[-1]
    prior = candles[-KNIFE_LOOKBACK_CANDLES-1:-1]   # last N candles before knife

    if knife['o'] == 0 or knife['l'] == 0: return None

    # Calculate wick sizes
    down_wick = (knife['o'] - knife['l']) / knife['o'] * 100  # wick below open
    up_wick   = (knife['h'] - knife['o']) / knife['o'] * 100  # wick above open
    close_pct = abs(knife['c'] - knife['o']) / knife['o'] * 100

    # Biggest-range check
    knife_range = (knife['h'] - knife['l']) / knife['l'] * 100
    prior_ranges = [(c['h'] - c['l']) / c['l'] * 100 for c in prior if c['l'] > 0]
    if prior_ranges and knife_range <= max(prior_ranges): return None

    # Trend filter: prior candles should NOT all be going in wick direction
    # (if they are, this is a cascade continuation, not a stop-hunt)
    prior_moves = [(c['c'] - c['o']) for c in prior[-3:] if c['o'] > 0]

    # Downside knife → long
    if down_wick >= KNIFE_WICK_MIN_PCT and close_pct <= KNIFE_CLOSE_MAX_PCT:
        # Veto: if last 3 candles were all down (trend, not isolated)
        if len(prior_moves) >= 3 and all(m < 0 for m in prior_moves): return None
        # Tape veto: check if buy CVD has recovered since wick
        st = E.sym_state.get(sym)
        if st:
            now_ms = time.time() * 1000  # type: ignore
            # Recent tape in last 30s should show buying, not continued selling
            tape_30s = [(v, b) for ts, _, v, b in list(st['trade_tape']) if now_ms-ts < 30_000]
            if tape_30s:
                buy_vol  = sum(v for v, b in tape_30s if b)
                sell_vol = sum(v for v, b in tape_30s if not b)
                if sell_vol > buy_vol * 2.0: return None  # tape still selling hard
        return {'dir': 'long',  'wick_pct': down_wick, 'close_pct': close_pct,
                'knife_candle': knife}

    # Upside knife → short
    if up_wick >= KNIFE_WICK_MIN_PCT and close_pct <= KNIFE_CLOSE_MAX_PCT:
        if len(prior_moves) >= 3 and all(m > 0 for m in prior_moves): return None
        st = E.sym_state.get(sym)
        if st:
            now_ms = time.time() * 1000  # type: ignore
            tape_30s = [(v, b) for ts, _, v, b in list(st['trade_tape']) if now_ms-ts < 30_000]
            if tape_30s:
                buy_vol  = sum(v for v, b in tape_30s if b)
                sell_vol = sum(v for v, b in tape_30s if not b)
                if buy_vol > sell_vol * 2.0: return None  # tape still buying hard
        return {'dir': 'short', 'wick_pct': up_wick, 'close_pct': close_pct,
                'knife_candle': knife}

    return None


# ══ Z: CROSS-EXCHANGE LAG SIGNAL ═════════════════════════════════
#
# Config constants — tune these based on monitoring session data.
# Z detection constants — tuned from 2026-05-10 session data
# div < 0.05% → 6% WR (too small); div >= 0.20% → 27% WR
# LAG_MOVE_THR raised: more Binance move = cleaner, more committed signal
# LAG_DIVERGENCE_MIN raised: require gap still open at entry time
LAG_MOVE_THR      = 0.30   # 2026-05-27: raised 0.20→0.30 after direction flip
                              # need larger moves to clear the 0.10% fee on 30s hold
LAG_WINDOW_S      = 8.0    # look-back window for Binance move detection (unchanged)
LAG_MIN_MS        = 80     # was 150ms — Tokyo server is ~8ms from Binance, ~15-25ms from MEXC/Bybit
                           # Real exchange repricing lag starts at ~50-80ms from Tokyo
                           # 150ms was calibrated for US/EU servers; too high for Asia
LAG_MAX_MS        = 3000   # if lag > 3s, exchange feed is probably down (unchanged)
LAG_MIN_EXCHANGES = 1      # at least N exchanges must show the lag (unchanged)
LAG_DIVERGENCE_MIN= 0.08   # was 0.05 — require more gap still open at entry
LAG_MONITOR_ONLY  = False  # True = log to CSV but don't trade (data collection)

# Mid-trade snap30 filter thresholds (used in check_outcomes for Z)
# Data: snap30 > +0.10% → 79% WR; snap30 < -0.10% → 1% WR
LAG_SNAP30_HOLD_THR  = 0.05   # snap30 > this → block rev exit (Z: 68% WR at 0.05-0.15%, 97% at >0.15%)
LAG_SNAP30_EXIT_THR  = -0.08  # snap30 < this → allow early inertia (Z: 0% WR at <-0.10%)


def _find_lag_signal(sym) -> Optional[dict]:
    """
    Detect Binance price move that hasn't propagated to other exchanges yet.

    Returns {
      'dir': 'long'|'short',
      'bnx_move': float,          % Binance moved in LAG_WINDOW_S
      'lagging': [                exchanges still behind
        {'exchange': str, 'lag_ms': float, 'divergence_pct': float}
      ],
      'best_lag_ms': float,       largest lag seen
      'best_div_pct': float,      largest price divergence
    } or None.
    """
    snap = E.get_lag_snapshot(sym)
    bnx  = snap.get('binance', {})
    bnx_px = bnx.get('price', 0.0)
    if bnx_px <= 0:
        return None

    # Measure Binance move over last LAG_WINDOW_S seconds
    st = E.sym_state.get(sym)
    if not st:
        return None
    now_ms    = time.time() * 1000
    window_ms = LAG_WINDOW_S * 1000
    ph        = list(st['price_hist'])
    old_px_list = [p for ts, p in ph if window_ms * 0.8 <= now_ms - ts <= window_ms * 1.2]
    if not old_px_list:
        return None
    old_px   = old_px_list[0]
    if old_px <= 0:
        return None
    bnx_move = (bnx_px - old_px) / old_px * 100

    if abs(bnx_move) < LAG_MOVE_THR:
        return None

    # DIRECTION: mean reversion
    # short when Binance ABOVE lag (it spiked up, will revert down) ✅
    # long only when lag exchanges RALLIED above Binance (Binance will follow up)
    # NOT long when Binance DROPPED below lag (lag will follow Binance down)
    if bnx_move > 0:
        direction = 'short'  # Binance above lag → short (revert down)
    else:
        # Binance below lag — check WHY
        # If Binance dropped (bnx_move < 0): lag exchanges will follow Binance DOWN → skip long
        # If lag exchanges rose above Binance: Binance will catch up UP → long OK
        # Heuristic: if |bnx_move| is large relative to divergence, Binance was the mover
        best_div_abs = max((abs((snap.get(ex,{}).get('price',bnx_px)-bnx_px)/bnx_px*100)
                           for ex in E.LAG_EXCHANGES if snap.get(ex,{}).get('price',0) > 0),
                          default=0)
        if best_div_abs > 0 and abs(bnx_move) > best_div_abs * 0.6:
            return None  # Binance was the mover (dropped) — lag will follow down
        direction = 'long'

    # Check each lag exchange
    lagging = []
    for ex in E.LAG_EXCHANGES:
        ex_data = snap.get(ex, {})
        ex_px   = ex_data.get('price', 0.0)
        lag_ms  = ex_data.get('lag_ms')

        if ex_px <= 0 or lag_ms is None:
            continue
        if not (LAG_MIN_MS <= lag_ms <= LAG_MAX_MS):
            continue

        # Check price still diverges (exchange hasn't caught up)
        divergence = (bnx_px - ex_px) / ex_px * 100
        # For a long signal (Binance up): divergence should be positive (ex still lower)
        # For a short signal (Binance down): divergence should be negative (ex still higher)
        if direction == 'long'  and divergence < LAG_DIVERGENCE_MIN:
            continue
        if direction == 'short' and divergence > -LAG_DIVERGENCE_MIN:
            continue

        lagging.append({
            'exchange':     ex,
            'lag_ms':       round(lag_ms, 1),
            'divergence_pct': round(divergence, 4),
            'ex_price':     ex_px,
        })

    if len(lagging) < LAG_MIN_EXCHANGES:
        return None

    best_lag = max(l['lag_ms']          for l in lagging)
    best_div = max(abs(l['divergence_pct']) for l in lagging)

    return {
        'dir':          direction,
        'bnx_move':     round(bnx_move, 4),
        'bnx_px':       bnx_px,
        'lagging':      lagging,
        'best_lag_ms':  best_lag,
        'best_div_pct': best_div,
        'monitor_only': LAG_MONITOR_ONLY,
    }


# ── Z_LEAD: Leading-exchange signal (inverted lag arb) ───────────────────────
# Instead of watching Binance move and waiting for lag exchanges to catch up
# (which we see 120ms late due to WS batching), watch MEXC/Bybit move FIRST
# and enter Binance before it reprices.
#
# Why this works:
#   MEXC WS latency p50=15ms — we see MEXC price changes in real-time
#   Binance WS latency p50=120ms — we CAN'T see Binance changes in real-time
#   When MEXC moves +0.3% but Binance hasn't moved yet → Binance will follow
#   Enter Binance LONG via REST (5ms) at T+20ms from signal
#   Binance reprices to match MEXC at T+60ms → exit with profit
#   Our entry is 2x faster than convergence window

LEAD_MOVE_THR     = 0.20    # % move on lead exchange to trigger signal
LEAD_WINDOW_S     = 5.0     # look-back window for lead exchange move
LEAD_BNX_STALE_S  = 2.0     # Binance price must be stale (not repriced yet)
LEAD_MIN_DIV      = 0.10    # minimum divergence between lead exchange and Binance

def _find_lead_signal(sym) -> Optional[dict]:
    """
    Detect when a lead exchange (MEXC/Bybit) has moved significantly
    but Binance has NOT yet repriced — enter Binance in the move direction.

    This is the inverse of _find_lag_signal:
      _find_lag_signal:  Binance moved → other exchanges lag → trade other exchanges
      _find_lead_signal: MEXC/Bybit moved → Binance lags → trade Binance

    Returns signal dict or None.
    """
    snap = E.get_lag_snapshot(sym)
    bnx  = snap.get('binance', {})
    bnx_px = bnx.get('price', 0.0)
    if bnx_px <= 0:
        return None

    # Binance price must be relatively stale (not repriced in last LEAD_BNX_STALE_S)
    bnx_ts = bnx.get('ts', 0.0)
    bnx_age_s = time.time() - bnx_ts
    # Note: bnx_ts is the exchange timestamp from the WS message
    # Due to WS batching, Binance price can be 100-150ms stale normally
    # We want it to be stale in a WAY that suggests it hasn't moved yet

    best_lead = None
    best_move = 0.0
    best_ex   = None

    # Check each lead exchange for a significant move
    for ex in E.LAG_EXCHANGES:
        ex_data = snap.get(ex, {})
        ex_px   = ex_data.get('price', 0.0)
        if ex_px <= 0:
            continue

        # Measure lead exchange move over last LEAD_WINDOW_S
        ex_hist = ex_data.get('hist')
        if not ex_hist or len(ex_hist) < 2:
            continue

        now_ms = time.time() * 1000
        window_ms = LEAD_WINDOW_S * 1000
        old_list = [p for ts, p in list(ex_hist)
                    if window_ms * 0.7 <= now_ms - ts <= window_ms * 1.3]
        if not old_list:
            continue
        old_px = old_list[0]
        if old_px <= 0:
            continue

        ex_move = (ex_px - old_px) / old_px * 100
        if abs(ex_move) < LEAD_MOVE_THR:
            continue

        # Binance must not have already repriced in the same direction
        div = (ex_px - bnx_px) / bnx_px * 100   # positive = ex higher than bnx
        move_dir = 'long' if ex_move > 0 else 'short'

        # For lead long: ex_px > bnx_px (exchange is above Binance = Binance will follow up)
        # For lead short: ex_px < bnx_px (exchange is below Binance = Binance will follow down)
        if move_dir == 'long' and div < LEAD_MIN_DIV:
            continue   # Binance already repriced up — too late
        if move_dir == 'short' and div > -LEAD_MIN_DIV:
            continue   # Binance already repriced down — too late

        if abs(ex_move) > abs(best_move):
            best_move = ex_move
            best_lead = div
            best_ex   = ex

    if best_ex is None:
        return None

    direction = 'long' if best_move > 0 else 'short'

    return {
        'dir':          direction,
        'lead_exchange': best_ex,
        'lead_move':    round(best_move, 4),
        'bnx_px':       bnx_px,
        'divergence':   round(best_lead, 4),
        'monitor_only': False,
    }


# ══ E: EMA5/EMA8 CROSSOVER ═══════════════════════════════════════════════
_ema_cache: dict = {}
EMA_CACHE_TTL = 5

def _calc_ema(prices: list, period: int) -> list:
    if len(prices) < period: return []
    k = 2.0 / (period + 1)
    emas = [sum(prices[:period]) / period]
    for px in prices[period:]:
        emas.append(px * k + emas[-1] * (1 - k))
    return emas


def _find_ema_signal(sym, trend_period: int = 21) -> Optional[dict]:
    """EMA5/EMA8 crossover on 1m candles with momentum confirmation.
    trend_period=0 disables the HTF trend filter entirely (for _b testing).
    """
    now = time.time()
    cached = _ema_cache.get(sym)
    if cached and now - cached.get('ts', 0) < EMA_CACHE_TTL:
        return cached.get('sig')

    st = E.sym_state.get(sym)
    if not st or not st.get('price'):
        _ema_cache[sym] = {'ts': now, 'sig': None}; return None

    candles = _get_klines(sym, '1m') or _build_candles(sym, lookback_ms=1_800_000)
    if len(candles) < 15:
        _ema_cache[sym] = {'ts': now, 'sig': None}; return None

    closes = [float(c.get('c', c.get('close', 0))) for c in candles
              if (c.get('c') or c.get('close'))]
    if len(closes) < 12 or closes[-1] == 0:
        _ema_cache[sym] = {'ts': now, 'sig': None}; return None

    ema5 = _calc_ema(closes, 5)
    ema8 = _calc_ema(closes, 8)
    if len(ema5) < 4 or len(ema8) < 4:
        _ema_cache[sym] = {'ts': now, 'sig': None}; return None

    # EMA20 trend filter — skip entirely when trend_period=0 (b-test mode)
    ema20 = _calc_ema(closes, trend_period) if trend_period > 0 else None
    htf_trend = None
    if ema20 and closes:
        htf_trend = 'long' if closes[-1] > ema20[-1] else 'short'

    sig = None
    for i in range(-2, 0):   # was -3: only check last 2 candles (fresher signal)
        e5_now, e8_now   = ema5[i],   ema8[i]
        e5_prev, e8_prev = ema5[i-1], ema8[i-1]
        bullish = (e5_prev < e8_prev) and (e5_now > e8_now)
        bearish = (e5_prev > e8_prev) and (e5_now < e8_now)
        if not (bullish or bearish): continue

        spread_pct = abs(e5_now - e8_now) / max(e8_now, 1e-9) * 100
        if spread_pct < 0.05: continue  # was 0.02 — tightened: need meaningful spread

        # Require cross to still be diverging (EMA gap widening, not converging back)
        spread_prev = abs(e5_prev - e8_prev) / max(e8_prev, 1e-9) * 100
        if spread_pct < spread_prev: continue  # was only checked at i==-1

        # Price confirmation: must have moved meaningfully in cross direction
        ci = len(closes) + i
        move_pct = (closes[-1] - closes[ci-1]) / max(closes[ci-1], 1e-9) * 100 if ci > 0 else 0

        direction = 'long' if bullish else 'short'
        if direction == 'long'  and move_pct < 0.08: continue  # was 0.03
        if direction == 'short' and move_pct > -0.08: continue # was 0.03

        # HTF trend filter: block trades against the 5m trend
        # Skipped when trend_period=0 (b-test: measure raw crossover performance)
        if trend_period > 0:
            if not htf_trend or htf_trend != direction: continue

        sig = {'dir': direction, 'ema5': e5_now, 'ema8': e8_now,
               'spread_pct': spread_pct, 'move_pct': move_pct,
               'candles_ago': abs(i), 'htf_trend': htf_trend}
        break

    _ema_cache[sym] = {'ts': now, 'sig': sig}
    return sig


# ══ P: PARABOLIC BLOWUP DETECTION ════════════════════════════════
# Detects the start of a distribution/crash after a rapid pump.
# Pattern (ALLO example):
#   - Coin up >20% in last 30 min (parabolic pump)
#   - First body DOWN candle >=5% on 5m (distribution starts)
#   - VPIN elevated (panic selling, real volume)
#   - No bounce candle between peak and current candle
#
# Signal returns dict with entry metadata or None.

PARA_PUMP_MIN_PCT   = 8.0    # lowered: 8% pump is still significant for liquid alts
PARA_PUMP_WINDOW_MS = 3_600_000  # extended: 1hr window (was 30min) — catches slower pumps
PARA_DROP_MIN_PCT   = 2.5    # lowered: 2.5% bearish candle trigger (was 4%)
PARA_BODY_RATIO     = 0.55   # slightly looser body requirement
PARA_MAX_BOUNCE     = 0.40   # if price bounced >40% of first drop before entry, skip

def _find_parabolic_blowup(sym) -> dict | None:
    """
    Detect the distribution top after a parabolic pump.
    Returns signal dict or None.

    Entry logic:
      1. Look back PARA_PUMP_WINDOW_MS for the peak price
      2. Verify peak is >=PARA_PUMP_MIN_PCT above the start of the window
      3. Check that the most recent completed 5m candle is a large body DOWN candle
      4. Verify no meaningful bounce between peak and current price
      5. Confirm elevated VPIN (real selling, not low-vol drift)
    """
    klines_5m = _get_klines(sym, '5m')
    klines_1m = _get_klines(sym, '1m')
    if len(klines_5m) < 6 or len(klines_1m) < 10:
        return None

    st = E.sym_state.get(sym)
    if not st or st['price'] == 0:
        return None

    now_ms  = time.time() * 1000
    cur_px  = st['price']

    # ── Step 1: find the pump ─────────────────────────────────────
    # Look at 1m candles in the pump window to find peak and base
    window_1m = [c for c in klines_1m if now_ms - (c['ts'] + 60_000) < PARA_PUMP_WINDOW_MS]
    if len(window_1m) < 6:
        return None

    base_px = min(c['l'] for c in window_1m)
    peak_px = max(c['h'] for c in window_1m)
    if base_px <= 0:
        return None

    pump_pct = (peak_px - base_px) / base_px * 100
    if pump_pct < PARA_PUMP_MIN_PCT:
        return None  # not parabolic enough

    # ── Step 2: confirm we are past the peak ─────────────────────
    # Current price must be meaningfully below the peak
    drawdown_from_peak = (peak_px - cur_px) / peak_px * 100
    if drawdown_from_peak < 3.0:
        return None  # still near peak, haven't started dropping

    # ── Step 3: check trigger candle ─────────────────────────────
    # Last completed 5m candle must be a large bearish body
    c_last = klines_5m[-1]
    c_prev = klines_5m[-2]

    # Candle must be bearish (close < open)
    if c_last['c'] >= c_last['o']:
        return None

    candle_rng = c_last['h'] - c_last['l']
    if candle_rng == 0:
        return None

    body = c_last['o'] - c_last['c']
    body_pct  = body / c_last['o'] * 100
    body_ratio = body / candle_rng

    if body_pct < PARA_DROP_MIN_PCT:
        return None  # candle not large enough
    if body_ratio < PARA_BODY_RATIO:
        return None  # wick-dominated, not a clean distribution candle

    # ── Step 4: no meaningful bounce before entry ─────────────────
    # Find peak candle index in 5m klines
    peak_5m_idx = max(range(len(klines_5m)), key=lambda i: klines_5m[i]['h'])
    # Check candles between peak and last candle for any large green
    candles_since_peak = klines_5m[peak_5m_idx+1:-1]  # exclude trigger candle
    if candles_since_peak:
        max_bounce = max(
            (c['c'] - c['o']) / c['o'] * 100
            for c in candles_since_peak
            if c['o'] > 0
        )
        if max_bounce > PARA_MAX_BOUNCE * body_pct:
            return None  # meaningful bounce = distribution absorbed, skip

    # ── Step 5: recency — trigger candle must be fresh ────────────
    c_age_ms = now_ms - (c_last['ts'] + 300_000)  # age since 5m candle closed
    if c_age_ms > 300_000 * 1.5:  # stale trigger
        return None

    return {
        'pump_pct':        round(pump_pct, 2),
        'drawdown_pct':    round(drawdown_from_peak, 2),
        'trigger_body_pct': round(body_pct, 2),
        'trigger_body_ratio': round(body_ratio, 3),
        'peak_px':         round(peak_px, 8),
        'base_px':         round(base_px, 8),
        'fade_dir':        'short',
        'ts_ms':           int(c_last['ts']),
        'timeframe':       '5m',
    }
