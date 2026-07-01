"""
PredictEngine - strategies_engine.py
StrategyEngine class — gates, fire, check_outcomes, snapshot.
Imported by strategies.py.
"""
import time, csv, json, os
from collections import deque
from datetime import datetime
from pathlib import Path
import engine as E
from config import (
    FEE_RT, VERSION, SPREAD_MAX_PCT,
    WARMUP_SEC,
    K_SHORT_ENTRY_DISABLED,
    CONFLUENCE_L_OI_AGREE_MULT, CONFLUENCE_L_OI_DISAGREE_MULT,
    E_SHORT_ONLY,
    O_SHORT_DISABLED, CONFLUENCE_O_TREND_BLOCK_TICKS,
    Q_SHORT_DISABLED, Q_LONG_DISABLED, Q_MAX_OPEN_PER_SYM,
    R_ENTRY_DELAY_SEC,
    U_LONG_DISABLED,
    DECOR_LONG_BTC_MIN, DECOR_SHORT_BTC_MIN,
    X_LONG_ENTRY_DISABLED, X_SHORT_ENTRY_DISABLED,
    CONFLUENCE_Z_OI_CHECK, Z_SHORT_DISABLED,
    B_SHORT_DISABLED,
    T_LONG_DISABLED,
)
import live_execution as _live  # noqa: E402  — live order layer (no-op when LIVE_ENABLED=False)
import maker_execution as _maker  # noqa: E402  — post-only GTX entry layer (E/CGYL/Q only)

# ── Per-tick EMA21 cache ──────────────────────────────────────────────────────
# _get_ema21_cached(sym) computes EMA21 from 30m of 1m candles once per tick
# per symbol, shared across K / Q / Y gates. advance_ema_tick() must be called
# at the TOP of pred_loop_multi to invalidate stale entries each tick.
# Without this, each gate independently rebuilt 30 candles x 90 coins x 3
# strategies = 270 _build_candles calls per 100ms loop.
_ema21_tick_counter: int = 0
_ema21_cache: dict = {}   # sym -> (tick_counter, ema21_value_or_None)

def advance_ema_tick() -> None:
    """Increment the EMA21 cache generation. Call once per pred_loop tick."""
    global _ema21_tick_counter
    _ema21_tick_counter += 1

def _get_ema21_cached(sym: str) -> float | None:
    """Return EMA21 for sym, computing at most once per tick per sym.
    Uses REST klines (via _get_klines) so value is available from session start,
    not just after 21+ minutes of price_hist accumulation."""
    cached = _ema21_cache.get(sym)
    if cached is not None and cached[0] == _ema21_tick_counter:
        return cached[1]
    try:
        from strategies_signals import _calc_ema, _get_klines
        # _get_klines returns REST klines if available, falls back to price_hist candles
        _c = _get_klines(sym, '1m')
        val = None
        if len(_c) >= 21:
            _closes = [x['c'] for x in _c]
            _ema = _calc_ema(_closes, 21)
            if _ema:
                val = _ema[-1]
    except Exception:
        val = None
    _ema21_cache[sym] = (_ema21_tick_counter, val)
    return val
# ─────────────────────────────────────────────────────────────────────────────

# ── Persistent state directory ────────────────────────────────────────────────
# Each StrategyEngine saves _cum_net / pnl_history / hist_* to logs/state_X.json
# Files survive hot reloads. Delete logs/state_X.json to reset that strategy.
_STATE_DIR = Path(os.getenv('ENGINE_DIR', Path(__file__).parent)) / 'logs'
_STATE_DIR.mkdir(exist_ok=True)
from strategies_config import StrategyConfig, STRATEGIES
from engine_logger import log_signal, log_trade_open, log_trade_close
from strategies_signals import (
    _build_candles, _get_klines,
    _detect_impulse, _in_fib_zone,
    _find_level_signal,
    _find_volume_wall, _wall_stable,
    _build_volume_profile, _find_vp_signal,
    _detect_consolidation, _find_range_signal, RANGE_MIN_BARS,
    _find_ema_signal,
    _update_breakout_state,
    _find_funding_signal,
    _find_cascade_signal,
    _find_oi_divergence,
    _find_density_signal,
    _find_decorrelation_signal,
    _find_knife_signal,
    _find_lag_signal, LAG_SNAP30_HOLD_THR, LAG_SNAP30_EXIT_THR,
    _find_star_pattern,
    _find_parabolic_blowup,
)
# New strategy signals — imported directly from engine/signals
# (calc_absorption, calc_mtf_bias, calc_microburst, calc_spoofing
#  are already computed in run_pred sigs dict and accessible via r['sigs'])

# ══ STRATEGY ENGINE ═══════════════════════════════════════════════

class StrategyEngine:
    def __init__(self, cfg: StrategyConfig, log_prefix='preds'):
        self.cfg = cfg
        self.preds = deque(maxlen=200)
        self.hist_win = self.hist_lose = self.hist_total = 0
        self.pnl_history: list = []
        self._cum_net          = 0.0
        self._session_start_cum    = 0.0  # baseline for session net display; preserved across hot-reloads
        self._session_start_total  = 0    # baseline trade count for session display
        self._session_start_wins   = 0
        self._session_start_loses  = 0
        self._bnb_cum_pnl          = 0.0  # cumulative realized PnL from Binance (USDT)
        self._bnb_cum_comm         = 0.0  # cumulative commission from Binance (USDT)
        self._open_syms: set = set()   # O(1) open-position lookup — kept in sync by fire()/_close()
        self._cooldowns: dict = {}
        self._loss_streak: dict = {}   # sym → consecutive losses; reset on win
        self._impulse_cache:   dict = {}
        self._impulse_wait_ts: dict = {}
        self._cascade_detect_ts: dict = {}
        self._oi_persist_cache:  dict = {}  # S: OI persistence check   # R: per-cascade detection timestamps
        self._start_ts: float = time.time()      # warmup: engine init timestamp
        self._no_log = (log_prefix is None)
        # Restore persisted state — must come after all fields are initialised above
        # and before the log setup below (which creates a new CSV on each restart)
        self._load_state()
        if not self._no_log:
            ts   = datetime.now().strftime('%Y%m%d_%H%M')
            import re as _re
            safe = _re.sub(r'[^A-Za-z0-9_-]','',cfg.name.replace(' ','_'))[:20]
            self.log_file = f"{log_prefix}_{ts}_{cfg.label}_{safe}.csv"
            self._init_log()

    # ── persistent state ──────────────────────────────────────────────
    def _state_path(self) -> Path:
        return _STATE_DIR / f"state_{self.cfg.label}.json"

    def _load_state(self):
        """Load persisted cum_net / pnl_history / hist counts from disk.
        Called once in __init__ so state survives hot reloads.
        If the state file doesn't exist (first run, or deliberate reset),
        starts fresh — just delete logs/state_X.json to reset a strategy.
        """
        path = self._state_path()
        if not path.exists():
            return
        try:
            with open(path) as f:
                d = json.load(f)
            # Only restore if config version matches — config change = reset
            if d.get('version') != VERSION['v']:
                path.rename(path.with_suffix(f".bak_{d.get('version','?')}"))
                return
            self._cum_net   = d.get('cum_net', 0.0)
            self.hist_win   = d.get('hist_win', 0)
            self.hist_lose  = d.get('hist_lose', 0)
            self.hist_total = d.get('hist_total', 0)
            self.pnl_history = d.get('pnl_history', [])
            # Safety: never restore preds from JSON — always keep as deque.
            # Older state files may have preds as a list which breaks appendleft.
            if not isinstance(self.preds, deque):
                self.preds = deque(maxlen=200)
        except Exception:
            pass   # corrupt file — start fresh
        # Rebuild O(1) open-symbol set from whatever preds exist (post-restore or empty)
        self._open_syms = {p['sym'] for p in self.preds if p.get('out3') is None}

    def _save_state(self):
        """Persist cum_net / pnl_history / hist counts to disk.
        Called after every resolved trade so restarts lose at most 1 trade.
        """
        try:
            path = self._state_path()
            d = {
                'version':     VERSION['v'],
                'label':       self.cfg.label,
                'cum_net':     round(self._cum_net, 6),
                'hist_win':    self.hist_win,
                'hist_lose':   self.hist_lose,
                'hist_total':  self.hist_total,
                'pnl_history': self.pnl_history[-200:],
                'saved_at':    int(time.time()),
            }
            # Write atomically via tmp file to avoid partial writes
            tmp = path.with_suffix('.tmp')
            with open(tmp, 'w') as f:
                json.dump(d, f)
            tmp.replace(path)
        except Exception:
            pass

    # ── dispatcher ────────────────────────────────────────────────
    def _score_sustained(self, sym: str, direction: str, ticks: int = 3) -> bool:
        """ITEM 7 (Kronos): score sustained in direction for N consecutive sig_hist ticks."""
        st = E.sym_state.get(sym)
        if not st: return True
        hist = list(st.get('sig_hist', []))
        if len(hist) < ticks: return True
        for h in hist[-ticks:]:
            sc = h.get('score', 0)
            if direction == 'long'  and sc <= 0: return False
            if direction == 'short' and sc >= 0: return False
        return True

    def _market_breadth_ok(self, sym: str, direction: str) -> bool:
        """ITEM 8 (Kronos): block fade when >70% of market moves same direction."""
        coins = E.ACTIVE_COINS
        if len(coins) < 10: return True
        bull = bear = 0
        for c in coins:
            if c == sym: continue
            st = E.sym_state.get(c)
            if not st: continue
            hist = list(st.get('sig_hist', []))
            if not hist: continue
            sc = hist[-1].get('score', 0)
            if   sc >  15: bull += 1
            elif sc < -15: bear += 1
        total = bull + bear
        if total < 5: return True
        if direction == 'long'  and bear / total > 0.70: return False
        if direction == 'short' and bull / total > 0.70: return False
        return True

    def gates_met(self, sym, r) -> bool:
        if self.cfg.disabled:             return False
        # Direction filter — long_only / short_only from StrategyConfig.
        # Hard-blocks before any signal computation — cheapest possible gate.
        # Set by signal_replay.py analysis (2026-06-10, 21,928 trades):
        #   B long_only=True: longs +0.005%/T vs shorts -0.025%/T across 3,053 trades.
        _dir = r.get('dir')
        if getattr(self.cfg, 'long_only',  False) and _dir == 'short': return False
        if getattr(self.cfg, 'short_only', False) and _dir == 'long':  return False
        # Warmup gate: block all firing until buffers have stabilised after restart.
        # VPIN/ATR/spread are unreliable for the first WARMUP_SEC seconds (empty history).
        # Per-strategy warmup_sec overrides global WARMUP_SEC
        # -1 = use global, 0 = no warmup, >0 = custom seconds
        _wsec = getattr(self.cfg, 'warmup_sec', -1.0)
        effective_warmup = _wsec if _wsec >= 0 else WARMUP_SEC
        if effective_warmup > 0 and (time.time() - self._start_ts) < effective_warmup:
            return False
        if not self._check_symbol(sym):   return False
        if not self._check_hours():       return False
        if self.cfg.impulse_fade_mode:    return self._k_gate(sym, r)
        if self.cfg.candle_level_mode:    return self._l_gate(sym, r)
        if self.cfg.volume_wall_mode:     return self._m_gate(sym, r)
        if self.cfg.volume_profile_mode:  return self._n_gate(sym, r)
        if self.cfg.consolidation_mode:   return self._o_gate(sym, r)
        if self.cfg.ema_cross_mode:       return self._e_gate(sym, r)
        if self.cfg.breakout_retest_mode: return self._p_gate(sym, r)
        if self.cfg.funding_fade_mode:    return self._q_gate(sym, r)
        if self.cfg.liq_cascade_mode:     return self._r_gate(sym, r)
        if self.cfg.oi_divergence_mode:   return self._s_gate(sym, r)
        if self.cfg.density_bounce_mode:  return self._u_gate(sym, r)
        if self.cfg.decorrelation_mode:   return self._w_gate(sym, r)
        if self.cfg.knife_catch_mode:     return self._x_gate(sym, r)
        if self.cfg.lag_monitor_mode:     return self._z_gate(sym, r)
        if self.cfg.regime_trend_mode:    return self._t_gate(sym, r)
        if self.cfg.absorption_reversal_mode: return self._v_gate(sym, r)
        if getattr(self.cfg,'star_pattern_mode',False): return self._y_gate(sym, r)
        if getattr(self.cfg,'parabolic_mode',False):        return self._p_gate(sym, r)
        if getattr(self.cfg,'cgy_mode',False):             return self._cgy_gate(sym, r)
        if getattr(self.cfg,'wb_mode',False):              return self._wb_gate(sym, r)
        if self.cfg.mtf_momentum_mode:        return self._b_gate(sym, r)
        if getattr(self.cfg,'ofi_mode',False):            return self._ofi_gate(sym, r)
        if self.cfg.spoof_fade_mode:          return self._h_gate(sym, r)
        return self._standard_gate(sym, r)

    # ── helpers ───────────────────────────────────────────────────
    def _check_loss_streak(self, sym) -> bool:
        if self.cfg.loss_streak_limit <= 0: return True
        streak = self._loss_streak.get(sym, 0)
        if streak >= self.cfg.loss_streak_limit:
            # Exponential backoff: each consecutive block doubles the cooldown.
            # streak=3 → 1×cd, streak=6 → 2×cd, streak=9 → 4×cd etc.
            # This prevents the "3 losses → 10min wait → 3 more losses" loop.
            multiplier = 2 ** ((streak - self.cfg.loss_streak_limit) // self.cfg.loss_streak_limit)
            effective_cd = self.cfg.loss_streak_cd * min(multiplier, 16)  # cap at 16× (~2.7h)
            return (time.time() - self._cooldowns.get(sym, 0)) >= effective_cd
        return True

    def _check_symbol(self, sym) -> bool:
        if sym in self.cfg.symbol_blacklist:
            return False
        # Block illiquid/alpha tokens unless strategy explicitly opts in.
        # Use module-level cache to avoid blocking requests.get() in hot path.
        # allow_alpha=True (B-test universe) or parabolic_mode bypasses illiquid filter
        if not getattr(self.cfg, 'parabolic_mode', False) and not getattr(self.cfg, 'allow_alpha', False):
            try:
                import live_execution as _le
                if _le._illiquid_syms and _le.is_illiquid(sym):
                    return False
            except Exception:
                pass
        return True

    def _check_hours(self) -> bool:
        if not self.cfg.active_hours_utc: return True
        now = time.time()
        if not hasattr(self, '_hour_cache') or now - self._hour_cache[0] > 60:
            from datetime import datetime, timezone
            self._hour_cache = (now, datetime.now(timezone.utc).hour)
        h = self._hour_cache[1]
        return any(s <= h < e for s, e in self.cfg.active_hours_utc)

    def _check_oi_filter(self, sym, r) -> bool:
        """Block entry when OI divergence actively opposes entry direction."""
        if not self.cfg.oi_stack_filter: return True
        try:
            oi_sig = _find_oi_divergence(sym)
            if oi_sig is not None and oi_sig['dir'] != r['dir']:
                return False
        except Exception: pass
        return True

    def _spread_ok(self, spread, mult=2.0) -> bool:
        """Check spread against per-strategy spread_max_mult multiplier."""
        if spread is None: return True
        _mult = getattr(self.cfg, 'spread_max_mult', mult)
        return spread <= SPREAD_MAX_PCT * _mult

    # ── standard gate (C/G) ───────────────────────────────────────
    def _standard_gate(self, sym, r) -> bool:
        st = E.sym_state.get(sym)
        if not st: return False
        if not (st.get('oi',0)>=5e6 or sym in E.LIQUID_WHITELIST): return False
        _ts = st.get('_tick_sigs') or {}
        vpin   = _ts.get('vpin',   E.calc_vpin(sym))
        lam    = _ts.get('lam',    E.calc_kyle_lambda(sym))
        spread = _ts.get('spread', E.calc_spread_pct(sym))
        accel  = _ts.get('accel',  E.calc_trade_accel(sym))
        atr    = _ts.get('atr',    E.get_atr(sym))
        # Fail closed on None: if VPIN/lambda data is unavailable (sparse tape),
        # don't trade — a missing signal is not a green light.
        if vpin is None:
            log_signal(self.cfg.label, sym, 'blocked', 'vpin=None (insufficient tape)', vpin=None)
            return False
        if vpin < self.cfg.vpin_min:
            log_signal(self.cfg.label, sym, 'blocked', f'vpin {vpin:.3f} < min {self.cfg.vpin_min}', vpin=vpin)
            return False
        if vpin > self.cfg.vpin_max: return False
        if lam is None or lam <= 0: return False   # no lambda data = no edge measurement = skip
        # FIX 2026-05-26: use spread_max_mult (default 1.5) so C/G aren't tighter than every other strategy
        if self.cfg.use_spread_gate and spread is not None and spread > SPREAD_MAX_PCT * getattr(self.cfg, 'spread_max_mult', 1.5): return False
        if self.cfg.use_accel_gate and self.cfg.min_accel>0:
            if accel is not None and accel < self.cfg.min_accel: return False
        if abs(r['score']) > self.cfg.max_score: return False
        if self.cfg.min_score_at_fire > 0 and abs(r['score']) < self.cfg.min_score_at_fire:
            return False
        if not self._check_loss_streak(sym): return False
        if not self._check_oi_filter(sym, r): return False
        hist=list(st['sig_hist']); sus=False
        n = self.cfg.sus_ticks
        if len(hist) >= n:
            last_n = hist[-n:]
            cur_d  = last_n[-1]['dir']
            sus = all(
                h['dir'] == cur_d and h['n_agree'] >= 2
                and abs(h['score']) > self.cfg.sus_score_thr
                for h in last_n
            )
        return (r['conf']>=self.cfg.min_conf and abs(r['score'])>=self.cfg.min_score and
                r['n_agree']>=2 and r['n_conflict']<=self.cfg.max_conflict and
                r['strength']>=40 and sus and atr>=self.cfg.min_vol_atr and
                not self._has_open(sym))

    # ── K: impulse fade ───────────────────────────────────────────
    def _k_gate(self, sym, r) -> bool:
        if self._has_open(sym): return False
        st = E.sym_state.get(sym)
        if not self._check_symbol(sym): return False
        if not st or st['price'] == 0: return False

        # FIX 2026-06-02: min_score/min_conf gate was missing from _k_gate entirely.
        # sc=-53 (strong trend continuation) entries were passing unchecked.
        # abs() because K fires in fade direction opposite to score sign.
        if r['conf'] < self.cfg.min_conf: return False
        if abs(r['score']) < self.cfg.min_score: return False
        # FIX 2026-06-03: block negative-score entries (score_avg_at_fire=-42.8 smoking gun).
        # K should only fade when aggregate signal is neutral/positive, not deeply negative.
        # long fade: score must be >=0 (don't fade when everything says it keeps falling)
        # short fade: score must be <=0 (don't fade when everything says it keeps rising)
        _msf = getattr(self.cfg, 'min_score_at_fire', 0.0)
        if r.get('dir', '') == 'long'  and r['score'] < -_msf: return False
        if r.get('dir', '') == 'short' and r['score'] >  _msf: return False

        imp = _detect_impulse(sym)
        if imp is None: return False
        log_signal('K', sym, 'impulse',
            f"{imp.get('pattern','?')} {imp['fade_dir']} {imp['size_pct']:.2f}% tf={imp.get('timeframe','1m')}",
            vpin=E.calc_vpin(sym), atr=E.get_atr(sym))

        # Dedup: don't re-enter on same candle
        cached = self._impulse_cache.get(sym)
        if cached and abs(cached.get('ts_ms', 0) - imp['ts_ms']) < 1000:
            return False

        fade_dir = imp['fade_dir']

        # Price must not have already bounced >60% before we enter
        if not _in_fib_zone(sym, imp):
            log_signal('K', sym, 'blocked', 'already bounced past 60%', vpin=E.calc_vpin(sym))
            return False

        vpin = E.calc_vpin(sym)
        if vpin is not None and vpin < self.cfg.vpin_min:
            log_signal('K', sym, 'blocked', f'vpin {vpin:.3f} < min {self.cfg.vpin_min}', vpin=vpin)
            return False
        if vpin is not None and vpin > 0.95:               # raised 0.90→0.95
            log_signal('K', sym, 'blocked', f'vpin {vpin:.3f} > 0.95 (still panic)', vpin=vpin)
            return False

        spread = E.calc_spread_pct(sym)
        if spread is not None and spread > SPREAD_MAX_PCT * 1.5: return False
        if E.get_atr(sym) < self.cfg.min_vol_atr:          return False

        # Signal fusion must not strongly oppose the fade direction
        # (don't fade a wick if ALL signals say the move continues)
        sigs_strongly_against = sum(
            1 for s in r['sigs'].values()
            if s is not None and (s < -30 if fade_dir == 'long' else s > 30)
        )
        if sigs_strongly_against > self.cfg.max_conflict: return False

        # EMA21 trend filter — cached per tick via _get_ema21_cached()
        _ema21_val = _get_ema21_cached(sym)
        if _ema21_val is None:
            return False   # fail closed — EMA unavailable = don't trade
        _trend_up = st['price'] > _ema21_val
        if fade_dir == 'short' and _trend_up:     return False  # don't short in uptrend
        if fade_dir == 'long'  and not _trend_up: return False  # don't long in downtrend

        if K_SHORT_ENTRY_DISABLED and fade_dir == 'short': return False

        # ITEM 2: block during active liquidation cascade
        _liq_k = E.calc_liq_pressure(sym)
        if _liq_k and _liq_k['rate'] > 5.0: return False
        # ITEM 3: require informed flow for wick entries
        _ltr_k = E.calc_large_trade_ratio(sym)
        if _ltr_k is not None and _ltr_k < 0.03: return False
        # ITEM 4: don't fade if pressure still strong
        _vwap_k = E.calc_vwap_deviation(sym)
        if _vwap_k is not None:
            if fade_dir == 'long'  and _vwap_k > 60: return False
            if fade_dir == 'short' and _vwap_k < -60: return False
        # v5: OBI confirmation — book must agree with fade direction at wick extreme
        # hammer long: real buyers should be in book (bid-heavy = obi > 0)
        # shooting_star short: sellers should be in book (ask-heavy = obi < 0)
        _obi_k = E.calc_depth_imbalance(sym, levels=5)
        if _obi_k is not None:
            if fade_dir == 'long'  and _obi_k < -20: return False
            if fade_dir == 'short' and _obi_k >  20: return False
        # ITEM 7: score direction check — K is time-sensitive (impulse just happened)
        # ticks=1: just verify current score agrees, don't wait 3 ticks (kills entry window)
        if not self._score_sustained(sym, fade_dir, ticks=1): return False
        # ITEM 8: market breadth — skip for K, impulse fade works even in systemic moves
        # if not self._market_breadth_ok(sym, fade_dir): return False
        # Store in cache to prevent duplicate fires on same wick
        self._impulse_cache[sym] = imp
        r['_fade_dir']     = fade_dir
        r['_impulse_size'] = imp['size_pct']
        r['_wick_type']    = imp.get('wick_type', 'wick')
        r['_wick_ratio']   = round(imp.get('wick_ratio', 0), 3)
        return True

    # ── L: candle S/R ─────────────────────────────────────────────
    def _l_gate(self, sym, r) -> bool:
        if self._has_open(sym): return False
        st=E.sym_state.get(sym)
        if not st or st['price']==0: return False
        sig=_find_level_signal(sym)
        if sig is None: return False
        log_signal('L', sym, 'detected',
            f"level {sig['type']} {sig['dir']} lvl={sig['level'].get('price',0):.2f}",
            vpin=E.calc_vpin(sym), atr=E.get_atr(sym))
        vpin=E.calc_vpin(sym); spread=E.calc_spread_pct(sym)
        if vpin is not None and vpin < self.cfg.vpin_min: return False
        # FIX 2026-05-25: use per-strategy spread_max_mult (L: tight spread = 52.5%WR)
        if not self._spread_ok(spread):
            log_signal('O', sym, 'blocked', f'spread {spread:.6f} too high', spread=spread)
            return False
        if E.get_atr(sym) < self.cfg.min_vol_atr: return False
        if not self._check_oi_filter(sym, r): return False
        # Confluence: dynamic score threshold based on OI agreement.
        try:
            oi_sig = _find_oi_divergence(sym)
            if oi_sig is not None:
                mult = CONFLUENCE_L_OI_AGREE_MULT if oi_sig['dir'] == sig['dir'] else CONFLUENCE_L_OI_DISAGREE_MULT
            else:
                mult = 1.0
            if abs(r['score']) < self.cfg.min_score * mult:
                return False
        except Exception:
            if abs(r['score']) < self.cfg.min_score:
                return False
        self._cooldowns[sym] = time.time()
        r['_level_dir']=sig['dir']; r['_level_type']=sig['type']; r['_level_info']=sig['level']
        return True

    # ── M: volume wall ────────────────────────────────────────────
    def _m_gate(self, sym, r) -> bool:
        if self._has_open(sym): return False
        st=E.sym_state.get(sym)
        if not st or st['price']==0: return False
        wall=_find_volume_wall(sym)
        if wall is None: return False
        now=time.time()*1000; px5=[p for ts,p in list(st['price_hist']) if now-ts<5000]
        if len(px5)<3: return False
        move=px5[-1]-px5[0]
        if wall['dir']=='long'  and move<=0: return False
        if wall['dir']=='short' and move>=0: return False
        if abs(move)/px5[0]*100 < 0.05: return False
        vpin=E.calc_vpin(sym); spread=E.calc_spread_pct(sym)
        if vpin   is not None and vpin   < self.cfg.vpin_min:    return False
        if spread is not None and spread > SPREAD_MAX_PCT*2.0:   return False
        if E.get_atr(sym) < self.cfg.min_vol_atr:                return False
        self._cooldowns[sym]=time.time()
        r['_wall_dir']=wall['dir']; r['_wall_usd']=wall['wall_usd']
        return True

    # ── N: volume profile ─────────────────────────────────────────
    def _n_gate(self, sym, r) -> bool:
        if self._has_open(sym): return False
        st=E.sym_state.get(sym)
        if not st or st['price']==0: return False
        if len(st['klines'].get('15m',[])) < 10: return False
        sig=_find_vp_signal(sym)
        if sig is None: return False
        log_signal('N', sym, 'detected',
            f"profile {sig.get('type','?')} poc={sig.get('poc',0):.2f}",
            vpin=E.calc_vpin(sym), atr=E.get_atr(sym))
        vpin=E.calc_vpin(sym); spread=E.calc_spread_pct(sym)
        if vpin   is not None and vpin   < self.cfg.vpin_min:    return False
        if spread is not None and spread > SPREAD_MAX_PCT*2.0:   return False
        if E.get_atr(sym) < self.cfg.min_vol_atr:                return False
        self._cooldowns[sym]=time.time()
        r['_vp_dir']=sig['dir']; r['_vp_poc']=sig['poc']
        r['_vp_vah']=sig['vah']; r['_vp_val']=sig['val']
        return True

    # ── E: EMA5/EMA8 crossover ────────────────────────────────────
    def _e_gate(self, sym, r) -> bool:
        if self._has_open(sym): return False
        st=E.sym_state.get(sym)
        if not st or st['price']==0: return False
        sig=_find_ema_signal(sym, trend_period=getattr(self.cfg,'ema_trend_period',21))
        if sig is None:
            log_signal('E', sym, 'blocked', 'no_crossover (htf15/spread/no_cross)',
                       vpin=E.calc_vpin(sym), atr=E.get_atr(sym))
            return False
        log_signal('E', sym, 'detected',
            f"ema {sig.get('dir','?')} ema5={sig.get('ema5',0):.2f} ema8={sig.get('ema8',0):.2f}",
            vpin=E.calc_vpin(sym), atr=E.get_atr(sym))
        vpin=E.calc_vpin(sym)
        if vpin is not None and vpin < self.cfg.vpin_min: return False
        spread=E.calc_spread_pct(sym)
        # E is momentum (EMA crossover) — spread gate much wider than fade strategies
        # Original tight gate was wrong; 4× allows Asian hours low-liquidity sessions
        if not self._spread_ok(spread, mult=4.0): return False
        if E.get_atr(sym) < self.cfg.min_vol_atr: return False
        if r['conf'] < self.cfg.min_conf: return False
        if abs(r['score']) < self.cfg.min_score:
            log_signal('O', sym, 'blocked', f'score {abs(r["score"]):.1f} < min {self.cfg.min_score}', score=r['score'])
            return False
        if sig['dir'] == 'long'  and r['score'] < 0: return False
        if sig['dir'] == 'short' and r['score'] > 0: return False
        # FIX 2026-05-25: short-only — tight+short = 66.7%WR +0.098%/T
        if E_SHORT_ONLY and sig['dir'] == 'long': return False
        self._cooldowns[sym]=time.time()
        r['_ema_dir']    = sig['dir']
        r['_ema_spread'] = sig['spread_pct']
        r['_ema_move']   = sig['move_pct']
        return True

    # ── O: consolidation range ────────────────────────────────────
    def _o_gate(self, sym, r) -> bool:
        if self._has_open(sym): return False
        st=E.sym_state.get(sym)
        if not st or st['price']==0: return False
        sig=_find_range_signal(sym)
        if sig is None: return False
        log_signal('O', sym, 'detected',
            f"range {sig.get('dir','?')} top={sig.get('top',0):.2f} bot={sig.get('bot',0):.2f} width={sig.get('width_pct',0):.1f}%",
            vpin=E.calc_vpin(sym), atr=E.get_atr(sym), spread=E.calc_spread_pct(sym))
        vpin=E.calc_vpin(sym); spread=E.calc_spread_pct(sym)
        if vpin   is not None and vpin   < self.cfg.vpin_min:  return False
        if spread is not None and spread > SPREAD_MAX_PCT*2.0: return False
        if E.get_atr(sym) < self.cfg.min_vol_atr:              return False
        # 2026-05-27: block O shorts — short 18%WR vs long 35%WR in latest session
        if O_SHORT_DISABLED and sig['dir'] == 'short': return False
        # v16.2: long VPIN gate removed — vpin_min=0.0 in config handles this; hardcoded 0.50 was overriding cfg
        # Confluence: block O when symbol is mid-trend (T strategy's regime signal).
        # O fires on consolidation bounces — a trending symbol has NO consolidation range.
        trend_ticks = st.get('trend_tick_count', 0)
        trend_dir   = st.get('trend_dir')
        if trend_ticks >= CONFLUENCE_O_TREND_BLOCK_TICKS and trend_dir is not None:
            if trend_dir != sig['dir']:
                return False  # trading against active trend — block
        self._cooldowns[sym]=time.time()
        r['_range_dir'] = sig['dir']
        r['_range_top'] = sig['top']
        r['_range_bot'] = sig['bot']
        r['_range_tp']  = sig.get('tp_pct', 0)
        return True

    # ── P: breakout + retest ──────────────────────────────────────
    def _p_gate(self, sym, r) -> bool:
        if self._has_open(sym): return False
        st=E.sym_state.get(sym)
        if not st or st['price']==0: return False
        sig=_update_breakout_state(sym)
        if sig is None: return False
        log_signal('P', sym, 'detected',
            f"breakout {sig.get('dir','?')} lvl={sig.get('level',0):.2f}",
            vpin=E.calc_vpin(sym), atr=E.get_atr(sym))
        vpin=E.calc_vpin(sym); spread=E.calc_spread_pct(sym)
        if vpin   is not None and vpin   < self.cfg.vpin_min:  return False
        # FIX 2026-05-25: use per-strategy spread_max_mult
        if not self._spread_ok(spread): return False
        if E.get_atr(sym) < self.cfg.min_vol_atr:              return False
        # FIX 2026-05-25: OI confirmation reduces false breakouts (P: SL=65% of exits)
        if not self._check_oi_filter(sym, r): return False
        self._cooldowns[sym]=time.time()
        r['_bo_dir']   = sig['dir']
        r['_bo_level'] = sig['level']
        r['_bo_top']   = sig.get('top')
        r['_bo_bot']   = sig.get('bot')
        return True

    # ── Q: funding rate fade ──────────────────────────────────────
    def _q_gate(self, sym, r) -> bool:
        if self._has_open(sym): return False
        st=E.sym_state.get(sym)
        if not st or st['price']==0: return False
        sig=_find_funding_signal(sym)
        if sig is None: return False
        log_signal('Q', sym, 'detected',
            f"funding {sig.get('dir','?')} rate={sig.get('rate',0):.4f}",
            vpin=E.calc_vpin(sym), atr=E.get_atr(sym))
        spread=E.calc_spread_pct(sym)
        # FIX 2026-05-25: tight spread = 53.1%WR vs wide = 38.4%WR (+15pp)
        if not self._spread_ok(spread): return False
        if E.get_atr(sym) < self.cfg.min_vol_atr: return False
        # FIX 2026-06-03: Q gate never checked min_conf/min_score — score_avg=0.0 at fire.
        # Gates existed in config but _q_gate silently ignored them.
        if r["conf"] < self.cfg.min_conf: return False
        if abs(r["score"]) < self.cfg.min_score: return False
        if not self._check_loss_streak(sym): return False
        # EMA21 trend filter — cached per tick via _get_ema21_cached()
        _ema21_val = _get_ema21_cached(sym)
        if _ema21_val is not None:
            _trend_up = st['price'] > _ema21_val
            if sig['dir'] == 'long'  and not _trend_up: return False
            if sig['dir'] == 'short' and _trend_up:     return False

        self._cooldowns[sym]=time.time()
        r['_funding_dir']  = sig['dir']
        r['_funding_rate'] = sig['rate']
        if Q_SHORT_DISABLED and r.get('dir') == 'short': return False
        if Q_LONG_DISABLED  and r.get('dir') == 'long':  return False
        # ITEM 5: OI velocity — if OI moving against trade direction, skip
        oi_v = E.calc_oi_velocity(sym)
        if oi_v is not None and abs(oi_v) > 0.5:
            fd = r.get('dir', 'long')
            if fd == 'long'  and oi_v < -0.3: return False
            if fd == 'short' and oi_v >  0.3: return False
        # ITEM 6: funding rate momentum — store for context, positive = rising toward extreme
        fm = E.calc_funding_momentum(sym)
        if fm is not None:
            r['_funding_momentum'] = fm
        return True

    # ── R: liquidation cascade ────────────────────────────────────
    def _r_gate(self, sym, r) -> bool:
        if self._has_open(sym): return False
        st=E.sym_state.get(sym)
        if not st or st['price']==0: return False
        sig=_find_cascade_signal(sym)
        if sig is None: return False
        log_signal('R', sym, 'detected',
            f"cascade {sig.get('dir','?')} lvls={sig.get('level_count',0)}",
            vpin=E.calc_vpin(sym), atr=E.get_atr(sym))
        vpin=E.calc_vpin(sym)
        if vpin is not None and vpin < self.cfg.vpin_min: return False
        spread=E.calc_spread_pct(sym)
        if spread is not None and spread > SPREAD_MAX_PCT*2.5: return False
        if E.get_atr(sym) < self.cfg.min_vol_atr: return False
        # FIX 2026-05-25: 100% rev exits = entry fires at cascade exhaustion.
        # Delay R_ENTRY_DELAY_SEC after first detection before allowing fire.
        if R_ENTRY_DELAY_SEC > 0:
            sig_key = (sym, round(sig.get('usd_30s', 0), -3))
            now = time.time()
            if sig_key not in self._cascade_detect_ts:
                self._cascade_detect_ts[sig_key] = now
                return False   # start the clock; don't fire yet
            if now - self._cascade_detect_ts[sig_key] < R_ENTRY_DELAY_SEC:
                return False   # still in delay window
            self._cascade_detect_ts.pop(sig_key, None)   # delay elapsed — allow fire
        self._cooldowns[sym]=time.time()
        r['_cascade_dir']   = sig['dir']
        r['_cascade_usd30'] = sig['usd_30s']
        r['_cascade_usd10'] = sig['usd_10s']
        # FIX: fire() reads r['dir'] directly (since 2026-06-03). Gates that only set
        # a mode-specific _*_dir key trade in the stale run_pred score direction instead
        # of the detected signal direction. Set it explicitly. (Q/S have same omission.)
        r['dir'] = sig['dir']
        return True

    # ── S: OI divergence ──────────────────────────────────────────
    def _s_gate(self, sym, r) -> bool:
        if self._has_open(sym): return False
        st=E.sym_state.get(sym)
        if not st or st['price']==0: return False
        sig=_find_oi_divergence(sym)
        if sig is None: return False
        log_signal('S', sym, 'detected',
            f"oi_div {sig.get('dir','?')} oi_change={sig.get('oi_change',0):.1f}%",
            vpin=E.calc_vpin(sym), atr=E.get_atr(sym))
        # OI persistence: signal must appear on 2 consecutive checks
        # Prevents reacting to single-tick OI noise (5s OI update cycle)
        cached_oi = self._oi_persist_cache.get(sym)
        now_ts = __import__('time').time()
        if cached_oi and cached_oi['dir'] == sig['dir'] and now_ts - cached_oi['ts'] < 15.0:
            pass  # confirmed — proceed
        else:
            self._oi_persist_cache[sym] = {'dir': sig['dir'], 'ts': now_ts}
            return False  # first sighting — wait for confirmation
        vpin=E.calc_vpin(sym)
        if vpin is not None and vpin < self.cfg.vpin_min: return False
        spread=E.calc_spread_pct(sym)
        # FIX 2026-05-25: use per-strategy spread_max_mult
        if not self._spread_ok(spread): return False
        if E.get_atr(sym) < self.cfg.min_vol_atr: return False
        self._cooldowns[sym]=time.time()
        r['_oi_dir']       = sig['dir']
        r['_oi_price_chg'] = sig['price_chg']
        r['_oi_chg']       = sig['oi_chg']
        r['_oi_type']      = sig['type']
        return True

    # ── U: density bounce ─────────────────────────────────────────
    def _u_gate(self, sym, r) -> bool:
        if self._has_open(sym): return False
        st=E.sym_state.get(sym)
        if not st or st['price']==0: return False
        sig=_find_density_signal(sym)
        if sig is None: return False
        log_signal('U', sym, 'detected',
            f"density {sig.get('dir','?')} lvls={sig.get('level_count',0)}",
            vpin=E.calc_vpin(sym), atr=E.get_atr(sym))
        # FIX 2026-05-25: long entries = 18%WR on 93% of signals — disable
        if U_LONG_DISABLED and sig['dir'] == 'long': return False
        vpin=E.calc_vpin(sym); spread=E.calc_spread_pct(sym)
        if vpin   is not None and vpin   < self.cfg.vpin_min:    return False
        if spread is not None and spread > SPREAD_MAX_PCT*2.0:   return False
        if E.get_atr(sym) < self.cfg.min_vol_atr:                return False
        self._cooldowns[sym]=time.time()
        r['_density_dir']    = sig['dir']
        r['_density_usd']    = sig['wall_usd']
        r['_density_level']  = sig['level']
        r['_density_touches']= sig['touches']
        r['_wall_dir']       = sig['dir']
        # FIX: see _r_gate — fire() uses r['dir'], so set it from the detected signal.
        r['dir'] = sig['dir']
        return True

    # ── W: BTC decorrelation ──────────────────────────────────────
    def _w_gate(self, sym, r) -> bool:
        if self._has_open(sym): return False
        st=E.sym_state.get(sym)
        if not st or st['price']==0: return False
        sig=_find_decorrelation_signal(sym)
        if sig is None: return False
        log_signal('W', sym, 'detected',
            f"decor {sig.get('dir','?')} btc={sig.get('btc_move',0):.2f}%",
            vpin=E.calc_vpin(sym), atr=E.get_atr(sym))
        now_ms=time.time()*1000
        liq_30s=sum(v for ts,il,v in list(st.get('liq_cascade_hist',[]))
                    if now_ms-ts < 30_000)
        if liq_30s > 500_000: return False
        btc_move = sig.get('btc_move', 0.0)
        if sig['dir'] == 'long'  and abs(btc_move) < DECOR_LONG_BTC_MIN:  return False
        if sig['dir'] == 'short' and abs(btc_move) < DECOR_SHORT_BTC_MIN: return False
        # VPIN gate: fail closed on None (sparse tape = no edge data = don't trade)
        vpin=E.calc_vpin(sym)
        if vpin is None or vpin < self.cfg.vpin_min:
            log_signal('W', sym, 'blocked', f'vpin {vpin} < min {self.cfg.vpin_min}', vpin=vpin)
            return False
        if vpin > self.cfg.vpin_max:
            log_signal('W', sym, 'blocked', f'vpin_max {vpin:.3f} > {self.cfg.vpin_max}', vpin=vpin); return False
        spread=E.calc_spread_pct(sym)
        if spread is not None and spread > SPREAD_MAX_PCT*3.0:
            log_signal('W', sym, 'blocked', f'spread {spread:.4f} > {SPREAD_MAX_PCT*3:.4f}', vpin=vpin); return False
        atr=E.get_atr(sym)
        if atr < self.cfg.min_vol_atr:
            log_signal('W', sym, 'blocked', f'atr {atr:.4f} < {self.cfg.min_vol_atr}', vpin=vpin); return False
        if r['conf'] < self.cfg.min_conf:
            log_signal('W', sym, 'blocked', f'conf {r["conf"]} < {self.cfg.min_conf}', vpin=vpin); return False
        if abs(r['score']) < self.cfg.min_score:
            log_signal('W', sym, 'blocked', f'score {abs(r["score"]):.1f} < {self.cfg.min_score}', vpin=vpin); return False
        # REMOVED 2026-06-01: score<-30 gate blocked ALL W short entries (BTC drop = neg score)
        # W signal direction set by _find_decorrelation_signal; score polarity irrelevant
        self._cooldowns[sym]=time.time()
        r['_decor_dir']       = sig['dir']
        r['_decor_btc_move']  = sig['btc_move']
        r['_decor_alt_move']  = sig['alt_move']
        r['_decor_divergence']= sig['divergence']
        return True

    # ── X: knife catch ────────────────────────────────────────────
    def _x_gate(self, sym, r) -> bool:
        if self._has_open(sym): return False
        st=E.sym_state.get(sym)
        if not st or st['price']==0: return False
        sig=_find_knife_signal(sym)
        if sig is None: return False
        log_signal('X', sym, 'detected',
            f"knife {sig.get('dir','?')} wick={sig.get('wick_pct',0):.2f}%",
            vpin=E.calc_vpin(sym), atr=E.get_atr(sym))
        # FIX 2026-05-25: long wick reversals = 22%WR; short = 45%WR
        if X_LONG_ENTRY_DISABLED  and sig['dir'] == 'long':  return False
        if X_SHORT_ENTRY_DISABLED and sig['dir'] == 'short': return False
        vpin=E.calc_vpin(sym)
        if vpin is not None and vpin < self.cfg.vpin_min: return False
        spread=E.calc_spread_pct(sym)
        if spread is not None and spread > SPREAD_MAX_PCT*2.0: return False
        if E.get_atr(sym) < self.cfg.min_vol_atr:              return False
        self._cooldowns[sym]=time.time()
        r['_knife_dir']   = sig['dir']
        r['_knife_wick']  = sig['wick_pct']
        r['_knife_close'] = sig['close_pct']
        return True

    # ── Z: cross-exchange lag ─────────────────────────────────────
    def _z_gate(self, sym, r) -> bool:
        if self._has_open(sym): return False
        st=E.sym_state.get(sym)
        if not st or st['price']==0: return False
        sig=_find_lag_signal(sym)
        if sig is None: return False
        log_signal('Z', sym, 'detected',
            f"lag {sig.get('dir','?')} lag={sig.get('best_lag_ms',0):.0f}ms div={sig.get('best_div_pct',0):.2f}%",
            vpin=E.calc_vpin(sym), atr=E.get_atr(sym))
        spread=E.calc_spread_pct(sym)
        if spread is not None and spread > SPREAD_MAX_PCT*3.0: return False
        if E.get_atr(sym) < self.cfg.min_vol_atr: return False
        self._cooldowns[sym]=time.time()
        r['_lag_dir']       = sig['dir']
        r['_lag_bnx_move']  = sig['bnx_move']
        r['_lag_best_ms']   = sig['best_lag_ms']
        r['_lag_best_div']  = sig['best_div_pct']
        r['_lag_exchanges'] = ','.join(l['exchange'] for l in sig['lagging'])
        r['_lag_monitor']   = sig['monitor_only']
        if sig['monitor_only']:
            self._log_lag_observation(sym, sig)
            return False
        # Confluence: if OI divergence exists and disagrees → block (configurable).
        if CONFLUENCE_Z_OI_CHECK:
            try:
                oi_sig = _find_oi_divergence(sym)
                if oi_sig is not None and oi_sig['dir'] != sig['dir']:
                    return False
            except Exception:
                pass
        if Z_SHORT_DISABLED and sig['dir'] == 'short': return False
        return True


    # ── Y: Morning/Evening Star (3-candle confirmed impulse fade) ──
    def _y_gate(self, sym, r) -> bool:
        if self._has_open(sym): return False
        # Global cap: max 2 concurrent Y positions across all symbols
        # Prevents flooding when a broad market move triggers many symbols at once
        y_open = sum(1 for p in self.preds
                     if p.get('out3') is None and p.get('label') == 'Y')
        if y_open >= 2: return False
        if not self._check_loss_streak(sym): return False
        st = E.sym_state.get(sym)
        if not st or st['price'] == 0: return False
        if not self._check_symbol(sym): return False
        sig = _find_star_pattern(sym, min_pct=getattr(self.cfg,'impulse_min_pct',0.50))
        if sig is None: return False
        log_signal('Y', sym, 'pattern',
            f"{sig['pattern']} {sig['fade_dir']} C1={sig['c1_size_pct']:.2f}% tf={sig.get('timeframe','1m')} conf={r.get('conf',0)} score={r.get('score',0):.0f}",
            vpin=E.calc_vpin(sym), atr=E.get_atr(sym))
        # Dedup: same C3 candle timestamp
        cached = self._impulse_cache.get('Y_' + sym)
        if cached and abs(cached - sig['ts_ms']) < 1000: return False
        fade_dir = sig['fade_dir']
        vpin = E.calc_vpin(sym)
        if vpin is not None and vpin < self.cfg.vpin_min: return False
        if vpin is not None and vpin > self.cfg.vpin_max: return False
        if not self._spread_ok(E.calc_spread_pct(sym)): return False
        if E.get_atr(sym) < self.cfg.min_vol_atr: return False

        # Conf + score checks (pattern is primary signal but need basic sanity)
        if r.get('conf', 0) < self.cfg.min_conf: return False
        if abs(r.get('score', 0)) < self.cfg.min_score: return False

        # Score direction sanity for exhaustion fades:
        # If fading SHORT (C1 was an UP move), score should NOT be strongly negative
        # (strongly negative score = market is actively selling = momentum, not exhaustion)
        # If fading LONG (C1 was a DOWN move), score should NOT be strongly positive
        # (strongly positive score = market is actively buying = momentum, not exhaustion)
        # Allow neutral (near zero) or mildly opposing scores — those indicate exhaustion.
        # Block if score strongly agrees with the fade direction (chasing momentum).
        _score_chase_thr = max(self.cfg.min_score * 3, 30.0)
        if fade_dir == 'short' and r.get('score', 0) < -_score_chase_thr: return False
        if fade_dir == 'long'  and r.get('score', 0) > _score_chase_thr:  return False

        # Block only extreme opposition (3-candle pattern is its own confirmation)
        sigs_against = sum(
            1 for s in r['sigs'].values()
            if s is not None and (s < -40 if fade_dir == 'long' else s > 40)
        )
        if sigs_against > self.cfg.max_conflict: return False
        # EMA21 trend filter — cached per tick via _get_ema21_cached()
        # Fail-open: if EMA unavailable (session <21min, klines not yet loaded),
        # allow trade — star pattern itself is sufficient confirmation.
        _ema21_val = _get_ema21_cached(sym)
        if _ema21_val is not None:
            _trend_up = st['price'] > _ema21_val
            if fade_dir == 'long'  and not _trend_up: return False  # no longs in downtrend
            if fade_dir == 'short' and _trend_up:     return False  # no shorts in uptrend

        self._impulse_cache['Y_' + sym] = sig['ts_ms']
        self._cooldowns[sym] = time.time()
        # ITEM 2+4: liq cascade + VWAP pressure checks
        _liq_y = E.calc_liq_pressure(sym)
        if _liq_y and _liq_y['rate'] > 3.0: return False
        _vwap_y = E.calc_vwap_deviation(sym)
        if _vwap_y is not None:
            if fade_dir == 'long'  and _vwap_y > 40: return False
            if fade_dir == 'short' and _vwap_y < -40: return False
        # ITEM 7+8: score sustained + not systemic
        if not self._score_sustained(sym, fade_dir, ticks=3): return False
        if not self._market_breadth_ok(sym, fade_dir): return False
        r['_star_dir']     = fade_dir
        r['_star_pattern'] = sig['pattern']
        r['_star_c1_size'] = sig['c1_size_pct']
        r['_star_c2_body'] = sig.get('c2_body_pct', 0)
        r['_star_fib_618'] = sig.get('fib_618')    # 61.8% retracement TP target (abs price)
        r['_star_fib_50']  = sig.get('fib_50')     # 50% retracement fallback
        r['_star_entry_px']= E.sym_state.get(sym, {}).get('price', 0)
        r['dir']           = fade_dir
        return True

    # ── V: CVD Absorption Reversal ────────────────────────────────
    def _v_gate(self, sym, r) -> bool:
        """
        Fire when large one-sided volume fails to move price (absorption).
        abs signal sign: positive = bullish (sell pressure absorbed by buyers)
                         negative = bearish (buy pressure absorbed by sellers)
        CVD confirms: same direction as abs (e.g. sell-heavy CVD + positive abs = buyers defending)
        """
        if self._has_open(sym): return False
        if not self._check_loss_streak(sym): return False
        st = E.sym_state.get(sym)
        if not st or st['price'] == 0: return False

        # Absorption signal from run_pred sigs dict (already computed this tick)
        sigs = r.get('sigs', {})
        abs_val = sigs.get('abs')
        if abs_val is None: return False

        # FIX 2026-05-26: lowered threshold 45→30 (45 was too strict, rarely fired)
        # FIX 2026-05-26: removed CVD block — absorption = flow vs price disagree,
        # the CVD check was inadvertently blocking the clearest absorption setups.
        abs_dir = 'long' if abs_val > 0 else 'short'
        if abs(abs_val) < 30: return False

        # Score must not strongly oppose abs direction
        if abs_dir == 'long'  and r['score'] < -25: return False
        if abs_dir == 'short' and r['score'] >  25: return False

        vpin  = E.calc_vpin(sym)
        if vpin is not None and vpin < self.cfg.vpin_min:    return False
        if vpin is not None and vpin > self.cfg.vpin_max:    return False
        spread = E.calc_spread_pct(sym)
        if not self._spread_ok(spread): return False
        if E.get_atr(sym) < self.cfg.min_vol_atr: return False
        if not self._check_symbol(sym): return False

        self._cooldowns[sym] = time.time()
        r['_abs_dir']    = abs_dir
        r['_abs_signal'] = round(abs_val, 1)
        # Override dir so fire() uses absorption direction
        r['dir'] = abs_dir
        return True

    # ── B: MTF Momentum Confirmation ─────────────────────────────
    def _b_gate(self, sym, r) -> bool:
        """
        Fire when 15s/60s/300s price windows all align in direction with meaningful magnitude.
        calc_mtf_bias returns -60 to +60.
        Microburst (trade acceleration) provides additional confirmation.
        """
        if self._has_open(sym): return False
        if not self._check_loss_streak(sym): return False
        st = E.sym_state.get(sym)
        if not st or st['price'] == 0: return False

        mtf = E.calc_mtf_bias(sym)
        if mtf is None: return False

        _mtf_min = getattr(self.cfg, 'mtf_bias_min', 20)   # prod default 20; stage config sets 8
        if abs(mtf) < _mtf_min:
            log_signal('B', sym, 'blocked', f'mtf_bias {mtf} abs < {_mtf_min}', vpin=E.calc_vpin(sym)); return False
        mtf_dir = 'long' if mtf > 0 else 'short'

        log_signal('B', sym, 'detected', f'mtf={mtf} dir={mtf_dir}', vpin=E.calc_vpin(sym), atr=E.get_atr(sym))

        # BTC 5-minute trend filter: don't fight the macro trend
        # Block LONG when BTC is falling over 5min; block SHORT when BTC is rising
        try:
            now_ms = time.time() * 1000
            btc5  = [p for ts, p in E.btc_hist if now_ms - ts < 300_000]
            btc15 = [p for ts, p in E.btc_hist if now_ms - ts < 900_000]
            if len(btc5) >= 5:
                btc_5m = (btc5[-1] - btc5[0]) / btc5[0] * 100
                if mtf_dir == 'long'  and btc_5m < -0.15:
                    log_signal('B', sym, 'blocked', f'btc_5m {btc_5m:.2f}% < -0.15 (downtrend, no longs)', vpin=E.calc_vpin(sym)); return False
                if mtf_dir == 'short' and btc_5m >  0.15:
                    log_signal('B', sym, 'blocked', f'btc_5m {btc_5m:.2f}% > +0.15 (uptrend, no shorts)', vpin=E.calc_vpin(sym)); return False
            # 15-min trend: block shorts in sustained uptrend (data: B shorts 12%WR today)
            if len(btc15) >= 10:
                btc_15m = (btc15[-1] - btc15[0]) / btc15[0] * 100
                if mtf_dir == 'short' and btc_15m > 0.30:
                    log_signal('B', sym, 'blocked', f'btc_15m {btc_15m:.2f}% > +0.30 (sustained uptrend, no shorts)', vpin=E.calc_vpin(sym)); return False
        except Exception:
            pass

        burst = E.calc_microburst(sym)
        if burst is not None and burst <= 0:
            log_signal('B', sym, 'blocked', f'microburst={burst} <= 0', vpin=E.calc_vpin(sym)); return False

        # Score direction gate removed: MTF (price-based) and score (flow-based) legitimately
        # disagree — e.g. price drops while buyers still present. abs(score) gate below is sufficient.

        vpin  = E.calc_vpin(sym)
        if vpin is not None and vpin < self.cfg.vpin_min:
            log_signal('B', sym, 'blocked', f'vpin {vpin:.3f} < {self.cfg.vpin_min}', vpin=vpin); return False
        if vpin is not None and vpin > self.cfg.vpin_max:
            log_signal('B', sym, 'blocked', f'vpin {vpin:.3f} > {self.cfg.vpin_max}', vpin=vpin); return False
        spread = E.calc_spread_pct(sym)
        if not self._spread_ok(spread):
            log_signal('B', sym, 'blocked', f'spread {spread:.4f} too wide', vpin=vpin); return False
        atr = E.get_atr(sym)
        if atr < self.cfg.min_vol_atr:
            log_signal('B', sym, 'blocked', f'atr {atr:.4f} < {self.cfg.min_vol_atr}', vpin=vpin); return False

        # B+Z confluence: if Z lag signal agrees → lower score bar; disagrees → raise it
        try:
            from strategies_signals import _find_lag_signal
            lag_sig = _find_lag_signal(sym)
            if lag_sig is not None and not lag_sig.get('monitor_only'):
                score_req = self.cfg.min_score * (0.60 if lag_sig['dir'] == mtf_dir else 1.40)
            else:
                score_req = self.cfg.min_score
        except Exception:
            score_req = self.cfg.min_score
        if abs(r['score']) < score_req:
            log_signal('B', sym, 'blocked', f'score {abs(r["score"]):.1f} < req {score_req:.1f}', vpin=vpin); return False
        if not self._check_symbol(sym): return False

        if B_SHORT_DISABLED and mtf_dir == 'short': return False
        self._cooldowns[sym] = time.time()
        r['_mtf_dir']   = mtf_dir
        r['_mtf_score'] = mtf
        r['dir'] = mtf_dir
        return True

    # ── OF: Order Flow Imbalance ──────────────────────────────────
    def _ofi_gate(self, sym, r) -> bool:
        """
        Real order-flow-imbalance gate. Two components must agree:
          1. STATIC depth-weighted book imbalance (calc_obi, already in r['sigs']):
             persistent bid-lean precedes up-moves, ask-lean precedes down-moves.
          2. DYNAMIC depth flow from book_history (the OFI piece): bid depth rising
             while ask depth falling = active buy pressure (and vice versa).
        Plus a price micro-confirmation so we don't fire into a defended wall.
        """
        if self._has_open(sym): return False
        if not self._check_loss_streak(sym): return False
        st = E.sym_state.get(sym)
        if not st or st['price'] == 0: return False

        # thresholds (locals — OFI is self-contained, no config.py wiring needed)
        OFI_OBI_MIN  = 25.0     # |calc_obi| floor (calc_obi range −100..+100)
        OFI_FLOW_MIN = 0.04     # |Δdepth / avg_depth| floor over the window
        OFI_WINDOW_MS = 5000.0  # depth-flow lookback

        sigs = r.get('sigs', {})
        obi = sigs.get('obi')
        if obi is None or abs(obi) < OFI_OBI_MIN: return False
        obi_dir = 'long' if obi > 0 else 'short'

        now_ms = time.time() * 1000
        bh = [(ts, b, a) for ts, b, a in list(st.get('book_history', []))
              if now_ms - ts < OFI_WINDOW_MS]
        if len(bh) < 5: return False
        bid_delta = bh[-1][1] - bh[0][1]
        ask_delta = bh[-1][2] - bh[0][2]
        avg_depth = sum(b + a for _, b, a in bh) / len(bh)
        if avg_depth <= 0: return False
        ofi = (bid_delta - ask_delta) / avg_depth   # >0 buy pressure, <0 sell
        flow_dir = 'long' if ofi > 0 else 'short'

        # static imbalance and dynamic flow must agree, and flow must be material
        if obi_dir != flow_dir: return False
        if abs(ofi) < OFI_FLOW_MIN: return False

        # price micro-confirmation (last 3s moving with the signal, not against)
        ph = [p for ts, p in list(st['price_hist']) if now_ms - ts < 3000]
        if len(ph) >= 3 and ph[0]:
            mv = (ph[-1] - ph[0]) / ph[0] * 100
            if obi_dir == 'long'  and mv < -0.02: return False
            if obi_dir == 'short' and mv >  0.02: return False

        vpin = E.calc_vpin(sym)
        if vpin is not None and vpin < self.cfg.vpin_min: return False
        if vpin is not None and vpin > self.cfg.vpin_max: return False
        spread = E.calc_spread_pct(sym)
        if not self._spread_ok(spread): return False
        if E.get_atr(sym) < self.cfg.min_vol_atr: return False
        if abs(r['score']) < self.cfg.min_score: return False
        if r['conf'] < self.cfg.min_conf: return False

        log_signal('OF', sym, 'detected',
            f'obi={obi:.0f} ofi={ofi:+.4f} dir={obi_dir}',
            vpin=vpin, atr=E.get_atr(sym))
        self._cooldowns[sym] = time.time()
        r['_ofi']     = round(ofi, 4)
        r['_obi']     = round(obi, 1)
        r['_ofi_dir'] = obi_dir
        r['dir']      = obi_dir
        return True

    # ── H: Spoofing Fade (monitor-only until validated) ──────────
    def _h_gate(self, sym, r) -> bool:
        """
        Detect spoofed order book walls (large bid/ask appearing then vanishing).
        Fade direction: bid spoof → short (spoofer scared sellers, price will dip)
                        ask spoof → long  (spoofer scared buyers, price will spike)
        MONITOR ONLY: logs observation rows, never fires live trades.
        Change to return True after _log_spoof_observation() once 50+ obs validated.
        """
        if self._has_open(sym): return False
        st = E.sym_state.get(sym)
        if not st or st['price'] == 0: return False
        if not self._check_symbol(sym): return False

        sigs = r.get('sigs', {})
        spoof_val = sigs.get('spoof', 0) or 0

        # calc_spoofing returns -60 (bid spoof) or +60 (ask spoof) or 0
        if abs(spoof_val) < 55: return False   # only clear spoof events

        # Bid spoof (spoofed bid wall) → fade = short (price will dip once wall gone)
        # Ask spoof (spoofed ask wall) → fade = long  (price will spike once wall gone)
        spoof_dir = 'short' if spoof_val < 0 else 'long'

        # Price must have moved in spoof direction already (confirming manipulation worked)
        now_ms = time.time() * 1000
        ph5 = [p for ts, p in list(st['price_hist']) if now_ms - ts < 5000]
        if len(ph5) < 3: return False
        price_moved = (ph5[-1] - ph5[0]) / ph5[0] * 100
        if spoof_dir == 'long'  and price_moved > -0.03: return False  # need dip first
        if spoof_dir == 'short' and price_moved < 0.03:  return False  # need spike first

        vpin = E.calc_vpin(sym)
        if vpin is not None and vpin < self.cfg.vpin_min: return False
        if vpin is not None and vpin > self.cfg.vpin_max: return False
        if E.get_atr(sym) < self.cfg.min_vol_atr: return False

        r['_spoof_dir']   = spoof_dir
        r['_spoof_signal'] = spoof_val

        # Log observation for validation — do NOT fire live trade yet
        self._log_spoof_observation(sym, r, spoof_dir, spoof_val)
        return False   # ← change to True once 50+ observations validated

    # ── T: regime trend follower ──────────────────────────────────
    def _t_gate(self, sym, r) -> bool:
        if self._has_open(sym): return False
        if not self._check_loss_streak(sym): return False
        st = E.sym_state.get(sym)
        if not st or st['price'] == 0: return False
        trend_count = st.get('trend_tick_count', 0)
        trend_dir   = st.get('trend_dir', None)
        if trend_count < 12 or trend_dir is None: return False
        if T_LONG_DISABLED and trend_dir == 'long': return False
        if r['dir'] != trend_dir: return False
        vpin = E.calc_vpin(sym)
        if vpin is not None and vpin < self.cfg.vpin_min: return False
        spread = E.calc_spread_pct(sym)
        if spread is not None and spread > SPREAD_MAX_PCT * 2: return False
        if E.get_atr(sym) < self.cfg.min_vol_atr: return False
        self._cooldowns[sym] = time.time()
        r['_trend_dir']   = trend_dir
        r['_trend_ticks'] = trend_count
        return (r['conf'] >= self.cfg.min_conf and abs(r['score']) >= self.cfg.min_score)

    # ── CGY: Combined mean-reversion confirmation ───────────────
    # ── P: Parabolic Blowup Short ─────────────────────────────────
    def _p_gate(self, sym, r) -> bool:
        """
        Fire a short at the start of a parabolic blowup (distribution crash).
        Requires:
          - Coin pumped >=15% in last 30min
          - Last 5m candle is a large bearish body >=4%
          - No meaningful bounce between peak and entry
          - VPIN >= vpin_min (real selling volume)
          - Score sanity check (don't fight strong buying flow)
        """
        if self._has_open(sym): return False
        if not self._check_loss_streak(sym): return False
        if not self._check_symbol(sym): return False
        st = E.sym_state.get(sym)
        if not st or st['price'] == 0: return False

        from strategies_signals import _find_parabolic_blowup as _fpb
        sig = _fpb(sym)
        if sig is None: return False

        # Dedup: don't re-enter on same trigger candle
        cached = self._impulse_cache.get('P_' + sym)
        if cached and abs(cached - sig['ts_ms']) < 1000:
            return False

        vpin = E.calc_vpin(sym)
        if vpin is None or vpin < self.cfg.vpin_min:
            return False

        # VPIN ceiling: if VPIN >0.90 it's a full panic cascade, too late to short
        if vpin > 0.90:
            log_signal('P', sym, 'blocked', f'vpin {vpin:.3f} > 0.90 (cascade, too late)', vpin=vpin)
            return False

        if not self._spread_ok(E.calc_spread_pct(sym)): return False
        if E.get_atr(sym) < self.cfg.min_vol_atr: return False

        # Score sanity: don't short into strong buying flow
        score = r.get('score', 0)
        if score > 40:
            log_signal('P', sym, 'blocked', f'score={score:.0f} buyers still active', vpin=vpin)
            return False

        # Liq cascade: if already in full cascade, too late (price gap risk)
        _liq = E.calc_liq_pressure(sym)
        if _liq and _liq['rate'] > 8.0:
            log_signal('P', sym, 'blocked', f'liq_rate={_liq["rate"]:.1f} cascade active', vpin=vpin)
            return False

        log_signal('P', sym, 'detected',
            f"pump={sig['pump_pct']:.1f}% drop={sig['trigger_body_pct']:.1f}% "
            f"drawdown={sig['drawdown_pct']:.1f}% vpin={vpin:.3f}",
            vpin=vpin, atr=E.get_atr(sym))

        self._impulse_cache['P_' + sym] = sig['ts_ms']
        self._cooldowns[sym] = time.time()
        r['dir']              = 'short'
        r['_para_pump_pct']   = sig['pump_pct']
        r['_para_drop_pct']   = sig['trigger_body_pct']
        r['_para_peak_px']    = sig['peak_px']
        return True

    def _cgy_gate(self, sym, r) -> bool:
        """CGY: 2 of 3 mean-reversion signals (C+G+Y) must agree on direction."""
        if self._has_open(sym): return False
        if not self._check_loss_streak(sym): return False
        st = E.sym_state.get(sym)
        if not st or st['price'] == 0: return False
        if not self._check_symbol(sym): return False
        if r.get('conf', 0) < self.cfg.min_conf: return False
        if abs(r.get('score', 0)) < self.cfg.min_score: return False

        votes = []
        vpin = E.calc_vpin(sym)

        # C vote: VPIN in range + high score + high conf
        if vpin is not None and 0.20 <= vpin <= 0.92:
            if abs(r.get('score', 0)) >= 20 and r.get('conf', 0) >= 25:
                votes.append(('C', 'long' if r['score'] > 0 else 'short'))

        # G vote: spike acceleration + VPIN
        accel = E.calc_trade_accel(sym)
        if accel is not None and accel >= 1.2 and vpin is not None and vpin >= 0.25:
            if abs(r.get('score', 0)) >= 15 and r.get('conf', 0) >= 20:
                votes.append(('G', 'long' if r['score'] > 0 else 'short'))

        # Y vote: star pattern detected
        try:
            from strategies_signals import _find_star_pattern
            sig = _find_star_pattern(sym, min_pct=1.5)
            if sig is not None:
                cached = self._impulse_cache.get('CGY_' + sym)
                if not cached or abs(cached - sig['ts_ms']) >= 1000:
                    # fib_618 preferred but not required — star pattern alone is sufficient Y vote
                    votes.append(('Y', sig['fade_dir']))
                    r['_cgy_star_sig'] = sig
        except Exception:
            pass

        if len(votes) < 2: return False

        long_votes  = [v for v in votes if v[1] == 'long']
        short_votes = [v for v in votes if v[1] == 'short']
        if len(long_votes) >= 2:   agreed_dir = 'long'
        elif len(short_votes) >= 2: agreed_dir = 'short'
        else: return False

        # Quality gate: at least one vote must be Y (star pattern)
        # C+G alone agree too easily on high-NATR coins with negative scores
        # Y requires an actual 3-candle exhaustion pattern = higher quality signal
        winning_votes = [v for v in votes if v[1] == agreed_dir]
        if not any(v[0] == 'Y' for v in winning_votes): return False

        # EMA21 trend filter — fail OPEN on exception (don't kill all CGY on import error)
        try:
            from strategies_signals import _calc_ema, _build_candles
            c = _build_candles(sym, lookback_ms=1_800_000, bucket_ms=60_000)
            if len(c) >= 21:
                closes = [x['c'] for x in c]
                ema21 = _calc_ema(closes, 21)
                if ema21:
                    trend_up = st['price'] > ema21[-1]
                    if agreed_dir == 'long'  and not trend_up: return False
                    if agreed_dir == 'short' and trend_up:     return False
        except Exception:
            pass  # fail open — missing candle data shouldn't kill valid CGY signals

        if not self._spread_ok(E.calc_spread_pct(sym)): return False
        if E.get_atr(sym) < self.cfg.min_vol_atr: return False
        # ITEM 2+3: liq cascade + informed flow for CGY
        _liq_c = E.calc_liq_pressure(sym)
        if _liq_c and _liq_c['rate'] > 1.5: return False
        _ltr_c = E.calc_large_trade_ratio(sym)
        if _ltr_c is not None and _ltr_c < 0.06: return False

        if r.get('_cgy_star_sig'):
            self._impulse_cache['CGY_' + sym] = r['_cgy_star_sig']['ts_ms']

        vote_str = '+'.join(v[0] for v in votes if v[1] == agreed_dir)
        r['_cgy_votes'] = vote_str
        r['_cgy_dir']   = agreed_dir
        r['dir']        = agreed_dir
        self._cooldowns[sym] = time.time()
        # Log vote composition for diagnostics
        try:
            from engine_logger import log_signal
            log_signal('CGY', sym, 'votes', f"votes={vote_str} dir={agreed_dir}")
        except Exception:
            pass
        return True

    # ── WB: Combined momentum confirmation ───────────────────────
    def _wb_gate(self, sym, r) -> bool:
        """WB: W (BTC decorrelation) + B (MTF momentum) must agree on direction."""
        if self._has_open(sym): return False
        if not self._check_loss_streak(sym): return False
        st = E.sym_state.get(sym)
        if not st or st['price'] == 0: return False
        if not self._check_symbol(sym): return False

        vpin = E.calc_vpin(sym)
        if vpin is not None and vpin < self.cfg.vpin_min: return False
        if vpin is not None and vpin > self.cfg.vpin_max: return False
        if not self._spread_ok(E.calc_spread_pct(sym)): return False
        if E.get_atr(sym) < self.cfg.min_vol_atr: return False

        # W signal: BTC decorrelation
        try:
            from strategies_signals import _find_decorrelation_signal
            w_sig = _find_decorrelation_signal(sym)
        except Exception:
            w_sig = None
        if w_sig is None: return False
        w_dir = w_sig.get('dir') or w_sig.get('direction')
        if not w_dir: return False

        # B signal: MTF momentum — use E.calc_mtf_bias directly (no signals function)
        mtf = E.calc_mtf_bias(sym)
        if mtf is None or abs(mtf) < 15: return False
        b_dir = 'long' if mtf > 0 else 'short'

        if w_dir != b_dir: return False
        agreed_dir = w_dir

        if r.get('conf', 0) < self.cfg.min_conf: return False
        if abs(r.get('score', 0)) < self.cfg.min_score: return False
        score = r.get('score', 0)
        if agreed_dir == 'long'  and score < 0: return False
        if agreed_dir == 'short' and score > 0: return False

        r['_wb_dir']    = agreed_dir
        r['_wb_w_div']  = w_sig.get('divergence', 0)
        r['_wb_b_bias'] = mtf
        r['dir']        = agreed_dir
        self._cooldowns[sym] = time.time()
        return True

        # ── fire ──────────────────────────────────────────────────────
    def fire(self, sym, r, force_sim: bool = False):
        # Defensive: ensure preds is always a deque (state restore may have set list)
        if not isinstance(self.preds, deque):
            self.preds = deque(list(self.preds), maxlen=200)
        # ── Live execution: pre-entry balance + position cap check ─────────
        # Fast-path: if live disabled this returns (True, 'sim_mode') immediately.
        _entry_ok, _entry_reason = _live.can_enter(required_usdt=_live.LIVE_ORDER_USDT)
        log_signal(self.cfg.label, sym, 'fire_attempt',
            f'ok={_entry_ok} reason={_entry_reason} force_sim={force_sim} '
            f'live_exec={self.cfg.live_exec} LIVE_ENABLED={_live.LIVE_ENABLED} '
            f'bal={_live._cache_balance} n_open={_live._cache_n_positions}')
        if not _entry_ok:
            log_signal(self.cfg.label, sym, 'fire_blocked', f'can_enter failed: {_entry_reason}')
            return   # Hard gate: don't fire at all if Binance won't accept the order
        # ─────────────────────────────────────────────────────────────────
        st=E.sym_state[sym]
        _now = time.time()
        self._cooldowns[sym]=_now
        # FIX 2026-06-03: use r['dir'] directly — every gate sets r['dir'] as its last
        # authoritative act. The old fallback chain caused cross-contamination: K sets
        # r['_fade_dir']='long' on a coin, then Y fires on the same coin same tick and
        # fire() picks _fade_dir='long' (priority 2) over _star_dir='short' (priority 20).
        # r['dir'] is always set last by whichever gate approved this fire(), so it's correct.
        trade_dir = r['dir']
        if self.cfg.funding_fade_mode:
            open_on_sym = sum(1 for p in self.preds
                              if p.get('out3') is None and p['sym'] == sym)
            if open_on_sym >= Q_MAX_OPEN_PER_SYM:
                return
        dyn_tp,dyn_sl=self._calc_tp_sl(sym,r['score'],r['strength'])

        if self.cfg.impulse_fade_mode and r.get('_impulse_size'):
            dyn_tp=max(dyn_tp,min(1.50,r['_impulse_size']*0.60))
        if self.cfg.candle_level_mode and r.get('_level_type')=='break':
            dyn_tp=min(1.50,dyn_tp*1.40)
        if r.get('_vp_poc') and st['price']>0:
            poc_dist=abs(r['_vp_poc']-st['price'])/st['price']*100
            if poc_dist>0.10: dyn_tp=max(dyn_tp,min(2.0,poc_dist*0.90))
        if r.get('_range_tp',0)>0.10:
            dyn_tp=max(dyn_tp,min(2.0,r['_range_tp']))
        if self.cfg.funding_fade_mode and r.get('_funding_rate'):
            rate_x = abs(r['_funding_rate']) / 0.0001
            dyn_tp = max(dyn_tp, min(2.0, 0.20 * rate_x))
        if self.cfg.liq_cascade_mode:
            dyn_tp = max(dyn_tp, 0.25)
        if self.cfg.density_bounce_mode and r.get('_density_usd'):
            wall_m = r['_density_usd'] / 1_000_000
            dyn_tp = max(dyn_tp, min(1.0, 0.15 * wall_m))
        if self.cfg.decorrelation_mode and r.get('_decor_divergence'):
            dyn_tp = max(dyn_tp, min(0.80, r['_decor_divergence'] * 0.80))
        if self.cfg.knife_catch_mode and r.get('_knife_wick'):
            dyn_tp = max(dyn_tp, min(1.0, r['_knife_wick'] * 0.60))
        if self.cfg.lag_monitor_mode and r.get('_lag_best_div'):
            dyn_tp = max(dyn_tp, min(0.40, r['_lag_best_div'] * 0.80))
        if getattr(self.cfg, 'cgy_mode', False) and r.get('_cgy_star_sig'):
            star = r['_cgy_star_sig']
            fib = star.get('fib_618')
            entry_px = st.get('price', 0) if st else 0
            if fib and entry_px > 0:
                fib_dist = abs(fib - entry_px) / entry_px * 100
                if fib_dist > 0.05:
                    dyn_tp = min(3.0, max(dyn_tp, fib_dist))
        if getattr(self.cfg, 'wb_mode', False) and r.get('_wb_w_div'):
            dyn_tp = max(dyn_tp, min(1.20, abs(r['_wb_w_div']) * 0.80))
        if self.cfg.star_pattern_mode and r.get('_star_fib_618'):
            # FIX 2026-06-03: TP = fib 61.8% retracement distance from entry.
            # Geometrically correct: fading a 2% C1 candle targets 1.236% from entry.
            # Better than ATR-based TP which ignores the actual pattern magnitude.
            entry_px = r.get('_star_entry_px', 0)
            fib_px   = r['_star_fib_618']
            if entry_px > 0 and fib_px > 0:
                fib_dist_pct = abs(fib_px - entry_px) / entry_px * 100
                if fib_dist_pct > 0.05:
                    dyn_tp = min(3.0, max(dyn_tp, fib_dist_pct))

        _atr_now    = E.get_atr(sym)
        _vpin_now   = E.calc_vpin(sym)
        _spread_now = E.calc_spread_pct(sym)
        p=dict(id=self.hist_total+1,ts=_now,sym=sym,dir=trade_dir,
               conf=r['conf'],score=r['score'],n_agree=r['n_agree'],n_avail=r['n_avail'],
               entry=st['price'],dyn_tp=dyn_tp,dyn_sl=dyn_sl,
               out3=None,pct3=None,
               max_dp=-999,min_dp=999,
               snap30=None,snap60=None,snap1=None,
               tp_touches=0,be_activated=False,be_activated_at=None,exit_price=None,
               atr_entry=round(_atr_now,4),
               vpin_entry=round(_vpin_now,3) if _vpin_now is not None else None,
               spread_entry=round(_spread_now,5) if _spread_now is not None else None,
               reason=None,dur=None,tp_extended=False,be_locked=False,
               _trail_widened=False,
               _strategy_label=self.cfg.label,
               _wall_dir=r.get('_wall_dir'),       _vp_poc=r.get('_vp_poc'),
               _range_top=r.get('_range_top'),     _range_bot=r.get('_range_bot'),
               _funding_rate=r.get('_funding_rate'),
               _cascade_usd30=r.get('_cascade_usd30'),
               _oi_type=r.get('_oi_type'),
               _density_level=r.get('_density_level'),
               _decor_divergence=r.get('_decor_divergence'),
               _knife_wick=r.get('_knife_wick'),
               _lag_best_ms=r.get('_lag_best_ms'),
               _lag_best_div=r.get('_lag_best_div'),
               _lag_exchanges=r.get('_lag_exchanges'),
               _inertia_floor=getattr(self.cfg,'inertia_floor_mode',False))
        self.preds.appendleft(p); self.hist_total+=1; self._log_pred(p)
        self._open_syms.add(sym)

        # ── Live execution: place entry market order ───────────────────────
        # can_enter() already passed at top of fire() — place the order directly.
        # No-op (returns {ok:False, skipped:True}) when LIVE_ENABLED=False.
        # Also skip if strategy is marked live_exec=False (sim-only strategy).
        _side = 'BUY' if trade_dir == 'long' else 'SELL'
        # n_agree gate: single sub-strategy fires are 17%WR, force sim
        n_agree = r.get('n_agree', 1) if isinstance(r, dict) else 1
        if int(n_agree) < 2 and not force_sim:
            force_sim = True  # single signal — sim only

        if getattr(self.cfg, 'live_exec', True) and not force_sim:
            # Route through maker-entry if eligible (E/CGYL/Q) and MAKER_ENTRIES=true.
            # Maker attempt posts a GTX LIMIT at the touch; if it doesn't fill within
            # the budget (signal expiry / price moved away) it falls back to taker.
            # All other strategies go straight to taker market order.
            if _maker.should_use_maker(self.cfg.label):
                _resp = _maker.fire_maker_entry(
                    sym, _side, _live.LIVE_ORDER_USDT,
                    strategy_label=self.cfg.label, sl_pct=p['dyn_sl']
                )
                if _resp.get('fallback'):
                    # Maker didn't fill — fall back to taker
                    _resp = _live.create_order(sym, _side, _live.LIVE_ORDER_USDT, sl_pct=p['dyn_sl'])
            else:
                _resp = _live.create_order(sym, _side, _live.LIVE_ORDER_USDT, sl_pct=p['dyn_sl'])
        else:
            _resp = {'ok': False, 'skipped': True, 'reason': 'sim_only'}
        p['_live_order_id'] = _resp.get('orderId')
        p['_live_ok']       = _resp.get('ok', False)
        # ─────────────────────────────────────────────────────────────────
        log_trade_open(p)
        # Log to signals CSV for gate funnel analysis
        log_signal(self.cfg.label, sym, 'fired',
                   f"{p['dir']} entry={p['entry']:.2f} tp={p['dyn_tp']:.3f}% sl={p['dyn_sl']:.3f}%",
                   vpin=p.get('vpin_entry'), price=p['entry'], conf=p.get('conf'), score=p.get('score'))

    def _calc_tp_sl(self,sym,score,strength):
        atr=max(E.get_atr(sym),0.08)
        tp=max(0.18,min(1.50,atr*self.cfg.atr_tp_mult)); sl=max(0.12,min(0.60,atr*self.cfg.atr_sl_mult))
        if strength>=70 and abs(score)>=70: tp=min(1.50,tp*1.20)
        if strength<50 or abs(score)<55:   tp=max(0.16,tp*0.80); sl=max(0.12,sl*0.85)
        return round(tp,4),round(sl,4)

    # ── check outcomes ────────────────────────────────────────────
    def check_outcomes(self):
        now=time.time()
        for p in self.preds:
            if p['out3'] is not None: continue
            st=E.sym_state.get(p['sym'])
            if not st or not p['entry'] or st['price']==0: continue
            elapsed=now-p['ts']
            raw=(st['price']-p['entry'])/p['entry']*100
            dp=raw if p['dir']=='long' else -raw
            if elapsed>=30 and p['snap30'] is None: p['snap30']=round(dp,4)
            if elapsed>=60 and p['snap60'] is None: p['snap60']=round(dp,4); p['snap1']=round(dp,4)
            p['max_dp']=max(p.get('max_dp',-999),dp)
            p['min_dp']=min(p.get('min_dp', 999),dp)
            # ── Break-even lock — evaluated EVERY tick, ABOVE the min_hold gate ──
            # Bug fix 2026-06-16: activation used to live below the `min_hold_any`
            # `continue` (and keyed off the current dp), so a trade that spiked to
            # the trigger and reversed inside the hold window never armed it.
            # exit_sim showed 37% of B's losers reached MFE>=0.15 and still took a
            # full SL. Arm off PEAK MFE (max_dp) so a fast spike can't slip past,
            # then exit at ~scratch (be_floor ~ FEE_RT → net≈0). Tunable per env via
            # cfg.be_trigger / cfg.be_floor without a schema change.
            _be_trig  = getattr(self.cfg, 'be_trigger', 0.15)
            _be_floor = getattr(self.cfg, 'be_floor', FEE_RT)
            if not p.get('be_locked') and p['max_dp'] >= _be_trig:
                p['be_locked']       = True
                p['be_activated']    = True
                p['be_activated_at'] = round(elapsed, 1)
            if p.get('be_locked') and dp <= _be_floor:
                self._close(p, dp, 'be'); continue
            tp=p['dyn_tp']; sl=p['dyn_sl']
            if dp<=-sl: self._close(p,dp,'sl'); continue
            if elapsed<self.cfg.min_hold_any: continue
            if elapsed>=self.cfg.inertia_sec and dp<0 and p['max_dp']<self.cfg.inertia_thr:
                # Inertia floor mode: wait for dp>=FEE_RT before closing (converts lose→flat).
                # Signal wiggles +-0.1% during dead periods — floor exit avoids guaranteed loss.
                # Timeout at 2x inertia_sec → close as normal inertia. SL remains active.
                if p.get('_inertia_floor'):
                    if not p.get('_inertia_floor_ts'):
                        p['_inertia_floor_ts'] = now  # mark floor mode start
                    if dp >= FEE_RT:
                        self._close(p, dp, 'inertia_floor_win'); continue  # recovered — flat/win
                    elif (now - p['_inertia_floor_ts']) >= self.cfg.inertia_sec:
                        self._close(p, dp, 'inertia'); continue  # timed out — normal inertia
                    # else: still in patience window, keep ticking
                else:
                    self._close(p,dp,'inertia'); continue
            if p['max_dp']>=self.cfg.win_thr and dp<=p['max_dp']-(p.get('_trail_dist') or self.cfg.trail_dist):
                self._close(p,dp,'trail'); continue
            # (BE lock moved to the top of check_outcomes, above the min_hold gate.)
            if dp>=tp:
                p['tp_touches']=p.get('tp_touches',0)+1
                if E.sig_still_valid(p['sym'],p['dir']) and not p.get('tp_extended',False):
                    p['dyn_tp']=min(1.50,tp+E.get_atr(p['sym'])*0.4); p['tp_extended']=True
                else: self._close(p,dp,'tp')
                continue
            # M: wall absorbed check
            if self.cfg.volume_wall_mode and p.get('_wall_dir') and elapsed>=10:
                if not _wall_stable(p['sym'],p['_wall_dir']):
                    self._close(p,dp,'rev'); continue
            # X: knife catch — exit if tape flips against position after 20s
            if self.cfg.knife_catch_mode and elapsed >= 20:
                if st:
                    now_ms=now*1000
                    tape_20s=[(v,b) for ts2,_,v,b in list(st.get('trade_tape',[]))
                              if now_ms-ts2 < 20_000]
                    if tape_20s:
                        buy_vol  = sum(v for v,b in tape_20s if b)
                        sell_vol = sum(v for v,b in tape_20s if not b)
                        if p['dir']=='long'  and sell_vol > buy_vol*2.5:
                            self._close(p,dp,'rev'); continue
                        if p['dir']=='short' and buy_vol  > sell_vol*2.5:
                            self._close(p,dp,'rev'); continue
            # R: exit when cascade dries up
            if self.cfg.liq_cascade_mode and elapsed>=10:
                if st:
                    now_ms=now*1000
                    recent_liqs=[(il,v) for ts2,il,v in list(st.get('liq_cascade_hist',[]))
                                 if now_ms-ts2 < 15_000]
                    cascade_dir_is_long = (p['dir']=='long')
                    driving_usd = sum(v for il,v in recent_liqs
                                      if il == (not cascade_dir_is_long))
                    if driving_usd < p.get('_cascade_usd30', 0) * 0.10:
                        self._close(p,dp,'rev'); continue
            # N: exit at POC
            if p.get('_vp_poc') and elapsed>=self.cfg.min_hold_any and p['entry']>0:
                poc_dp=(p['_vp_poc']-p['entry'])/p['entry']*100
                if p['dir']=='short': poc_dp=-poc_dp
                if dp>=poc_dp*0.90: self._close(p,dp,'tp'); continue
            # O: exit at opposite range boundary — only when profitable
            # FIX 2026-05-26: boundary was firing 'tp' even when dp < 0 (118 neg-TP
            # trades, avg dur=20s). Price races through boundary during range breakdown.
            # Guard: require dp > FEE_RT before accepting boundary as a TP exit.
            # If underwater at boundary, let trade fluctuate — trail or SL will handle it.
            if self.cfg.consolidation_mode and p.get('_range_top') and elapsed>=self.cfg.min_hold_any:
                boundary_hit = (
                    (p['dir']=='long'  and st['price']>=p['_range_top']*0.998) or
                    (p['dir']=='short' and st['price']<=p['_range_bot']*1.002)
                )
                if boundary_hit and dp > FEE_RT:
                    self._close(p,dp,'tp'); continue
            # snap30 mid-trade filter + dynamic trail — ALL strategies
            # DATA 2026-05-26: snap30>+0.05 → 77-100%WR; snap30<-0.05 → 0-23%WR.
            # Also: avg trail gap = +0.139% (wins peak at +0.30%, capture only +0.16%).
            # Dynamic trail: widen when snap30 strongly positive (trade is working),
            # tighten back to default once snap30 evidence expires.
            # BE tightening: 275/491 losses had positive max_dp before reversing —
            # lowering BE activation from 50%→30% of TP locks in scratch instead of loss.
            if elapsed >= 35:
                snap30 = p.get('snap30')
                hold_thr = LAG_SNAP30_HOLD_THR if self.cfg.lag_monitor_mode else 0.05
                exit_thr = LAG_SNAP30_EXIT_THR if self.cfg.lag_monitor_mode else -0.06
                if snap30 is not None:
                    if snap30 > hold_thr:
                        # Trade strongly moving in our favour at 30s.
                        # Widen trail by 50% so we don't get shaken out of a real move.
                        # Only apply once (check flag) and only if not already widened.
                        if not p.get('_trail_widened'):
                            p['_trail_dist_orig'] = self.cfg.trail_dist
                            p['_trail_widened']   = True
                            # Widen is stored on the pred dict so it persists per-trade.
                            # We reference it in the trail check above via p['_trail_dist'].
                            p['_trail_dist'] = min(self.cfg.trail_dist * 1.5, 0.25)
                        # Also tighten BE: activate at 30% of TP instead of 50%
                        # so we lock in scratch on the 275 trades that were +ve then reversed.
                        if not p.get('be_locked') and dp >= p['dyn_tp'] * 0.30:
                            p['dyn_sl'] = FEE_RT  # break-even
                            p['be_locked'] = True
                            p['be_activated'] = True
                            p['be_activated_at'] = round(elapsed, 1)
                        pass  # skip rev — trade is working
                    elif snap30 < exit_thr and dp < 0:
                        # Restore original trail if we widened it and trade went against us
                        if p.get('_trail_widened'):
                            p['_trail_dist'] = p.get('_trail_dist_orig', self.cfg.trail_dist)
                            p['_trail_widened'] = False
                        if elapsed >= self.cfg.inertia_sec * 0.40:
                            # Floor mode applies to snap30 early-inertia too
                            if p.get('_inertia_floor'):
                                if not p.get('_inertia_floor_ts'):
                                    p['_inertia_floor_ts'] = now
                                if dp >= FEE_RT:
                                    self._close(p, dp, 'inertia_floor_win'); continue
                                elif (now - p['_inertia_floor_ts']) >= self.cfg.inertia_sec:
                                    self._close(p, dp, 'inertia'); continue
                            else:
                                self._close(p, dp, 'inertia'); continue
                        elif elapsed >= self.cfg.rev_min_hold and E.sig_reversed(p['sym'], p['dir']):
                            self._close(p, dp, 'rev'); continue
                    else:
                        if elapsed >= self.cfg.rev_min_hold and E.sig_reversed(p['sym'], p['dir']):
                            self._close(p, dp, 'rev'); continue
                elif elapsed >= self.cfg.rev_min_hold and E.sig_reversed(p['sym'], p['dir']):
                    self._close(p, dp, 'rev'); continue
            elif elapsed >= self.cfg.rev_min_hold and E.sig_reversed(p['sym'], p['dir']):
                self._close(p, dp, 'rev'); continue
            # QQ: exit when funding returns to neutral
            if self.cfg.funding_normalise_exit and elapsed >= self.cfg.min_hold_any:
                if st:
                    rate = st.get('funding_rate', None)
                    if rate is not None:
                        from strategies_signals import FUNDING_LONG_THR, FUNDING_SHORT_THR
                        normalised = (
                            (p['dir']=='short' and rate < FUNDING_LONG_THR  * 0.6) or
                            (p['dir']=='long'  and rate > FUNDING_SHORT_THR * 0.6)
                        )
                        if normalised:
                            self._close(p, dp, 'tp'); continue
            # T: exit if regime flips
            if self.cfg.regime_trend_mode and elapsed >= self.cfg.min_hold_any:
                if st and st.get('trend_dir') != p.get('_trend_dir'):
                    self._close(p, dp, 'rev'); continue
            if elapsed>=self.cfg.max_window: self._close(p,dp,'time')

    def _close(self,p,dp,reason):
        # ── Release slot and fire real Binance close when the LIVE OWNER exits ──
        # _release_open returns (is_last, close_real). close_real=True only when THIS
        # strategy placed the real order (live owner) — so the real position's
        # lifecycle is tied to the strategy that owns the capital, not the last sim
        # holder. This keeps swing + scalp strategies on the same symbol from
        # contaminating each other's real fills / PnL attribution.
        try:
            import strategies_runtime as _rt2
            if getattr(self.cfg, 'shadow', False):
                # Shadow strategy: never registered in the global slot, so never
                # release it and never fire a real close (the untracked fail-safe
                # path would otherwise close a real position it doesn't own).
                is_last, close_real = False, False
            else:
                is_last, close_real = _rt2._release_open(p['sym'], self.cfg.label)
        except Exception:
            is_last, close_real = True, bool(p.get('_live_ok'))  # fail safe

        if close_real:
            _close_resp = _live.close_position(p['sym'], p['dir'], reason=reason)
            p['_bnb_pnl']  = _close_resp.get('realized_pnl')
            p['_bnb_comm'] = _close_resp.get('commission')
            if p['_bnb_pnl'] is not None:
                self._bnb_cum_pnl  += p['_bnb_pnl']
                self._bnb_cum_comm += (p['_bnb_comm'] or 0)
        else:
            # Not the live owner — slot tracked above, no real close from this exit
            p['_bnb_pnl'] = None; p['_bnb_comm'] = None
        # ─────────────────────────────────────────────────────────────────
        if   reason=='tp':                result='win'
        elif reason=='sl':                result='lose'
        elif reason=='trail':             result='win' if (dp-FEE_RT)>0 else ('lose' if (dp-FEE_RT)<-FEE_RT else 'flat')
        elif reason=='inertia':           result='lose'
        elif reason=='inertia_floor_win': result='flat' if dp < self.cfg.win_thr else 'win'  # floor recovery
        else: result='win' if dp>=self.cfg.win_thr else 'lose' if dp<=-self.cfg.win_thr else 'flat'
        p['out3']=result; p['pct3']=dp; p['reason']=reason; p['dur']=time.time()-p['ts']
        self._open_syms.discard(p['sym'])
        st_exit=E.sym_state.get(p['sym'])
        if st_exit and st_exit.get('price'): p['exit_price']=st_exit['price']
        if result=='win':
            self.hist_win+=1
            self._loss_streak[p['sym']] = 0
        elif result=='lose':
            self.hist_lose+=1
            self._loss_streak[p['sym']] = self._loss_streak.get(p['sym'], 0) + 1
        self._cum_net+=dp-FEE_RT
        self.pnl_history.append({'ts':int(p['ts']*1000),'cum':round(self._cum_net,4),
                                  'out':result,'sym':p['sym'].replace('USDT','')})
        if len(self.pnl_history)>200: self.pnl_history=self.pnl_history[-200:]
        self._save_state()   # persist after every resolved trade
        self._log_outcome(p)
        log_trade_close(p)
        # Log close event to signals CSV for complete funnel
        net = (p.get('pct3') or 0.0) - (FEE_RT or 0.06)
        log_signal(p.get('_strategy_label','?'), p['sym'], 'closed',
                   f"{p['dir']} {p.get('reason','?')} net={net:+.4f}% dur={p.get('dur',0):.0f}s",
                   price=p.get('entry'), score=p.get('score'))
    def _init_log(self):
        if self._no_log: return
        self._log_header = [
            ['# STRATEGY',self.cfg.name,
             f'vpin≥{self.cfg.vpin_min}',f'conf≥{self.cfg.min_conf}',
             f'score≥{self.cfg.min_score}',f'atr≥{self.cfg.min_vol_atr}',
             f'win_thr={self.cfg.win_thr}',f'sl_mult={self.cfg.atr_sl_mult}',
             f'inertia={self.cfg.inertia_sec}s',f'max_window={self.cfg.max_window}'],
            ['time','ts_epoch','sym','dir','conf','score','n_agree','n_avail',
             'entry_px','dyn_tp','dyn_sl',
             'atr_entry','vpin_entry','spread_entry',
             'exit_px','pct_exit','net_exit','outcome','reason','dur_sec',
             'max_dp','min_dp','snap30','snap60',
             'be_activated','be_at_sec','tp_extended','tp_touches',
             'version'],
        ]

    def _has_open(self, sym) -> bool:
        return sym in self._open_syms

    def _ensure_log_created(self):
        if self._no_log or not hasattr(self, '_log_header'): return
        if not hasattr(self, '_log_created'):
            with open(self.log_file,'w',newline='') as f:
                w = csv.writer(f)
                for row in self._log_header:
                    w.writerow(row)
            self._log_created = True

    def _log_pred(self,p):
        if self._no_log: return
        self._ensure_log_created()
        with open(self.log_file,'a',newline='') as f:
            csv.writer(f).writerow([
                datetime.fromtimestamp(p['ts']).strftime('%H:%M:%S'),
                round(p['ts'], 3),
                p['sym'], p['dir'], p['conf'], round(p['score']),
                p.get('n_agree',''), p.get('n_avail',''),
                f"{p['entry']:.6f}", f"{p['dyn_tp']:.4f}", f"{p['dyn_sl']:.4f}",
                f"{p['atr_entry']:.4f}"    if p.get('atr_entry')    is not None else '',
                f"{p['vpin_entry']:.3f}"   if p.get('vpin_entry')   is not None else '',
                f"{p['spread_entry']:.5f}" if p.get('spread_entry') is not None else '',
                '','','','','','',
                '','','','',
                '','','','',
                VERSION['v'],
            ])

    def snapshot(self):
        now=time.time(); resolved=[p for p in list(self.preds) if p['out3'] is not None]
        gross=sum(p['pct3'] for p in resolved); net_t=sum(p['pct3']-FEE_RT for p in resolved)
        decided=self.hist_win+self.hist_lose; wr=round(self.hist_win/max(decided,1)*100,1)
        wins_p=[p['pct3'] for p in resolved if p['out3']=='win']
        loses_p=[p['pct3'] for p in resolved if p['out3']=='lose']
        by_r={}
        for p in resolved:
            k=p.get('reason','?'); by_r.setdefault(k,{'w':0,'l':0,'f':0})
            by_r[k][p['out3'][0]]=by_r[k].get(p['out3'][0],0)+1
        live_preds=[]
        for p in list(self.preds)[:20]:
            elapsed=now-p['ts']; st2=E.sym_state.get(p['sym']); live_dp=None
            if p['out3'] is None and st2 and st2['price'] and p['entry']:
                raw=(st2['price']-p['entry'])/p['entry']*100
                live_dp=round(raw if p['dir']=='long' else -raw,3)
            live_preds.append({'id':p['id'],'ts':int(p['ts']*1000),
                'sym':p['sym'].replace('USDT',''),'dir':p['dir'],
                'conf':p['conf'],'score':round(p['score'],1),
                'entry':p['entry'],'dyn_tp':p['dyn_tp'],'dyn_sl':p['dyn_sl'],
                'out':p['out3'],
                'pct':round(p['pct3'],4) if p['pct3'] is not None else None,
                'net':round(p['pct3']-FEE_RT,4) if p['pct3'] is not None else None,
                'reason':p.get('reason'),
                'dur':round(p['dur'],1) if p.get('dur') else None,
                'live_ok':p.get('_live_ok', False),'bnb_pnl':p.get('_bnb_pnl'),'live_dp':live_dp,'elapsed':round(elapsed)})
        max_dp_vals = [p['max_dp'] for p in resolved if p.get('max_dp',-999) > -999]
        snap30_vals = [p['snap30'] for p in resolved if p.get('snap30') is not None]
        snap60_vals = [p['snap60'] for p in resolved if p.get('snap60') is not None]
        inertia_exits = [p for p in resolved if p.get('reason') == 'inertia']
        be_count    = sum(1 for p in resolved if p.get('be_activated'))
        tp_touch_all= sum(p.get('tp_touches',0) for p in resolved)
        tp_ext_count= sum(1 for p in resolved if p.get('tp_extended'))
        by_dir = {}
        for d in ['long','short']:
            sub = [p for p in resolved if p.get('dir')==d]
            if sub:
                sw = sum(1 for p in sub if p.get('out3')=='win')
                by_dir[d] = {'count':len(sub),'wins':sw,
                             'wr':round(sw/len(sub)*100,1),
                             'net':round(sum(p['pct3'] for p in sub)/len(sub),4)}
        sym_streaks = {k:v for k,v in self._loss_streak.items() if v>0}
        trail_wins  = [p for p in resolved if p.get('reason')=='trail' and p.get('out3')=='win']
        tp_gap_vals = [p['max_dp']-p['pct3'] for p in trail_wins if p.get('max_dp',-999)>-999]
        return {'label':self.cfg.label,'name':self.cfg.name,'color':self.cfg.color,'disabled':self.cfg.disabled,
                'live_exec':getattr(self.cfg,'live_exec',True),
                'bnb_cum_pnl':round(self._bnb_cum_pnl,4),'bnb_cum_comm':round(self._bnb_cum_comm,4),
                'total':self.hist_total,'wins':self.hist_win,'loses':self.hist_lose,
                'gross':round(gross,3),'net':round(net_t,3),'cum_net':round(self._cum_net,3),
                'expect':round(net_t/max(len(resolved),1),4),'wr':wr,
                'avg_win':round(sum(wins_p)/max(len(wins_p),1),3),
                'avg_loss':round(sum(loses_p)/max(len(loses_p),1),3),
                'avg_max_dp':   round(sum(max_dp_vals)/max(len(max_dp_vals),1),4),
                'avg_snap30':   round(sum(snap30_vals)/max(len(snap30_vals),1),4) if snap30_vals else None,
                'avg_snap60':   round(sum(snap60_vals)/max(len(snap60_vals),1),4) if snap60_vals else None,
                'inertia_count':len(inertia_exits),
                'inertia_pct':  round(len(inertia_exits)/max(len(resolved),1)*100,1),
                'be_activated': be_count,
                'tp_hits':      tp_touch_all,
                'tp_extended_count': tp_ext_count,
                'avg_tp_gap':   round(sum(tp_gap_vals)/max(len(tp_gap_vals),1),4) if tp_gap_vals else None,
                'by_dir':       by_dir,
                'sym_streaks':  sym_streaks,
                'by_reason':by_r,'pnl_history':list(reversed(self.pnl_history[-50:])),
                'preds':live_preds,'log_file':self.log_file,
                'params':{'vpin_min':self.cfg.vpin_min,'vpin_max':self.cfg.vpin_max,
                          'min_conf':self.cfg.min_conf,
                          'min_score':self.cfg.min_score,'win_thr':self.cfg.win_thr,
                          'trail_dist':self.cfg.trail_dist,'inertia_sec':self.cfg.inertia_sec,
                          'cooldown':self.cfg.cooldown_sec,
                          'spread_gate':self.cfg.use_spread_gate,'accel_gate':self.cfg.use_accel_gate,
                          'spread_max_mult':getattr(self.cfg,'spread_max_mult',2.0),
                          'min_vol_atr':self.cfg.min_vol_atr,
                          'loss_streak_limit':self.cfg.loss_streak_limit,
                          'blacklist_count':len(self.cfg.symbol_blacklist),
                          'max_conflict':self.cfg.max_conflict,
                          'impulse_fade_mode':self.cfg.impulse_fade_mode,
                          'candle_level_mode':self.cfg.candle_level_mode,
                          'volume_wall_mode':self.cfg.volume_wall_mode,
                          'volume_profile_mode':self.cfg.volume_profile_mode,
                          'consolidation_mode':self.cfg.consolidation_mode,
                          'breakout_retest_mode':self.cfg.breakout_retest_mode}}

    def _log_outcome(self,p):
        if self._no_log: return
        self._ensure_log_created()
        net=p['pct3']-FEE_RT if p['pct3'] is not None else None
        with open(self.log_file,'a',newline='') as f:
            csv.writer(f).writerow([
                'OUT_'+datetime.fromtimestamp(p['ts']).strftime('%H:%M:%S'),
                round(p['ts'], 3),
                p['sym'], p['dir'], p['conf'], round(p['score']),
                p.get('n_agree',''), p.get('n_avail',''),
                f"{p['entry']:.6f}", f"{p['dyn_tp']:.4f}", f"{p['dyn_sl']:.4f}",
                f"{p['atr_entry']:.4f}"    if p.get('atr_entry')    is not None else '',
                f"{p['vpin_entry']:.3f}"   if p.get('vpin_entry')   is not None else '',
                f"{p['spread_entry']:.5f}" if p.get('spread_entry') is not None else '',
                f"{p['exit_price']:.6f}"   if p.get('exit_price')   is not None else '',
                f"{p['pct3']:.4f}"         if p['pct3']             is not None else '',
                f"{net:.4f}"               if net                    is not None else '',
                p['out3'] or '',
                p.get('reason',''),
                f"{p['dur']:.1f}"          if p.get('dur')          else '',
                f"{p['max_dp']:.4f}"       if p.get('max_dp',-999)  > -999 else '',
                f"{p['min_dp']:.4f}"       if p.get('min_dp', 999)  <  999 else '',
                f"{p['snap30']:.4f}"       if p.get('snap30')       is not None else '',
                f"{p['snap60']:.4f}"       if p.get('snap60')       is not None else '',
                '1' if p.get('be_activated') else '0',
                f"{p['be_activated_at']:.1f}" if p.get('be_activated_at') is not None else '',
                '1' if p.get('tp_extended')  else '0',
                p.get('tp_touches', 0),
                VERSION['v'],
            ])

    def _log_spoof_observation(self, sym: str, r: dict, spoof_dir: str, spoof_val: float):
        """Log spoofing observation for validation. Does not fire a live trade."""
        if self._no_log: return
        try:
            self._ensure_log_created()
            with open(self.log_file, 'a', newline='') as f:
                csv.writer(f).writerow([
                    'SPOOF_' + datetime.now().strftime('%H:%M:%S'),
                    sym, spoof_dir,
                    r.get('conf',''), round(r.get('score',0)),
                    r.get('n_agree',''), r.get('n_avail',''),
                    f"{E.sym_state[sym]['price']:.6f}",
                    '', '',   # no dyn_tp/sl
                    f"{E.get_atr(sym):.4f}",
                    f"{E.calc_vpin(sym):.3f}" if E.calc_vpin(sym) is not None else '',
                    f"{E.calc_spread_pct(sym):.5f}" if E.calc_spread_pct(sym) is not None else '',
                    '', '', '', '', '', '',
                    '', '', '', '',
                    '', '', '', '',
                    f"spoof={spoof_val:.0f} dir={spoof_dir}",
                ])
        except Exception:
            pass

    def _log_lag_observation(self, sym: str, sig: dict):
        if self._no_log: return
        try:
            with open(self.log_file, 'a', newline='') as f:
                csv.writer(f).writerow([
                    'LAG_' + datetime.now().strftime('%H:%M:%S'),
                    sym, sig['dir'],
                    '', '', '', '',
                    f"{sig['bnx_px']:.6f}",
                    '', '',
                    f"{E.get_atr(sym):.4f}", '', '',
                    '', '', '', '', '', '',
                    '', '', '', '',
                    '', '', '', '',
                    f"lag_ms={sig['best_lag_ms']:.0f} div={sig['best_div_pct']:.4f}% "
                    f"exch={','.join(l['exchange'] for l in sig['lagging'])} "
                    f"bnx_move={sig['bnx_move']:.4f}%",
                ])
        except Exception:
            pass


# ══ GLOBAL ════════════════════════════════════════════════════════
_engines_a: list[StrategyEngine] = []
_engines_b: list[StrategyEngine] = []
