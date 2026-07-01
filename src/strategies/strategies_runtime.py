"""
strategies_runtime.py — Global strategy lifecycle: init, tick, check, hot-reload.

Imported by strategies.py (public API) and predict_engine.py.
StrategyEngine instances live in _engines_a / _engines_b.
"""
import time, importlib
from datetime import datetime
from pathlib import Path
import engine as E
from strategies_config import STRATEGIES, StrategyConfig
import strategies_engine as _se  # for StrategyEngine class
from strategies_engine import StrategyEngine  # noqa: F401
from config import CONFLUENCE_REGIME_BLOCK_TICKS, CONFLUENCE_REGIME_ENABLED, POSITION_LOCK_MODE, GLOBAL_MAX_OPEN_PER_SYM
import live_execution as _live  # for LIVE_ENABLED check in z_fast_check + _apply_reload

# ── Global cross-strategy position registry ──────────────────────
# Maps sym → label of strategy currently holding a position on that symbol.
# Prevents multiple strategies trading the same coin simultaneously on live exec.
#
# OPTION A (prod): first-come-first-served lock.
#   Any strategy that opens a position claims the sym slot.
#   All other strategies are blocked until the position closes.
#
# OPTION B (b-test): priority-ranked lock.
#   Higher-priority strategies can always take the slot.
#   Lower-priority strategies blocked if a higher-priority one is open.
#   Priority: E > CGY > B > L > Y > W > K (signal quality descending)
#
# Controlled by config: POSITION_LOCK_MODE = 'A' or 'B'
#   Same-direction entries allowed: multiple strategies can hold the same
#   Binance position as long as they all agree on direction.
#   Opposite-direction entries blocked: would flip or close the live position.
#   The real Binance close order fires only when the LAST strategy on a symbol exits.
_global_positions: dict[str, dict] = {}
_post_close_cd:   dict[str, float] = {}  # sym → close_ts; blocks re-entry
REENTRY_COOLDOWN_SEC = 120  # block all re-entry on a coin for 2min after close
# sym → {'holders': {label: dir}, 'direction': 'long'|'short'|None}

# Priority for Option B (lower index = higher priority)
_STRATEGY_PRIORITY = ['B', 'E', 'CGY', 'L', 'Y', 'W', 'K', 'Q', 'S', 'WB', 'G', 'C']

def _priority(label: str) -> int:
    try: return _STRATEGY_PRIORITY.index(label)
    except ValueError: return 999

def _can_enter(sym: str, label: str, mode: str, direction: str = None) -> bool:
    """
    Check if strategy `label` can open a position on `sym` in `direction`.

    Rules:
      - Slot free → always allow
      - Same direction as current holders → allow (shared Binance position)
      - Opposite direction → block (would flip live position)
      - direction=None (unknown yet) → fall back to old behaviour
    """
    # Post-close cooldown: block re-entry for REENTRY_COOLDOWN_SEC after any close
    import time as _t
    cd_ts = _post_close_cd.get(sym)
    if cd_ts is not None:
        if _t.time() - cd_ts < REENTRY_COOLDOWN_SEC:
            return False  # coin in post-close cooldown, prevents whipsawing
        del _post_close_cd[sym]

    state = _global_positions.get(sym)
    if state is None or not state.get('holders'):
        return True   # slot free
    if label in state['holders']:
        return True   # already holding (shouldn't re-enter but safe)

    held_dir = state.get('direction')

    # If direction known: allow same, block opposite
    if direction is not None and held_dir is not None:
        if direction == held_dir:
            return True   # same direction → shared position, allow
        else:
            return False  # opposite direction → would flip, block

    # direction unknown — fall back to original mode logic
    if mode == 'A':
        return False
    if mode == 'B':
        holder_labels = list(state['holders'].keys())
        best_holder = min(holder_labels, key=_priority) if holder_labels else None
        return best_holder is None or _priority(label) < _priority(best_holder)
    return False

def _register_open(sym: str, label: str, direction: str = None, live_ok: bool = False):
    """Called when a strategy successfully fires on sym.
    live_ok=True means a real Binance order was placed for this entry."""
    if sym not in _global_positions:
        _global_positions[sym] = {'holders': {}, 'direction': direction,
                                  'any_live': False, 'live_owner': None}
    _global_positions[sym]['holders'][label] = direction
    if direction is not None:
        _global_positions[sym]['direction'] = direction
    if live_ok:
        _global_positions[sym]['any_live'] = True
        # Record the strategy that actually placed the real order. The real
        # position's lifecycle follows THIS strategy's exit (see _release_open),
        # not the last sim holder — so a 30-min swing sim-holder can't drag a
        # 2-min scalp's real fill, and the realized PnL is attributed correctly.
        if _global_positions[sym].get('live_owner') is None:
            _global_positions[sym]['live_owner'] = label

def _release_open(sym: str, label: str):
    """
    Called when a strategy closes its position on sym.
    Returns (is_last: bool, close_real: bool).
      is_last    — True if this was the LAST holder; slot is freed + re-entry cooldown set.
      close_real — True if the caller should send the real Binance close NOW.

    The real Binance close fires when the LIVE OWNER (the strategy that placed the
    order) exits — NOT when the last sim holder exits. This decouples the real
    position's lifecycle from sim holders on other timeframes. Previously the real
    close was deferred to the last holder, which let a long-hold sim strategy drag a
    scalp's real fill (and misattribute its PnL). After the live owner closes, any
    remaining sim holders continue tracking sim-only.
    """
    state = _global_positions.get(sym)
    if state is None:
        return True, True  # wasn't tracked — assume live, close safely
    state['holders'].pop(label, None)

    close_real = False
    if state.get('any_live') and label == state.get('live_owner'):
        close_real = True
        state['any_live']  = False   # real position now closed
        state['live_owner'] = None   # prevent later sim holders re-closing it

    if not state['holders']:
        del _global_positions[sym]
        import time as _t
        _post_close_cd[sym] = _t.time()  # 2min cooldown before any re-entry
        return True, close_real   # last holder
    return False, close_real      # others still holding


def init_strategies(strategies_a=None, strategies_b=None, no_log=False):
    global _engines_a, _engines_b
    prefix   = None if no_log else 'preds'
    prefix_b = None if no_log else 'preds_b'
    _engines_a = [StrategyEngine(cfg, prefix)   for cfg in (strategies_a or STRATEGIES)]
    _engines_b = [StrategyEngine(cfg, prefix_b) for cfg in strategies_b] if strategies_b else []

    # ── Wire event-driven fast-path handlers ──────────────────────
    # engine.py calls these inline on every WebSocket price tick,
    # bypassing the 100ms pred_loop for latency-sensitive paths.

    def _z_fast_check(sym: str):
        """Z gate check fired directly from on_ticker / _update_lag_price."""
        st = E.sym_state.get(sym)
        if not st or st['price'] == 0:
            return
        # Lightweight pre-filter: skip full gate if no meaningful lag
        snap = E.get_lag_snapshot(sym)
        best_lag = min(
            (v['lag_ms'] for v in snap.values()
             if isinstance(v, dict) and v.get('lag_ms') is not None),
            default=None,
        )
        if best_lag is None or best_lag < 30:
            return
        r = E.run_pred(sym)
        fired_z_dir = r.get('dir')
        for eng in _engines_a + _engines_b:
            if eng.cfg.disabled or not eng.cfg.lag_monitor_mode:
                continue
            if eng._has_open(sym):
                continue
            if eng.gates_met(sym, r):
                eng.fire(sym, r)
                # Register in global position slot so _release_open correctly tracks
                # last-holder and any_live for z-path entries (same as tick_all path).
                # Z-path is always a single engine (lag_monitor_mode only), so it is
                # always the winner and live_ok reflects whether a real order was placed
                # (both the strategy config AND the global LIVE_ENABLED must be true).
                live_ok = getattr(eng.cfg, 'live_exec', True) and _live.LIVE_ENABLED
                _register_open(sym, eng.cfg.label, direction=fired_z_dir, live_ok=live_ok)

    def _fast_check_outcomes():
        """Exit checker fired on every Binance price tick (trail/SL/TP)."""
        check_all()

    E.register_z_handler(_z_fast_check)
    E.register_check_outcomes_handler(_fast_check_outcomes)
    # ──────────────────────────────────────────────────────────────

    return _engines_a, _engines_b

def get_engines(): return _engines_a

def _update_trend_state(sym, r):
    st = E.sym_state.get(sym)
    if not st: return
    try:
        from core_signals import detect_regime  # FIX: module is core_signals, not signals
        regime_name, _ = detect_regime(sym)
    except Exception:
        regime_name = 'neutral'
    is_trend = regime_name in ('trend_up', 'trend_down', 'breakout')
    cur_dir   = 'long' if regime_name in ('trend_up', 'breakout') and r['score'] > 0 else (
                'short' if regime_name in ('trend_down', 'breakout') and r['score'] < 0 else None)
    prev_dir   = st.get('trend_dir')
    prev_count = st.get('trend_tick_count', 0)
    if is_trend and cur_dir is not None and cur_dir == prev_dir:
        st['trend_tick_count'] = prev_count + 1
    elif is_trend and cur_dir is not None:
        st['trend_tick_count'] = 1
        st['trend_dir'] = cur_dir
    else:
        st['trend_tick_count'] = 0
        st['trend_dir'] = None

# Priority for live order winner selection (lower = higher priority = gets real Binance order).
# Separate from _STRATEGY_PRIORITY (used for Mode B slot gating) — these encode
# empirical live-execution quality ranking, not signal priority.
_LIVE_ORDER_PRIORITY: dict[str, int] = {
    'B': 1, 'L': 2, 'W': 3, 'CGY': 4, 'Y': 5, 'E': 6, 'K': 7,
}

def tick_all(coins):
    for sym in coins:
        st = E.sym_state.get(sym)
        if not st or st['price'] == 0: continue
        r = E.run_pred(sym); st['sig_hist'].append(r)
        _update_trend_state(sym, r)
        st['_tick_sigs'] = {
            'vpin':   E.calc_vpin(sym),
            'lam':    E.calc_kyle_lambda(sym),
            'spread': E.calc_spread_pct(sym),
            'accel':  E.calc_trade_accel(sym),
            'atr':    E.get_atr(sym),
        }
        # Master regime gate: block mean-reversion strategies counter-trend.
        trend_ticks  = st.get('trend_tick_count', 0)
        trend_dir    = st.get('trend_dir')
        strong_trend = (CONFLUENCE_REGIME_ENABLED and
                        trend_ticks >= CONFLUENCE_REGIME_BLOCK_TICKS and
                        trend_dir is not None)

        _tick_candidates = []  # strategies that pass gates this tick for this sym
        for eng in _engines_a + _engines_b:
            if eng.cfg.disabled: continue
            is_shadow = getattr(eng.cfg, 'shadow', False)
            if strong_trend and (
                eng.cfg.consolidation_mode or
                eng.cfg.candle_level_mode or
                eng.cfg.knife_catch_mode or
                eng.cfg.impulse_fade_mode
            ):
                trade_dir = (r.get('_fade_dir') or r.get('_ema_dir') or
                             r.get('_level_dir') or r.get('_wall_dir') or
                             r.get('dir'))
                if trade_dir and trade_dir != trend_dir:
                    continue
            # Shadow strategies bypass the global position lock entirely — they never
            # claim a slot, never block others, and ignore the per-sym cap. Their own
            # _has_open() (inside each gate) still prevents self-double-opens.
            if not is_shadow:
                # Direction-aware lock: allow same direction, block opposite
                sig_dir = r.get('dir') or r.get('_fade_dir') or r.get('_ema_dir') or r.get('_level_dir')
                if not _can_enter(sym, eng.cfg.label, POSITION_LOCK_MODE, direction=sig_dir):
                    continue
                # Global per-sym cap: count how many strategies already hold this sym open.
                sym_state = _global_positions.get(sym)
                if sym_state and len(sym_state.get('holders', {})) >= GLOBAL_MAX_OPEN_PER_SYM:
                    continue
            if eng.gates_met(sym, r):
                # Priority live order: collect all passing strategies, give real order to best
                _tick_candidates.append(eng)

        if not _tick_candidates:
            continue

        # Among live_exec=True candidates pick highest WR priority for real Binance order.
        # Shadow strategies are excluded — they can never own the real order.
        live_cands = [e for e in _tick_candidates
                      if getattr(e.cfg, 'live_exec', True) and not getattr(e.cfg, 'shadow', False)]
        winner = (min(live_cands, key=lambda e: _LIVE_ORDER_PRIORITY.get(e.cfg.label, 99))
                  if live_cands else None)

        fired_dir = r.get('dir') or r.get('_fade_dir') or r.get('_ema_dir') or r.get('_level_dir')
        fired_engines = set()
        for eng in _tick_candidates:
            force_sim = getattr(eng.cfg, 'shadow', False) or (winner is not None and eng is not winner)
            # fire() returns None on early exit (can_enter failed) — track which
            # engines actually created a pred so _register_open is not called for
            # engines that returned early (would leave a phantom holder in
            # _global_positions that is never released, poisoning the sym slot).
            before = eng.hist_total
            eng.fire(sym, r, force_sim=force_sim)
            if eng.hist_total > before:
                fired_engines.add(eng)
                # Log fired event so analyze.sh signal detail shows vpin/conf/score for ALL strategies
                try:
                    from engine_logger import log_signal as _ls
                    _st = E.sym_state.get(sym) or {}
                    _ts = _st.get('_tick_sigs') or {}
                    _ls(eng.cfg.label, sym, 'fired',
                        f"{'live' if (winner is not None and eng is winner) else 'sim'} dir={fired_dir}",
                        vpin=_ts.get('vpin'), conf=r.get('conf'), score=r.get('score'))
                except Exception:
                    pass

        # Register only engines that actually fired a pred as holders.
        # Mark live_ok=True only for the winner (the strategy that sent the real order).
        # Shadow strategies are skipped — they stay out of the global lock entirely.
        for eng in fired_engines:
            if getattr(eng.cfg, 'shadow', False):
                continue
            is_winner = (eng is winner)
            _register_open(sym, eng.cfg.label, direction=fired_dir, live_ok=is_winner)

def check_all():
    for eng in _engines_a + _engines_b:
        eng.check_outcomes()

def snapshots_all():
    return {
        'a': [eng.snapshot() for eng in _engines_a],
        'b': [eng.snapshot() for eng in _engines_b],
    }

# ── Hot-reload ────────────────────────────────────────────────────
import importlib.util as _iutil

RELOAD_POLL_SEC  = 3
_watched_files   = ['strategies_config.py', 'strategies_config_b.py']
_file_mtimes: dict = {}

def _get_mtimes() -> dict:
    import os
    base = os.path.dirname(os.path.abspath(__file__))
    result = {}
    for fname in _watched_files:
        path = os.path.join(base, fname)
        try:    result[fname] = os.path.getmtime(path)
        except: result[fname] = None
    return result

def _load_fresh(name: str, filepath: str):
    spec = _iutil.spec_from_file_location(name, filepath)
    mod  = _iutil.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def _apply_reload(no_log: bool = False):
    global _engines_a, _engines_b
    import os
    base = os.path.dirname(os.path.abspath(__file__))
    try:
        _sc   = _load_fresh('strategies_config',   os.path.join(base, 'strategies_config.py'))
        new_a = _sc.STRATEGIES
    except Exception as ex:
        print(f"[HOT-RELOAD] ERROR loading strategies_config: {ex}", flush=True)
        return
    new_b = []
    try:
        _scb  = _load_fresh('strategies_config_b', os.path.join(base, 'strategies_config_b.py'))
        new_b = _scb.STRATEGIES_B
    except Exception:
        pass
    prefix   = None if no_log else 'preds'
    prefix_b = None if no_log else 'preds_b'
    new_engines_a = [StrategyEngine(cfg, prefix)   for cfg in new_a]
    new_engines_b = [StrategyEngine(cfg, prefix_b) for cfg in new_b] if new_b else []
    old_by_label = {eng.cfg.label: eng for eng in _engines_a + _engines_b}
    for eng in new_engines_a + new_engines_b:
        old = old_by_label.get(eng.cfg.label)
        if not old: continue
        open_preds = [p for p in old.preds if p.get('out3') is None]
        # Safety: patch any open trades that have _trail_dist=None (from old engine)
        # to avoid TypeError on the trail check after hot-reload
        for p in open_preds:
            if p.get('_trail_dist') is None and '_trail_dist' in p:
                del p['_trail_dist']   # remove key so p.get() falls back to cfg default
            if p.get('_trail_dist_orig') is None and '_trail_dist_orig' in p:
                del p['_trail_dist_orig']
        eng.preds.extendleft(reversed(open_preds))
        eng._open_syms = {p['sym'] for p in open_preds}  # rebuild O(1) set from restored preds
        eng._cooldowns          = dict(old._cooldowns)
        eng._start_ts           = getattr(old, '_start_ts', __import__('time').time())   # preserve warmup
        eng._loss_streak        = dict(old._loss_streak)
        eng._cascade_detect_ts  = dict(getattr(old, '_cascade_detect_ts', {}))
        eng.hist_win   = old.hist_win
        eng.hist_lose  = old.hist_lose
        eng.hist_total = old.hist_total
        eng._cum_net   = old._cum_net
        eng._session_start_cum    = getattr(old, '_session_start_cum',   0.0)
        eng._session_start_total  = getattr(old, '_session_start_total', 0)
        eng._session_start_wins   = getattr(old, '_session_start_wins',  0)
        eng._session_start_loses  = getattr(old, '_session_start_loses', 0)
        eng.pnl_history      = list(old.pnl_history)
        eng._bnb_cum_pnl     = getattr(old, '_bnb_cum_pnl', 0.0)
        eng._bnb_cum_comm    = getattr(old, '_bnb_cum_comm', 0.0)
        if eng.cfg.impulse_fade_mode and old.cfg.impulse_fade_mode:
            eng._impulse_cache   = dict(old._impulse_cache)
            eng._impulse_wait_ts = dict(old._impulse_wait_ts)
    _engines_a = new_engines_a
    _engines_b = new_engines_b
    n_a = len(_engines_a); n_b = len(_engines_b)
    ab  = f" + {n_b}B" if n_b else ""
    ts  = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [HOT-RELOAD] applied — {n_a}A{ab} engines active", flush=True)

    # ── Bug fix: rebuild _global_positions from restored open preds ──────────
    # After hot-reload, _global_positions is empty for all transferred symbols.
    # Without this rebuild, _can_enter() sees every sym slot as free, allowing a
    # second strategy to fire a real Binance entry on a sym that already has one
    # open — doubling Binance qty without the engine knowing. Also ensures
    # _release_open() correctly tracks is_last and any_live for restored preds.
    _global_positions.clear()
    for eng in _engines_a + _engines_b:
        if getattr(eng.cfg, 'shadow', False):
            continue  # shadow strategies are not part of the global lock
        for p in eng.preds:
            if p.get('out3') is not None:
                continue  # already resolved — skip
            _register_open(
                p['sym'],
                eng.cfg.label,
                direction=p.get('dir'),
                live_ok=p.get('_live_ok', False),
            )
    n_open = len(_global_positions)
    if n_open:
        print(f"[{ts}] [HOT-RELOAD] rebuilt {n_open} open position slot(s) in _global_positions", flush=True)

async def config_watcher_task(no_log: bool = False):
    import asyncio
    global _file_mtimes
    _file_mtimes = _get_mtimes()
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [HOT-RELOAD] watching {', '.join(_watched_files)} (every {RELOAD_POLL_SEC}s)", flush=True)
    while True:
        await asyncio.sleep(RELOAD_POLL_SEC)
        current = _get_mtimes()
        if current != _file_mtimes:
            changed = [f for f in _watched_files if current.get(f) != _file_mtimes.get(f)]
            ts = datetime.now().strftime('%H:%M:%S')
            print(f"[{ts}] [HOT-RELOAD] change detected: {', '.join(changed)}", flush=True)
            _apply_reload(no_log=no_log)
            _file_mtimes = _get_mtimes()

def start_config_watcher():
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [HOT-RELOAD] use config_watcher_task() in asyncio.gather() instead", flush=True)
