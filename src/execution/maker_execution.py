#!/usr/bin/env python3
"""
maker_execution.py — Phase 1: Passive limit order execution layer
=================================================================
Drop-in companion to live_execution.py for maker (limit) entries.

Key differences from live_execution.py:
  - Posts LIMIT orders instead of MARKET
  - Tracks pending orders with fill_window timeout
  - Cancels if signal expires or price moves too far
  - Chase logic: re-posts up to MAKER_CHASE_LIMIT times
  - Fills detected via polling (WebSocket USER_DATA integration optional)
  - Partial fill handling: tracks remaining qty

Config (env vars):
  MAKER_ENTRIES=true          master switch (default false)
  MAKER_PRICE_OFFSET=0.0002   how far inside mid to post (default 0.02%)
  MAKER_FILL_WINDOW_E=120     fill timeout seconds for E strategy
  MAKER_FILL_WINDOW_CGYL=180  fill timeout for CGYL
  MAKER_FILL_WINDOW_Q=3600    fill timeout for Q
  MAKER_CHASE_LIMIT=2         max re-posts chasing price

Usage (from strategies_engine.py):
  import maker_execution
  if maker_execution.MAKER_ENTRIES and strategy in maker_execution.ELIGIBLE:
      result = await maker_execution.fire(sym, dir, qty, price, strategy, signal_ts)
  else:
      result = await live_execution.fire(...)
"""

import asyncio
import hashlib
import hmac
import logging
import os
import time
import urllib.parse
from decimal import Decimal
from typing import Optional

import requests

log = logging.getLogger('maker_exec')

# ── Config ────────────────────────────────────────────────────────────────────
def _eb(k, d): return os.environ.get(k, str(d)).lower() in ('1','true','yes')
def _ef(k, d):
    try: return float(os.environ.get(k, d))
    except: return d

MAKER_ENTRIES     = _eb('MAKER_ENTRIES', False)
LIVE_MODE         = _eb('LIVE_MODE', False)
PRICE_OFFSET      = _ef('MAKER_PRICE_OFFSET', 0.0002)   # 0.02% inside mid
CHASE_LIMIT       = int(_ef('MAKER_CHASE_LIMIT', 2))
POLL_INTERVAL     = _ef('MAKER_POLL_INTERVAL', 1.0)     # seconds between order polls

# Per-strategy fill windows (seconds before cancellation)
FILL_WINDOWS: dict[str, float] = {
    'E':    _ef('MAKER_FILL_WINDOW_E',    120),
    'CGYL': _ef('MAKER_FILL_WINDOW_CGYL', 180),
    'Q':    _ef('MAKER_FILL_WINDOW_Q',    3600),
}
DEFAULT_FILL_WINDOW = _ef('MAKER_FILL_WINDOW_DEFAULT', 60)

# Strategies eligible for maker execution (others always use taker)
ELIGIBLE = set(os.environ.get('MAKER_ELIGIBLE', 'E,CGYL,Q').split(','))

BASE_URL = ('https://fapi.binance.com'
            if LIVE_MODE else 'https://demo-fapi.binance.com')

# ── State ─────────────────────────────────────────────────────────────────────
# sym → {order_id, limit_price, qty, side, strategy, ts_posted, chase_count, filled_qty}
_pending: dict[str, dict] = {}
_lock = asyncio.Lock()

# ── Exchange info ─────────────────────────────────────────────────────────────
_qty_prec:   dict[str, int] = {}
_price_prec: dict[str, int] = {}
_tick_size:  dict[str, float] = {}
_step_size:  dict[str, float] = {}

def load_exchange_info():
    global _qty_prec, _price_prec, _tick_size, _step_size
    if _qty_prec:
        return
    try:
        r = requests.get(BASE_URL + '/fapi/v1/exchangeInfo', timeout=15).json()
        for s in r.get('symbols', []):
            sym = s['symbol']
            for f in s.get('filters', []):
                if f['filterType'] == 'LOT_SIZE':
                    step = float(f['stepSize'])
                    _step_size[sym] = step
                    _qty_prec[sym] = len(f['stepSize'].rstrip('0').split('.')[-1]) if '.' in f['stepSize'] else 0
                if f['filterType'] == 'PRICE_FILTER':
                    tick = float(f['tickSize'])
                    _tick_size[sym] = tick
                    _price_prec[sym] = len(f['tickSize'].rstrip('0').split('.')[-1]) if '.' in f['tickSize'] else 0
        log.info(f'Exchange info loaded for {len(_qty_prec)} symbols')
    except Exception as e:
        log.error(f'load_exchange_info: {e}')

def _round_qty(sym: str, qty: float) -> float:
    step = _step_size.get(sym, 0.001)
    rounded = round(int(qty / step) * step, _qty_prec.get(sym, 3))
    return rounded

def _round_price(sym: str, price: float) -> float:
    tick = _tick_size.get(sym, 0.0001)
    rounded = round(round(price / tick) * tick, _price_prec.get(sym, 4))
    return rounded

# ── Auth ──────────────────────────────────────────────────────────────────────
def _plain(v): return format(Decimal(str(v)), 'f') if isinstance(v, float) else v
def _sign(p): return hmac.new(
    os.environ.get('BINANCE_API_SECRET','').encode(),
    urllib.parse.urlencode(p).encode(), hashlib.sha256).hexdigest()
def _prepare(p: dict) -> dict:
    p = {k: _plain(v) for k,v in p.items()}
    p.setdefault('recvWindow', 5000)
    p['timestamp'] = int(time.time() * 1000)
    p['signature'] = _sign(p)
    return p
def _headers(): return {'X-MBX-APIKEY': os.environ.get('BINANCE_API_KEY','')}

def _post(path, params):
    try:
        r = requests.post(BASE_URL+path, headers=_headers(),
                          params=_prepare(params), timeout=8)
        return r.json()
    except Exception as e:
        log.error(f'POST {path}: {e}'); return {}

def _delete(path, params):
    try:
        r = requests.delete(BASE_URL+path, headers=_headers(),
                            params=_prepare(params), timeout=8)
        return r.json()
    except Exception as e:
        log.error(f'DELETE {path}: {e}'); return {}

def _get(path, params=None):
    try:
        r = requests.get(BASE_URL+path, headers=_headers(),
                         params=_prepare(params or {}), timeout=8)
        return r.json()
    except Exception as e:
        log.error(f'GET {path}: {e}'); return {}

# ── Order placement ───────────────────────────────────────────────────────────
def _get_mid_price(sym: str) -> Optional[float]:
    try:
        r = requests.get(BASE_URL+'/fapi/v1/ticker/bookTicker',
                         params={'symbol': sym}, timeout=4).json()
        bid = float(r['bidPrice']); ask = float(r['askPrice'])
        return (bid + ask) / 2
    except: return None

def _limit_price(sym: str, side: str, mid: float) -> float:
    """Calculate limit price inside spread."""
    if side == 'BUY':
        raw = mid * (1 - PRICE_OFFSET)   # long: post below mid
    else:
        raw = mid * (1 + PRICE_OFFSET)   # short: post above mid
    return _round_price(sym, raw)

def _place_limit(sym: str, side: str, qty: float, price: float) -> dict:
    """Place a LIMIT MAKER order."""
    r = _post('/fapi/v1/order', {
        'symbol':      sym,
        'side':        side,
        'type':        'LIMIT',
        'timeInForce': 'GTX',           # Post-Only: reject if would take
        'price':       price,
        'quantity':    qty,
    })
    return r

def _cancel_order(sym: str, order_id: int) -> dict:
    return _delete('/fapi/v1/order', {'symbol': sym, 'orderId': order_id})

def _query_order(sym: str, order_id: int) -> dict:
    return _get('/fapi/v1/order', {'symbol': sym, 'orderId': order_id})

# ── Main fire function ────────────────────────────────────────────────────────
async def fire(sym: str, direction: str, notional_usdt: float,
               strategy: str, signal_ts: float) -> dict:
    """
    Post a maker limit order. Tracks fill status asynchronously.

    Args:
        sym:           e.g. 'ETHUSDT'
        direction:     'long' or 'short'
        notional_usdt: USDT notional (e.g. 100.0)
        strategy:      strategy name (for fill window lookup)
        signal_ts:     unix timestamp when signal fired (for expiry)

    Returns:
        {'status': 'pending'|'filled'|'cancelled'|'rejected',
         'order_id': int, 'fill_price': float, 'filled_qty': float, ...}
    """
    load_exchange_info()

    mid = _get_mid_price(sym)
    if not mid:
        log.error(f'  {sym}: cannot get mid price')
        return {'status': 'rejected', 'reason': 'no_mid_price'}

    side  = 'BUY' if direction == 'long' else 'SELL'
    qty   = _round_qty(sym, notional_usdt / mid)
    price = _limit_price(sym, side, mid)

    if qty <= 0:
        log.error(f'  {sym}: qty rounds to 0')
        return {'status': 'rejected', 'reason': 'qty_zero'}

    log.info(f'  MAKER {direction.upper()} {sym}  '
             f'qty={qty}  limit={price:.6f}  mid={mid:.6f}  '
             f'offset={PRICE_OFFSET*100:.3f}%')

    r = _place_limit(sym, side, qty, price)

    if 'orderId' not in r:
        # GTX rejection means our limit would have taken — price moved
        if r.get('code') == -5022:
            log.warning(f'  {sym}: GTX rejected (would take) — price moved')
            return {'status': 'rejected', 'reason': 'gtx_rejected', 'mid': mid}
        log.error(f'  {sym}: limit placement failed: {r}')
        return {'status': 'rejected', 'reason': str(r.get('msg', 'unknown'))}

    order_id = r['orderId']
    log.info(f'  ✅ {sym} limit posted  orderId={order_id}  price={price}')

    # Track in pending state
    fill_window = FILL_WINDOWS.get(strategy, DEFAULT_FILL_WINDOW)
    async with _lock:
        _pending[sym] = {
            'order_id':    order_id,
            'limit_price': price,
            'qty':         qty,
            'side':        side,
            'strategy':    strategy,
            'ts_posted':   time.time(),
            'signal_ts':   signal_ts,
            'fill_window': fill_window,
            'chase_count': 0,
            'filled_qty':  0.0,
            'fill_price':  None,
            'status':      'pending',
        }

    return {'status': 'pending', 'order_id': order_id,
            'limit_price': price, 'qty': qty, 'sym': sym}


# ── Fill tracking loop ────────────────────────────────────────────────────────
async def fill_tracker_loop(on_fill=None, on_cancel=None):
    """
    Background asyncio task. Poll pending orders, handle fills/cancellations.

    Callbacks:
        on_fill(sym, order_info)   — called when order fills
        on_cancel(sym, reason)     — called when order cancelled/expired
    """
    log.info('Maker fill tracker started')
    while True:
        await asyncio.sleep(POLL_INTERVAL)
        if not _pending:
            continue

        now = time.time()
        async with _lock:
            syms = list(_pending.keys())

        for sym in syms:
            async with _lock:
                info = _pending.get(sym)
            if not info:
                continue

            order_id = info['order_id']
            elapsed  = now - info['ts_posted']
            status_r = _query_order(sym, order_id)
            status   = status_r.get('status', 'UNKNOWN')
            filled_qty = float(status_r.get('executedQty', 0))

            # ── Filled ────────────────────────────────────────────────────────
            if status in ('FILLED', 'PARTIALLY_FILLED') and filled_qty > 0:
                fill_price = float(status_r.get('avgPrice', info['limit_price']))
                log.info(f'  ✅ MAKER FILL {sym}  qty={filled_qty}  '
                         f'price={fill_price:.6f}  delay={elapsed:.0f}s  '
                         f'strategy={info["strategy"]}')
                async with _lock:
                    if sym in _pending:
                        _pending[sym]['status']     = 'filled'
                        _pending[sym]['filled_qty'] = filled_qty
                        _pending[sym]['fill_price'] = fill_price
                        completed = _pending.pop(sym)
                if on_fill:
                    await on_fill(sym, completed)
                continue

            # ── Expired or price moved away ───────────────────────────────────
            should_cancel = False
            cancel_reason = ''

            if elapsed > info['fill_window']:
                should_cancel = True
                cancel_reason = f'timeout_{elapsed:.0f}s'

            else:
                # Check if price has moved away (chase or cancel)
                mid = _get_mid_price(sym)
                if mid:
                    limit = info['limit_price']
                    side  = info['side']
                    if side == 'BUY':
                        moved_away = mid > limit * (1 + PRICE_OFFSET * 3)
                    else:
                        moved_away = mid < limit * (1 - PRICE_OFFSET * 3)

                    if moved_away:
                        if info['chase_count'] < CHASE_LIMIT:
                            # Cancel and re-post at new price
                            log.info(f'  {sym}: price moved, chasing '
                                     f'(attempt {info["chase_count"]+1}/{CHASE_LIMIT})')
                            _cancel_order(sym, order_id)
                            new_price = _limit_price(sym, side, mid)
                            new_r = _place_limit(sym, side, info['qty'], new_price)
                            if 'orderId' in new_r:
                                async with _lock:
                                    if sym in _pending:
                                        _pending[sym]['order_id']    = new_r['orderId']
                                        _pending[sym]['limit_price'] = new_price
                                        _pending[sym]['chase_count'] += 1
                                        _pending[sym]['ts_posted']   = now
                                log.info(f'  {sym}: re-posted  orderId={new_r["orderId"]}  '
                                         f'price={new_price:.6f}')
                            else:
                                should_cancel = True
                                cancel_reason = 'chase_failed'
                        else:
                            should_cancel = True
                            cancel_reason = f'chase_limit_reached_{CHASE_LIMIT}'

            if should_cancel:
                log.info(f'  ❌ {sym}: cancelling  reason={cancel_reason}  '
                         f'elapsed={elapsed:.0f}s')
                _cancel_order(sym, order_id)
                async with _lock:
                    cancelled = _pending.pop(sym, None)
                if on_cancel and cancelled:
                    await on_cancel(sym, cancel_reason)


# ── Status helpers ────────────────────────────────────────────────────────────
def get_pending() -> dict:
    """Return current pending orders (snapshot)."""
    return dict(_pending)

def cancel_all():
    """Cancel all pending maker orders (e.g. on shutdown)."""
    for sym, info in list(_pending.items()):
        log.info(f'  Cancelling pending {sym} orderId={info["order_id"]}')
        _cancel_order(sym, info['order_id'])
    _pending.clear()

def is_pending(sym: str) -> bool:
    return sym in _pending


# ── Integration test (standalone) ────────────────────────────────────────────
if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--test',   action='store_true', help='Post a test limit order')
    ap.add_argument('--symbol', default='ETHUSDT')
    ap.add_argument('--usdt',   type=float, default=10.0)
    args = ap.parse_args()

    if args.test:
        load_exchange_info()
        mid = _get_mid_price(args.symbol)
        print(f'{args.symbol} mid={mid}')
        qty   = _round_qty(args.symbol, args.usdt / mid)
        price = _limit_price(args.symbol, 'SELL', mid)
        print(f'Would post: SELL {qty} @ {price}  (offset={PRICE_OFFSET*100:.3f}%)')
        print('Add --confirm to actually place (not implemented in test mode)')


# ── Phase 2: strategies_engine.py integration helpers ────────────────────────

def should_use_maker(strategy_label: str) -> bool:
    """Return True if this strategy should attempt maker entry."""
    return MAKER_ENTRIES and strategy_label in ELIGIBLE


def fire_maker_entry(sym: str, side: str, notional_usdt: float,
                     strategy_label: str, sl_pct: float) -> dict:
    """
    Synchronous maker entry for strategies_engine.py integration.
    Posts a GTX LIMIT order. Polls for fill up to FILL_WINDOW seconds.
    Returns {'ok': True, 'orderId': ..., 'maker': True} on fill,
            {'ok': False, 'fallback': True} if not filled (caller uses taker).

    Note: This is a blocking call (uses time.sleep polling) to fit the
    existing synchronous fire() call site in strategies_engine.py.
    For full async integration, use fire() + fill_tracker_loop() instead.
    """
    load_exchange_info()

    mid = _get_mid_price(sym)
    if not mid:
        log.warning(f'  {sym}: maker — no mid price, falling back to taker')
        return {'ok': False, 'fallback': True, 'reason': 'no_mid'}

    qty   = _round_qty(sym, notional_usdt / mid)
    price = _limit_price(sym, side, mid)

    if qty <= 0:
        return {'ok': False, 'fallback': True, 'reason': 'qty_zero'}

    log.info(f'  MAKER {side} {sym}  qty={qty}  limit={price:.6f}  '
             f'mid={mid:.6f}  strategy={strategy_label}')

    r = _place_limit(sym, side, qty, price)

    # GTX rejection: limit would take — price moved, fall back immediately
    if r.get('code') == -5022:
        log.info(f'  {sym}: GTX rejected → taker fallback')
        return {'ok': False, 'fallback': True, 'reason': 'gtx_rejected'}

    if 'orderId' not in r:
        log.error(f'  {sym}: maker placement failed: {r}')
        return {'ok': False, 'fallback': True, 'reason': str(r.get('msg','unknown'))}

    order_id    = r['orderId']
    fill_window = FILL_WINDOWS.get(strategy_label, DEFAULT_FILL_WINDOW)
    deadline    = time.time() + fill_window
    chase_count = 0
    current_price = price

    log.info(f'  ✅ {sym} limit posted orderId={order_id}  '
             f'window={fill_window:.0f}s')

    # ── Polling loop ──────────────────────────────────────────────────────────
    while time.time() < deadline:
        time.sleep(POLL_INTERVAL)
        status_r   = _query_order(sym, order_id)
        status     = status_r.get('status', 'UNKNOWN')
        filled_qty = float(status_r.get('executedQty', 0))

        if status == 'FILLED' or (status == 'PARTIALLY_FILLED' and filled_qty >= qty * 0.95):
            fill_price = float(status_r.get('avgPrice', current_price))
            elapsed    = fill_window - (deadline - time.time())
            log.info(f'  ✅ MAKER FILLED {sym}  qty={filled_qty}  '
                     f'price={fill_price:.6f}  delay={elapsed:.0f}s')
            return {
                'ok':         True,
                'orderId':    order_id,
                'maker':      True,
                'fill_price': fill_price,
                'filled_qty': filled_qty,
                'delay_s':    elapsed,
            }

        if status in ('CANCELED', 'EXPIRED', 'REJECTED'):
            log.info(f'  {sym}: order {status} — taker fallback')
            return {'ok': False, 'fallback': True, 'reason': status.lower()}

        # Chase: if price moved away, cancel and re-post
        mid_now = _get_mid_price(sym)
        if mid_now:
            if side == 'BUY':
                moved = mid_now > current_price * (1 + PRICE_OFFSET * 3)
            else:
                moved = mid_now < current_price * (1 - PRICE_OFFSET * 3)

            if moved and chase_count < CHASE_LIMIT:
                log.info(f'  {sym}: chasing price '
                         f'(attempt {chase_count+1}/{CHASE_LIMIT})')
                _cancel_order(sym, order_id)
                new_price = _limit_price(sym, side, mid_now)
                new_r     = _place_limit(sym, side, qty, new_price)
                if 'orderId' in new_r:
                    order_id      = new_r['orderId']
                    current_price = new_price
                    chase_count  += 1
                    log.info(f'  {sym}: re-posted orderId={order_id} '
                             f'price={new_price:.6f}')
                else:
                    log.warning(f'  {sym}: chase re-post failed → taker fallback')
                    return {'ok': False, 'fallback': True, 'reason': 'chase_failed'}

    # Timeout — cancel and fall back to taker
    log.info(f'  {sym}: maker timeout ({fill_window:.0f}s) → taker fallback')
    _cancel_order(sym, order_id)
    return {'ok': False, 'fallback': True, 'reason': 'timeout'}
