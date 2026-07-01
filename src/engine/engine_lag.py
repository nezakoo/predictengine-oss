"""
engine_lag.py — Cross-exchange lag price feeds (MEXC, Bybit, Gate).

Feeds exchange_prices dict used by _find_lag_signal in strategies_signals.py.
Imported by predict_engine.py for lag_ws_task.
"""
import asyncio, json, time
from collections import deque, defaultdict
import aiohttp
import engine as E
from engine import _is_benign_ws_error



# exchange_prices[exchange][binance_sym] = {'price': 0.0, 'ts': 0.0, 'hist': deque}
exchange_prices: dict = defaultdict(dict)
LAG_EXCHANGES = ['mexc', 'bybit', 'gate']
LAG_HIST_MAXLEN = 500   # ~50s at 100ms updates


def _init_lag_sym(sym: str):
    """Ensure exchange_prices has an entry for sym on all lag exchanges."""
    for ex in LAG_EXCHANGES:
        if sym not in exchange_prices[ex]:
            exchange_prices[ex][sym] = {
                'price': 0.0,
                'ts':    0.0,
                'hist':  deque(maxlen=LAG_HIST_MAXLEN),
            }


def _update_lag_price(exchange: str, sym: str, price: float):
    """Record a price update from a lag exchange."""
    if price <= 0:
        return
    now = time.time()
    d = exchange_prices[exchange].get(sym)
    if d is None:
        exchange_prices[exchange][sym] = {
            'price': price, 'ts': now,
            'hist':  deque([(now, price)], maxlen=LAG_HIST_MAXLEN),
        }
        return
    d['price'] = price
    d['ts']    = now
    d['hist'].append((now, price))

    # ── Z FAST-PATH: lag exchange just repriced — check divergence now ──
    # This is the primary Z trigger: Binance already moved, lag exchange
    # just caught up (or vice versa). Fire Z check inline rather than
    # waiting up to 100ms for pred_loop. Total latency: ~1ms from message.
    if E._z_fast_handler is not None and sym in E.sym_state:
        try:
            E._z_fast_handler(sym)
        except Exception:
            pass
    # ───────────────────────────────────────────────────────────────────


def _sym_to_mexc(sym: str) -> str:
    """BTCUSDT → BTC_USDT  (MEXC contract format)"""
    if sym.endswith('USDT'):
        return sym[:-4] + '_USDT'
    return sym


def _sym_to_gate(sym: str) -> str:
    """BTCUSDT → BTC_USDT  (Gate.io futures format)"""
    if sym.endswith('USDT'):
        return sym[:-4] + '_USDT'
    return sym


def _mexc_to_sym(mexc_sym: str) -> str:
    """BTC_USDT → BTCUSDT"""
    return mexc_sym.replace('_USDT', 'USDT').replace('_', '')


def _gate_to_sym(gate_sym: str) -> str:
    """BTC_USDT → BTCUSDT"""
    return gate_sym.replace('_USDT', 'USDT').replace('_', '')


# ── MEXC WS ───────────────────────────────────────────────────────

async def _mexc_ws_loop(coins: list[str]):
    """
    MEXC Contract WS — perpetual futures tickers.
    URL: wss://contract.mexc.com/edge
    Sub: {"method":"sub.ticker","param":{"symbol":"BTC_USDT"}}
    Msg: {"channel":"push.ticker","data":{"symbol":"BTC_USDT","lastPrice":"..."},"ts":...}
    """
    url     = 'wss://contract.mexc.com/edge'
    symbols = [_sym_to_mexc(s) for s in coins]
    backoff = 1.0
    while E.running:
        try:
            async with E.ws_connect(url, open_timeout=15, ping_interval=None,
                                    ping_timeout=None, max_size=2**22) as ws:
                # Subscribe to each symbol
                for ms in symbols:
                    await ws.send_str(json.dumps({
                        'method': 'sub.ticker',
                        'param':  {'symbol': ms},
                    }))
                backoff = 1.0
                while True:
                    try:
                        msg = await ws.receive(timeout=60.0)
                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR): break
                        raw = msg.data if msg.type == aiohttp.WSMsgType.TEXT else msg.data.decode()
                    except asyncio.TimeoutError:
                        break  # silent connection — reconnect
                    if not E.running:
                        return
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    # MEXC sends {"channel":"pong"} or {"method":"ping"} keepalives
                    ch = msg.get('channel', '')
                    if ch == 'pong' or msg.get('method') == 'ping':
                        await ws.send_str(json.dumps({'method': 'pong'}))
                        continue
                    if ch != 'push.ticker':
                        continue
                    d    = msg.get('data', {})
                    ms   = d.get('symbol', '')
                    px_s = d.get('lastPrice') or d.get('last') or d.get('price')
                    if not ms or not px_s:
                        continue
                    try:
                        px = float(px_s)
                    except (ValueError, TypeError):
                        continue
                    sym = _mexc_to_sym(ms)
                    if sym in E.sym_state:
                        _update_lag_price('mexc', sym, px)
        except Exception:
            await asyncio.sleep(min(backoff, 30.0))
            backoff = min(backoff * 2, 30.0)


# ── BYBIT WS ──────────────────────────────────────────────────────

async def _bybit_ws_loop(coins: list[str]):
    """
    Bybit V5 linear perpetual tickers.
    URL: wss://stream.bybit.com/v5/public/linear
    Sub: {"op":"subscribe","args":["tickers.BTCUSDT",...]}
    Msg: {"topic":"tickers.BTCUSDT","data":{"lastPrice":"..."}}
    """
    url  = 'wss://stream.bybit.com/v5/public/linear'
    args = [f'tickers.{s}' for s in coins]
    backoff = 1.0
    while E.running:
        try:
            async with E.ws_connect(url, open_timeout=15, ping_interval=None,
                                    ping_timeout=None, max_size=2**22) as ws:
                # Bybit accepts up to 10 topics per sub; chunk if needed
                for i in range(0, len(args), 10):
                    await ws.send_str(json.dumps({
                        'op':   'subscribe',
                        'args': args[i:i+10],
                    }))
                backoff = 1.0
                # Bybit requires client-initiated ping every <=20s.
                # We manage this ourselves since library pings go unanswered.
                last_ping = time.time()
                while True:
                    try:
                        msg = await ws.receive(timeout=60.0)
                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR): break
                        raw = msg.data if msg.type == aiohttp.WSMsgType.TEXT else msg.data.decode()
                    except asyncio.TimeoutError:
                        break  # silent connection — reconnect
                    if not E.running:
                        return
                    now = time.time()
                    if now - last_ping >= 20:
                        await ws.send_str(json.dumps({'op': 'ping'}))
                        last_ping = now
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    # Bybit pong: {"op":"pong"} or {"ret_msg":"pong"}
                    if msg.get('op') == 'pong' or msg.get('ret_msg') == 'pong':
                        continue
                    topic = msg.get('topic', '')
                    if not topic.startswith('tickers.'):
                        continue
                    d    = msg.get('data', {})
                    sym  = topic.split('.', 1)[1]   # "tickers.BTCUSDT" → "BTCUSDT"
                    px_s = d.get('lastPrice') or d.get('last_price')
                    if not px_s:
                        continue
                    try:
                        px = float(px_s)
                    except (ValueError, TypeError):
                        continue
                    if sym in E.sym_state:
                        _update_lag_price('bybit', sym, px)
        except Exception:
            await asyncio.sleep(min(backoff, 30.0))
            backoff = min(backoff * 2, 30.0)


# ── GATE WS ───────────────────────────────────────────────────────

async def _gate_ws_loop(coins: list[str]):
    """
    Gate.io Futures WS (USDT-margined).
    URL: wss://fx-ws.gateio.ws/v4/ws/usdt
    Sub: {"time":ts,"channel":"futures.tickers","event":"subscribe","payload":["BTC_USDT",...]}
    Msg: {"channel":"futures.tickers","event":"update","result":[{"contract":"BTC_USDT","last":"..."}]}
    """
    url      = 'wss://fx-ws.gateio.ws/v4/ws/usdt'
    payloads = [_sym_to_gate(s) for s in coins]
    backoff  = 1.0
    while E.running:
        try:
            async with E.ws_connect(url, open_timeout=15, ping_interval=None,
                                    ping_timeout=None, max_size=2**22) as ws:
                await ws.send_str(json.dumps({
                    'time':    int(time.time()),
                    'channel': 'futures.tickers',
                    'event':   'subscribe',
                    'payload': payloads,
                }))
                backoff = 1.0
                # Gate sends {"channel":"futures.ping","event":"ping"} every 30s
                # and expects a pong reply or it closes with 1011.
                while True:
                    try:
                        msg = await ws.receive(timeout=60.0)
                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR): break
                        raw = msg.data if msg.type == aiohttp.WSMsgType.TEXT else msg.data.decode()
                    except asyncio.TimeoutError:
                        break  # silent connection — reconnect
                    if not E.running:
                        return
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    # Gate keepalive ping — reply with pong
                    if msg.get('channel') == 'futures.ping' or msg.get('event') == 'ping':
                        await ws.send_str(json.dumps({
                            'time':    int(time.time()),
                            'channel': 'futures.pong',
                            'event':   'pong',
                            'error':   None,
                            'result':  None,
                        }))
                        continue
                    if msg.get('channel') != 'futures.tickers': continue
                    if msg.get('event')   != 'update':          continue
                    for item in (msg.get('result') or []):
                        contract = item.get('contract', '')
                        px_s     = item.get('last') or item.get('last_price')
                        if not contract or not px_s:
                            continue
                        try:
                            px = float(px_s)
                        except (ValueError, TypeError):
                            continue
                        sym = _gate_to_sym(contract)
                        if sym in E.sym_state:
                            _update_lag_price('gate', sym, px)
        except Exception:
            await asyncio.sleep(min(backoff, 30.0))
            backoff = min(backoff * 2, 30.0)


# ── PUBLIC ENTRY POINT ────────────────────────────────────────────

async def lag_ws_task(coins: list[str]):
    """
    Run MEXC + Bybit + Gate WS connections concurrently.
    Call from predict_engine.py alongside ws_task() and rest_task().
    """
    for sym in coins:
        _init_lag_sym(sym)
    await asyncio.gather(
        _mexc_ws_loop(coins),
        _bybit_ws_loop(coins),
        _gate_ws_loop(coins),
    )


def get_lag_snapshot(sym: str) -> dict:
    """
    Return current cross-exchange price state for sym.
    Used by strategy Z signal detector.

    Returns:
        {
          'binance': {'price': float, 'ts': float},
          'mexc':    {'price': float, 'ts': float, 'lag_ms': float},
          'bybit':   {'price': float, 'ts': float, 'lag_ms': float},
          'gate':    {'price': float, 'ts': float, 'lag_ms': float},
        }
    """
    st      = E.sym_state.get(sym)
    bnx_px  = st['price'] if st else 0.0
    bnx_ts  = time.time()   # Binance price is always current (100ms WS)
    result  = {'binance': {'price': bnx_px, 'ts': bnx_ts}}
    for ex in LAG_EXCHANGES:
        d = exchange_prices[ex].get(sym, {})
        ex_px = d.get('price', 0.0)
        ex_ts = d.get('ts',    0.0)
        lag_ms = (bnx_ts - ex_ts) * 1000 if ex_ts > 0 else None
        result[ex] = {'price': ex_px, 'ts': ex_ts, 'lag_ms': lag_ms}
    return result
