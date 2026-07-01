from config import API_URL
"""
engine_scanner.py — Coin scanner (fetch top coins by volume/ATR).

Imported by predict_engine.py for coin_scanner_task.
State written to engine.ACTIVE_COINS.
"""
import asyncio, time
import aiohttp
import engine as E

# Event fired when ACTIVE_COINS changes — predict_engine.py uses this
# to reconnect WebSocket with the updated coin list
coins_changed_event = None   # initialized by init_scanner_events() in async context

def init_scanner_events():
    """Call once from async context to create the asyncio.Event."""
    global coins_changed_event
    if coins_changed_event is None:
        coins_changed_event = asyncio.Event()
from config import (
    SCANNER_TOP_N        as SCAN_TOP_N,
    SCANNER_MIN_NATR     as SCAN_MIN_NATR,
    SCANNER_MIN_VOL_USD  as SCAN_MIN_VOL_USDT,
    SCANNER_REFRESH_SEC  as SCAN_INTERVAL_S,
    SCANNER_SORT_BY      as SCAN_SORT_BY,
    API_URL,
)
SCAN_STABLE_THRESH = 3  # default: coin must appear in N consecutive scans

# Coins to always skip (stablecoins, index tokens)
SCANNER_EXCLUDE = {
    'USDCUSDT', 'BUSDUSDT', 'TUSDUSDT', 'USDTUSDT',
    'FDUSDUSDT', 'DAIUSDT', 'EURUSDT', 'GBPUSDT',
    # Non-ASCII symbols — cause encoding issues in CSV filenames and log parsing
    '龙虾USDT', '币安人生USDT', '4USDT', 'HUSDT', 'DUSDT',
}

# Coins we never want to remove even if they fall off top-N
SCANNER_ALWAYS_KEEP = {
    # BTC required: btc_hist global + W decorrelation signal + score contribution
    'BTCUSDT',
    # Anchor alts: high OI, always relevant for strategy signals
    'JTOUSDT','PENDLEUSDT','ONDOUSDT','SUIUSDT','NEARUSDT','TONUSDT',
    # Additional anchors kept in sync with engine.py SCANNER_ALWAYS_KEEP
    'ETHUSDT','SOLUSDT',
    'STRKUSDT','JUPUSDT','ENAUSDT','ARBUSDT','OPUSDT','DOTUSDT',
    'WIFUSDT','1000PEPEUSDT','TIAUSDT',
    # NOTE: HYPEUSDT, NILUSDT, DYDXUSDT, GALAUSDT, NOTUSDT, BIOUSDT removed —
    # confirmed illiquid/alpha tokens. Will be detected via maintMarginPercent
    # and routed to P-only or excluded entirely.
}

# Internal aliases matching original engine.py naming
_SCAN_TOP_N    = SCAN_TOP_N
_SCAN_MIN_NATR = SCAN_MIN_NATR
_SCAN_MIN_VOL  = SCAN_MIN_VOL_USDT
_SCAN_SORT_BY  = SCAN_SORT_BY
from engine_logger import log_scanner_change

async def fetch_top_coins(session,
                          top_n=None,
                          min_natr=None,
                          min_vol=None,
                          sort_by=None) -> list[str]:
    """
    Fetch all USDT-M futures 24h tickers and return top-N symbols,
    filtered by minimum nATR and minimum volume.

    sort_by='volume'     → top-N by 24h quote volume (old behaviour)
    sort_by='volatility' → top-N by nATR (daily range %) after volume floor
                           This finds the most volatile coins, not the biggest.

    nATR = (highPrice - lowPrice) / lastPrice * 100  (daily range %)
    """
    top_n   = top_n   if top_n   is not None else _SCAN_TOP_N
    min_natr= min_natr if min_natr is not None else _SCAN_MIN_NATR
    min_vol = min_vol  if min_vol  is not None else _SCAN_MIN_VOL
    sort_by = sort_by  if sort_by  is not None else _SCAN_SORT_BY

    try:
        async with session.get(
            f'{API_URL}/fapi/v1/ticker/24hr',
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            tickers = await r.json()
    except Exception:
        return []

    candidates = []
    for t in tickers:
        sym = t.get('symbol', '')
        if not sym.endswith('USDT'): continue
        if sym in SCANNER_EXCLUDE: continue
        try:
            vol = float(t.get('quoteVolume', 0) or 0)
            hi  = float(t.get('highPrice',   0) or 0)
            lo  = float(t.get('lowPrice',    0) or 0)
            px  = float(t.get('lastPrice',   0) or 0)
        except (ValueError, TypeError):
            continue
        if vol < min_vol or px <= 0: continue
        natr = (hi - lo) / px * 100 if px > 0 else 0
        if natr < min_natr: continue
        candidates.append((sym, vol, natr))

    # Sort: 'volatility' → by nATR desc; 'volume' → by vol desc
    if sort_by == 'volatility':
        candidates.sort(key=lambda x: -x[2])   # sort by nATR
    else:
        candidates.sort(key=lambda x: -x[1])   # sort by volume (original)

    result = [sym for sym, _, _ in candidates[:top_n]]

    # Always include anchor coins
    for sym in SCANNER_ALWAYS_KEEP:
        if sym not in result:
            result.append(sym)

    return result


async def coin_scanner_task(refresh_interval: int = 1800):
    """
    Periodically refreshes ACTIVE_COINS by fetching the top coins by
    24h quote volume on Binance USDT-M Futures.

    refresh_interval: seconds between rescans (default 30 min).
    New coins are init'd into E.sym_state; coins that drop off the list
    are kept in E.sym_state (open trades may still be running) but removed
    from ACTIVE_COINS so no new entries are fired for them.

    The WS connection is NOT restarted — ws_task subscribes to a fixed
    stream list at startup. To get live data for new coins discovered
    mid-session you'd need a WS reconnect; for now new coins discovered
    by the scanner will have REST data only until next restart.
    That's acceptable: the scanner's main value is picking the right
    coins at startup.
    """
        # First scan runs immediately at startup so we don't wait 30 min
    first = True
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=5)
    ) as session:
        while E.running:
            if not first:
                await asyncio.sleep(refresh_interval)
            first = False

            new_coins = await fetch_top_coins(session)
            if not new_coins:
                continue   # network error — keep existing list

            added   = [s for s in new_coins if s not in E.ACTIVE_COINS]
            removed = [s for s in E.ACTIVE_COINS if s not in new_coins
                       and s not in SCANNER_ALWAYS_KEEP]

            for sym in added:
                E.init_sym(sym)

            if added or removed:
                E.ACTIVE_COINS = new_coins
                log_scanner_change(added, removed)
                # Signal WS reconnect with new coin list
                if coins_changed_event is not None:
                    coins_changed_event.set()
            # unchanged coins: no log needed — noise



# ══ CROSS-EXCHANGE LAG MONITOR ════════════════════════════════════
#
# Connects to MEXC, Bybit, and Gate.io spot/perp WS feeds for the same
# symbols we trade on Binance. Records per-exchange price and timestamp
# so strategy Z can detect when Binance has moved but another exchange
# hasn't repriced yet.
#
# Architecture:
#   - exchange_prices[exchange][sym] = {'price': float, 'ts': float, 'hist': deque}
#   - hist = rolling deque(maxlen=300) of (ts, price) tuples
#   - lag_ws_task() runs three parallel reconnecting WS loops
#
# Exchange WS endpoints (perpetual futures, USDT pairs):
#   MEXC:  wss://contract.mexc.com/edge      sub: {"method":"sub.ticker","param":{"symbol":"BTC_USDT"}}
#   Bybit: wss://stream.bybit.com/v5/public/linear  sub: {"op":"subscribe","args":["tickers.BTCUSDT"]}
#   Gate:  wss://fx-ws.gateio.ws/v4/ws/usdt  sub: {"time":ts,"channel":"futures.tickers","event":"subscribe","payload":["BTC_USDT"]}
#
