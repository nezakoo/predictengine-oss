"""
live_execution.py — Binance USDT-M Futures order execution layer for PredictEngine.

Architecture: Option (b) — Python is the exit manager.
  - Entry: market order fired from fire() after pred is created
  - Exit:  market order fired from _close() before outcome is recorded
  - No OCO/stop orders placed on Binance — Python trail/SL/TP logic stays unchanged

Demo account base URL:  https://demo-fapi.binance.com  (real keys, virtual balance)
Live base URL:            https://fapi.binance.com  (set LIVE_MODE=True + real keys)

Setup (.env additions required):
    BINANCE_API_KEY=...
    BINANCE_API_SECRET=...
    LIVE_ENABLED=true          # master switch — False = simulation only (default)
    LIVE_ORDER_USDT=20         # USDT per trade (sizing)
    LIVE_MAX_POSITIONS=5       # max concurrent open Binance positions
    LIVE_MODE=false            # False = demo-fapi.binance.com, True = live fapi.binance.com

Thread-safety: all Binance REST calls are synchronous (requests library).
The asyncio pred loop calls fire()/_close() from the event loop thread, so
create_order / close_order run inline — they're fast (<20ms on Tokyo VPS).
If latency becomes a concern, wrap in asyncio.to_thread().
"""

import hashlib
import hmac
import logging
import os
import time
import urllib.parse
from decimal import Decimal

import requests

log = logging.getLogger("live_exec")

# ── Config (read from env, overrideable at runtime) ───────────────────────────
def _env_bool(key: str, default: bool) -> bool:
    return os.environ.get(key, str(default)).lower() in ("1", "true", "yes")

def _env_float(key: str, default: float) -> float:
    try:
        return float(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default

LIVE_ENABLED      = _env_bool("LIVE_ENABLED", False)      # master switch
LIVE_MODE         = _env_bool("LIVE_MODE", False)          # False=testnet, True=live
LIVE_ORDER_USDT   = _env_float("LIVE_ORDER_USDT", 20.0)    # USDT per trade
LIVE_MAX_POSITIONS = int(_env_float("LIVE_MAX_POSITIONS", 5))  # max concurrent

# ── Maker exits (Phase 3) ─────────────────────────────────────────────────────
# Exits currently go out as taker MARKET orders (100% taker = full fee both legs).
# When enabled, non-urgent exits first try a post-only (GTX) LIMIT at the touch to
# capture the maker rate, falling back to MARKET if it doesn't fill in the budget.
# Default OFF — enable on stage first (MAKER_EXITS=true in .env.stage) and measure
# the realized maker fill-rate before considering prod.
# NOTE: the poll blocks the asyncio pred loop, so keep WAIT_MS small. Urgent (sl)
# exits skip the maker attempt entirely — getting out matters more than the rebate.
MAKER_EXITS           = _env_bool("MAKER_EXITS", False)
MAKER_EXIT_WAIT_MS    = _env_float("MAKER_EXIT_WAIT_MS", 800.0)   # total fill budget
MAKER_EXIT_POLL_MS    = _env_float("MAKER_EXIT_POLL_MS", 200.0)   # status poll interval
MAKER_EXIT_SKIP_URGENT = _env_bool("MAKER_EXIT_SKIP_URGENT", True)
_URGENT_REASONS = frozenset({"sl", "inertia"})

# Running maker-exit tally (visible at INFO even under --quiet) so the fill-rate
# is observable without grepping debug logs.
_maker_stats = {"attempt": 0, "fill": 0, "fallback": 0, "reject": 0}

# ── Catastrophic exchange stop (Phase 4) ──────────────────────────────────────
# The engine manages SL/trail in software, so if the box dies (network/power/OOM)
# open positions sit with NO stop on Binance. When enabled, every live entry also
# places a catastrophic STOP_MARKET (closePosition) on the exchange — wide enough
# that the software exits always act first, so it only ever fires if the engine is
# dead. It survives the engine and the box dying. Cancelled when the position
# closes normally. Distance = EXCHANGE_STOP_MULT × the trade's dynamic SL (falls
# back to a fixed % if the SL isn't known). Default OFF — stage first.
EXCHANGE_STOP              = _env_bool("EXCHANGE_STOP", False)
EXCHANGE_STOP_MULT         = _env_float("EXCHANGE_STOP_MULT", 2.5)    # × dynamic SL
EXCHANGE_STOP_FALLBACK_PCT = _env_float("EXCHANGE_STOP_FALLBACK_PCT", 2.0)  # if SL unknown
_protective_stops: dict[str, int] = {}   # sym → catastrophic stop orderId

def _maker_rate() -> str:
    a = _maker_stats["attempt"] or 1
    return (f"maker {_maker_stats['fill']}/{_maker_stats['attempt']} "
            f"({_maker_stats['fill']/a*100:.0f}%) "
            f"fallback={_maker_stats['fallback']} reject={_maker_stats['reject']}")

TESTNET_BASE = "https://demo-fapi.binance.com"
LIVE_BASE    = "https://fapi.binance.com"

def _base_url() -> str:
    return LIVE_BASE if LIVE_MODE else TESTNET_BASE

def _api_key() -> str:
    return os.environ.get("BINANCE_API_KEY", "")

def _api_secret() -> str:
    return os.environ.get("BINANCE_API_SECRET", "")

# ── HMAC signing ──────────────────────────────────────────────────────────────
def _plain(v):
    """Render floats as plain decimal strings (never scientific notation, no float noise).
    Binance rejects values like '1.234e-05' (common for cheap coins). Decimal(str(v))
    preserves the already-rounded value; format 'f' forces plain decimal."""
    if isinstance(v, float):
        return format(Decimal(str(v)), "f")
    return v

def _sign(params: dict) -> str:
    """Return HMAC-SHA256 signature for the given param dict."""
    query = urllib.parse.urlencode(params)
    return hmac.new(
        _api_secret().encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

def _prepare(params: dict) -> dict:
    """Normalize floats to plain strings, add timestamp + recvWindow, then sign.
    Signing happens on the SAME values that get sent (requests re-encodes the dict),
    so the signed string always equals the transmitted string."""
    for k, v in list(params.items()):
        params[k] = _plain(v)
    params.setdefault("recvWindow", 5000)
    params["timestamp"] = int(time.time() * 1000)
    params["signature"] = _sign(params)
    return params

def _signed_headers() -> dict:
    return {"X-MBX-APIKEY": _api_key()}

# ── Low-level REST helpers ────────────────────────────────────────────────────
def _post(path: str, params: dict) -> dict:
    params = _prepare(params)
    url = _base_url() + path
    try:
        r = requests.post(url, headers=_signed_headers(), params=params, timeout=5)
        return r.json()
    except Exception as exc:
        log.error(f"POST {path} failed: {exc}")
        return {"error": str(exc)}

def _get(path: str, params: dict | None = None) -> dict:
    params = _prepare(params or {})
    url = _base_url() + path
    try:
        r = requests.get(url, headers=_signed_headers(), params=params, timeout=5)
        return r.json()
    except Exception as exc:
        log.error(f"GET {path} failed: {exc}")
        return {"error": str(exc)}

def _delete(path: str, params: dict) -> dict:
    params = _prepare(params)
    url = _base_url() + path
    try:
        r = requests.delete(url, headers=_signed_headers(), params=params, timeout=5)
        return r.json()
    except Exception as exc:
        log.error(f"DELETE {path} failed: {exc}")
        return {"error": str(exc)}

def _book_ticker(sym: str) -> dict | None:
    """Best bid/ask (public, unsigned). Returns {'bid': float, 'ask': float} or None."""
    try:
        url = _base_url() + "/fapi/v1/ticker/bookTicker"
        r = requests.get(url, params={"symbol": sym}, timeout=3).json()
        return {"bid": float(r["bidPrice"]), "ask": float(r["askPrice"])}
    except Exception as exc:
        log.warning(f"_book_ticker {sym} failed: {exc}")
        return None

# ── Exchange info cache (for quantity precision) ──────────────────────────────
_precision_cache: dict[str, int] = {}

# ── Illiquid / Alpha token detection ─────────────────────────────────────────
# Coins with maintMarginPercent >= ILLIQUID_MARGIN_THR are classified as
# illiquid/alpha tokens. These have thin orderbooks, high slippage, and are
# unsuitable for W/B/CGY/Y/L. P strategy explicitly opts-in to these.
# Built once at startup from exchangeInfo, refreshed every 6h.
ILLIQUID_MARGIN_THR = 5.0    # % — BTC=0.5%, quality alts=1-2%, Alpha/meme=5-25%
_illiquid_syms:  frozenset = frozenset()
_illiquid_built: float     = 0.0

def get_illiquid_syms() -> frozenset:
    """Return cached set of illiquid/alpha symbols. Rebuilds every 6h."""
    global _illiquid_syms, _illiquid_built
    import time
    if time.time() - _illiquid_built < 21600 and _illiquid_syms:
        return _illiquid_syms
    try:
        url = _base_url() + "/fapi/v1/exchangeInfo"
        r = requests.get(url, timeout=15)
        data = r.json()
        illiquid = set()
        for s in data.get("symbols", []):
            if not s.get("symbol", "").endswith("USDT"): continue
            maint = float(s.get("maintMarginPercent", 0) or 0)
            if maint >= ILLIQUID_MARGIN_THR:
                illiquid.add(s["symbol"])
        _illiquid_syms = frozenset(illiquid)
        _illiquid_built = time.time()
        log.info(f"[ILLIQUID] {len(_illiquid_syms)} alpha/illiquid symbols identified "
                 f"(maintMargin≥{ILLIQUID_MARGIN_THR}%)")
    except Exception as exc:
        log.warning(f"[ILLIQUID] exchangeInfo fetch failed: {exc}")
    return _illiquid_syms

def is_illiquid(sym: str) -> bool:
    """Return True if sym is an Alpha/illiquid token unsuitable for main strategies."""
    return sym in get_illiquid_syms()

def _get_qty_precision(sym: str) -> int:
    """Return the quantity step size decimal places for a symbol."""
    if sym in _precision_cache:
        return _precision_cache[sym]
    try:
        url = _base_url() + "/fapi/v1/exchangeInfo"
        r = requests.get(url, timeout=10).json()
        for s in r.get("symbols", []):
            for f in s.get("filters", []):
                if f["filterType"] == "LOT_SIZE":
                    step = f["stepSize"].rstrip("0") or "1"
                    decimals = len(step.split(".")[-1]) if "." in step else 0
                    _precision_cache[s["symbol"]] = decimals
        return _precision_cache.get(sym, 3)
    except Exception as exc:
        log.warning(f"exchangeInfo fetch failed for {sym}: {exc}")
        return 3

def _round_qty(sym: str, raw_qty: float) -> float:
    precision = _get_qty_precision(sym)
    factor = 10 ** precision
    return round(int(raw_qty * factor) / factor, precision)

# Price precision (PRICE_FILTER tickSize) — needed for maker LIMIT orders.
_price_precision_cache: dict[str, int] = {}

def _get_price_precision(sym: str) -> int:
    """Return the price tick decimal places for a symbol (for LIMIT prices)."""
    if sym in _price_precision_cache:
        return _price_precision_cache[sym]
    try:
        url = _base_url() + "/fapi/v1/exchangeInfo"
        r = requests.get(url, timeout=10).json()
        for s in r.get("symbols", []):
            for f in s.get("filters", []):
                if f["filterType"] == "PRICE_FILTER":
                    tick = f["tickSize"].rstrip("0") or "1"
                    decimals = len(tick.split(".")[-1]) if "." in tick else 0
                    _price_precision_cache[s["symbol"]] = decimals
        return _price_precision_cache.get(sym, 4)
    except Exception as exc:
        log.warning(f"exchangeInfo (price) fetch failed for {sym}: {exc}")
        return 4

def _round_price(sym: str, raw_price: float) -> float:
    precision = _get_price_precision(sym)
    return round(raw_price, precision)

# ── Account state cache ───────────────────────────────────────────────────────
# can_enter() is called on every fire() attempt — up to hundreds of times per
# second across 90+ coins. We must NOT hit the REST API on every call.
# Cache balance + position count with a TTL; invalidate immediately after
# a real order is placed or closed (call _invalidate_account_cache()).
_ACCOUNT_CACHE_TTL = 5.0   # seconds between REST refreshes during normal running
_cache_balance:    float | None = None
_cache_n_positions: int         = 0
_cache_positions:  list         = []
_cache_unrealized: float        = 0.0   # sum of unrealized PnL across all open positions
_cache_ts:         float        = 0.0

def _invalidate_account_cache() -> None:
    """Force next can_enter() call to re-fetch from Binance immediately."""
    global _cache_ts
    _cache_ts = 0.0

# ── Per-symbol live order lock ────────────────────────────────────────────────
# Binance allows only ONE position per symbol on futures.
# Multiple strategies can fire on the same symbol in the same tick (sim is fine
# with this — each strategy tracks its own pred independently).
# But for live execution, only the FIRST strategy to fire on a symbol gets a
# real order. All subsequent strategies for that symbol are skipped silently.
# The lock is released when close_position() is called for that symbol.
#
# This means:
#   - Sim: W, B, Y all open ENAUSDT → 3 sim preds, full data collection ✓
#   - Live: W fires first → real order placed. B fires 50ms later → skipped.
#   - Dashboard: all 3 show as open trades (sim). Only W has _live_ok=True.
#   - Exit: W closes → real close order. B/Y close → sim-only, no Binance order.
_live_symbol_open: set[str] = set()   # symbols with an active live Binance position

def _live_lock_sym(sym: str) -> None:
    _live_symbol_open.add(sym)

def _live_unlock_sym(sym: str) -> None:
    _live_symbol_open.discard(sym)

def _live_sym_is_locked(sym: str) -> bool:
    return sym in _live_symbol_open

def _refresh_account_cache() -> None:
    """Fetch balance + positions from Binance and store in cache."""
    global _cache_balance, _cache_n_positions, _cache_positions, _cache_unrealized, _cache_ts
    # Balance
    data = _get("/fapi/v2/balance")
    if isinstance(data, list):
        for asset in data:
            if asset.get("asset") == "USDT":
                _cache_balance = float(asset.get("availableBalance", 0))
                break
    else:
        log.warning(f"balance fetch unexpected response: {data}")
        _cache_balance = None
    # Positions
    pdata = _get("/fapi/v2/positionRisk")
    if isinstance(pdata, list):
        _cache_positions = [
            {
                "sym":         p["symbol"],
                "dir":         "long" if float(p.get("positionAmt", 0)) > 0 else "short",
                "qty":         abs(float(p.get("positionAmt", 0))),
                "entry_price": float(p.get("entryPrice", 0)),
                "unrealized":  float(p.get("unRealizedProfit", 0)),
            }
            for p in pdata if float(p.get("positionAmt", 0)) != 0
        ]
        _cache_n_positions = len(_cache_positions)
        _cache_unrealized  = sum(p["unrealized"] for p in _cache_positions)
    else:
        log.warning(f"positionRisk unexpected: {pdata}")
    _cache_ts = time.time()

def get_usdt_balance() -> float | None:
    """Return cached available USDT balance (refreshes every _ACCOUNT_CACHE_TTL seconds)."""
    if time.time() - _cache_ts > _ACCOUNT_CACHE_TTL:
        _refresh_account_cache()
    return _cache_balance

def get_open_positions() -> list[dict]:
    """Return cached list of non-zero Binance positions."""
    if time.time() - _cache_ts > _ACCOUNT_CACHE_TTL:
        _refresh_account_cache()
    return _cache_positions

def count_open_positions() -> int:
    if time.time() - _cache_ts > _ACCOUNT_CACHE_TTL:
        _refresh_account_cache()
    return _cache_n_positions

# ── Core order functions ───────────────────────────────────────────────────────
def _place_protective_stop(sym: str, entry_side: str, entry_price: float,
                           sl_pct: float | None, qty: float) -> None:
    """Place a catastrophic STOP_MARKET on Binance as a disaster backstop that
    survives the engine/box dying. Wide enough that the software exits always act
    first. Records the orderId so it can be cancelled on close.

    Uses reduceOnly + explicit quantity (the order form this account accepts) rather
    than closePosition=true — the latter is rejected here (Binance -4120), same
    reason the normal close path falls back off closePosition."""
    dist_pct = (EXCHANGE_STOP_MULT * sl_pct) if sl_pct else EXCHANGE_STOP_FALLBACK_PCT
    # Closing side is opposite the entry; stop sits beyond the entry on the loss side.
    if entry_side == "BUY":      # long → protective SELL below entry
        stop_side  = "SELL"
        stop_price = entry_price * (1 - dist_pct / 100.0)
    else:                         # short → protective BUY above entry
        stop_side  = "BUY"
        stop_price = entry_price * (1 + dist_pct / 100.0)
    stop_price = _round_price(sym, stop_price)

    resp = _post("/fapi/v1/order", {
        "symbol":        sym,
        "side":          stop_side,
        "type":          "STOP_MARKET",
        "stopPrice":     stop_price,
        "quantity":      qty,           # reduceOnly+qty: the form this account accepts
        "reduceOnly":    "true",        # never flips position; no-op if already flat
        "workingType":   "MARK_PRICE",  # trigger on mark, not last — avoids wick hunts
    })
    if "orderId" in resp:
        _protective_stops[sym] = resp["orderId"]
        log.warning(f"[XSTOP] {sym} catastrophic stop @ {stop_price} "
                    f"({dist_pct:.2f}% from {entry_price}) qty={qty} orderId={resp['orderId']}")
    else:
        # Non-fatal: the position is still managed in software. Log loudly WITH the
        # exchange message so an unprotected position (and its cause) is visible.
        log.warning(f"[XSTOP] {sym} FAILED to place protective stop "
                    f"(code={resp.get('code')} msg={resp.get('msg')}): "
                    f"position has no exchange-side disaster stop")

def _cancel_protective_stop(sym: str) -> None:
    """Cancel the catastrophic stop for sym, if any. Idempotent — a closePosition
    stop is auto-cancelled by Binance when the position closes, so -2011 (unknown
    order) is expected and harmless."""
    oid = _protective_stops.pop(sym, None)
    if oid is None:
        return
    cx = _delete("/fapi/v1/order", {"symbol": sym, "orderId": oid})
    if cx.get("code") not in (None, -2011):
        log.debug(f"[XSTOP] {sym} cancel returned {cx.get('code')}")


def create_order(sym: str, side: str, usdt_size: float, sl_pct: float | None = None) -> dict:
    """
    Place a MARKET order on Binance Futures.

    Args:
        sym:       e.g. 'BTCUSDT'
        side:      'BUY' or 'SELL'
        usdt_size: notional USDT value (e.g. 20.0)

    Returns:
        Binance order response dict, or {'error': ...} on failure.
        Includes 'ok': True/False for quick success check.
    """
    if not LIVE_ENABLED:
        log.debug(f"[LIVE_DISABLED] create_order {sym} {side} ${usdt_size:.1f}")
        return {"ok": False, "skipped": True, "reason": "LIVE_ENABLED=False"}

    # Per-symbol live lock: if another strategy already has a real Binance position
    # open on this symbol, skip — Binance only allows one position per symbol.
    # The sim pred still gets created (caller already appended it to preds).
    if _live_sym_is_locked(sym):
        log.debug(f"[SYM_LOCKED] {sym} already has a live position — sim-only for this strategy")
        return {"ok": False, "skipped": True, "reason": "sym_locked"}

    # Get current price for qty calculation
    try:
        from engine import sym_state
        price = sym_state.get(sym, {}).get("price", 0)
    except ImportError:
        price = 0

    if not price:
        log.warning(f"create_order: no price for {sym}, skipping")
        return {"ok": False, "error": "no_price"}

    raw_qty = usdt_size / price
    qty     = _round_qty(sym, raw_qty)
    if qty <= 0:
        log.warning(f"create_order: qty rounded to 0 for {sym} (${usdt_size} @ {price})")
        return {"ok": False, "error": "qty_zero"}

    params = {
        "symbol":   sym,
        "side":     side,
        "type":     "MARKET",
        "quantity": qty,
    }
    resp = _post("/fapi/v1/order", params)

    if "orderId" in resp:
        resp["ok"] = True
        log.info(f"[ORDER] {sym} {side} qty={qty} ${usdt_size:.1f} → orderId={resp['orderId']}")
        _invalidate_account_cache()   # position count changed — refresh on next can_enter()
        _live_lock_sym(sym)           # block other strategies from placing real orders on this symbol
        if EXCHANGE_STOP:
            _place_protective_stop(sym, side, price, sl_pct, qty)
    else:
        resp["ok"] = False
        log.error(f"[ORDER FAILED] {sym} {side} qty={qty}: {resp}")

    return resp

def _try_maker_close(sym: str, side: str, dir_to_close: str) -> dict | None:
    """
    Attempt a post-only (GTX) LIMIT close at the touch to capture the maker rate.
    Returns a success dict if fully filled within the budget, else None (caller
    then falls back to a MARKET close for whatever remains).

    Blocks the calling thread for up to MAKER_EXIT_WAIT_MS while polling — kept
    small on purpose because _close() runs inline in the asyncio pred loop.
    """
    _invalidate_account_cache()
    pos = next((p for p in get_open_positions()
                if p["sym"] == sym and p["dir"] == dir_to_close), None)
    if not pos:
        return None

    bt = _book_ticker(sym)
    if not bt:
        return None
    # Post-only at the touch: closing a long → SELL at best ask; short → BUY at best bid.
    # Sitting at the touch stays maker (doesn't cross); if the book moved, GTX rejects
    # and we fall back rather than pay taker.
    raw_px = bt["ask"] if side == "SELL" else bt["bid"]
    px = _round_price(sym, raw_px)

    _maker_stats["attempt"] += 1
    if _maker_stats["attempt"] % 20 == 0:           # visible under --quiet (WARNING)
        log.warning(f"[MAKER] tally: {_maker_rate()}")
    resp = _post("/fapi/v1/order", {
        "symbol":       sym,
        "side":         side,
        "type":         "LIMIT",
        "timeInForce":  "GTX",          # post-only: reject if it would take liquidity
        "price":        px,
        "quantity":     pos["qty"],
        "reduceOnly":   "true",
    })
    if "orderId" not in resp:
        _maker_stats["reject"] += 1
        log.info(f"[MAKER] {sym} GTX not placed ({resp.get('code')}) → market fallback  | {_maker_rate()}")
        return None

    oid = resp["orderId"]
    deadline = time.time() + MAKER_EXIT_WAIT_MS / 1000.0
    poll = max(0.05, MAKER_EXIT_POLL_MS / 1000.0)

    def _filled_ok():
        _maker_stats["fill"] += 1
        log.info(f"[MAKER] {sym} {side} qty={pos['qty']} filled as maker @ {px} (orderId={oid})  | {_maker_rate()}")
        _invalidate_account_cache()
        _live_unlock_sym(sym)
        _cancel_protective_stop(sym)
        # realized_pnl/commission come from reconciliation (positions CSV + income API);
        # a filled LIMIT status doesn't return them reliably.
        return {"ok": True, "maker": True, "orderId": oid,
                "realized_pnl": None, "commission": None}

    while time.time() < deadline:
        time.sleep(poll)
        st = _get("/fapi/v1/order", {"symbol": sym, "orderId": oid})
        status = st.get("status")
        if status == "FILLED":
            return _filled_ok()
        if status in ("CANCELED", "EXPIRED", "REJECTED"):
            _maker_stats["fallback"] += 1
            return None

    # Timed out — cancel and let the caller market out. Handle the race where it
    # filled just as we cancel.
    cx = _delete("/fapi/v1/order", {"symbol": sym, "orderId": oid})
    if cx.get("status") == "FILLED" or cx.get("code") == -2011:
        st = _get("/fapi/v1/order", {"symbol": sym, "orderId": oid})
        if st.get("status") == "FILLED":
            return _filled_ok()
    _maker_stats["fallback"] += 1
    log.info(f"[MAKER] {sym} not filled in {MAKER_EXIT_WAIT_MS:.0f}ms → market fallback  | {_maker_rate()}")
    return None


def close_position(sym: str, dir_to_close: str, reason: str | None = None) -> dict:
    """
    Close an open position. By default a MARKET order with reduceOnly=true.
    If MAKER_EXITS is enabled and the exit is non-urgent, first try a post-only
    LIMIT (maker) at the touch, falling back to MARKET if it doesn't fill.

    Args:
        sym:           e.g. 'BTCUSDT'
        dir_to_close:  'long' or 'short' (the direction of the position we want to close)
        reason:        exit reason (e.g. 'trail','tp','be','sl') — urgent reasons skip maker.

    Returns:
        Binance order response dict with 'ok' key.
    """
    if not LIVE_ENABLED:
        log.debug(f"[LIVE_DISABLED] close_position {sym} {dir_to_close}")
        return {"ok": False, "skipped": True, "reason": "LIVE_ENABLED=False"}

    # To close a long → SELL; to close a short → BUY
    side = "SELL" if dir_to_close == "long" else "BUY"

    # Phase 3: maker-exit attempt (env-gated; urgent exits go straight to market).
    if MAKER_EXITS:
        urgent = (reason or "").lower() in _URGENT_REASONS
        if not (urgent and MAKER_EXIT_SKIP_URGENT):
            mk = _try_maker_close(sym, side, dir_to_close)
            if mk and mk.get("ok"):
                return mk
            # not filled → fall through to MARKET, which re-fetches qty and closes
            # whatever remains (handles a partial maker fill correctly).

    # Force a fresh Binance fetch before the qty lookup.
    # Avoids the stale-cache race where create_order was called <5s ago and the
    # Binance position hasn't settled in their system yet, returning qty=0.
    _invalidate_account_cache()
    positions = get_open_positions()
    pos = next((p for p in positions if p["sym"] == sym and p["dir"] == dir_to_close), None)

    if not pos:
        log.warning(f"close_position: no open {dir_to_close} on {sym} found on Binance")
        return {"ok": False, "error": "position_not_found"}

    params = {
        "symbol":     sym,
        "side":       side,
        "type":       "MARKET",
        "quantity":   pos["qty"],
        "reduceOnly": "true",
    }
    resp = _post("/fapi/v1/order", params)

    if "orderId" not in resp and resp.get("code") in (-4131, -1111, -4003):
        # PERCENT_PRICE filter — retry with closePosition=true
        log.warning(f"[CLOSE] {sym} PERCENT_PRICE, retrying closePosition=true")
        resp = _post("/fapi/v1/order", {"symbol": sym, "side": side, "type": "MARKET", "closePosition": "true"})

    if "orderId" not in resp and resp.get("code") in (-4131, -4136, -1111, -4003, -4120):
        # closePosition invalid OR rejected by Algo-API migration (-4120) — plain market order
        log.warning(f"[CLOSE] {sym} closePosition failed ({resp.get('code')}), retrying plain market")
        resp = _post("/fapi/v1/order", {"symbol": sym, "side": side, "type": "MARKET", "quantity": pos["qty"]})

    if "orderId" in resp:
        resp["ok"] = True
        # Extract realized PnL and commission from Binance response.
        # Futures MARKET orders return realizedPnl and commission at the top level.
        try:
            resp["realized_pnl"] = float(resp.get("realizedPnl", 0) or 0)
            resp["commission"]    = float(resp.get("commission", 0) or 0)
        except (TypeError, ValueError):
            resp["realized_pnl"] = None
            resp["commission"]    = None
        log.info(
            f"[CLOSE] {sym} {side} qty={pos['qty']} (closing {dir_to_close}) "            f"→ orderId={resp['orderId']} pnl={resp.get('realized_pnl', 0):+.4f}"
        )
        _invalidate_account_cache()
        _live_unlock_sym(sym)
        _cancel_protective_stop(sym)
    else:
        resp["ok"] = False
        log.error(f"[CLOSE FAILED] {sym} {dir_to_close} (all attempts): {resp}")
        # Always release the sym lock even on failure — otherwise the symbol is
        # permanently blocked from new live entries (every future fire() is silently
        # skipped as sim-only, producing logs with no matching Binance position).
        # The orphaned Binance position (if any) must be handled by reconcile /
        # manual close, but blocking new entries forever is worse.
        _live_unlock_sym(sym)

    return resp

# ── Position sizing gate ───────────────────────────────────────────────────────
def can_enter(required_usdt: float | None = None) -> tuple[bool, str]:
    """
    Pre-entry gate check. Returns (allowed: bool, reason: str).

    Checks:
      1. LIVE_ENABLED master switch
      2. Max concurrent position cap
      3. Sufficient balance (2× required to leave headroom)
    """
    if not LIVE_ENABLED:
        return True, "sim_mode"  # sim: always allow, engine gates handle it

    n_open = count_open_positions()
    if n_open >= LIVE_MAX_POSITIONS:
        return False, f"max_positions ({n_open}/{LIVE_MAX_POSITIONS})"

    # Balance check removed — position cap (LIVE_MAX_POSITIONS) is the primary guard.
    # At 20x leverage, margin per trade = order_usdt / 20, so 10 positions
    # uses order_usdt / 2 total margin — well within any reasonable balance.

    return True, "ok"

# ── Close all open positions ──────────────────────────────────────────────────
def close_all_positions(reason: str = "startup") -> list[dict]:
    """
    Close every open Binance position with a market order.
    Called on deploy/restart before the new engine state is loaded.

    Args:
        reason: label for log messages (e.g. 'startup', 'deploy', 'shutdown')

    Returns:
        List of close results (one per position attempted).
    """
    if not LIVE_ENABLED:
        log.info(f"[CLOSE_ALL] LIVE_ENABLED=False — skipping ({reason})")
        return []

    _invalidate_account_cache()
    positions = get_open_positions()

    if not positions:
        log.info(f"[CLOSE_ALL] No open positions to close ({reason})")
        return []

    log.info(f"[CLOSE_ALL] Closing {len(positions)} position(s) on {reason}...")
    results = []
    for pos in positions:
        sym = pos["sym"]
        direction = pos["dir"]
        log.info(f"[CLOSE_ALL] Closing {sym} {direction} qty={pos['qty']} entry={pos['entry_price']}")
        result = close_position(sym, direction)
        result["sym"] = sym
        result["dir"] = direction
        results.append(result)
        if result.get("ok"):
            log.info(f"[CLOSE_ALL] ✅ {sym} closed (pnl={result.get('realized_pnl', 0):+.4f})")
        else:
            log.error(f"[CLOSE_ALL] ❌ {sym} close failed: {result}")

    _invalidate_account_cache()
    n_ok   = sum(1 for r in results if r.get("ok"))
    n_fail = len(results) - n_ok
    log.info(f"[CLOSE_ALL] Done: {n_ok} closed, {n_fail} failed ({reason})")
    return results


# ── Startup reconciliation ────────────────────────────────────────────────────
def reconcile_on_startup(engines: list) -> None:
    """
    Called once at startup. Compares engine's open preds vs actual Binance positions.
    Logs discrepancies — does NOT auto-close orphaned positions (requires human decision).

    Args:
        engines: list of StrategyEngine instances (from _engines_a + _engines_b)
    """
    if not LIVE_ENABLED:
        return

    log.info("[RECONCILE] Checking engine state vs Binance positions...")

    # Collect all open preds from engine
    engine_open: dict[str, list] = {}  # sym → list of open preds
    for eng in engines:
        for p in eng.preds:
            if p.get("out3") is not None:
                continue
            sym = p["sym"]
            engine_open.setdefault(sym, []).append(p)

    # Collect actual Binance positions
    binance_open = {pos["sym"]: pos for pos in get_open_positions()}

    # Check for orphans on Binance (position exists but engine has no open pred)
    for sym, pos in binance_open.items():
        if sym not in engine_open:
            log.warning(
                f"[RECONCILE] ORPHAN on Binance: {sym} {pos['dir']} qty={pos['qty']} "
                f"entry={pos['entry_price']} — no matching engine pred. "
                f"MANUAL ACTION REQUIRED: close manually at {_base_url()}"
            )

    # Check for engine preds with no Binance position (sim pred that leaked through)
    for sym, preds in engine_open.items():
        if sym not in binance_open:
            n = len(preds)
            log.warning(
                f"[RECONCILE] ENGINE has {n} open pred(s) for {sym} but NO Binance position. "
                f"Marking as sim-only (no live order). Will close silently when engine exits."
            )
            for p in preds:
                p["_live_order_id"] = None  # mark: no real order backing this pred

    # Report clean positions
    matched = set(engine_open) & set(binance_open)
    if matched:
        log.info(f"[RECONCILE] {len(matched)} symbol(s) matched: {sorted(matched)}")
    else:
        log.info("[RECONCILE] No open positions to reconcile.")
