#!/usr/bin/env python3
"""
spot_exec.py — SPOT-leg persistent maker executor (the carry hedge leg)
=======================================================================
Mirror of carry_exec.py for Binance SPOT. Spot's post-only order is type=LIMIT_MAKER
(rejected if it would take), so — like the perp GTX path — taker fills are structurally
impossible. Persistent: rest at the touch, follow the book, never cross.

NOTE: Binance has no spot testnet that pairs with the futures testnet, so the spot leg's
first REAL execution is real capital. That is why dry_run defaults True and a real send
requires explicit flags. Uses SEPARATE spot API keys (SPOT_API_KEY/SPOT_API_SECRET).

SAFETY: dry_run default; refuses api.binance.com unless allow_prod=True AND dry_run=False.
"""
import os, time, hmac, hashlib, urllib.parse, logging
from decimal import Decimal
import requests

log = logging.getLogger("spot_exec")
if not log.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SPOT_LIVE = os.getenv("SPOT_LIVE", "false").lower() == "true"   # False -> dry/guarded
PROD_BASE = "https://api.binance.com"
_KEY = lambda: os.environ["SPOT_API_KEY"]
_SEC = lambda: os.environ["SPOT_API_SECRET"]
_POST_ONLY_REJECT = (-2010, -1013)   # LIMIT_MAKER would immediately match -> re-quote


def _base():
    return PROD_BASE


def _plain(v):
    return format(Decimal(str(v)), "f") if isinstance(v, float) else v


def _sign(params):
    return hmac.new(_SEC().encode(), urllib.parse.urlencode(params).encode(), hashlib.sha256).hexdigest()


def _signed(method, path, params):
    for k, v in list(params.items()):
        params[k] = _plain(v)
    params.setdefault("recvWindow", 5000)
    params["timestamp"] = int(time.time() * 1000)
    params["signature"] = _sign(params)
    try:
        r = requests.request(method, _base() + path, headers={"X-MBX-APIKEY": _KEY()}, params=params, timeout=5)
        return r.json()
    except Exception as e:
        log.error(f"{method} {path} failed: {e}"); return {"error": str(e)}


def _book(sym):
    try:
        r = requests.get(_base() + "/api/v3/ticker/bookTicker", params={"symbol": sym}, timeout=3).json()
        return {"bid": float(r["bidPrice"]), "ask": float(r["askPrice"])}
    except Exception as e:
        log.warning(f"spot book {sym}: {e}"); return None


_filters = {}
def _precision(sym):
    if sym in _filters:
        return _filters[sym]
    try:
        info = requests.get(_base() + "/api/v3/exchangeInfo", params={"symbol": sym}, timeout=5).json()
        s = info["symbols"][0]
        def dec(step): return max(0, len(step.rstrip("0").split(".")[1]) if "." in step.rstrip("0") else 0)
        qp = pp = 8
        for f in s["filters"]:
            if f["filterType"] == "LOT_SIZE": qp = dec(f["stepSize"])
            if f["filterType"] == "PRICE_FILTER": pp = dec(f["tickSize"])
        _filters[sym] = (qp, pp)
    except Exception:
        _filters[sym] = (6, 6)
    return _filters[sym]

def _rq(sym, q): qp, _ = _precision(sym); f = 10 ** qp; return int(q * f) / f
def _rp(sym, p): _, pp = _precision(sym); return round(p, pp)


def _guard(dry_run, allow_prod):
    if not (allow_prod and not dry_run):
        if not dry_run:
            raise RuntimeError("spot_exec refuses real send unless allow_prod=True (spot is real Binance, real money).")


def maker_fill(sym, side, qty, *, max_wait_s=180.0, requote_after_s=8.0, poll_s=1.0,
               dry_run=True, allow_prod=False):
    _guard(dry_run, allow_prod)
    side = side.upper(); assert side in ("BUY", "SELL")
    filled = 0.0; notional = 0.0; fills = []; requotes = 0; t0 = time.time()
    remaining = _rq(sym, qty)
    while remaining > 0 and (time.time() - t0) < max_wait_s:
        bk = _book(sym)
        if not bk: time.sleep(poll_s); continue
        px = _rp(sym, bk["bid"] if side == "BUY" else bk["ask"])
        q = _rq(sym, remaining)
        if dry_run:
            log.info(f"[DRY-SPOT] would place LIMIT_MAKER {side} {q} {sym} @ {px} — no order sent")
            return {"sym": sym, "side": side, "requested": qty, "filled": 0.0, "avg_price": None,
                    "fills": [], "remaining": remaining, "status": "DRY_RUN", "requotes": 0}
        resp = _signed("POST", "/api/v3/order",
                       {"symbol": sym, "side": side, "type": "LIMIT_MAKER", "price": px, "quantity": q})
        if resp.get("code") in _POST_ONLY_REJECT or "orderId" not in resp:
            if resp.get("code") in _POST_ONLY_REJECT:
                requotes += 1; time.sleep(0.4); continue
            log.error(f"[ERR-SPOT] {sym} rejected: {resp}"); break
        oid = resp["orderId"]; prev = 0.0; waited = 0.0
        while waited < requote_after_s and (time.time() - t0) < max_wait_s:
            st = _signed("GET", "/api/v3/order", {"symbol": sym, "orderId": oid})
            ex = float(st.get("executedQty", 0) or 0)
            if ex > prev:
                avg = float(st.get("price", px) or px); inc = ex - prev
                fills.append((avg, inc)); filled += inc; notional += inc * avg; prev = ex
            if st.get("status") == "FILLED":
                remaining = _rq(sym, max(0.0, qty - filled)); break
            bk = _book(sym)
            if bk:
                touch = bk["bid"] if side == "BUY" else bk["ask"]
                if (side == "BUY" and touch > px) or (side == "SELL" and touch < px): break
            time.sleep(poll_s); waited += poll_s
        cx = _signed("DELETE", "/api/v3/order", {"symbol": sym, "orderId": oid})
        ex = float(cx.get("executedQty", prev) or prev)
        if ex > prev:
            inc = ex - prev; fills.append((px, inc)); filled += inc; notional += inc * px
        remaining = _rq(sym, max(0.0, qty - filled)); requotes += 1
        if requotes > 200: break
    avg = (notional / filled) if filled > 0 else None
    status = "FILLED" if remaining <= 0 else ("PARTIAL" if filled > 0 else "UNFILLED")
    return {"sym": sym, "side": side, "requested": qty, "filled": filled, "avg_price": avg,
            "fills": fills, "remaining": remaining, "status": status, "requotes": requotes}


def maker_buy(sym, usdt, **kw):
    bk = _book(sym)
    if not bk: return {"status": "NO_BOOK", "sym": sym}
    qty = _rq(sym, usdt / ((bk["bid"] + bk["ask"]) / 2))
    return maker_fill(sym, "BUY", qty, **kw)

def maker_sell(sym, qty, **kw):
    return maker_fill(sym, "SELL", qty, **kw)
