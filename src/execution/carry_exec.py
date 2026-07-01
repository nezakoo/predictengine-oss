#!/usr/bin/env python3
"""
carry_exec.py — perp-leg PERSISTENT MAKER execution (plumbing rehearsal)
========================================================================
The one thing carry needs that scalping didn't: fills that are *always* maker.
Scalping's create_order is MARKET (taker) and _try_maker_close falls back to MARKET
if it doesn't fill fast — both fine for a 2-minute scalp, fatal for carry (taker fees
erase the edge). This module rests a post-only (GTX) order at the touch and FOLLOWS
the book — cancel & re-quote if price moves away or on partial fill — and NEVER crosses.
Patience is free for a multi-day hold.

Reuses your fixed live_execution.py for signing / endpoint / precision / book ticker.
Perp leg only (Binance futures testnet has no spot) — this is an execution REHEARSAL,
not a strategy test. The spot leg and real basis come later, with real capital.

SAFETY:
  * dry_run=True by default — logs intended orders, sends nothing.
  * testnet by default (live_execution.LIVE_MODE=False -> demo-fapi).
  * refuses to run against PROD (fapi.binance.com) unless allow_prod=True is passed
    explicitly AND dry_run is False.
  * GTX post-only on every order: if it would cross, Binance rejects it and we re-quote
    — so we can never accidentally pay taker.

Usage (as a library, from the carry engine), or smoke-test from CLI:
  python3 carry_exec.py --sym TONUSDT --side BUY --usdt 100            # dry-run (default)
  python3 carry_exec.py --sym TONUSDT --side BUY --usdt 100 --send      # send to testnet
"""
import argparse, time, logging
import live_execution as lx

log = logging.getLogger("carry_exec")
if not log.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Binance "post-only would immediately match" rejection codes -> means touch moved, re-quote.
_POST_ONLY_REJECT = (-5022, -2010, -1111)


def _is_prod() -> bool:
    return "demo" not in lx._base_url() and "testnet" not in lx._base_url()


def _guard(dry_run: bool, allow_prod: bool):
    if _is_prod() and not (allow_prod and not dry_run):
        raise RuntimeError(
            f"carry_exec refuses PROD ({lx._base_url()}) unless allow_prod=True and dry_run=False. "
            f"Run on testnet (LIVE_MODE=false) first.")


def maker_fill(sym: str, side: str, qty: float, *, reduce_only: bool = False,
               max_wait_s: float = 180.0, requote_after_s: float = 8.0, poll_s: float = 1.0,
               dry_run: bool = True, allow_prod: bool = False) -> dict:
    """Fill `qty` of `sym` on `side` ('BUY'/'SELL') using ONLY post-only orders.
    Rests at the touch, re-quotes when the book moves away or after requote_after_s,
    accumulates partial fills, never crosses. Returns a fill summary."""
    _guard(dry_run, allow_prod)
    side = side.upper()
    assert side in ("BUY", "SELL")
    remaining = lx._round_qty(sym, qty)
    filled = 0.0
    notional = 0.0
    fills = []
    t0 = time.time()
    requotes = 0

    while remaining > 0 and (time.time() - t0) < max_wait_s:
        book = lx._book_ticker(sym)
        if not book:
            time.sleep(poll_s); continue
        px = lx._round_price(sym, book["bid"] if side == "BUY" else book["ask"])  # touch = maker
        q = lx._round_qty(sym, remaining)

        if dry_run:
            log.info(f"[DRY] would place GTX {side} {q} {sym} @ {px} "
                     f"(reduceOnly={reduce_only}) — no order sent")
            return {"sym": sym, "side": side, "requested": qty, "filled": 0.0,
                    "avg_price": None, "fills": [], "remaining": remaining,
                    "status": "DRY_RUN", "requotes": 0}

        params = {"symbol": sym, "side": side, "type": "LIMIT", "timeInForce": "GTX",
                  "price": px, "quantity": q}
        if reduce_only:
            params["reduceOnly"] = "true"
        resp = lx._post("/fapi/v1/order", params)

        if resp.get("code") in _POST_ONLY_REJECT or "orderId" not in resp:
            if resp.get("code") in _POST_ONLY_REJECT:
                requotes += 1
                log.info(f"[REQUOTE] {sym} post-only would cross at {px}; book moved, re-quoting")
                time.sleep(0.4); continue
            log.error(f"[ERR] {sym} order rejected: {resp}")
            break

        oid = resp["orderId"]
        prev_exec = 0.0
        waited = 0.0
        while waited < requote_after_s and (time.time() - t0) < max_wait_s:
            st = lx._get("/fapi/v1/order", {"symbol": sym, "orderId": oid})
            ex = float(st.get("executedQty", 0) or 0)
            if ex > prev_exec:
                avg = float(st.get("avgPrice", px) or px)
                inc = ex - prev_exec
                fills.append((avg, inc)); filled += inc; notional += inc * avg; prev_exec = ex
            if st.get("status") == "FILLED":
                remaining = lx._round_qty(sym, max(0.0, qty - filled))
                break
            book = lx._book_ticker(sym)
            if book:
                touch = book["bid"] if side == "BUY" else book["ask"]
                moved = (side == "BUY" and touch > px) or (side == "SELL" and touch < px)
                if moved:
                    break  # book left our resting order behind -> cancel & re-quote
            time.sleep(poll_s); waited += poll_s

        # cancel whatever's left of this order, then loop re-quotes the remainder
        cx = lx._delete("/fapi/v1/order", {"symbol": sym, "orderId": oid})
        ex = float(cx.get("executedQty", prev_exec) or prev_exec)
        if ex > prev_exec:
            inc = ex - prev_exec
            fills.append((px, inc)); filled += inc; notional += inc * px
        remaining = lx._round_qty(sym, max(0.0, qty - filled))
        requotes += 1
        if requotes > 200:
            log.warning(f"[STOP] {sym} hit requote cap with {remaining} unfilled"); break

    avg_price = (notional / filled) if filled > 0 else None
    status = "FILLED" if remaining <= 0 else ("PARTIAL" if filled > 0 else "UNFILLED")
    log.info(f"[DONE] {sym} {side} filled {filled}/{qty} @ {avg_price} "
             f"({status}, {requotes} requotes, {time.time()-t0:.0f}s)")
    return {"sym": sym, "side": side, "requested": qty, "filled": filled,
            "avg_price": avg_price, "fills": fills, "remaining": remaining,
            "status": status, "requotes": requotes}


def maker_open(sym: str, side: str, usdt: float, **kw) -> dict:
    """Open a perp position of ~`usdt` notional, maker-only. side = BUY(long)/SELL(short)."""
    book = lx._book_ticker(sym)
    if not book:
        return {"status": "NO_BOOK", "sym": sym}
    mid = (book["bid"] + book["ask"]) / 2
    qty = lx._round_qty(sym, usdt / mid)
    return maker_fill(sym, side, qty, reduce_only=False, **kw)


def maker_close(sym: str, qty: float, position_side: str, **kw) -> dict:
    """Close a perp position maker-only (reduceOnly). position_side = the side you HOLD
    ('LONG'/'SHORT'); we send the opposite side."""
    side = "SELL" if position_side.upper() == "LONG" else "BUY"
    return maker_fill(sym, side, qty, reduce_only=True, **kw)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sym", required=True)
    ap.add_argument("--side", required=True, choices=["BUY", "SELL"])
    ap.add_argument("--usdt", type=float, default=100.0)
    ap.add_argument("--send", action="store_true", help="actually send (default is dry-run)")
    ap.add_argument("--allow-prod", action="store_true", help="DANGER: permit fapi.binance.com")
    ap.add_argument("--max-wait", type=float, default=180.0)
    args = ap.parse_args()
    res = maker_open(args.sym, args.side, args.usdt,
                     dry_run=not args.send, allow_prod=args.allow_prod, max_wait_s=args.max_wait)
    print(res)


if __name__ == "__main__":
    main()
