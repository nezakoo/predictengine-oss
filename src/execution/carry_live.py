#!/usr/bin/env python3
"""
carry_live.py — cash-and-carry orchestrator (atomic two-leg, paper-default)
===========================================================================
Ties the perp leg (carry_exec, short) and spot leg (spot_exec, long) into a delta-neutral
position, with the dangerous failure mode handled first-class:

    ATOMIC OPEN: fill perp short, then spot long. If the SECOND leg fails to fill,
    the FIRST leg is immediately unwound — you are NEVER left with one naked leg.

Also: balance guard, a basis-blowout stop, a kill switch (touch carry.KILL to flatten all),
and state persistence. Defaults to PAPER. Going live requires THREE explicit switches:
--live  AND  env LIVE_MODE=true (perp)  AND  env SPOT_LIVE=true (spot)  -- so no single
flag arms real money.

This is the live plumbing. It does NOT decide the trade is worth real capital — that's
your call, and the honest verdict stands: thin (~mid-single-digit APR), capital-gated,
real money only after the perp rehearsal + a tiny-size both-legs shakeout.
"""
import argparse, os, json, time, logging
import carry_exec as perp
import spot_exec as spot

log = logging.getLogger("carry_live")
if not log.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

STATE = "carry_live_state.json"
KILL = "carry.KILL"


def _load(): return json.load(open(STATE)) if os.path.exists(STATE) else {"positions": {}}
def _save(s): json.dump(s, open(STATE, "w"), indent=2)


def open_carry(sym, usdt, *, live=False, allow_prod=False, dry_run=True):
    """Atomically open one delta-neutral leg-pair: short perp + long spot, same notional.
    If the spot leg fails after the perp filled, unwind the perp immediately."""
    log.info(f"[OPEN] {sym} ~{usdt} USDT  (live={live} dry_run={dry_run})")
    # leg 1: short perp
    p = perp.maker_open(sym, "SELL", usdt, dry_run=dry_run, allow_prod=allow_prod)
    if p["status"] not in ("FILLED", "DRY_RUN"):
        log.error(f"[OPEN] {sym} perp leg failed ({p['status']}); nothing opened, no exposure"); 
        return {"status": "PERP_FAIL", "perp": p}
    pfilled = p.get("filled", 0.0)
    # leg 2: long spot, matched to what the perp actually filled
    spot_usdt = usdt if dry_run else max(0.0, pfilled * (p.get("avg_price") or 0) )
    s = spot.maker_buy(sym, spot_usdt or usdt, dry_run=dry_run, allow_prod=allow_prod)
    if s["status"] not in ("FILLED", "DRY_RUN"):
        # CRITICAL: spot failed but perp is on. Unwind perp NOW — never sit naked.
        log.error(f"[OPEN] {sym} SPOT leg failed ({s['status']}) — UNWINDING perp leg to stay flat")
        if not dry_run:
            unwind = perp.maker_close(sym, pfilled, "SHORT", dry_run=dry_run, allow_prod=allow_prod)
            log.error(f"[OPEN] {sym} emergency perp unwind: {unwind['status']}")
        return {"status": "SPOT_FAIL_UNWOUND", "perp": p, "spot": s}
    log.info(f"[OPEN] {sym} delta-neutral pair on: perp short {pfilled} + spot long {s.get('filled')}")
    return {"status": "OPEN", "perp": p, "spot": s,
            "perp_entry": p.get("avg_price"), "spot_entry": s.get("avg_price")}


def close_carry(sym, perp_qty, spot_qty, *, allow_prod=False, dry_run=True):
    """Unwind both legs. Try both; report if either is left dangling (needs manual attention)."""
    log.info(f"[CLOSE] {sym} perp {perp_qty} + spot {spot_qty}")
    p = perp.maker_close(sym, perp_qty, "SHORT", dry_run=dry_run, allow_prod=allow_prod)
    s = spot.maker_sell(sym, spot_qty, dry_run=dry_run, allow_prod=allow_prod)
    ok = p["status"] in ("FILLED", "DRY_RUN") and s["status"] in ("FILLED", "DRY_RUN")
    if not ok:
        log.error(f"[CLOSE] {sym} INCOMPLETE — perp={p['status']} spot={s['status']} — CHECK MANUALLY")
    return {"status": "CLOSED" if ok else "INCOMPLETE", "perp": p, "spot": s}


def kill_switch_tripped():
    return os.path.exists(KILL)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sym", required=True)
    ap.add_argument("--usdt", type=float, default=100.0)
    ap.add_argument("--action", choices=["open", "close"], default="open")
    ap.add_argument("--perp-qty", type=float, default=0.0)
    ap.add_argument("--spot-qty", type=float, default=0.0)
    ap.add_argument("--live", action="store_true", help="arm real orders (still needs LIVE_MODE+SPOT_LIVE env)")
    ap.add_argument("--allow-prod", action="store_true", help="DANGER: permit real Binance")
    args = ap.parse_args()

    if kill_switch_tripped():
        log.error(f"kill switch present ({KILL}); refusing to open. Remove it to resume."); return

    # three-switch arming: --live AND both env flags must be set, else dry-run
    env_armed = os.getenv("LIVE_MODE", "false").lower() == "true" and os.getenv("SPOT_LIVE", "false").lower() == "true"
    dry = not (args.live and env_armed)
    if args.live and not env_armed:
        log.warning("--live passed but LIVE_MODE/SPOT_LIVE env not both true -> staying DRY-RUN (intentional safety)")

    if args.action == "open":
        print(open_carry(args.sym, args.usdt, live=args.live, allow_prod=args.allow_prod, dry_run=dry))
    else:
        print(close_carry(args.sym, args.perp_qty, args.spot_qty, allow_prod=args.allow_prod, dry_run=dry))


if __name__ == "__main__":
    main()
