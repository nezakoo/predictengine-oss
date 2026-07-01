#!/usr/bin/env python3
"""
PredictEngine - dashboard_multi.py v3
Rewritten 2026-06-04: clean single-file dashboard.
Backend: FastAPI + WebSocket broadcaster (unchanged structure).
Frontend: full rewrite — live execution panel, Binance status, clean layout.
"""

import asyncio, json, time, sys, os
from datetime import datetime, timezone

try:
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    from fastapi.responses import HTMLResponse
    import uvicorn
except ImportError:
    print("❌  pip install fastapi uvicorn"); sys.exit(1)

try:
    import engine as E
    import strategies as S
    import strategies_runtime as _SR
    ENGINE_OK = True
except ImportError:
    ENGINE_OK = False
    _SR = None

try:
    import live_execution as _live
    LIVE_OK = True
except ImportError:
    LIVE_OK = False

PORT   = int(os.environ.get('DASHBOARD_PORT', 8080))
PREFIX = os.environ.get('TELEGRAM_PREFIX', '')   # e.g. "[STAGE]"
IS_STAGE = bool(PREFIX) or not os.environ.get('LIVE_ENABLED', 'false').lower() in ('true', '1')
app  = FastAPI(title="PredictEngine")

_client_locks: dict = {}
clients: list[WebSocket] = []


def build_payload():
    if not ENGINE_OK:
        return {"error": "engine not loaded"}
    try:
        snaps = S.snapshots_all()
    except Exception as ex:
        return {"error": f"snapshots_all: {ex}",
                "ws_status": getattr(E, 'ws_status', '?'),
                "version": "?", "strategies": [], "strategies_a": [],
                "strategies_b": [], "coins": [], "coin_count": 0,
                "total_net": 0, "total_trades": 0, "total_wr": 0,
                "ab_mode": False, "live": None,
                "utc": datetime.now(timezone.utc).strftime('%H:%M:%S UTC'),
                "ts": int(time.time()*1000)}
    try:
        coins = [c.replace('USDT','') for c in E.ACTIVE_COINS]
    except Exception:
        coins = []

    ab_mode = isinstance(snaps, dict) and bool(snaps.get('b'))
    sa      = snaps['a'] if isinstance(snaps, dict) else snaps
    sb      = snaps.get('b', []) if isinstance(snaps, dict) else []
    sa_vis  = [s for s in sa if not s.get("disabled", False)]
    sb_vis  = [s for s in sb if not s.get("disabled", False)]

    total_net    = sum(s.get('net', 0) for s in sa)
    total_trades = sum(s.get('total', 0) for s in sa)
    total_wins   = sum(s.get('wins', 0) for s in sa)

    # Live execution status
    live_status = None
    if LIVE_OK:
        try:
            live_status = {
                "enabled":       _live.LIVE_ENABLED,
                "mode":          "LIVE" if _live.LIVE_MODE else "DEMO",
                "is_demo":       not _live.LIVE_MODE,
                "order_usdt":    _live.LIVE_ORDER_USDT,
                "max_positions": _live.LIVE_MAX_POSITIONS,
                "n_open":        _live._cache_n_positions,
                "balance":       _live._cache_balance,
                "unrealized":    round(_live._cache_unrealized, 4),
                "cache_age_sec": round(time.time() - _live._cache_ts, 0) if _live._cache_ts else None,
                "locked_syms":   sorted(_live._live_symbol_open),  # syms with active live position
                "shared_syms":   {                                  # v18: multi-strategy shared positions
                    sym: list(state.get('holders', {}).keys())
                    for sym, state in getattr(_SR, '_global_positions', {}).items()
                    if len(state.get('holders', {})) > 1
                },
            }
        except Exception:
            live_status = {"enabled": False, "error": "live_execution load failed"}

    return {
        'ts':           int(time.time() * 1000),
        'utc':          datetime.now(timezone.utc).strftime('%H:%M:%S UTC'),
        'ws_status':    getattr(E, 'ws_status', '?'),
        'version':      E.VERSION['v'] if hasattr(E, 'VERSION') else '?',
        'is_stage':     IS_STAGE,
        'stage_label':  PREFIX or ('STAGE' if IS_STAGE else ''),
        'total_net':    round(total_net, 4),
        'total_trades': total_trades,
        'total_wr':     round(total_wins / total_trades * 100, 1) if total_trades else 0,
        'strategies':   sa_vis,
        'strategies_a': sa_vis,
        'strategies_b': sb_vis,
        'ab_mode':      ab_mode,
        'coins':        coins,
        'coin_count':   len(coins),
        'live':         live_status,
    }


async def broadcaster():
    while True:
        if clients:
            try:
                msg  = json.dumps(build_payload())
                dead = []
                for ws in list(clients):
                    lock = _client_locks.get(id(ws))
                    if lock is None: dead.append(ws); continue
                    try:
                        acquired = False
                        try:
                            await asyncio.wait_for(lock.acquire(), timeout=0.4)
                            acquired = True
                        except asyncio.TimeoutError:
                            continue
                        try:
                            await ws.send_text(msg)
                        except Exception:
                            dead.append(ws)
                        finally:
                            if acquired: lock.release()
                    except Exception:
                        dead.append(ws)
                for ws in dead: _remove_client(ws)
            except Exception:
                pass
        await asyncio.sleep(0.5)


def _remove_client(ws: WebSocket):
    if ws in clients: clients.remove(ws)
    _client_locks.pop(id(ws), None)


@app.on_event("startup")
async def startup():
    asyncio.create_task(broadcaster())

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    _client_locks[id(ws)] = asyncio.Lock()
    clients.append(ws)
    try:
        while True: await ws.receive_text()
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        _remove_client(ws)


HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<title>PredictEngine</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700&family=Inter:wght@300;400;500;600&display=swap" rel="stylesheet">
<style>
:root {
  --bg:     #080b14;
  --bg2:    #0d1120;
  --bg3:    #131826;
  --bg4:    #1a2035;
  --bdr:    #1e2640;
  --bdr2:   #2a3558;
  --fg:     #dde4ff;
  --dim:    #5a6a99;
  --dim2:   #8090bb;
  --green:  #4ade80;
  --red:    #f87171;
  --yellow: #fbbf24;
  --cyan:   #22d3ee;
  --blue:   #60a5fa;
  --purple: #a78bfa;
  --orange: #fb923c;
  --teal:   #2dd4bf;
  --mono:   'JetBrains Mono', monospace;
  --sans:   'Inter', sans-serif;
}
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html { height: -webkit-fill-available; }
body {
  background: var(--bg);
  color: var(--fg);
  font-family: var(--mono);
  font-size: 11px;
  line-height: 1.5;
  -webkit-font-smoothing: antialiased;
  min-height: 100vh;
  min-height: -webkit-fill-available;
  overflow-x: hidden;
  overflow-y: auto;
  -webkit-overflow-scrolling: touch;
}

/* ── GRID NOISE BACKGROUND ─────────────────────────── */
body::before {
  content: '';
  position: fixed; inset: 0;
  background-image:
    linear-gradient(rgba(255,255,255,.015) 1px, transparent 1px),
    linear-gradient(90deg, rgba(255,255,255,.015) 1px, transparent 1px);
  background-size: 40px 40px;
  pointer-events: none; z-index: 0;
}
body::after {
  content: '';
  position: fixed; inset: 0;
  background: radial-gradient(ellipse 80% 50% at 50% -20%, rgba(96,165,250,.06), transparent);
  pointer-events: none; z-index: 0;
}
#app { position: relative; z-index: 1; }

/* ── TOPBAR ─────────────────────────────────────────── */
#topbar {
  position: sticky; top: 0; z-index: 200;
  height: 46px;
  background: rgba(8,11,20,.92);
  backdrop-filter: blur(20px);
  -webkit-backdrop-filter: blur(20px);
  border-bottom: 1px solid var(--bdr);
  display: flex; align-items: center;
  padding: 0 16px;
  padding-left: max(16px, env(safe-area-inset-left));
  padding-right: max(16px, env(safe-area-inset-right));
  gap: 0;
}
.tb-logo {
  font-size: 12px; font-weight: 700;
  letter-spacing: .18em; color: var(--cyan);
  text-transform: uppercase; margin-right: 16px;
  white-space: nowrap;
}
.tb-logo span { color: var(--dim2); font-weight: 300; }
.tb-sep { width: 1px; height: 18px; background: var(--bdr2); margin: 0 10px; flex-shrink: 0; }
.tb-item { font-size: 10px; color: var(--dim2); display: flex; align-items: center; gap: 5px; white-space: nowrap; }
.tb-item b { color: var(--fg); font-weight: 500; }
#tb-net { font-size: 13px; font-weight: 600; }
#tb-ws { font-size: 13px; line-height: 1; }
.ws-live { color: #4ade80; }
.ws-off  { color: #f87171; }
#tb-live-badge {
  font-size: 10px; font-weight: 600; letter-spacing: .05em;
  text-transform: uppercase; display: none;
}
.live-demo { color: var(--yellow); }
.live-real { color: var(--green); }
#tb-utc { margin-left: auto; color: var(--dim); font-size: 10px; }
.tb-btn {
  background: none; border: 1px solid var(--bdr2); border-radius: 3px;
  color: var(--dim2); font-family: var(--mono); font-size: 9px;
  padding: 4px 9px; cursor: pointer; letter-spacing: .05em; margin-left: 8px;
  transition: all .15s; -webkit-tap-highlight-color: transparent;
  touch-action: manipulation; white-space: nowrap;
}
.tb-btn:hover { border-color: var(--cyan); color: var(--cyan); }
.tb-btn.danger:hover { border-color: var(--red); color: var(--red); }
@media(max-width:600px) {
  .hide-sm { display: none !important; }
  #tb-utc { display: none; }
  .tb-sep { display: none; }
  .tb-item b { font-size: 12px; }
  #tb-net { font-size: 12px; }
  .tb-logo { font-size: 11px; letter-spacing: .1em; margin-right: 8px; }
  .tb-btn { font-size: 8px; padding: 3px 7px; margin-left: 4px; }
}

/* ── LIVE STRIP ─────────────────────────────────────── */
#live-strip {
  background: var(--bg2); border-bottom: 1px solid var(--bdr);
  height: 34px; min-height: 34px;
  display: flex; align-items: center; gap: 8px;
  padding: 0 16px;
  padding-left: max(16px, env(safe-area-inset-left));
  padding-right: max(16px, env(safe-area-inset-right));
  overflow: hidden;
}
.ls-label { color: var(--dim); font-size: 9px; text-transform: uppercase; letter-spacing: .12em; flex-shrink: 0; }
#live-pills {
  display: flex; gap: 4px; flex-wrap: nowrap;
  overflow-x: auto; overflow-y: hidden; flex: 1; min-width: 0;
  -webkit-overflow-scrolling: touch; scrollbar-width: none;
}
#live-pills::-webkit-scrollbar { display: none; }
.lpill {
  display: inline-flex; align-items: center; gap: 4px;
  padding: 2px 8px 2px 5px; border-radius: 3px;
  border: 1px solid var(--bdr2); background: var(--bg3);
  font-size: 10px; cursor: pointer; white-space: nowrap;
  transition: background .1s;
}
@media(max-width:600px) {
  .lpill { font-size: 9px; padding: 2px 5px 2px 4px; gap: 3px; }
  .lpill-age { display: none; }
}
.lpill:hover { background: var(--bg4); }
.lpill-lbl { font-size: 9px; font-weight: 700; }
.lpill-dp  { font-weight: 600; }
.lpill-age { color: var(--dim); font-size: 9px; }
#live-empty { color: var(--dim); font-size: 10px; }
#live-summary { margin-left: auto; font-size: 10px; color: var(--dim2); white-space: nowrap; }

/* ── STATS BAR ──────────────────────────────────────── */
#stats-bar {
  background: var(--bg2); border-bottom: 1px solid var(--bdr);
  display: flex; align-items: stretch;
  padding-left: max(0px, env(safe-area-inset-left));
  padding-right: max(0px, env(safe-area-inset-right));
  overflow-x: auto; scrollbar-width: none;
}
#stats-bar::-webkit-scrollbar { display: none; }
.sb-stat {
  display: flex; flex-direction: column; justify-content: center;
  padding: 8px 18px; border-right: 1px solid var(--bdr); flex-shrink: 0;
}
.sb-stat:last-child { border-right: none; }
.sb-lbl { font-size: 8px; text-transform: uppercase; letter-spacing: .1em; color: var(--dim); margin-bottom: 2px; }
.sb-val { font-size: 16px; font-weight: 600; }

/* ── LIVE EXECUTION BAR ─────────────────────────────── */
#exec-bar {
  display: none;
  background: rgba(251,191,36,.04);
  border-bottom: 1px solid rgba(251,191,36,.15);
  padding: 6px 16px;
  padding-left: max(16px, env(safe-area-inset-left));
  align-items: center; gap: 12px; flex-wrap: wrap; font-size: 10px;
}
.eb-label { color: var(--yellow); font-weight: 600; letter-spacing: .1em; text-transform: uppercase; font-size: 9px; }
.eb-item { color: var(--dim2); display: flex; align-items: center; gap: 4px; }
.eb-item b { color: var(--fg); }
.eb-sep { width: 1px; height: 14px; background: rgba(251,191,36,.2); }
#eb-balance { font-size: 12px; font-weight: 600; color: var(--green); }
#eb-positions { }

/* ── COIN DRAWER ────────────────────────────────────── */
#coin-drawer {
  display: none; background: var(--bg2); border-bottom: 1px solid var(--bdr);
  padding: 8px 16px; gap: 4px; flex-wrap: wrap;
}
.coin-tag {
  padding: 1px 7px; border-radius: 2px;
  background: var(--bg3); border: 1px solid var(--bdr);
  color: var(--dim2); font-size: 9px;
}

/* ── CONTENT ────────────────────────────────────────── */
#content {
  padding: 12px;
  padding-bottom: max(20px, env(safe-area-inset-bottom));
  padding-left: max(12px, env(safe-area-inset-left));
  padding-right: max(12px, env(safe-area-inset-right));
}

/* ── SECTION HEADER ─────────────────────────────────── */
.sec-hdr {
  font-size: 9px; letter-spacing: .14em; text-transform: uppercase;
  color: var(--dim2); margin-bottom: 10px; margin-top: 18px;
  display: flex; align-items: center; gap: 8px;
}
.sec-hdr::after { content: ''; flex: 1; height: 1px; background: var(--bdr); }
.sec-hdr:first-child { margin-top: 0; }

/* ── STRATEGY GRID ──────────────────────────────────── */
#strat-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 10px;
}
@media(max-width:720px) { #strat-grid { grid-template-columns: 1fr; } }

/* ── STRATEGY CARD ──────────────────────────────────── */
.scard {
  background: var(--bg2);
  border: 1px solid var(--bdr);
  border-radius: 8px; overflow: hidden;
  transition: border-color .2s;
}
.scard:hover { border-color: var(--bdr2); }

.scard-hdr {
  display: flex; align-items: flex-start; gap: 10px;
  padding: 10px 12px; border-bottom: 1px solid var(--bdr);
  background: var(--bg3);
}
.scard-badge {
  width: 30px; height: 30px; border-radius: 5px;
  display: flex; align-items: center; justify-content: center;
  font-weight: 800; font-size: 12px; color: #080b14; flex-shrink: 0;
}
.scard-name { font-size: 12px; font-weight: 600; color: var(--fg); letter-spacing: .02em; }
.scard-desc { font-size: 9px; color: var(--dim2); margin-top: 3px; line-height: 1.6; }
.scard-net { text-align: right; flex-shrink: 0; }
.scard-net .val { font-size: 16px; font-weight: 700; }
.scard-net .lbl { font-size: 8px; color: var(--dim); text-transform: uppercase; letter-spacing: .08em; }

.live-indicator {
  display: inline-flex; align-items: center; gap: 4px;
  font-size: 9px; color: var(--green);
}
.exec-badge {
  display: inline-flex; align-items: center;
  padding: 1px 6px; border-radius: 3px;
  font-size: 9px; font-weight: 700; letter-spacing: .08em;
  text-transform: uppercase;
}
.exec-demo { background: rgba(251,191,36,.15); color: var(--yellow); border: 1px solid rgba(251,191,36,.3); }
.exec-live { background: rgba(74,222,128,.12); color: var(--green); border: 1px solid rgba(74,222,128,.25); }
.exec-sim  { background: rgba(251,191,36,.08);  color: var(--yellow); border: 1px solid rgba(251,191,36,.2); }
.live-dot {
  width: 5px; height: 5px; border-radius: 50%;
  background: var(--green); animation: blink 1.2s ease-in-out infinite;
}
@keyframes blink { 0%,100%{opacity:1} 50%{opacity:.2} }
.streak-warn { color: var(--red); font-size: 9px; margin-left: 4px; }

/* ── STAT ROWS ──────────────────────────────────────── */
.stat-row {
  display: grid; border-bottom: 1px solid var(--bdr);
}
.r4 { grid-template-columns: repeat(4,1fr); }
.r6 { grid-template-columns: repeat(6,1fr); }
.r5 { grid-template-columns: repeat(5,1fr); }
@media(max-width:479px) {
  .r4 { grid-template-columns: repeat(2,1fr); }
  .r6 { grid-template-columns: repeat(3,1fr); }
  .r5 { grid-template-columns: repeat(3,1fr); }
}
.stat {
  padding: 6px 10px; border-right: 1px solid var(--bdr);
}
.stat:last-child { border-right: none; }
.stat-lbl { font-size: 8px; text-transform: uppercase; letter-spacing: .08em; color: var(--dim2); margin-bottom: 2px; }
.stat-val { font-size: 14px; font-weight: 500; }
@media(max-width:600px) { .stat-val { font-size: 13px; } }
.stat-val.sm { font-size: 11px; }
.stat-val.xs { font-size: 10px; }

/* ── DIR ROW ────────────────────────────────────────── */
.dir-row {
  display: flex; gap: 5px; padding: 5px 10px;
  border-bottom: 1px solid var(--bdr); flex-wrap: wrap;
}
.dir-pill {
  display: inline-flex; align-items: center; gap: 4px;
  padding: 2px 8px; border-radius: 3px;
  background: var(--bg3); border: 1px solid var(--bdr); font-size: 10px;
}

/* ── EXIT ROW ───────────────────────────────────────── */
.exit-row {
  display: flex; gap: 4px; padding: 4px 10px;
  border-bottom: 1px solid var(--bdr); flex-wrap: wrap; min-height: 28px; align-items: center;
}
.exit-tag {
  padding: 1px 7px; border-radius: 3px; font-size: 9px; letter-spacing: .03em;
}
.et-trail, .et-tp { color: var(--green); background: rgba(74,222,128,.08); }
.et-sl            { color: var(--red);   background: rgba(248,113,113,.08); }
.et-time          { color: var(--dim2);  background: rgba(90,106,153,.1); }
.et-inertia       { color: var(--blue);  background: rgba(96,165,250,.08); }

/* ── PNL CHART ──────────────────────────────────────── */
.chart-wrap { padding: 4px 6px; border-bottom: 1px solid var(--bdr); }
canvas { display: block; width: 100%; height: 52px; }

/* ── BNB PNL ROW ─────────────────────────────────────── */
.bnb-row {
  display: flex; align-items: center;
  padding: 6px 10px; border-bottom: 1px solid var(--bdr);
  background: rgba(96,165,250,.03); flex-wrap: wrap; gap: 0;
}
.bnb-cell { display: flex; flex-direction: column; padding: 2px 10px; min-width: 80px; }
.bnb-cell:first-child { padding-left: 2px; }
.bnb-sep { color: var(--dim); font-size: 11px; padding: 0 4px; align-self: center; }

/* ── TRADE TABLE ────────────────────────────────────── */
.tbl-wrap {
  overflow-x: auto; overflow-y: auto; max-height: 200px;
  -webkit-overflow-scrolling: touch; transform: translateZ(0);
}
table { width: 100%; border-collapse: collapse; min-width: 480px; }
th {
  background: var(--bg3); color: var(--dim2);
  padding: 4px 8px; text-align: right;
  font-size: 8px; font-weight: 600; letter-spacing: .1em; text-transform: uppercase;
  position: sticky; top: 0; z-index: 1; border-bottom: 1px solid var(--bdr);
}
th:first-child { text-align: left; }
td {
  padding: 4px 8px; text-align: right;
  border-bottom: 1px solid rgba(30,38,64,.5);
  font-size: 10px; white-space: nowrap;
}
td:first-child { text-align: left; }
tr:hover td { background: rgba(26,32,53,.6); }
.hm { }
@media(max-width:479px) { .hm { display: none; } }
/* Mobile trade table — tighter */
@media(max-width:600px) {
  .tbl-wrap { max-height: 160px; }
  table { min-width: 320px; font-size: 9px; }
  td, th { padding: 3px 5px; }
  .scard-net .val { font-size: 13px; }
  .sb-val { font-size: 13px; }
  /* bnb-row wraps to 2-col on mobile */
  .bnb-row { gap: 4px; }
  .bnb-cell { min-width: 70px; padding: 2px 6px; }
  /* exec bar wraps gracefully */
  #exec-bar { gap: 6px; font-size: 9px; }
  .eb-sep { display: none; }
  /* stats bar scrolls */
  .sb-stat { padding: 6px 12px; }
  .sb-lbl { font-size: 7px; }
}

/* ── A/B MODE ───────────────────────────────────────── */
.ab-pair {
  display: grid; grid-template-columns: 1fr 1fr; gap: 8px;
  grid-column: 1 / -1; position: relative;
}
@media(max-width:599px) { .ab-pair { grid-template-columns: 1fr; } }
.ab-delta {
  position: absolute; top: -10px; left: 50%; transform: translateX(-50%);
  background: var(--bg3); border: 1px solid var(--bdr2); border-radius: 3px;
  padding: 1px 8px; font-size: 9px; white-space: nowrap; z-index: 2;
}

/* ── COLORS ─────────────────────────────────────────── */
.g  { color: var(--green); }
.r  { color: var(--red); }
.y  { color: var(--yellow); }
.c  { color: var(--cyan); }
.b  { color: var(--blue); }
.m  { color: var(--purple); }
.p  { color: var(--orange); }
.t  { color: var(--teal); }
.d  { color: var(--dim2); }
.dd { color: var(--dim); }

/* ── SCROLLBAR ──────────────────────────────────────── */
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: var(--bg2); }
::-webkit-scrollbar-thumb { background: var(--bdr2); border-radius: 2px; }
</style>
</head>
<body>
<div id="app">

<!-- TOPBAR -->
<div id="topbar">
  <div class="tb-logo">Predict<span>Engine</span></div>
  <div class="tb-sep"></div>
  <div class="tb-item"><span class="dd">v</span><b id="tb-ver">?</b></div>
  <div class="tb-sep"></div>
  <div id="tb-ws" class="ws-off" title="connecting">●</div>
  <div id="tb-live-badge">DEMO</div>
  <div class="tb-sep"></div>
  <div class="tb-item">NET&nbsp;<b id="tb-net" class="dd">—</b></div>
  <div class="tb-sep hide-sm"></div>
  <div class="tb-item hide-sm"><b id="tb-trades">0</b>T · <b id="tb-wr" class="dd">—</b>%WR</div>
  <span id="tb-utc"></span>
  <button class="tb-btn" onclick="toggleCoins()" id="tb-coins-btn">0 coins ▾</button>
  <button class="tb-btn danger" onclick="resetPnL()">↺ reset</button>
</div>

<!-- LIVE STRIP -->
<div id="live-strip">
  <span class="ls-label">open</span>
  <div id="live-pills"></div>
  <span id="live-empty" style="display:none">no open trades</span>
  <span id="live-summary"></span>
</div>

<!-- EXECUTION BAR (shown when live enabled) -->
<div id="exec-bar">
  <span class="eb-label">⚡ live exec</span>
  <div class="eb-sep"></div>
  <div class="eb-item">balance&nbsp;<b id="eb-balance">—</b></div>
  <div class="eb-sep"></div>
  <div class="eb-item">unPnL&nbsp;<b id="eb-unrealized">—</b></div>
  <div class="eb-sep"></div>
  <div class="eb-item">open&nbsp;<b id="eb-positions">—</b>/<b id="eb-max">—</b></div>
  <div class="eb-sep"></div>
  <div class="eb-item">order size&nbsp;<b id="eb-size">—</b> USDT</div>
  <div class="eb-sep"></div>
  <div class="eb-item" id="eb-locked" style="display:none">locked:&nbsp;<b id="eb-locked-syms" style="color:var(--yellow)">—</b></div>
  <div class="eb-sep"></div>
  <div class="eb-item" id="eb-shared" style="display:none">shared:&nbsp;<b id="eb-shared-syms" style="color:var(--cyan)">—</b></div>
  <div class="eb-sep"></div>
  <div class="eb-item dd" id="eb-cache">cache —</div>
</div>

<!-- STATS BAR -->
<div id="stats-bar">
  <div class="sb-stat">
    <div class="sb-lbl">session net</div>
    <div class="sb-val dd" id="ssb-net">—</div>
  </div>
  <div class="sb-stat">
    <div class="sb-lbl">trades</div>
    <div class="sb-val" id="ssb-trades">0</div>
  </div>
  <div class="sb-stat">
    <div class="sb-lbl">win rate</div>
    <div class="sb-val dd" id="ssb-wr">—</div>
  </div>
  <div class="sb-stat">
    <div class="sb-lbl">expect / trade</div>
    <div class="sb-val dd" id="ssb-exp">—</div>
  </div>
  <div class="sb-stat">
    <div class="sb-lbl">strategies</div>
    <div class="sb-val dd" id="ssb-strats">—</div>
  </div>
  <div class="sb-stat">
    <div class="sb-lbl">coins</div>
    <div class="sb-val dd" id="ssb-coins">—</div>
  </div>
</div>

<!-- COIN DRAWER -->
<div id="coin-drawer"></div>

<!-- CONTENT -->
<div id="content">
  <div class="sec-hdr">strategies</div>
  <div id="strat-grid"></div>
</div>

</div><!-- #app -->

<script>
// ── UTILS ────────────────────────────────────────────────────────
const $ = id => document.getElementById(id)
const fmt   = (v, d=3) => v == null ? '—' : (v >= 0 ? '+' : '') + v.toFixed(d)
const fmtP  = p => p == null ? '—' : (Math.abs(p) >= 1 ? p.toFixed(4) : p.toFixed(8))
const cPN   = v => v == null ? 'dd' : v > 0 ? 'g' : v < 0 ? 'r' : 'dd'
const cWR   = v => v >= 50 ? 'g' : v >= 35 ? 'y' : 'r'
const dirC  = d => d === 'long' ? 'g' : 'r'
const arr   = d => d === 'long' ? '▲' : '▼'
const rc    = r => ({'tp':'et-tp','trail':'et-trail','sl':'et-sl','rev':'et-trail',
                     'inertia':'et-inertia','time':'et-time'}[r] || '')

document.addEventListener('touchstart', function(){}, {passive: true})

// ── PNL CHART ────────────────────────────────────────────────────
function drawChart(canvas, history, color) {
  if (!history || !history.length) return
  // Redraw when length OR last cumulative value changes
  const lastCum = history.length ? String((history[0] || {}).cum || 0) : '0'
  const sig = String(history.length) + '_' + lastCum
  if (canvas.dataset.sig === sig && canvas.dataset.drawn) return
  canvas.dataset.sig = sig; canvas.dataset.drawn = '1'
  const dpr = window.devicePixelRatio || 1
  const W0 = canvas.offsetWidth || 340
  const H0 = 52
  // Always reset canvas dimensions to avoid stale size
  canvas.width = W0 * dpr; canvas.height = H0 * dpr
  canvas.style.width = W0 + 'px'; canvas.style.height = H0 + 'px'
  const ctx = canvas.getContext('2d')
  ctx.clearRect(0, 0, canvas.width, canvas.height)
  // Save/restore to prevent ctx.scale accumulation on repeated calls
  ctx.save()
  ctx.scale(dpr, dpr)
  const W = W0, H = H0
  const ordered = [...history].reverse()
  const vals = [0, ...ordered.map(d => d.cum)]
  const mn = Math.min(0, ...vals), mx = Math.max(0, ...vals)
  const rng = Math.max(0.001, mx - mn)
  const sy = v => H - 4 - ((v - mn) / rng * (H - 8))
  const sx = i => (i / Math.max(vals.length - 1, 1)) * W
  // Baseline
  const zy = sy(0)
  ctx.strokeStyle = 'rgba(90,106,153,.25)'; ctx.lineWidth = 1
  ctx.setLineDash([3, 4])
  ctx.beginPath(); ctx.moveTo(0, zy); ctx.lineTo(W, zy); ctx.stroke()
  ctx.setLineDash([])
  const last = vals[vals.length - 1] || 0
  const lineColor = last > 0.001 ? '#4ade80' : last < -0.001 ? '#f87171' : color
  // Fill
  ctx.beginPath()
  ctx.moveTo(sx(0), zy)
  vals.forEach((v, i) => ctx.lineTo(sx(i), sy(v)))
  ctx.lineTo(sx(vals.length - 1), zy)
  ctx.closePath()
  const grad = ctx.createLinearGradient(0, last >= 0 ? 0 : H, 0, last >= 0 ? H : 0)
  grad.addColorStop(0, lineColor + '28')
  grad.addColorStop(1, 'transparent')
  ctx.fillStyle = grad; ctx.fill()
  // Line
  ctx.strokeStyle = lineColor; ctx.lineWidth = 1.5
  ctx.beginPath()
  vals.forEach((v, i) => i === 0 ? ctx.moveTo(sx(i), sy(v)) : ctx.lineTo(sx(i), sy(v)))
  ctx.stroke()
  // Dot
  if (vals.length > 1) {
    ctx.beginPath()
    ctx.arc(sx(vals.length - 1), sy(last), 2.5, 0, Math.PI * 2)
    ctx.fillStyle = lineColor; ctx.fill()
  }
  ctx.restore()
}

// ── RENDER CARD ──────────────────────────────────────────────────
function renderCard(s, el) {
  const isNew = !el
  if (isNew) { el = document.createElement('div'); el.className = 'scard'; el.id = 'scard-' + s.label }

  const p     = s.params || {}
  const byR   = Object.entries(s.by_reason || {})
    .map(([k,v]) => `<span class="exit-tag ${rc(k)}">${k}&nbsp;${v.w||0}W/${v.l||0}L</span>`).join('')
  const byDir = s.by_dir || {}
  const dirHTML = ['long','short'].map(d => {
    const dv = byDir[d]; if (!dv) return ''
    return `<span class="dir-pill">
      <span class="${dirC(d)}">${arr(d)} ${d}</span>
      <span class="d">·</span>
      <span class="${cWR(dv.wr)}">${dv.wr}%</span>
      <span class="d">${dv.count}T</span>
      <span class="${cPN(dv.net)}">${fmt(dv.net,3)}%</span>
    </span>`
  }).join('')

  const liveCount = (s.preds || []).filter(p2 => !p2.out).length
  const streaks   = s.sym_streaks || {}
  const worst     = Object.entries(streaks).sort((a,b) => b[1]-a[1])[0]
  const streakH   = worst && worst[1] >= 2 ? `<span class="streak-warn">⚠ ${worst[0]} ×${worst[1]}</span>` : ''

  const paramsArr = [
    `vpin ${p.vpin_min||'?'}`,
    `conf≥${p.min_conf||'?'}`,
    `sc≥${p.min_score||'?'}`,
    `trail=${p.trail_dist||'?'}%`,
    p.spread_max_mult ? `sp×${p.spread_max_mult}` : '',
  ].filter(Boolean).join(' · ')

  const s30c  = s.avg_snap30 == null ? 'dd' : s.avg_snap30 > 0.05 ? 'g' : s.avg_snap30 < -0.05 ? 'r' : 'y'
  const s60c  = s.avg_snap60 == null ? 'dd' : s.avg_snap60 > 0.05 ? 'g' : s.avg_snap60 < -0.05 ? 'r' : 'y'
  const mfec  = s.avg_max_dp == null ? 'dd' : s.avg_max_dp > 0.15 ? 'g' : s.avg_max_dp > 0.05 ? 'y' : 'r'
  const gpc   = s.avg_tp_gap == null ? 'dd' : s.avg_tp_gap < 0.05 ? 'g' : s.avg_tp_gap < 0.15 ? 'y' : 'r'

  const predsH = (s.preds || []).slice(0, 10).map(p2 => {
    const ts2 = new Date(p2.ts).toTimeString().slice(0,8)
    const dc  = dirC(p2.dir)
    const liveTag = p2.live_ok ? '<span style="color:var(--green);font-size:8px;margin-left:2px" title="live order">●</span>' : '<span style="color:var(--dim);font-size:8px;margin-left:2px" title="sim only">○</span>'
    let liveH = '—', outH = '—', netH = '—'
    if (p2.out) {
      const oc = p2.out === 'win' ? 'g' : p2.out === 'lose' ? 'r' : 'dd'
      outH  = `<span class="${oc}">${p2.out}</span>`
      netH  = `<span class="${cPN(p2.net)}">${fmt(p2.net)}%</span>`
      liveH = `<span class="${oc}">${fmt(p2.pct)}%</span>`
    } else if (p2.live_dp != null) {
      const lc = p2.live_dp > 0.05 ? 'g' : p2.live_dp < -0.05 ? 'r' : 'dd'
      liveH = `<span class="${lc}">${fmt(p2.live_dp)}%</span><span class="dd"> ${p2.elapsed}s</span>`
    }
    const rc2 = p2.reason ? `<span class="${rc(p2.reason)||''}">${p2.reason}</span>` : '<span class="dd">…</span>'
    return `<tr>
      <td class="dd">${ts2}</td>
      <td class="${dc}" style="font-weight:700">${arr(p2.dir)} ${p2.sym}${liveTag}</td>
      <td class="hm dd">${p2.conf}%</td>
      <td class="hm y">${p2.dyn_tp.toFixed(2)}%</td>
      <td class="hm r">${p2.dyn_sl.toFixed(2)}%</td>
      <td class="hm dd">${fmtP(p2.entry)}</td>
      <td>${liveH}</td><td>${outH}</td><td>${netH}</td>
      <td>${rc2}</td>
      <td class="hm dd">${p2.dur != null ? p2.dur.toFixed(0)+'s' : p2.elapsed+'s'}</td>
    </tr>`
  }).join('')

  el.innerHTML = `
  <div class="scard-hdr" style="border-left:3px solid ${s.color}">
    <div class="scard-badge" style="background:${s.color}">${s.label}</div>
    <div style="flex:1;min-width:0">
      <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
        <span class="scard-name">${s.name}</span>
        <span class="exec-badge ${s.live_exec !== false ? (window._liveData && window._liveData.is_demo ? 'exec-demo' : 'exec-live') : 'exec-sim'}">${s.live_exec !== false ? (window._liveData && window._liveData.is_demo ? 'demo' : 'live') : 'sim'}</span>
        ${liveCount > 0 ? `<span class="live-indicator"><span class="live-dot"></span>${liveCount} open</span>` : ''}
        ${streakH}
      </div>
      <div class="scard-desc">${paramsArr}</div>
    </div>
    <div class="scard-net">
      <div class="val ${cPN(s.cum_net)}">${fmt(s.cum_net)}%</div>
      <div class="lbl">cum net</div>
    </div>
  </div>

  <div class="stat-row r4">
    <div class="stat"><div class="stat-lbl">win rate</div><div class="stat-val ${cWR(s.wr)}">${s.wr}%</div></div>
    <div class="stat"><div class="stat-lbl">trades</div><div class="stat-val">${s.total}</div></div>
    <div class="stat"><div class="stat-lbl">expect/T</div><div class="stat-val sm ${s.expect>=0?'g':'r'}">${fmt(s.expect,4)}%</div></div>
    <div class="stat"><div class="stat-lbl">net</div><div class="stat-val sm ${cPN(s.net)}">${fmt(s.net,3)}%</div></div>
  </div>

  <div class="stat-row r6">
    <div class="stat"><div class="stat-lbl">avg win</div><div class="stat-val xs g">${fmt(s.avg_win)}%</div></div>
    <div class="stat"><div class="stat-lbl">avg loss</div><div class="stat-val xs r">${fmt(s.avg_loss)}%</div></div>
    <div class="stat"><div class="stat-lbl">snap30</div><div class="stat-val xs ${s30c}">${s.avg_snap30 != null ? fmt(s.avg_snap30,3)+'%' : '—'}</div></div>
    <div class="stat"><div class="stat-lbl">snap60</div><div class="stat-val xs ${s60c}">${s.avg_snap60 != null ? fmt(s.avg_snap60,3)+'%' : '—'}</div></div>
    <div class="stat"><div class="stat-lbl">MFE avg</div><div class="stat-val xs ${mfec}">${s.avg_max_dp != null ? fmt(s.avg_max_dp,3)+'%' : '—'}</div></div>
    <div class="stat"><div class="stat-lbl">trail gap</div><div class="stat-val xs ${gpc}">${s.avg_tp_gap != null ? s.avg_tp_gap.toFixed(3)+'%' : '—'}</div></div>
  </div>

  <div class="stat-row r5">
    <div class="stat"><div class="stat-lbl">inertia</div><div class="stat-val xs ${(s.inertia_pct||0)>20?'r':'dd'}">${s.inertia_count||0} (${(s.inertia_pct||0).toFixed(0)}%)</div></div>
    <div class="stat"><div class="stat-lbl">BE lock</div><div class="stat-val xs ${(s.be_activated||0)>0?'g':'dd'}">${s.be_activated||0}</div></div>
    <div class="stat"><div class="stat-lbl">TP ext</div><div class="stat-val xs ${(s.tp_extended_count||0)>0?'g':'dd'}">${s.tp_extended_count||0}</div></div>
    <div class="stat"><div class="stat-lbl">TP hits</div><div class="stat-val xs ${(s.tp_hits||0)>0?'t':'dd'}">${s.tp_hits||0}</div></div>
    <div class="stat"><div class="stat-lbl">gross</div><div class="stat-val xs ${cPN(s.gross)}">${fmt(s.gross)}%</div></div>
  </div>

  ${dirHTML ? `<div class="dir-row">${dirHTML}</div>` : ''}
  <div class="exit-row">${byR || '<span class="dd" style="font-size:9px">no exits yet</span>'}</div>

  <div class="bnb-row">
    <div class="bnb-cell">
      <div class="stat-lbl">sim net</div>
      <div class="stat-val sm ${cPN(s.cum_net)}">${fmt(s.cum_net, 3)}%</div>
    </div>
    <div class="bnb-sep">→</div>
    <div class="bnb-cell">
      <div class="stat-lbl">binance PnL</div>
      <div class="stat-val sm ${s.bnb_cum_pnl != null && s.bnb_cum_pnl !== 0 ? cPN(s.bnb_cum_pnl) : 'dd'}">
        ${s.bnb_cum_pnl != null && s.bnb_cum_pnl !== 0 ? (s.bnb_cum_pnl >= 0 ? '+' : '') + s.bnb_cum_pnl.toFixed(4) + ' USDT' : '—'}
      </div>
    </div>
    <div class="bnb-sep">Δ</div>
    <div class="bnb-cell">
      <div class="stat-lbl">comm paid</div>
      <div class="stat-val sm ${s.bnb_cum_comm ? 'r' : 'dd'}">
        ${s.bnb_cum_comm ? '-' + Math.abs(s.bnb_cum_comm).toFixed(4) + ' USDT' : '—'}
      </div>
    </div>
    <div class="bnb-sep">→</div>
    <div class="bnb-cell">
      <div class="stat-lbl">net after fees</div>
      <div class="stat-val sm ${s.bnb_cum_pnl != null && s.bnb_cum_comm != null && (s.bnb_cum_pnl + s.bnb_cum_comm) !== 0 ? cPN(s.bnb_cum_pnl + s.bnb_cum_comm) : 'dd'}">
        ${s.bnb_cum_pnl != null && s.bnb_cum_comm != null && (s.bnb_cum_pnl !== 0 || s.bnb_cum_comm !== 0) ? ((s.bnb_cum_pnl + s.bnb_cum_comm) >= 0 ? '+' : '') + (s.bnb_cum_pnl + s.bnb_cum_comm).toFixed(4) + ' USDT' : '—'}
      </div>
    </div>
  </div>

  <div class="chart-wrap"><canvas id="chart-${s.label}"></canvas></div>
  <div class="tbl-wrap">
    <table>
      <thead><tr>
        <th style="text-align:left">TIME</th>
        <th style="text-align:left">SYM</th>
        <th class="hm">CONF</th>
        <th class="hm">TP</th>
        <th class="hm">SL</th>
        <th class="hm">ENTRY</th>
        <th>LIVE</th><th>OUT</th><th>NET</th><th>EXIT</th>
        <th class="hm">DUR</th>
      </tr></thead>
      <tbody>${predsH || '<tr><td colspan="11" class="dd" style="text-align:center;padding:14px;font-size:9px">no trades yet</td></tr>'}</tbody>
    </table>
  </div>`
  return el
}

// ── PATCH CARD (surgical update, no scroll jump) ─────────────────
function patchCard(el, s) {
  const ec   = s.expect >= 0 ? 'g' : 'r'
  const s30c = s.avg_snap30 == null ? 'dd' : s.avg_snap30 > 0.05 ? 'g' : s.avg_snap30 < -0.05 ? 'r' : 'y'
  const s60c = s.avg_snap60 == null ? 'dd' : s.avg_snap60 > 0.05 ? 'g' : s.avg_snap60 < -0.05 ? 'r' : 'y'
  const mfec = s.avg_max_dp == null ? 'dd' : s.avg_max_dp > 0.15 ? 'g' : s.avg_max_dp > 0.05 ? 'y' : 'r'
  const gpc  = s.avg_tp_gap == null ? 'dd' : s.avg_tp_gap < 0.05 ? 'g' : s.avg_tp_gap < 0.15 ? 'y' : 'r'

  // cum net header
  const cumEl = el.querySelector('.scard-net .val')
  if (cumEl) {
    const v = fmt(s.cum_net) + '%', c = 'val ' + cPN(s.cum_net)
    if (cumEl.textContent !== v) cumEl.textContent = v
    if (cumEl.className   !== c) cumEl.className   = c
  }

  // stat cells by label
  el.querySelectorAll('.stat').forEach(stat => {
    const lbl = stat.querySelector('.stat-lbl'), val = stat.querySelector('.stat-val')
    if (!lbl || !val) return
    const key = lbl.textContent.trim()
    let t = null, c = null
    if      (key === 'win rate')  { t = s.wr+'%';                       c = 'stat-val ' + cWR(s.wr) }
    else if (key === 'trades')    { t = String(s.total) }
    else if (key === 'expect/T')  { t = fmt(s.expect,4)+'%';            c = 'stat-val sm ' + ec }
    else if (key === 'net')       { t = fmt(s.net,3)+'%';               c = 'stat-val sm ' + cPN(s.net) }
    else if (key === 'avg win')   { t = fmt(s.avg_win)+'%' }
    else if (key === 'avg loss')  { t = fmt(s.avg_loss)+'%' }
    else if (key === 'snap30')    { t = s.avg_snap30 != null ? fmt(s.avg_snap30,3)+'%':'—'; c='stat-val xs '+s30c }
    else if (key === 'snap60')    { t = s.avg_snap60 != null ? fmt(s.avg_snap60,3)+'%':'—'; c='stat-val xs '+s60c }
    else if (key === 'MFE avg')   { t = s.avg_max_dp != null ? fmt(s.avg_max_dp,3)+'%':'—'; c='stat-val xs '+mfec }
    else if (key === 'trail gap') { t = s.avg_tp_gap != null ? s.avg_tp_gap.toFixed(3)+'%':'—'; c='stat-val xs '+gpc }
    else if (key === 'inertia')   { t = (s.inertia_count||0)+' ('+(s.inertia_pct||0).toFixed(0)+'%)'; c='stat-val xs '+((s.inertia_pct||0)>20?'r':'dd') }
    else if (key === 'BE lock')   { t = String(s.be_activated||0); c='stat-val xs '+((s.be_activated||0)>0?'g':'dd') }
    else if (key === 'TP ext')    { t = String(s.tp_extended_count||0); c='stat-val xs '+((s.tp_extended_count||0)>0?'g':'dd') }
    else if (key === 'TP hits')   { t = String(s.tp_hits||0); c='stat-val xs '+((s.tp_hits||0)>0?'t':'dd') }
    else if (key === 'gross')     { t = fmt(s.gross)+'%'; c='stat-val xs '+cPN(s.gross) }
    if (t !== null && val.textContent !== t) val.textContent = t
    if (c !== null && val.className   !== c) val.className   = c
  })

  // live dot
  const liveCount = (s.preds || []).filter(p => !p.out).length
  let liveSpan = el.querySelector('.live-indicator')
  if (liveCount > 0) {
    if (!liveSpan) { /* will be rebuilt on next full render */ }
    else { const n = liveSpan.childNodes[1]; if (n) n.textContent = liveCount + ' open' }
  }

  // exit row
  const byR = Object.entries(s.by_reason || {})
    .map(([k,v]) => `<span class="exit-tag ${rc(k)}">${k} ${v.w||0}W/${v.l||0}L</span>`).join('')
  const exitRow = el.querySelector('.exit-row')
  if (exitRow) {
    const nh = byR || '<span class="dd" style="font-size:9px">no exits yet</span>'
    if (exitRow.innerHTML !== nh) exitRow.innerHTML = nh
  }

  // dir row
  const byDir = s.by_dir || {}
  const dirHTML = ['long','short'].map(d => {
    const dv = byDir[d]; if (!dv) return ''
    return `<span class="dir-pill"><span class="${dirC(d)}">${arr(d)} ${d}</span><span class="d">·</span><span class="${cWR(dv.wr)}">${dv.wr}%</span><span class="d">${dv.count}T</span><span class="${cPN(dv.net)}">${fmt(dv.net,3)}%</span></span>`
  }).join('')
  const dirRow = el.querySelector('.dir-row')
  if (dirRow && dirHTML && dirRow.innerHTML !== dirHTML) dirRow.innerHTML = dirHTML

  // bnb pnl row
  const bnbRow = el.querySelector('.bnb-row')
  if (bnbRow && s.bnb_cum_pnl !== undefined) {
    const cells = bnbRow.querySelectorAll('.bnb-cell .stat-val')
    if (cells[0]) { cells[0].textContent = fmt(s.cum_net, 3) + '%'; cells[0].className = 'stat-val sm ' + cPN(s.cum_net) }
    if (cells[1] && s.bnb_cum_pnl != null) {
      const v = s.bnb_cum_pnl !== 0 ? (s.bnb_cum_pnl >= 0 ? '+' : '') + s.bnb_cum_pnl.toFixed(4) + ' USDT' : '—'
      cells[1].textContent = v; cells[1].className = 'stat-val sm ' + (s.bnb_cum_pnl !== 0 ? cPN(s.bnb_cum_pnl) : 'dd')
    }
    if (cells[2] && s.bnb_cum_comm != null) {
      cells[2].textContent = s.bnb_cum_comm ? '-' + Math.abs(s.bnb_cum_comm).toFixed(4) + ' USDT' : '—'
      cells[2].className = 'stat-val sm ' + (s.bnb_cum_comm ? 'r' : 'dd')
    }
    if (cells[3] && s.bnb_cum_pnl != null && s.bnb_cum_comm != null) {
      const net = s.bnb_cum_pnl + s.bnb_cum_comm
      cells[3].textContent = (net !== 0) ? (net >= 0 ? '+' : '') + net.toFixed(4) + ' USDT' : '—'
      cells[3].className = 'stat-val sm ' + (net !== 0 ? cPN(net) : 'dd')
    }
  }

  // trade table body
  const predsH = (s.preds || []).slice(0, 10).map(p2 => {
    const ts2 = new Date(p2.ts).toTimeString().slice(0,8)
    const dc  = dirC(p2.dir)
    let liveH = '—', outH = '—', netH = '—'
    if (p2.out) {
      const oc = p2.out === 'win' ? 'g' : p2.out === 'lose' ? 'r' : 'dd'
      outH = `<span class="${oc}">${p2.out}</span>`
      netH = `<span class="${cPN(p2.net)}">${fmt(p2.net)}%</span>`
      liveH= `<span class="${oc}">${fmt(p2.pct)}%</span>`
    } else if (p2.live_dp != null) {
      const lc = p2.live_dp > 0.05 ? 'g' : p2.live_dp < -0.05 ? 'r' : 'dd'
      liveH = `<span class="${lc}">${fmt(p2.live_dp)}%</span><span class="dd"> ${p2.elapsed}s</span>`
    }
    const rc2 = p2.reason ? `<span class="${rc(p2.reason)||''}">${p2.reason}</span>` : '<span class="dd">…</span>'
    const liveTag2 = p2.live_ok ? '<span style="color:var(--green);font-size:8px;margin-left:2px">●</span>' : '<span style="color:var(--dim);font-size:8px;margin-left:2px">○</span>'
    return `<tr><td class="dd">${ts2}</td><td class="${dc}" style="font-weight:700">${arr(p2.dir)} ${p2.sym}${liveTag2}</td><td class="hm dd">${p2.conf}%</td><td class="hm y">${p2.dyn_tp.toFixed(2)}%</td><td class="hm r">${p2.dyn_sl.toFixed(2)}%</td><td class="hm dd">${fmtP(p2.entry)}</td><td>${liveH}</td><td>${outH}</td><td>${netH}</td><td>${rc2}</td><td class="hm dd">${p2.dur != null ? p2.dur.toFixed(0)+'s' : p2.elapsed+'s'}</td></tr>`
  }).join('')
  const tbody = el.querySelector('tbody')
  if (tbody) {
    const fp = (s.preds||[]).slice(0,10).map(p2 => p2.ts+'|'+p2.out+'|'+(p2.net||0)+'|'+(p2.live_dp!=null?Math.round(p2.live_dp*100):'')).join(';')
    if (tbody.dataset.fp !== fp) {
      tbody.dataset.fp = fp
      tbody.innerHTML = predsH || '<tr><td colspan="11" class="dd" style="text-align:center;padding:14px;font-size:9px">no trades yet</td></tr>'
    } else {
      ;(s.preds||[]).slice(0,10).forEach((p2, i) => {
        if (p2.out) return
        const row = tbody.rows[i]; if (!row) return
        const cell = row.cells[6]
        if (cell && p2.live_dp != null) {
          const lc = p2.live_dp > 0.05 ? 'g' : p2.live_dp < -0.05 ? 'r' : 'dd'
          const nh = `<span class="${lc}">${fmt(p2.live_dp)}%</span><span class="dd"> ${p2.elapsed}s</span>`
          if (cell.innerHTML !== nh) cell.innerHTML = nh
        }
      })
    }
  }
}

// ── LIVE BAR ─────────────────────────────────────────────────────
function updateLiveBar(data) {
  const allS = [...(data.strategies_a||data.strategies||[]),...(data.strategies_b||[])]
  const seen = new Set(), open = []
  allS.forEach(s => {
    ;(s.preds||[]).forEach(p => {
      if (p.out) return
      const key = s.label+'|'+p.sym+'|'+p.dir+'|'+Math.round((p.ts||0)/10)
      if (seen.has(key)) return
      seen.add(key); open.push({s, p})
    })
  })

  // Summary
  let totR = 0, totW = 0
  allS.forEach(s => { totR += s.total||0; totW += s.wins||0 })
  const sumEl = $('live-summary')
  if (totR > 0) {
    const wr2 = Math.round(totW/totR*100)
    const wc = wr2>=50?'var(--green)':wr2>=35?'var(--yellow)':'var(--red)'
    const ns = `${totR}T · <span style="color:${wc}">${wr2}%WR</span>`
    if (sumEl.dataset.sum !== ns) { sumEl.dataset.sum = ns; sumEl.innerHTML = ns }
  }

  const pillsEl = $('live-pills')
  const emptyEl = $('live-empty')
  open.sort((a,b) => (a.p.elapsed||0) - (b.p.elapsed||0))
  const openKey = open.map(({s,p}) => s.label+'|'+p.sym+'|'+p.dir+'|'+(p.live_dp!=null?p.live_dp.toFixed(2):'')).join(';')
  if (!open.length) { emptyEl.style.display='inline'; pillsEl.innerHTML=''; return }
  emptyEl.style.display = 'none'
  if (pillsEl.dataset.openKey === openKey) {
    open.forEach(({s,p}, i) => {
      const pill = pillsEl.children[i]; if (!pill) return
      const ageEl = pill.querySelector('.lpill-age')
      if (ageEl) ageEl.textContent = (p.elapsed!=null?p.elapsed+'s':'?')
    })
    return
  }
  pillsEl.dataset.openKey = openKey
  pillsEl.innerHTML = ''
  open.forEach(({s,p}) => {
    const dp    = p.live_dp
    const dpTxt = dp!=null ? (dp>=0?'+':'')+dp.toFixed(3)+'%' : '…'
    const dpCol = dp==null?'var(--dim2)':dp>0.05?'var(--green)':dp<-0.05?'var(--red)':'var(--fg)'
    const pill  = document.createElement('span')
    pill.className = 'lpill'
    pill.style.borderColor = s.color + '55'
    pill.innerHTML =
      `<span class="lpill-lbl" style="color:${s.color}">${s.label}</span>` +
      `<span style="color:${p.dir==='long'?'var(--green)':'var(--red)'}">${p.dir==='long'?'▲':'▼'}</span>` +
      `<span>${p.sym}</span>` +
      `<span class="lpill-dp" style="color:${dpCol}">${dpTxt}</span>` +
      `<span class="lpill-age">${p.elapsed!=null?p.elapsed+'s':'?'}</span>`
    pill.onclick = () => {
      const card = document.getElementById('scard-'+s.label)
      if (!card) return
      card.scrollIntoView({behavior:'smooth', block:'start'})
      card.style.outline = `1px solid ${s.color}`
      setTimeout(() => card.style.outline = '', 1200)
    }
    pillsEl.appendChild(pill)
  })
}

// ── EXECUTION BAR ─────────────────────────────────────────────────
function updateExecBar(live) {
  const bar   = $('exec-bar')
  // Update title for stage
  if (live && live.stage_label) {
    document.title = `[${live.stage_label}] PredictEngine`
  }
  window._liveData = live || null
  const badge = $('tb-live-badge')
  if (!live || !live.enabled) {
    bar.style.display = 'none'
    badge.style.display = 'none'
    return
  }
  bar.style.display = 'flex'
  badge.style.display = 'inline-block'
  badge.textContent = ' ' + live.mode
  badge.className = live.mode === 'LIVE' ? 'live-real' : 'live-demo'

  const bal = live.balance != null ? '$'+parseFloat(live.balance).toFixed(2) : '—'
  $('eb-balance').textContent     = bal
  $('eb-balance').className       = parseFloat(live.balance||0) < (live.order_usdt||20)*2 ? 'r' : 'g'
  const unPnl = live.unrealized
  const unPnlEl = $('eb-unrealized')
  if (unPnl != null && unPnl !== 0) {
    unPnlEl.textContent = (unPnl >= 0 ? '+' : '') + unPnl.toFixed(2) + ' USDT'
    unPnlEl.className = unPnl >= 0 ? 'g' : 'r'
  } else {
    unPnlEl.textContent = '—'; unPnlEl.className = 'dd'
  }
  $('eb-positions').textContent   = live.n_open
  $('eb-max').textContent         = live.max_positions
  $('eb-size').textContent        = '$' + (live.order_usdt||'?')
  const age = live.cache_age_sec
  $('eb-cache').textContent       = age != null ? `cache ${age}s ago` : 'cache —'
  // Locked symbols
  const locked = live.locked_syms || []
  const lockedEl = $('eb-locked')
  if (locked.length > 0) {
    lockedEl.style.display = 'flex'
    $('eb-locked-syms').textContent = locked.map(s => s.replace('USDT','')).join(', ')
  } else {
    lockedEl.style.display = 'none'
  }
  // v18: shared positions (multiple strategies same direction)
  const shared = live.shared_syms || {}
  const sharedKeys = Object.keys(shared)
  const sharedEl = $('eb-shared')
  if (sharedKeys.length > 0) {
    sharedEl.style.display = 'flex'
    $('eb-shared-syms').textContent = sharedKeys.map(s =>
      s.replace('USDT','') + '(' + shared[s].join('+') + ')'
    ).join(', ')
  } else {
    sharedEl.style.display = 'none'
  }
}

// ── MAIN RENDER ──────────────────────────────────────────────────
function render(data) {
  if (data.error) {
    $('tb-ver').textContent = '⚠'
    $('ssb-net').textContent = data.error
    $('ssb-net').className = 'sb-val r'
    $('ssb-net').style.fontSize = '9px'
    return
  }

  // Topbar
  $('tb-ver').textContent   = data.version || '?'
  $('tb-utc').textContent   = data.utc || ''
  const wsEl = $('tb-ws')
  const live = data.ws_status && data.ws_status.includes('live')
  wsEl.textContent = '●'
  wsEl.title       = live ? 'connected' : (data.ws_status || 'disconnected')
  wsEl.className   = live ? 'ws-live' : 'ws-off'

  const allSA  = [...(data.strategies_a||data.strategies||[])]
  const cumNet = allSA.reduce((a,s) => a + (s.cum_net||0), 0)
  $('tb-net').textContent = fmt(cumNet, 3) + '%'
  $('tb-net').className   = cPN(cumNet)
  $('tb-trades').textContent = data.total_trades || 0
  $('tb-wr').textContent  = (data.total_wr || 0) + ''
  $('tb-wr').className    = cWR(data.total_wr || 0)

  // Stats bar
  $('ssb-net').textContent  = fmt(cumNet, 3) + '%'
  $('ssb-net').className    = 'sb-val ' + cPN(cumNet)
  $('ssb-trades').textContent = data.total_trades || 0
  $('ssb-wr').textContent   = (data.total_wr || 0) + '%'
  $('ssb-wr').className     = 'sb-val ' + cWR(data.total_wr || 0)
  const totR  = allSA.reduce((a,s) => a + (s.total||0), 0)
  const expT  = totR ? cumNet / totR : 0
  $('ssb-exp').textContent  = fmt(expT, 4) + '%'
  $('ssb-exp').className    = 'sb-val ' + cPN(expT)
  $('ssb-strats').textContent = allSA.length
  $('ssb-coins').textContent  = data.coin_count || 0

  // Coins button + drawer
  $('tb-coins-btn').textContent = `${data.coin_count||0} coins ▾`
  const drawer = $('coin-drawer')
  const coinsKey = (data.coins||[]).join(',')
  if (drawer.dataset.coins !== coinsKey) {
    drawer.dataset.coins = coinsKey
    drawer.innerHTML = (data.coins||[]).map(c => `<span class="coin-tag">${c}</span>`).join('')
  }

  // Execution bar
  updateExecBar(data.live)

  // Strategy grid
  const grid = $('strat-grid')
  if (data.ab_mode && (data.strategies_b||[]).length) {
    const mapA = {}, mapB = {}
    data.strategies_a.forEach(s => mapA[s.label] = s)
    data.strategies_b.forEach(s => mapB[s.label] = s)
    // Sort: live_exec=true pairs first
    const labels = [...new Set([...data.strategies_a.map(s=>s.label), ...data.strategies_b.map(s=>s.label)])]
      .sort((la, lb) => {
        const aLive = (mapA[la]||mapB[la]||{}).live_exec !== false ? 1 : 0
        const bLive = (mapA[lb]||mapB[lb]||{}).live_exec !== false ? 1 : 0
        return bLive - aLive
      })
    // Remove stale pairs
    grid.querySelectorAll('.ab-pair').forEach(p => {
      if (!labels.includes(p.id.replace('ab-pair-',''))) p.remove()
    })
    labels.forEach((label, idx) => {
      const sA = mapA[label], sB = mapB[label]
      const pairId = 'ab-pair-' + label
      let pair = document.getElementById(pairId)
      if (!pair) { pair = document.createElement('div'); pair.id = pairId; pair.className = 'ab-pair' }
      // Reorder without full wipe
      const current = grid.children[idx]
      if (current !== pair) grid.insertBefore(pair, current || null)
      // Patch/render A side
      let elA = pair.querySelector('[data-side="a"]')
      if (sA) {
        if (!elA) { elA = document.createElement('div'); elA.dataset.side='a'; elA.style.borderTop='2px solid var(--green)'; pair.insertBefore(elA, pair.firstChild) }
        const cardA = elA.querySelector('.scard')
        if (cardA) patchCard(cardA, sA); else { const c = renderCard(sA); elA.appendChild(c) }
        const ca = elA.querySelector('#chart-'+sA.label) || document.getElementById('chart-'+sA.label)
        if (ca) drawChart(ca, sA.pnl_history, sA.color)
      }
      // Patch/render B side
      let elB = pair.querySelector('[data-side="b"]')
      if (sB) {
        if (!elB) { elB = document.createElement('div'); elB.dataset.side='b'; elB.style.borderTop='2px solid var(--orange)'; pair.appendChild(elB) }
        const cardB = elB.querySelector('.scard')
        if (cardB) patchCard(cardB, sB); else { const c = renderCard(sB); elB.appendChild(c) }
        const cb = elB.querySelector('#chart-'+sB.label) || document.getElementById('chart-'+sB.label)
        if (cb) drawChart(cb, sB.pnl_history, sB.color)
      }
      // Delta label
      let dEl = pair.querySelector('.ab-delta')
      if (sA && sB) {
        const d2 = sB.expect - sA.expect
        if (!dEl) { dEl = document.createElement('div'); dEl.className = 'ab-delta'; pair.appendChild(dEl) }
        dEl.style.color = d2 > 0 ? 'var(--green)' : 'var(--red)'
        dEl.textContent = `${label}  ${d2>=0?'+':''}${d2.toFixed(4)}% ${d2>0?'▲':'▼'}`
      } else if (dEl) { dEl.remove() }
    })
  } else {
    const activeLabels = new Set((data.strategies||[]).map(s => s.label))
    grid.querySelectorAll('.scard').forEach(card => {
      const lbl = card.id.replace('scard-', '')
      if (!activeLabels.has(lbl)) card.remove()
    })
    ;[...(data.strategies||[])].filter(s => !s.disabled).sort((a,b) => (b.live_exec===false?0:1)-(a.live_exec===false?0:1)).forEach(s => {
      const existing = document.getElementById('scard-' + s.label)
      if (existing) {
        patchCard(existing, s)
        const canvas = document.getElementById('chart-' + s.label)
        if (canvas) drawChart(canvas, s.pnl_history, s.color)
      } else {
        const el = renderCard(s, null)
        grid.appendChild(el)
        const canvas = document.getElementById('chart-' + s.label)
        if (canvas) drawChart(canvas, s.pnl_history, s.color)
      }
    })
  }

  updateLiveBar(data)
}

// ── COIN DRAWER ───────────────────────────────────────────────────
function toggleCoins() {
  const d = $('coin-drawer')
  d.style.display = d.style.display === 'flex' ? 'none' : 'flex'
}

// ── RESET PNL ─────────────────────────────────────────────────────
async function resetPnL() {
  if (!confirm('Reset all strategy PnL stats?\n\nClears cum_net, trade history, charts. Cannot be undone.')) return
  const btn = document.querySelector('.tb-btn.danger')
  btn.textContent = '…'; btn.disabled = true
  try {
    const r = await fetch('/reset', {method:'POST'})
    const d = await r.json()
    if (d.ok) {
      btn.textContent = '✓'; btn.style.color = 'var(--green)'
      setTimeout(() => { btn.textContent = '↺ reset'; btn.style.color = ''; btn.disabled = false }, 2000)
    } else {
      btn.textContent = '✗'; btn.style.color = 'var(--red)'
      setTimeout(() => { btn.textContent = '↺ reset'; btn.style.color = ''; btn.disabled = false }, 3000)
    }
  } catch(e) {
    btn.textContent = '✗'; btn.style.color = 'var(--red)'
    setTimeout(() => { btn.textContent = '↺ reset'; btn.style.color = ''; btn.disabled = false }, 3000)
  }
}

// ── WEBSOCKET ─────────────────────────────────────────────────────
let ws, wsAlive = false
function connectWS() {
  try {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws'
    ws = new WebSocket(`${proto}://${location.host}/ws`)
    ws.onopen  = () => { wsAlive = true }
    ws.onerror = () => { wsAlive = false }
    ws.onclose = () => { wsAlive = false; setTimeout(connectWS, 3000) }
    ws.onmessage = ev => {
      try {
        const d = JSON.parse(ev.data)
        requestAnimationFrame(() => render(d))
      } catch(e) { console.error('parse:', e) }
    }
  } catch(e) { wsAlive = false }
}
function restPoll() {
  if (wsAlive) return
  fetch('/data').then(r=>r.json()).then(d=>requestAnimationFrame(()=>render(d))).catch(()=>{})
}
if ('scrollRestoration' in history) history.scrollRestoration = 'manual'
connectWS()
const isMobile = /iPhone|iPad|iPod|Android/i.test(navigator.userAgent)
setInterval(restPoll, isMobile ? 2000 : 1500)
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def root(): return HTML

@app.get("/data")
async def data(): return build_payload()

@app.post("/reset")
async def reset_pnl():
    """Reset all strategy PnL stats — zeros memory AND rewrites state JSON files."""
    if not ENGINE_OK:
        return {"ok": False, "error": "engine not loaded"}
    try:
        import sys, json as _json, time as _time
        RT_mod = sys.modules.get('strategies_runtime')
        if RT_mod is None:
            return {"ok": False, "error": "strategies_runtime not in sys.modules"}
        engines = list(getattr(RT_mod, '_engines_a', [])) + list(getattr(RT_mod, '_engines_b', []))
        if not engines:
            return {"ok": False, "error": "no engines found in strategies_runtime"}
        reset_labels = []
        file_errors  = []
        for eng in engines:
            eng._cum_net    = 0.0
            eng.hist_win    = 0
            eng.hist_lose   = 0
            eng.hist_total  = 0
            eng.pnl_history = []
            eng.preds       = [p for p in getattr(eng, 'preds', []) if not p.get('out')]
            try:
                path = eng._state_path()
                SE_mod2 = sys.modules.get('strategies_engine')
                ver = SE_mod2.VERSION['v'] if SE_mod2 and hasattr(SE_mod2, 'VERSION') else '?'
                zeroed = {'version': ver, 'label': eng.cfg.label, 'cum_net': 0.0,
                          'hist_win': 0, 'hist_lose': 0, 'hist_total': 0,
                          'pnl_history': [], 'saved_at': int(_time.time()), 'reset_at': int(_time.time())}
                tmp = path.with_suffix('.tmp')
                with open(tmp, 'w') as f: _json.dump(zeroed, f)
                tmp.replace(path)
            except Exception as fe:
                file_errors.append(f"{eng.cfg.label}: {fe}")
            reset_labels.append(eng.cfg.label)
        result = {"ok": True, "reset": reset_labels}
        if file_errors: result["file_errors"] = file_errors
        return result
    except Exception as ex:
        import traceback
        return {"ok": False, "error": str(ex), "trace": traceback.format_exc()[-800:]}


def main():
    print(f"\nMulti-Strategy Dashboard → http://<REDACTED_IP>:{PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning")

if __name__ == '__main__':
    main()
