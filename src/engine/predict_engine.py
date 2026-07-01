#!/usr/bin/env python3
"""
PredictEngine - predict_engine.py
Entry point. Supports three modes:

  python3 predict_engine.py                          # terminal display, default coins
  python3 predict_engine.py BTC ETH SOL             # terminal display, custom coins
  python3 predict_engine.py --multi --dashboard      # multi-strategy + web dashboard
  python3 predict_engine.py --multi --dashboard --quiet  # same, no terminal output (server)
"""

import asyncio, sys, os, re, time
import faulthandler
faulthandler.enable()  # print Python traceback to stderr on SIGABRT/SIGSEGV → journalctl
from datetime import datetime, timezone

import engine as E
try:
    from market_maker_paper import mm_paper_loop as _mm_paper_loop  # Phase-1 paper MM (stage only)
except Exception:
    _mm_paper_loop = None
from engine_logger import setup_logging, log_engine_start, log_engine_stop, engine_log
setup_logging()   # must be first — sets up handlers before any other import logs
from config import (
    DEFAULT_COINS, VERSION, LOG_FILE,
    LOOP_MS, DISPLAY_MS, PRED_COOLDOWN,
    FEE_RT, WIN_THR, ATR_TP_MULT, ATR_SL_MULT,
    TRAIL_DIST, MIN_HOLD_ANY, REV_MIN_HOLD, MAX_WINDOW,
    MIN_CONF, MIN_SCORE, MIN_VOL_ATR, INERTIA_SEC, INERTIA_THR,
    VPIN_MIN, VPIN_HIGH, SPREAD_MAX_PCT, ACCEL_MIN,
    USE_COIN_SCANNER, SCANNER_REFRESH_SEC, SCANNER_TOP_N,
)

# ══ ANSI ══════════════════════════════════════════════════════════
CLEAR = '\033[2J\033[H'
HIDE  = '\033[?25l'
SHOW  = '\033[?25h'
RESET = '\033[0m'
_ANSI = re.compile(r'\033\[[0-9;]*m')

def _c(code, t):  return f'\033[{code}m{t}{RESET}'
def grn(t):       return _c('92', t)
def red(t):       return _c('91', t)
def yel(t):       return _c('93', t)
def cyn(t):       return _c('96', t)
def dim(t):       return _c('2',  t)
def bld(t):       return _c('1',  t)
def ora(t):       return _c('33', t)
def mag(t):       return _c('95', t)
def raw_len(s):   return len(_ANSI.sub('', s))
def pad(s, w, a='left'):
    d = w - raw_len(s)
    return ' '*max(0,d)+s if a=='right' else s+' '*max(0,d)
def tw():
    try:    return os.get_terminal_size().columns
    except: return 130

def sig_col(val, direction):
    if val is None: return dim('  -')
    bull = (direction=='long'  and val >  15) or (direction=='short' and val < -15)
    bear = (direction=='long'  and val < -15) or (direction=='short' and val >  15)
    s    = f"{'+' if val>=0 else ''}{val:.0f}"
    return grn(s) if bull else red(s) if bear else dim(s)

def vpin_col(v):
    if v is None: return dim('  - ')
    s = f"{v:.2f}"
    if   v >= VPIN_HIGH: return grn(s)
    elif v >= VPIN_MIN:  return yel(s)
    else:                return red(s)

def lam_col(lam):
    if lam is None: return dim('  - ')
    return grn('  +λ') if lam > 0 else red('  -λ')

def spread_col(s):
    if s is None: return dim('  - ')
    if   s <= 0.02: return grn(f"{s:.3f}")
    elif s <= SPREAD_MAX_PCT: return yel(f"{s:.3f}")
    else: return red(f"{s:.3f}")

def accel_col(a):
    if a is None: return dim('  - ')
    if   a >= 2.0: return grn(f"{a:.1f}×")
    elif a >= ACCEL_MIN: return yel(f"{a:.1f}×")
    else: return red(f"{a:.1f}×")

# ══ TERMINAL DISPLAY ══════════════════════════════════════════════
def draw():
    W   = tw()
    sep = dim('-' * W)
    now = time.time()
    out = [
        f"{bld(cyn('PredictEngine'))}  "
        f"{dim(datetime.now(timezone.utc).strftime('%H:%M:%S')+' UTC')}  "
        f"{cyn(E.ws_status) if 'live' in E.ws_status else ora(E.ws_status)}  "
        f"{dim(VERSION['v']+' · '+VERSION['notes'])}",
        sep,
        dim(f" {'SYM':<10} {'PRICE':>11} {'SCORE':>7} {'CONF':>5} "
            f"{'OBI':>6} {'CVD*':>6} {'LIQ':>6} {'ABS':>6} "
            f"{'ATR':>7} {'VPIN':>6} {'λ':>4} {'SPRD':>6} {'ACC':>5}  GATES"),
        sep,
    ]

    for sym in E.ACTIVE_COINS:
        st = E.sym_state.get(sym)
        if not st: continue
        r    = E.run_pred(sym)
        # FIX #2: do NOT append to sig_hist here.  draw() is read-only display;
        # sig_hist is written exclusively by pred_loop()/tick_all() (one entry
        # per logic tick).  The old code added an extra entry on every display
        # refresh (DISPLAY_MS != LOOP_MS), which inflated the sustained-signal
        # window and caused spurious gate failures / passes.
        atr  = E.get_atr(sym)
        ok,_ = E.gates_met(sym, r)
        vpin = E.calc_vpin(sym)
        lam  = E.calc_kyle_lambda(sym)
        sprd = E.calc_spread_pct(sym)
        acc  = E.calc_trade_accel(sym)
        base  = sym.replace('USDT','')
        price = E.fp(st['price']) if st['price'] else '-'
        score = r['score']
        arrow = '▲' if score > 0 else '▼'
        sc    = grn if score >  20 else red if score < -20 else dim
        cc    = grn if r['conf'] >= 70 else yel if r['conf'] >= 50 else dim
        ac    = grn if atr >= MIN_VOL_ATR else dim
        cd    = now - st['last_pred_ts']
        cd_s  = dim(f' cd:{int(cd)}s') if cd < PRED_COOLDOWN else ''
        stat  = bld(grn('► FIRE')) if ok else dim(E.gate_count(sym,r)+' gates')
        dyn_tp, dyn_sl = E.calc_dynamic_tp_sl(sym, r['score'], r['strength'])
        live = next((p for p in list(E.preds) if p['sym']==sym and p['out3'] is None), None)
        if live: dyn_tp, dyn_sl = live['dyn_tp'], live['dyn_sl']

        out.append(
            f" {pad(bld(base) if ok else base, 10)} "
            f"{pad(dim(price), 11, 'right')} "
            f"{pad(sc(arrow+str(abs(round(score)))), 7, 'right')} "
            f"{pad(cc(str(r['conf'])+'%'), 5, 'right')} "
            f"{pad(sig_col(r['sigs'].get('obi'), r['dir']), 6, 'right')} "
            f"{pad(sig_col(r['sigs'].get('cvd'), r['dir']), 6, 'right')} "
            f"{pad(sig_col(r['sigs'].get('liq'), r['dir']), 6, 'right')} "
            f"{pad(sig_col(r['sigs'].get('abs'), r['dir']), 6, 'right')} "
            f"{pad(ac(f'{atr:.3f}%'), 7, 'right')} "
            f"{pad(vpin_col(vpin), 6, 'right')} "
            f"{pad(lam_col(lam), 4, 'right')} "
            f"{pad(spread_col(sprd), 6, 'right')} "
            f"{pad(accel_col(acc), 5, 'right')}  "
            f"{stat}{cd_s}"
        )

    decided = E.hist_win + E.hist_lose
    wr      = round(E.hist_win / max(decided, 1) * 100)
    out += [
        sep,
        f" {dim('PREDICTIONS')}  total:{yel(str(E.hist_total))}  "
        f"✅ {grn(str(E.hist_win))}  ❌ {red(str(E.hist_lose))}  "
        f"wr:{yel(str(wr)+'%')}  {dim('log→ '+LOG_FILE)}",
        sep,
    ]

    for p in list(E.preds)[:10]:
        elapsed  = now - p['ts']
        dc       = grn if p['dir']=='long' else red
        arrow    = '▲' if p['dir']=='long' else '▼'
        sym_s    = p['sym'].replace('USDT','')
        ts_s     = datetime.fromtimestamp(p['ts']).strftime('%H:%M:%S')
        conf_c   = grn if p['conf'] >= 70 else yel
        score_s  = f"{round(p['score']):+d}"
        sigs_s   = f"{p['n_agree']}/{p['n_avail']}"
        snap_s   = f"1m:{dim(str(round(p['snap1'],3))+'%')} " if p.get('snap1') is not None else ''
        tp_sl_s  = dim(f"tp:{p['dyn_tp']:.2f}% sl:{p['dyn_sl']:.2f}%  ")

        if p['out3'] is not None:
            net   = p['pct3'] - FEE_RT
            oc    = grn if p['out3']=='win' else red if p['out3']=='lose' else dim
            nc    = grn if net > 0 else red
            rc    = {'tp':grn,'trail':grn,'sl':red,'rev':yel,'inertia':dim,'time':dim}.get(p.get('reason',''), dim)
            dur_s = f"{p['dur']:.0f}s" if p.get('dur') else '?'
            right = (oc(f"{p['pct3']:+.3f}%({p['out3']})") + ' ' +
                     nc(f"net:{net:+.3f}%") + ' ' +
                     rc(f"[{p.get('reason','?')} {dur_s}]"))
        elif elapsed < MAX_WINDOW:
            rem = MAX_WINDOW - elapsed
            st2 = E.sym_state.get(p['sym'])
            if st2 and st2['price'] and p['entry']:
                raw2 = (st2['price'] - p['entry']) / p['entry'] * 100
                dp2  = raw2 if p['dir']=='long' else -raw2
                pk   = p.get('max_dp', dp2)
                cc2  = grn if dp2 > 0.05 else red if dp2 < -0.05 else dim
                pk_s = dim(f" pk:{pk:+.3f}%") if pk > 0.08 else ''
                right = cc2(f"{dp2:+.3f}%") + pk_s + dim(f" ({rem:.0f}s)")
            else:
                right = dim(f"…{rem:.0f}s")
        else:
            right = dim("resolving…")

        out.append(
            f"  {dim(ts_s)}  {dc(arrow)} {bld(sym_s.ljust(7))} "
            f"{conf_c(str(p['conf'])+'%')} {dim('s:')}{score_s:<5} "
            f"{dim(sigs_s):<4} @ {dim(E.fp(p['entry']))}  "
            f"{tp_sl_s}{snap_s}{right}"
        )

    out.append(sep)
    resolved = [p for p in list(E.preds) if p['out3'] is not None]
    if resolved:
        gross    = sum(p['pct3'] for p in resolved)
        net_t    = sum(p['pct3'] - FEE_RT for p in resolved)
        exp      = net_t / len(resolved)
        wins_p   = [p['pct3'] for p in resolved if p['out3']=='win']
        loses_p  = [p['pct3'] for p in resolved if p['out3']=='lose']
        avg_w    = sum(wins_p)  / max(len(wins_p),  1)
        avg_l    = sum(loses_p) / max(len(loses_p), 1)
        avg_dur  = sum(p['dur'] for p in resolved if p.get('dur')) / max(len(resolved),1)
        by_r     = {}
        for p in resolved:
            k = p.get('reason','?')
            by_r.setdefault(k, {'w':0,'l':0,'f':0})
            by_r[k][p['out3'][0]] = by_r[k].get(p['out3'][0], 0) + 1
        r_str = '  '.join(
            f"{dim(k+':')} {grn(str(v['w']))}/{red(str(v['l']))}"
            for k, v in by_r.items()
        )
        out += [
            f"  P&L ({len(resolved)} · avg {avg_dur:.0f}s)  "
            f"gross {(grn if gross>=0 else red)(f'{gross:+.3f}%')}  "
            f"net {(grn if net_t>=0 else red)(f'{net_t:+.3f}%')}  "
            f"expect {(grn if exp>=0 else red)(f'{exp:+.4f}%')}  "
            f"wr {yel(str(wr)+'%')}  "
            f"avg_win {grn(f'{avg_w:+.3f}%')}  avg_loss {red(f'{avg_l:+.3f}%')}",
            f"  exits: {r_str}  "
            f"{dim('tp/trail=profit  sl=stoploss  rev=reversal  inertia=noevent  time=max')}",
        ]
    else:
        out.append(f"  {dim('no resolved predictions yet')}")

    out += [
        sep,
        dim(f"  {LOOP_MS}ms pred · {DISPLAY_MS}ms display · "
            f"dyn TP(atr×{ATR_TP_MULT}) SL(atr×{ATR_SL_MULT}) · "
            f"inertia={INERTIA_SEC}s@{INERTIA_THR}% · "
            f"VPIN≥{VPIN_MIN} · Kyle+λ gate · "
            f"fee={FEE_RT}%rt · {VERSION['v']} · ctrl+c to quit"),
        dim(f"  CVD*=divergence · ABS=absorption · "
            f"SPRD≤{SPREAD_MAX_PCT}%(gate) · ACC≥{ACCEL_MIN}×(gate) · "
            f"VPIN:{red(str(VPIN_MIN)+'=noise')} {yel('mid')} {grn(str(VPIN_HIGH)+'=informed')}"),
    ]
    sys.stdout.write(CLEAR + '\n'.join(out) + '\n')
    sys.stdout.flush()

async def display_task():
    while E.running:
        t0 = time.time()
        try: draw()
        except Exception as ex:
            sys.stdout.write(f'\ndisplay error: {ex}\n'); sys.stdout.flush()
        await asyncio.sleep(max(0, DISPLAY_MS/1000 - (time.time()-t0)))



# ══ MAIN - TERMINAL MODE ══════════════════════════════════════════
async def main_terminal(coins, *, quiet: bool = False, multi: bool = False):
    E.setup(coins)
    if not quiet:
        sys.stdout.write(HIDE)
    mode = 'MULTI-STRATEGY' if multi else VERSION['v']
    print(f"PredictEngine {mode} starting")
    print(f"Coins  : {', '.join(c.replace('USDT','') for c in coins)}")
    print(f"Signals: OBI CVD* LIQ ABS")
    print(f"Gates  : conf≥{MIN_CONF}% · score≥{MIN_SCORE} · vol≥{MIN_VOL_ATR}% · "
          f"VPIN≥{VPIN_MIN} · Kyle+λ")
    print(f"Exit   : dyn TP(atr×{ATR_TP_MULT}) · SL(atr×{ATR_SL_MULT}) · "
          f"trail={TRAIL_DIST}% · inertia={INERTIA_SEC}s@{INERTIA_THR}%")
    print(f"Log    : {LOG_FILE}\n")
    loop_task = pred_loop_multi(coins) if multi else E.pred_loop(coins)
    # Dynamic WS: restart when scanner adds/removes coins
    from engine_scanner import init_scanner_events, coins_changed_event as _dummy
    init_scanner_events()   # creates the asyncio.Event in this loop
    from engine_scanner import coins_changed_event

    async def dynamic_ws_task():
        """Restarts ws_task when ACTIVE_COINS changes (new coins added by scanner)."""
        while E.running:
            current_coins = list(E.ACTIVE_COINS)
            ws = asyncio.create_task(E.ws_task(current_coins))
            ws.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
            # Wait until either WS finishes or coins change
            change = asyncio.create_task(coins_changed_event.wait())
            done, pending = await asyncio.wait(
                {ws, change}, return_when=asyncio.FIRST_COMPLETED
            )
            # Cancel whichever didn't finish
            for t in pending: t.cancel()
            try:
                await asyncio.gather(*pending, return_exceptions=True)
            except BaseException:
                pass
            if coins_changed_event is not None:
                coins_changed_event.clear()
            if not E.running:
                break
            new_coins = list(E.ACTIVE_COINS)
            if set(new_coins) != set(current_coins):
                added = set(new_coins) - set(current_coins)
                print(f"[WS] Reconnecting for {len(added)} new coins: {list(added)[:5]}{'...' if len(added)>5 else ''}", flush=True)
            await asyncio.sleep(0.5)   # brief pause before reconnect

    async def parabolic_scan_task():
        """
        REST-only scanner: every 60s fetch ALL Binance futures tickers,
        find coins that pumped ≥8% in 24h with unusual volume, add them
        to ACTIVE_COINS temporarily so P strategy can evaluate them.
        Runs independently of WS — no stream slots used.
        """
        import aiohttp
        while E.running:
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.get(
                        f'{API_URL}/fapi/v1/ticker/24hr',
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as r:
                        tickers = await r.json()
                if not isinstance(tickers, list):
                    await asyncio.sleep(60); continue

                added = []
                for t in tickers:
                    sym = t.get('symbol','')
                    if not sym.endswith('USDT'): continue
                    if sym in E.ACTIVE_COINS: continue
                    try:
                        pct_change = float(t.get('priceChangePercent', 0))
                        vol = float(t.get('quoteVolume', 0))
                    except: continue
                    # Parabolic candidate: pumped ≥8% in 24h AND reasonable volume
                    if pct_change >= 8.0 and vol >= 1_000_000:
                        E.ACTIVE_COINS.add(sym)
                        E.init_sym(sym)
                        added.append(f"{sym}(+{pct_change:.1f}%)")

                if added:
                    print(f"[P-SCAN] Added {len(added)} pump candidates: {added[:5]}", flush=True)

            except Exception as ex:
                print(f"[P-SCAN] error: {ex}", flush=True)
            await asyncio.sleep(60)

    tasks = [dynamic_ws_task(), E.rest_task(coins), loop_task, parabolic_scan_task()]
    if not quiet:
        tasks.append(display_task())
    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        E.running = False
    finally:
        if not quiet:
            sys.stdout.write(SHOW + '\n')
        print(f"Stopped. → {LOG_FILE}")

async def pred_loop_multi(coins):
    """Prediction loop that routes to all strategy engines simultaneously.
    Uses E.ACTIVE_COINS so coins added by the scanner mid-session are included.
    """
    import strategies as S
    import strategies_engine as _se
    while E.running:
        t0 = time.time()
        E._tick_id += 1
        _se.advance_ema_tick()   # invalidate EMA21 cache — all gates share fresh value this tick
        S.check_all()
        S.tick_all(E.ACTIVE_COINS)
        await asyncio.sleep(max(0, LOOP_MS/1000 - (time.time()-t0)))


# ══ MAIN - MULTI-STRATEGY + DASHBOARD MODE ════════════════════════
async def main_multi(coins, quiet=False, no_log=False):
    import strategies as S
    import uvicorn
    import dashboard_multi as DM

    # Skip legacy init_log() in multi mode — per-strategy CSVs handle all logging
    E.ACTIVE_COINS = coins
    for sym in set(coins + ['BTCUSDT']):
        E.init_sym(sym)
    E._tg_send(f"\U0001f7e2 <b>Engine started</b>\nSeed coins: {len(coins)} (scanner expands in 30 min)")
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(E._tg_async_exception_handler)

    # ── Load shadow config (B) if strategies_config_b.py exists ───
    # Enables A/B comparison on one dashboard with zero extra WS connections.
    # To enable:  deploy strategies_config_b.py alongside this file.
    # To disable: remove strategies_config_b.py — engine reverts to single mode.
    strategies_b = None
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            'strategies_config_b',
            os.path.join(os.path.dirname(__file__), 'strategies_config_b.py')
        )
        if spec and spec.loader:
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            strategies_b = mod.STRATEGIES_B
            if not quiet:
                print(f"A/B mode  : loaded {len(strategies_b)} shadow strategies from strategies_config_b.py")
    except Exception:
        pass   # no shadow config — single mode

    # init_strategies MUST come after strategies_b is resolved
    S.init_strategies(strategies_b=strategies_b, no_log=no_log)

    # ── Live execution: startup reconciliation ─────────────────────────────
    # Compares engine's in-memory open preds vs actual Binance positions.
    # No-op when LIVE_ENABLED=False. Must run after init_strategies so engines exist.
    import live_execution as _live
    if _live.LIVE_ENABLED:
        all_engines = S._engines_a + S._engines_b
        _live.close_all_positions(reason="restart")   # close any positions left from previous run
        _live.reconcile_on_startup(all_engines)
        bal = _live.get_usdt_balance()
        if bal is not None:
            print(f"[LIVE] Execution ENABLED — testnet={not _live.LIVE_MODE} "
                  f"order_size=${_live.LIVE_ORDER_USDT:.0f} "
                  f"max_positions={_live.LIVE_MAX_POSITIONS} "
                  f"balance=${bal:.2f}")
        else:
            print("[LIVE] balance fetch failed")
    # ──────────────────────────────────────────────────────────────────────

    if not quiet:
        print(f"PredictEngine {VERSION['v']} - multi-strategy mode")
        print(f"Coins     : {', '.join(c.replace('USDT','') for c in coins)}")
        n_a = len(S._engines_a); n_b = len(S._engines_b)
        mode = f"A/B ({n_a} prod  +  {n_b} shadow)" if n_b else f"{n_a} strategies"
        print(f"Strategies: {mode}")
        port_display = os.environ.get('DASHBOARD_PORT', 8080)
        print(f"Dashboard : http://<REDACTED_IP>:{port_display}")
        print(f"Scanner   : {'auto (top '+str(SCANNER_TOP_N)+' coins, refresh '+str(SCANNER_REFRESH_SEC)+'s)' if USE_COIN_SCANNER else 'disabled'}")
        print(f"Log dir   : {os.getcwd()}\n")

    log_engine_start(
        VERSION['v'], coins,
        [eng.cfg.label for eng in S._engines_a if not eng.cfg.disabled],
    )

    port   = int(os.environ.get('DASHBOARD_PORT', 8080))
    config = uvicorn.Config(DM.app, host='0.0.0.0', port=port, log_level='warning',
                            ws='auto',        # auto: picks websockets if installed, then wsproto
                            ws_ping_interval=None,  # disable WS pings — event loop too busy to respond on time
                            ws_ping_timeout=None)
    server = uvicorn.Server(config)

    # Pre-warm illiquid symbol cache in background (non-blocking)
    # is_illiquid() uses requests.get() — must NOT be called in the pred loop hot path
    # before this completes. _check_symbol() guards against empty cache.
    async def _warm_illiquid_cache():
        try:
            import live_execution as _le
            import asyncio as _aio
            # Run blocking requests.get() in thread pool to avoid blocking event loop
            await _aio.get_event_loop().run_in_executor(None, _le.get_illiquid_syms)
            engine_log.info(f"[startup] illiquid cache: {len(_le._illiquid_syms)} symbols")
        except Exception as ex:
            engine_log.warning(f"[startup] illiquid cache failed: {ex}")

    tasks = [
        _warm_illiquid_cache(),
        E.ws_task(coins),
        E.rest_task(coins),
        pred_loop_multi(coins),
        server.serve(),
        *([ E.lag_ws_task(coins) ] if os.getenv('ENGINE_ENV','prod').lower() == 'stage' else []),  # Z: stage only
        *([ _mm_paper_loop() ] if (os.getenv('ENGINE_ENV','prod').lower() == 'stage' and _mm_paper_loop) else []),  # Paper MM: stage only
        S.config_watcher_task(no_log=no_log),
    ]
    if USE_COIN_SCANNER:
        tasks.append(E.coin_scanner_task(refresh_interval=SCANNER_REFRESH_SEC))

    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        E.running = False
        log_engine_stop()

# ══ ENTRY POINT ═══════════════════════════════════════════════════
if __name__ == '__main__':
    raw_args = sys.argv[1:]

    multi     = '--multi'     in raw_args
    dashboard = '--dashboard' in raw_args
    quiet     = '--quiet'     in raw_args
    no_log    = '--no-log'    in raw_args

    flag_set  = {'--multi', '--dashboard', '--quiet', '--no-log', '--gate-debug'}
    coin_args = [a for a in raw_args if a not in flag_set]
    coins = (
        [a.upper() + ('' if a.upper().endswith('USDT') else 'USDT') for a in coin_args]
        if coin_args else DEFAULT_COINS
    )

    try:
        if multi or dashboard:
            asyncio.run(main_multi(coins, quiet=quiet, no_log=no_log))
        else:
            asyncio.run(main_terminal(coins, quiet=quiet, multi=multi))
    except KeyboardInterrupt:
        pass
