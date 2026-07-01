#!/usr/bin/env python3
"""
debug_lag.py — Cross-exchange lag arbitrage diagnostic
=======================================================
Measures every delay in the Z pipeline to find where time is lost:

  Exchange WS feed latency   → how stale is MEXC/Bybit/Gate price?
  Signal detection latency   → time from price move to signal ready
  Order submission latency   → Binance API round-trip (simulated)
  Opportunity window         → how long does the lag persist?
  Profitability window       → at what latency does edge disappear?

No engine import needed — connects directly to exchange WebSockets.

Usage:
    python3 debug_lag.py                    # monitor all default coins
    python3 debug_lag.py DRIFT XPL ESPORTS  # specific coins
    python3 debug_lag.py --report           # print latency report and exit
    python3 debug_lag.py --duration 60      # run for 60 seconds then report
    python3 debug_lag.py --sim              # simulate Z strategy on live data
"""

import asyncio, json, time, sys, argparse, statistics
from collections import defaultdict, deque
from datetime import datetime, timezone

import websockets  # already in engine venv (predict-engine-venv)

# ── Config ────────────────────────────────────────────────────────────────────

DEFAULT_COINS = [
    "DRIFTUSDT", "ESPORTSUSDT", "XPLUSDT", "BEATUSDT",
    "WIFUSDT", "HYPEUSDT", "NEARUSDT", "STRKUSDT",
    "ONDOUSDT", "ARBUSDT", "PENDLEUSDT",
]

LAG_MOVE_THR = 0.20   # % Binance move to trigger signal (matches Z config)
LAG_WINDOW_S = 8.0    # look-back window seconds

# Minimum divergence thresholds to test
# Fee is 0.6% RT — need divergence > fee to be profitable
MIN_DIV_THRESHOLDS = [0.05, 0.20, 0.40, 0.60, 0.80, 1.00]  # % thresholds to analyse

C_RED    = "\033[91m"
C_GREEN  = "\033[92m"
C_YEL    = "\033[93m"
C_CYAN   = "\033[96m"
C_DIM    = "\033[2m"
C_BOLD   = "\033[1m"
C_RESET  = "\033[0m"

# ── State ─────────────────────────────────────────────────────────────────────

# prices[exchange][sym] = {'price': float, 'ts': float (wall clock), 'recv_ts': float}
prices = {
    'binance': defaultdict(lambda: {'price': 0.0, 'ts': 0.0, 'recv_ts': 0.0}),
    'mexc':    defaultdict(lambda: {'price': 0.0, 'ts': 0.0, 'recv_ts': 0.0}),
    'bybit':   defaultdict(lambda: {'price': 0.0, 'ts': 0.0, 'recv_ts': 0.0}),
    'gate':    defaultdict(lambda: {'price': 0.0, 'ts': 0.0, 'recv_ts': 0.0}),
}

# history[sym] = deque of {'ts', 'binance', 'mexc', 'bybit', 'gate', 'divergence'}
history = defaultdict(lambda: deque(maxlen=600))
price_hist = defaultdict(lambda: deque(maxlen=600))  # (ts, price) for binance

# stats
stats = {
    'ws_latency':      defaultdict(list),   # exchange → [ms latencies]
    'inter_exchange':  defaultdict(list),   # sym → [ms between bnx move and lag close]
    'signal_events':   [],                   # list of signal dicts
    'divergences':     defaultdict(list),   # sym → [divergence_pct]
    'rest_latency':    [],                   # Binance REST API round-trip ms
    'clock_offset':    [],                   # our clock vs Binance server clock ms
    'start_ts':        time.time(),
}

# ── WebSocket connections ─────────────────────────────────────────────────────

async def binance_ws(coins: list):
    streams = "/".join(f"{s.lower()}@aggTrade" for s in coins)
    url = f"wss://fstream.binance.com/market/stream?streams={streams}"  # /market/ required for aggTrade since 2026
    while True:
        try:
            print(f"  [binance] connecting ({len(coins)} streams)...")
            async with websockets.connect(url, ping_interval=None, ping_timeout=None,
                                          open_timeout=15) as ws:
                print(f"  [binance] connected OK")
                _bnx_msg_count = 0
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=60.0)
                    except asyncio.TimeoutError:
                        print(f"  [binance] 60s silence — reconnecting")
                        break
                    recv_ts = time.time()
                    msg = json.loads(raw)
                    _bnx_msg_count += 1

                    if not isinstance(msg, dict):
                        print(f"[binance] msg#{_bnx_msg_count} weird type: {type(msg)}")
                        continue

                    # Debug first 3 messages to see what we get
                    if _bnx_msg_count <= 3:
                        print(f"  [binance] msg#{_bnx_msg_count}: {str(msg)[:200]}")

                    d = msg.get('data', msg)
                    evt = d.get('e', '?')
                    if evt != 'aggTrade':
                        if _bnx_msg_count <= 5:
                            print(f"  [binance] skipping event type: {evt}")
                        continue
                    sym = d['s'].upper()
                    px  = float(d['p'])
                    ex_ts = d.get('T', recv_ts * 1000) / 1000
                    latency_ms = (recv_ts - ex_ts) * 1000
                    if 0 <= latency_ms < 10000:  # filter clock skew
                        stats['ws_latency']['binance'].append(latency_ms)
                        if len(stats['ws_latency']['binance']) == 1:
                            print(f"  [binance] ✅ first aggTrade: {sym} px={px} latency={latency_ms:.0f}ms")
                    # Track queue depth: if >500ms old, message was queued (not network latency)
                    if latency_ms > 500:
                        stats.setdefault('binance_queue_backlog', []).append(latency_ms)
                    if sym not in prices['binance']:
                        prices['binance'][sym] = {}

                    if sym not in price_hist:
                        price_hist[sym] = deque(maxlen=5000)

                    prices['binance'][sym].update({
                        'price': px,
                        'ts': ex_ts,
                        'recv_ts': recv_ts
                    })

                    price_hist[sym].append((recv_ts, px))
        except Exception as e:
            print(f"  [binance] ERROR: {type(e).__name__}: {e} — retrying in 2s")
            await asyncio.sleep(2)


async def mexc_ws(coins: list):
    url = "wss://contract.mexc.com/edge"
    symbols = [f"{s[:-4]}_USDT" for s in coins]
    while True:
        try:
            print(f"  [mexc] connecting ({len(symbols)} symbols)...")
            async with websockets.connect(url, ping_interval=None, ping_timeout=None,
                                          open_timeout=15) as ws:
                print(f"  [mexc] connected OK")
                for sym in symbols:
                    await ws.send(json.dumps({"method": "sub.deal", "param": {"symbol": sym}}))
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=60.0)
                    except asyncio.TimeoutError:
                        print(f"  [mexc] 60s silence — reconnecting")
                        break
                    recv_ts = time.time()
                    msg = safe_dict(json.loads(raw))

                    # MEXC sometimes sends arrays / heartbeats
                    if not isinstance(msg, dict):
                        continue

                    if msg.get('channel') != 'push.deal':
                        continue

                    d = msg.get('data', {})

                    # Sometimes data itself is a list
                    if isinstance(d, list):
                        if not d:
                            continue
                        d = d[0]

                    if not isinstance(d, dict):
                        continue
                    sym_raw = msg.get('symbol', '')
                    sym = sym_raw.replace('_USDT', 'USDT').replace('_', '')
                    px = float(d.get('p', 0) or d.get('price', 0))
                    if px <= 0: continue
                    ex_ts = d.get('t', recv_ts * 1000)
                    if isinstance(ex_ts, (int, float)) and ex_ts > 1e12:
                        ex_ts = ex_ts / 1000
                    else:
                        ex_ts = recv_ts
                    latency_ms = (recv_ts - ex_ts) * 1000
                    if 0 <= latency_ms < 5000:
                        stats['ws_latency']['mexc'].append(latency_ms)
                    prices['mexc'][sym].update({'price': px, 'ts': ex_ts, 'recv_ts': recv_ts})
        except Exception as e:
            print(f"  [mexc] ERROR: {type(e).__name__}: {e} — retrying in 2s")
            await asyncio.sleep(2)


async def bybit_ws(coins: list):
    url = "wss://stream.bybit.com/v5/public/linear"
    args = [f"publicTrade.{s}" for s in coins]
    while True:
        try:
            print(f"  [bybit] connecting ({len(args)} symbols)...")
            async with websockets.connect(url, ping_interval=None, ping_timeout=None,
                                          open_timeout=15) as ws:
                print(f"  [bybit] connected OK")
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                last_ping = time.time()
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=60.0)
                    except asyncio.TimeoutError:
                        print(f"  [bybit] 60s silence — reconnecting")
                        break
                    recv_ts = time.time()
                    now = time.time()
                    if now - last_ping >= 20:
                        await ws.send(json.dumps({"op": "ping"}))
                        last_ping = now
                    msg = safe_dict(json.loads(raw))
                    if msg.get('topic', '').startswith('publicTrade.'):
                        sym = msg['topic'].split('.', 1)[1]
                        for trade in msg.get('data', []):
                            px = float(trade.get('p', 0))
                            if px <= 0: continue
                            ex_ts = trade.get('T', recv_ts * 1000)
                            if isinstance(ex_ts, (int, float)) and ex_ts > 1e12:
                                ex_ts = ex_ts / 1000
                            else:
                                ex_ts = recv_ts
                            latency_ms = (recv_ts - ex_ts) * 1000
                            if 0 <= latency_ms < 5000:
                                stats['ws_latency']['bybit'].append(latency_ms)
                            prices['bybit'][sym].update({'price': px, 'ts': ex_ts, 'recv_ts': recv_ts})
        except Exception as e:
            print(f"  [bybit] ERROR: {type(e).__name__}: {e} — retrying in 2s")
            await asyncio.sleep(2)


async def gate_ws(coins: list):
    url = "wss://fx-ws.gateio.ws/v4/ws/usdt"
    channels = [{"channel": "futures.trades", "event": "subscribe",
                 "payload": [f"{s[:-4]}_USDT"]} for s in coins]
    while True:
        try:
            print(f"  [gate] connecting ({len(channels)} channels)...")
            async with websockets.connect(url, ping_interval=None, ping_timeout=None,
                                          open_timeout=15) as ws:
                print(f"  [gate] connected OK")
                for sub in channels:
                    await ws.send(json.dumps(sub))
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=60.0)
                    except asyncio.TimeoutError:
                        print(f"  [gate] 60s silence — reconnecting")
                        break
                    recv_ts = time.time()
                    msg = safe_dict(json.loads(raw))
                    # Gate keepalive ping — reply with pong
                    if msg.get('channel') == 'futures.ping' or msg.get('event') == 'ping':
                        await ws.send(json.dumps({
                            'time': int(time.time()), 'channel': 'futures.pong',
                            'event': 'pong', 'error': None, 'result': None,
                        }))
                        continue

                    if not isinstance(msg, dict):
                        continue

                    if msg.get('channel') != 'futures.trades':
                        continue

                    result = msg.get('result', [])

                    # Gate subscribe ACK:
                    # "result": "success"
                    if not isinstance(result, list):
                        continue

                    for trade in result:
                        if not isinstance(trade, dict):
                            continue
                        contract = trade.get('contract', '')
                        sym = contract.replace('_USDT', 'USDT')
                        px = float(trade.get('price', 0))
                        if px <= 0: continue
                        ex_ts = trade.get('create_time', recv_ts)
                        if isinstance(ex_ts, (int, float)) and ex_ts < 2e10:
                            pass  # already seconds
                        else:
                            ex_ts = recv_ts
                        latency_ms = (recv_ts - ex_ts) * 1000
                        if 0 <= latency_ms < 5000:
                            stats['ws_latency']['gate'].append(latency_ms)
                        prices['gate'][sym].update({'price': px, 'ts': ex_ts, 'recv_ts': recv_ts})
        except Exception as e:
            print(f"  [gate] ERROR: {type(e).__name__}: {e} — retrying in 2s")
            await asyncio.sleep(2)


def safe_dict(x):
    return x if isinstance(x, dict) else {}

# ── Signal detection & opportunity measurement ────────────────────────────────

async def signal_detector(coins: list):
    """
    Continuously scan for Z-like signals and measure:
    1. How large the divergence is when we detect it
    2. How long it persists (opportunity window)
    3. What price does 1s/5s/30s after detection
    """
    await asyncio.sleep(3)  # let prices fill

    while True:
        now = time.time()

        for sym in coins:
            bnx = prices['binance'][sym]
            if bnx['price'] <= 0: continue

            # Measure Binance move over last LAG_WINDOW_S
            ph = list(price_hist[sym])
            window_ago = now - LAG_WINDOW_S
            old_prices = [p for ts, p in ph if window_ago - 0.5 <= ts <= window_ago + 1.0]
            if not old_prices: continue
            old_px   = old_prices[0]
            bnx_move = (bnx['price'] - old_px) / old_px * 100

            if abs(bnx_move) < LAG_MOVE_THR: continue

            # Check lag exchanges
            lag_summary = {}
            max_div = 0.0
            for ex in ['mexc', 'bybit', 'gate']:
                ex_data = prices[ex][sym]
                if ex_data['price'] <= 0: continue
                age_ms = (now - ex_data['recv_ts']) * 1000 if ex_data['recv_ts'] > 0 else None
                div = (bnx['price'] - ex_data['price']) / bnx['price'] * 100
                lag_summary[ex] = {
                    'price':    ex_data['price'],
                    'age_ms':   age_ms,
                    'div_pct':  div,
                }
                if abs(div) > abs(max_div):
                    max_div = div

            if abs(max_div) < 0.05: continue  # tiny divergence, not interesting

            stats['divergences'][sym].append(abs(max_div))

            direction = 'long' if bnx_move > 0 else 'short'
            event = {
                'sym':        sym,
                'ts':         now,
                'bnx_px':     bnx['price'],
                'bnx_move':   bnx_move,
                'direction':  direction,
                'max_div':    max_div,
                'lag_detail': lag_summary,
                'px_1s':      None,
                'px_5s':      None,
                'px_30s':     None,
                # Opportunity window: track how long divergence stays open
                'div_at_100ms': None,
                'div_at_250ms': None,
                'div_at_500ms': None,
                'div_at_1s':    None,
            }
            stats['signal_events'].append(event)

            # Log it live
            ts_str = datetime.now(tz=timezone.utc).strftime('%H:%M:%S')
            d_col = C_GREEN if max_div > 0 else C_RED
            lag_str = " ".join(
                f"{ex}:{d['div_pct']:+.3f}%({'?ms' if d['age_ms'] is None else f"{d['age_ms']:.0f}ms"})"
                for ex, d in lag_summary.items() if d['price'] > 0
            )
            print(f"  {C_CYAN}{ts_str}{C_RESET} {C_BOLD}{sym:15s}{C_RESET} "
                  f"bnx_move={bnx_move:+.2f}% {d_col}max_div={max_div:+.3f}%{C_RESET} "
                  f"dir={direction}  {C_DIM}{lag_str}{C_RESET}")

        await asyncio.sleep(0)  # yield to event loop but don't wait


async def measure_rest_latency():
    """
    Measure Binance REST API round-trip latency.
    Uses /fapi/v1/time (lightweight ping endpoint) repeatedly.
    This is the actual latency for order submission.
    """
    import aiohttp as _aiohttp
    await asyncio.sleep(2)
    url = "https://fapi.binance.com/fapi/v1/time"
    samples = []
    async with _aiohttp.ClientSession() as session:
        while True:
            t0 = time.time()
            try:
                async with session.get(url, timeout=_aiohttp.ClientTimeout(total=3)) as r:
                    body = await r.json()
                    t1 = time.time()
                    rtt_ms = (t1 - t0) * 1000
                    server_time = body.get('serverTime', 0) / 1000
                    # Clock offset: how far our clock is from Binance's
                    clock_offset_ms = (t1 - server_time) * 1000
                    samples.append(rtt_ms)
                    stats['rest_latency'].append(rtt_ms)
                    stats['clock_offset'].append(clock_offset_ms)
            except Exception as e:
                pass
            await asyncio.sleep(1)  # measure every second


async def price_tracker(coins: list):
    """Fill in px_1s, px_5s, px_30s and divergence decay for each signal event."""
    while True:
        now = time.time()
        for ev in stats['signal_events']:
            if ev['px_30s'] is not None: continue  # already filled
            age = now - ev['ts']
            sym = ev['sym']

            # Track price outcomes
            px = prices['binance'][sym]['price']
            if px > 0:
                if age >= 1   and ev['px_1s']  is None: ev['px_1s']  = px
                if age >= 5   and ev['px_5s']  is None: ev['px_5s']  = px
                if age >= 30  and ev['px_30s'] is None: ev['px_30s'] = px

            # Track divergence decay — how quickly does the gap close?
            # Compute current max divergence across lag exchanges
            bnx_px = prices['binance'][sym]['price']
            if bnx_px > 0:
                cur_divs = []
                for ex in ['mexc', 'bybit', 'gate']:
                    ex_px = prices[ex][sym]['price']
                    if ex_px > 0:
                        cur_divs.append(abs(bnx_px - ex_px) / bnx_px * 100)
                cur_max_div = max(cur_divs) if cur_divs else 0

                if age >= 0.10 and ev['div_at_100ms'] is None: ev['div_at_100ms'] = cur_max_div
                if age >= 0.25 and ev['div_at_250ms'] is None: ev['div_at_250ms'] = cur_max_div
                if age >= 0.50 and ev['div_at_500ms'] is None: ev['div_at_500ms'] = cur_max_div
                if age >= 1.0  and ev['div_at_1s']    is None: ev['div_at_1s']    = cur_max_div

        await asyncio.sleep(0.05)  # 50ms resolution for divergence tracking


# ── Reporting ─────────────────────────────────────────────────────────────────

def print_report():
    print(f"\n{'='*70}")
    print(f"  {C_BOLD}LAG ARBITRAGE DIAGNOSTIC REPORT{C_RESET}")
    elapsed = time.time() - stats['start_ts']
    print(f"  Elapsed: {elapsed:.0f}s  |  {datetime.now(tz=timezone.utc).strftime('%H:%M:%S UTC')}")
    print(f"{'='*70}")

    # ── WebSocket latencies ──────────────────────────────────────────
    print(f"\n{C_BOLD}1. WebSocket Feed Latency{C_RESET}  (exchange_timestamp → recv_time)")
    print(f"  {C_DIM}This is how stale each price feed is. Lower = fresher data.{C_RESET}\n")
    for ex in ['binance', 'mexc', 'bybit', 'gate']:
        lats = stats['ws_latency'][ex]
        if len(lats) < 5:
            print(f"  {ex:10s}: {C_DIM}not enough data ({len(lats)} samples){C_RESET}")
            continue
        p50 = statistics.median(lats)
        p95 = sorted(lats)[int(len(lats)*0.95)]
        p99 = sorted(lats)[int(len(lats)*0.99)]
        mn  = min(lats)
        mx  = max(lats)
        true_net = sorted(lats)[:max(1, len(lats)//10)]  # bottom 10% = true network
        true_p50 = statistics.median(true_net)
        col = C_GREEN if mn < 20 else C_YEL if mn < 60 else C_RED
        print(f"  {ex:10s}: {col}min={mn:.0f}ms (true network){C_RESET}  "
              f"p50={p50:.0f}ms  p95={p95:.0f}ms  n={len(lats)}")
        if ex == 'binance' and p50 > 200:
            backlog = stats.get('binance_queue_backlog', [])
            print(f"  {C_YEL}  ⚠ p50={p50:.0f}ms = READ QUEUE BACKUP, not network latency{C_RESET}")
            print(f"  {C_YEL}  True network latency = min={mn:.0f}ms (p10={true_p50:.0f}ms){C_RESET}")
            print(f"  {C_DIM}  {len(backlog)} messages were >500ms stale when read{C_RESET}")

    # ── Signal events ────────────────────────────────────────────────
    events = stats['signal_events']
    completed = [e for e in events if e['px_30s'] is not None]

    print(f"\n{C_BOLD}2. Signal Events{C_RESET}  (Z-like divergence detections)\n")
    print(f"  Total signals detected:  {len(events)}")
    print(f"  With 30s outcome data:   {len(completed)}")

    if completed:
        # What happened 1s/5s/30s after signal?
        def ret_pct(ev, px_key):
            px = ev.get(px_key)
            if px is None or ev['bnx_px'] == 0: return None
            # For a LONG signal, we want price to go UP
            r = (px - ev['bnx_px']) / ev['bnx_px'] * 100
            return r if ev['direction'] == 'long' else -r

        r1s  = [r for e in completed if (r:=ret_pct(e,'px_1s'))  is not None]
        r5s  = [r for e in completed if (r:=ret_pct(e,'px_5s'))  is not None]
        r30s = [r for e in completed if (r:=ret_pct(e,'px_30s')) is not None]

        print(f"\n  {C_BOLD}Returns AFTER signal (in signal direction):{C_RESET}")
        for label, returns in [("1s", r1s), ("5s", r5s), ("30s", r30s)]:
            if not returns: continue
            avg   = statistics.mean(returns)
            wins  = sum(1 for r in returns if r > 0)
            wr    = wins / len(returns) * 100
            col   = C_GREEN if avg > 0 else C_RED
            print(f"  After {label:4s}: avg={col}{avg:+.4f}%{C_RESET}  WR={wr:.0f}%  n={len(returns)}")

        print(f"\n  {C_BOLD}Divergence sizes (Binance vs lagging exchange):{C_RESET}")
        all_divs = [abs(e['max_div']) for e in completed]
        if all_divs:
            print(f"  avg={statistics.mean(all_divs):.4f}%  "
                  f"median={statistics.median(all_divs):.4f}%  "
                  f"max={max(all_divs):.4f}%")

        # The key question: is the signal already gone when detected?
        print(f"\n  {C_BOLD}Fee analysis (0.6% round-trip):{C_RESET}")
        fee_rt = 0.60
        for horizon, key in [('1s','px_1s'), ('5s','px_5s'), ('30s','px_30s')]:
            above = sum(1 for e in completed
                        if (r:=ret_pct(e,key)) is not None and r > fee_rt)
            total_h = sum(1 for e in completed if e.get(key) is not None)
            pct = above / total_h * 100 if total_h else 0
            col = C_GREEN if pct > 50 else C_YEL if pct > 30 else C_RED
            print(f"  Profitable after fees at {horizon:4s}: {col}{above}/{total_h} ({pct:.0f}%){C_RESET}")

        # Divergence threshold analysis — which minimum div filter is optimal?
        print(f"\n  {C_BOLD}Minimum divergence filter analysis:{C_RESET}")
        print(f"  {C_DIM}How profitability changes as we require larger divergence to enter{C_RESET}\n")
        print(f"  {'min_div':>10s}  {'signals':>8s}  {'WR@1s':>7s}  {'WR@5s':>7s}  {'avg@5s':>9s}  {'profitable_pct':>14s}")
        for thr in MIN_DIV_THRESHOLDS:
            filtered = [e for e in completed if abs(e['max_div']) >= thr]
            if not filtered:
                print(f"  {thr:>9.2f}%  {'0':>8s}  {'—':>7s}  {'—':>7s}  {'—':>9s}  {'—':>14s}")
                continue
            r1 = [r for e in filtered if (r:=ret_pct(e,'px_1s')) is not None]
            r5 = [r for e in filtered if (r:=ret_pct(e,'px_5s')) is not None]
            wr1 = sum(1 for r in r1 if r>0)/len(r1)*100 if r1 else 0
            wr5 = sum(1 for r in r5 if r>0)/len(r5)*100 if r5 else 0
            avg5 = statistics.mean(r5) if r5 else 0
            prof5 = sum(1 for r in r5 if r > fee_rt)/len(r5)*100 if r5 else 0
            col5 = C_GREEN if prof5 > 50 else C_YEL if prof5 > 30 else C_RED
            print(f"  {thr:>9.2f}%  {len(filtered):>8d}  {wr1:>6.1f}%  {wr5:>6.1f}%  {col5}{avg5:>+8.4f}%{C_RESET}  {col5}{prof5:>13.1f}%{C_RESET}")

        # Opportunity window — how fast does the divergence close?
        div_tracked = [e for e in completed
                       if e.get('div_at_100ms') is not None and e.get('div_at_1s') is not None]
        if div_tracked:
            print(f"\n  {C_BOLD}Divergence decay (opportunity window):{C_RESET}")
            print(f"  {C_DIM}How much of the original divergence remains at each time step{C_RESET}\n")
            orig_divs = [abs(e['max_div']) for e in div_tracked]
            avg_orig = statistics.mean(orig_divs)
            for label, key in [('at 100ms','div_at_100ms'), ('at 250ms','div_at_250ms'),
                                ('at 500ms','div_at_500ms'), ('at  1.0s','div_at_1s')]:
                remaining = [e[key] for e in div_tracked if e[key] is not None]
                if not remaining: continue
                avg_rem = statistics.mean(remaining)
                pct_left = avg_rem / avg_orig * 100 if avg_orig > 0 else 0
                col = C_GREEN if pct_left > 70 else C_YEL if pct_left > 40 else C_RED
                print(f"  {label}: avg_div={col}{avg_rem:.4f}%{C_RESET}  ({col}{pct_left:.0f}%{C_RESET} of original {avg_orig:.4f}% remains)")
            print(f"\n  {C_BOLD}Pipeline context:{C_RESET}")
            print(f"  Your Z fast-path fires from WS handler directly (bypasses 100ms tick loop)")
            print(f"  WS recv min=6ms + gate=5ms + REST order=7ms = ~18ms to fill")
            print(f"  Key question: what % of divergence remains at 18ms?")
            print(f"  {C_DIM}(run with --duration 300+ for statistically significant decay data){C_RESET}")

    # ── REST latency ────────────────────────────────────────────────
    rest_lats = stats.get('rest_latency', [])
    clock_offsets = stats.get('clock_offset', [])
    print(f"\n{C_BOLD}2b. Binance REST API Latency{C_RESET}  (order submission round-trip)\n")
    if len(rest_lats) >= 3:
        rp50 = statistics.median(rest_lats)
        rp95 = sorted(rest_lats)[int(len(rest_lats)*0.95)]
        rmin = min(rest_lats)
        col  = C_GREEN if rp50 < 50 else C_YEL if rp50 < 100 else C_RED
        print(f"  REST p50={col}{rp50:.0f}ms{C_RESET}  p95={rp95:.0f}ms  min={rmin:.0f}ms  n={len(rest_lats)}")
        if clock_offsets:
            co_med = statistics.median(clock_offsets)
            print(f"  Clock offset vs Binance: {co_med:+.0f}ms  {C_DIM}(positive = our clock ahead){C_RESET}")
        print(f"\n  {C_BOLD}Japan server context:{C_RESET}")
        print(f"  Binance matching engine: Tokyo AWS ap-northeast-1")
        if rp50 < 10:
            print(f"  {C_GREEN}Excellent — <10ms REST RTT. Co-located or very close.{C_RESET}")
        elif rp50 < 30:
            print(f"  {C_GREEN}Very good — {rp50:.0f}ms. Japan server is working well.{C_RESET}")
        elif rp50 < 80:
            print(f"  {C_YEL}Good — {rp50:.0f}ms. Reasonable for retail VPS.{C_RESET}")
        else:
            print(f"  {C_RED}High — {rp50:.0f}ms. Check server region and provider.{C_RESET}")
    else:
        print(f"  {C_DIM}Not enough REST samples yet{C_RESET}")

    # ── The pipeline delay estimate ──────────────────────────────────
    print(f"\n{C_BOLD}3. Pipeline Delay Estimate{C_RESET}")
    print(f"  {C_DIM}Minimum time from divergence to fill:{C_RESET}\n")

    bnx_lat = stats['ws_latency'].get('binance', [])
    # Use min latency (true network) not p50 (which includes queue backup)
    bnx_p50 = min(bnx_lat) if bnx_lat else 50
    if bnx_lat and statistics.median(bnx_lat) > 200:
        print(f"  {C_YEL}Note: using min={bnx_p50:.0f}ms as true network latency")
        print(f"  (p50={statistics.median(bnx_lat):.0f}ms includes read queue backup){C_RESET}\n")
    rest_p50 = statistics.median(rest_lats) if len(rest_lats) >= 3 else 200

    pipeline = [
        ("Binance WS → engine recv",    f"{bnx_p50:.0f}ms",       "measured above"),
        ("Signal detection (tick loop)", "50ms",                    "engine runs ~20 ticks/s"),
        ("Gate checks (vpin/atr/etc)",   "5ms",                     "in-memory, fast"),
        ("Binance REST API order",       f"{rest_p50:.0f}ms",       "measured above"),
        ("Order acknowledgement",        "~10ms",                   "matching engine latency"),
        ("TOTAL to fill",               f"{bnx_p50+50+5+rest_p50+10:.0f}ms est.", ""),
    ]

    for step, timing, note in pipeline:
        note_str = f"  {C_DIM}← {note}{C_RESET}" if note else ""
        print(f"  {step:40s}  {C_YEL}{timing:15s}{C_RESET}{note_str}")

    mexc_lat = stats['ws_latency'].get('mexc', [])
    bybit_lat = stats['ws_latency'].get('bybit', [])
    if mexc_lat and bybit_lat:
        mexc_p50  = statistics.median(mexc_lat)
        bybit_p50 = statistics.median(bybit_lat)
        print(f"\n  Lag exchange update frequency:")
        print(f"    MEXC  p50 latency: {mexc_p50:.0f}ms")
        print(f"    Bybit p50 latency: {bybit_p50:.0f}ms")
        print(f"\n  {C_BOLD}Opportunity window vs our latency:{C_RESET}")
        min_exchange_lag = min(mexc_p50, bybit_p50)
        our_latency = bnx_p50 + 50 + 5 + rest_p50
        print(f"    Exchange updates every: ~{min_exchange_lag:.0f}ms")
        print(f"    Our entry latency:      ~{our_latency:.0f}ms")
        if our_latency > min_exchange_lag * 3:
            print(f"    {C_RED}We are {our_latency/min_exchange_lag:.1f}x slower than exchange update frequency{C_RESET}")
            print(f"    {C_RED}The gap closes before our order fills — structural latency problem confirmed{C_RESET}")
        else:
            print(f"    {C_GREEN}Latency within range — execution may be viable{C_RESET}")

    # ── Conclusion ───────────────────────────────────────────────────
    print(f"\n{C_BOLD}4. Verdict{C_RESET}\n")
    rest_p50_v = statistics.median(rest_lats) if len(rest_lats) >= 3 else 999
    bnx_p50_v  = statistics.median(bnx_lat) if len(bnx_lat) > 5 else 999
    total_lat  = bnx_p50_v + 50 + rest_p50_v

    if completed:
        r1_avg = statistics.mean(r1s) if r1s else 0
        r5_avg = statistics.mean(r5s) if r5s else 0
        if r1_avg > 0.05 and r5_avg > 0.05:
            print(f"  {C_GREEN}Signal valid at 1-5s — edge may exist at current latency{C_RESET}")
        elif r1_avg > 0 and r5_avg <= 0:
            print(f"  {C_YEL}Signal valid at 1s but reverses by 5s — window is very tight{C_RESET}")
        else:
            print(f"  {C_RED}Signal already fading before 1s — gap closed by HFT{C_RESET}")
    else:
        print(f"  {C_DIM}Not enough completed signals yet — run longer{C_RESET}")

    print(f"\n{C_BOLD}5. Latency Reduction Options{C_RESET}\n")
    print(f"  Current total pipeline: ~{total_lat:.0f}ms")
    print(f"  Breakdown: WS={bnx_p50_v:.0f}ms + tick=50ms + REST={rest_p50_v:.0f}ms")
    print()

    opts = [
        ("Already done", C_GREEN, [
            "Japan server — correct region for Binance Tokyo matching engine",
            "websockets library — faster than REST polling for price data",
        ]),
        ("High impact, easy", C_YEL, [
            "WebSocket order submission (wss://ws-fapi.binance.com/ws-fapi/v1)",
            f"  Saves ~{rest_p50_v*0.7:.0f}ms vs REST — WS orders skip HTTP overhead",
            "  Engine currently uses REST for orders — change create_order() to use WS API",
            "Reduce tick_all() interval: 50ms → 10ms",
            "  Engine scans all coins every 50ms — reduce to 10ms to detect signals faster",
            "  Change: asyncio.sleep(0.05) → asyncio.sleep(0.01) in main tick loop",
        ]),
        ("Medium impact", C_DIM, [
            "Disable gate checks for Z specifically (VPIN, ATR, spread)",
            "  Z fires on timing — gate checks add 5-20ms and may block valid signals",
            "  Replace with simpler: only check cooldown and has_open",
            "Pre-subscribe to all coin streams at startup (avoid sub latency)",
            "Use /fapi/v1/order with reduceOnly for faster fills",
        ]),
        ("Hard / requires new infra", C_RED, [
            "Co-location at AWS ap-northeast-1 (same datacenter as Binance)",
            "  Would reduce REST to <2ms and WS to <1ms",
            "  Cost: ~$500-2000/mo for dedicated instance",
            "Kernel bypass networking (DPDK) — sub-millisecond but complex",
            "Direct market access (DMA) via prime brokerage",
        ]),
    ]
    for category, col, items in opts:
        print(f"  {col}{C_BOLD}{category}:{C_RESET}")
        for item in items:
            print(f"    {'→' if not item.startswith(' ') else ' '} {item}")
        print()

    print(f"  {C_BOLD}Most actionable right now:{C_RESET}")
    print(f"  1. Switch orders to WebSocket API — saves ~{rest_p50_v*0.7:.0f}ms, same venv")
    print(f"  2. Reduce tick interval 50ms→10ms — saves ~40ms signal detection")
    print(f"  3. Measure: run this tool with --duration 300 for statistically significant data")

    print(f"\n{'='*70}\n")


# ── Main ──────────────────────────────────────────────────────────────────────

async def test_binance_endpoints(coins: list):
    """
    Test all Binance futures WS endpoints to find lowest latency.
    Binance has numbered endpoints that bypass the CDN.
    From Japan, fstream numbered endpoints may give 10ms to 20ms vs fstream 120ms.
    """
    import aiohttp

    endpoints = [
        "wss://fstream.binance.com",     # market data (confirmed correct by engine.py)
    ]
    # fstream1-4 are SPOT only — redirect to www.binance.com for futures
    # WS Order API is separate: ws-fapi.binance.com (for order submission)
    ws_order_url = "wss://ws-fapi.binance.com/ws-fapi/v1"

    streams = "/".join(f"{s.lower()}@aggTrade" for s in coins[:3])
    results = {}

    print(f"\n{C_BOLD}Testing Binance WS endpoints for lowest latency...{C_RESET}")
    print(f"{C_DIM}(using {', '.join(coins[:3])}){C_RESET}\n")

    for base_url in endpoints:
        url = f"{base_url}/market/stream?streams={streams}"
        latencies = []
        print(f"  Testing {base_url.replace('wss://','')}... ", end='', flush=True)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url, heartbeat=20,
                    timeout=aiohttp.ClientTimeout(connect=5, total=15)) as ws:
                    deadline = time.time() + 12
                    async for msg in ws:
                        if time.time() > deadline: break
                        if msg.type != aiohttp.WSMsgType.TEXT: continue
                        recv_ts = time.time()
                        try:
                            d = json.loads(msg.data).get('data', {})
                            if d.get('e') != 'aggTrade': continue
                            T = d.get('T')
                            if not T: continue
                            lat_ms = (recv_ts - float(T)/1000.0) * 1000.0
                            if 0 <= lat_ms < 2000:
                                latencies.append(lat_ms)
                        except Exception:
                            continue
                        if len(latencies) >= 30: break
        except Exception as e:
            print(f"{C_RED}FAILED: {type(e).__name__}: {e}{C_RESET}")
            results[base_url] = None
            continue

        if latencies:
            p50 = statistics.median(latencies)
            p95 = sorted(latencies)[int(len(latencies)*0.95)]
            mn  = min(latencies)
            col = C_GREEN if p50 < 30 else C_YEL if p50 < 80 else C_RED
            print(f"{col}p50={p50:.0f}ms{C_RESET}  p95={p95:.0f}ms  min={mn:.0f}ms  n={len(latencies)}")
            results[base_url] = p50
        else:
            print(f"{C_DIM}no samples{C_RESET}")
            results[base_url] = None

    # Test WS Order API (ws-fapi.binance.com) — for order submission latency
    print(f"\n  Testing WS Order API ({ws_order_url.replace('wss://','')})...")
    print(f"  {C_DIM}For order submission — separate from market data feed{C_RESET}")
    ws_order_lats = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(ws_order_url, heartbeat=10,
                timeout=aiohttp.ClientTimeout(connect=5, total=10)) as ws:
                for _ in range(10):
                    t0 = time.time()
                    await ws.send_str(json.dumps({"id": "ping", "method": "ping"}))
                    msg = await asyncio.wait_for(ws.receive(), timeout=3)
                    t1 = time.time()
                    if msg.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                        ws_order_lats.append((t1 - t0) * 1000)
                    await asyncio.sleep(0.2)
    except Exception as e:
        print(f"  {C_YEL}{type(e).__name__}: {e}{C_RESET}")

    if ws_order_lats:
        p50_o = statistics.median(ws_order_lats)
        mn_o  = min(ws_order_lats)
        col   = C_GREEN if p50_o < 10 else C_YEL if p50_o < 20 else C_RED
        print(f"  WS Order API: {col}p50={p50_o:.0f}ms{C_RESET}  min={mn_o:.0f}ms  n={len(ws_order_lats)}")
        results['ws_order'] = p50_o
    else:
        print(f"  {C_DIM}WS Order API: no response (may need auth){C_RESET}")
        results['ws_order'] = None

    # Summary
    mkt_p50   = results.get("wss://fstream.binance.com") or 120
    order_p50 = results.get('ws_order') or 5  # REST fallback
    rest_lats = stats.get('rest_latency', [])
    rest_p50  = statistics.median(rest_lats) if rest_lats else 5

    print(f"\n  {C_BOLD}Pipeline summary:{C_RESET}")
    print(f"  Market data WS:  {mkt_p50:.0f}ms  (fstream /market/stream)")
    print(f"  REST order:      {rest_p50:.0f}ms  (fapi.binance.com)")
    if results.get('ws_order'):
        print(f"  WS order:        {results['ws_order']:.0f}ms  (ws-fapi.binance.com)")
    best_order = min(rest_p50, results.get('ws_order') or rest_p50)
    total = mkt_p50 + 1 + 5 + best_order + 10
    bybit = 40
    col = C_GREEN if total < bybit else C_RED
    print(f"  TOTAL: {mkt_p50:.0f}+1+5+{best_order:.0f}+10 = {col}{total:.0f}ms{C_RESET}  vs Bybit {bybit}ms")
    if total < bybit:
        print(f"  {C_GREEN}✅ Inside Bybit window — Z may be viable, re-run --duration 120{C_RESET}")
    else:
        print(f"  {C_RED}❌ Outside Bybit window by {total-bybit:.0f}ms{C_RESET}")


async def main(coins: list, duration: int, sim_mode: bool):
    print(f"\n{C_BOLD}PredictEngine — Lag Arbitrage Diagnostic{C_RESET}")
    print(f"Monitoring {len(coins)} coins  |  Ctrl+C or --duration to stop\n")
    print(f"Connecting to Binance, MEXC, Bybit, Gate WebSockets...")
    print(f"{'='*70}\n")

    tasks = [
        binance_ws(coins),
        mexc_ws(coins),
        bybit_ws(coins),
        gate_ws(coins),
        signal_detector(coins),
        price_tracker(coins),
        measure_rest_latency(),
    ]

    if duration > 0:
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=duration
            )
        except asyncio.TimeoutError:
            pass
    else:
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except KeyboardInterrupt:
            pass

    print_report()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Lag arbitrage diagnostic')
    parser.add_argument('coins', nargs='*', help='Coins to monitor (default: engine universe)')
    parser.add_argument('--duration', type=int, default=0, metavar='SECONDS',
                        help='Run for N seconds then print report (0 = run until Ctrl+C)')
    parser.add_argument('--report', action='store_true',
                        help='Run for 60s then print report')
    parser.add_argument('--sim', action='store_true',
                        help='Simulate Z strategy execution and measure fill latency')
    parser.add_argument('--test-endpoints', action='store_true',
                        help='Test all Binance WS endpoints and find lowest latency one')
    args = parser.parse_args()

    coins = [c.upper() if c.upper().endswith('USDT') else c.upper() + 'USDT'
             for c in args.coins] if args.coins else DEFAULT_COINS

    duration = 60 if args.report else args.duration

    if args.test_endpoints:
        asyncio.run(test_binance_endpoints(coins))
        sys.exit(0)

    try:
        asyncio.run(main(coins, duration, args.sim))
    except KeyboardInterrupt:
        print_report()