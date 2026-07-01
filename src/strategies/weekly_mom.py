#!/usr/bin/env python3
"""
weekly_mom.py — Bear-regime cross-sectional momentum executor
=============================================================
Strategy:
  1. Check if BTC is in bear regime (close < 50d MA)
  2. If yes: rank UNIVERSE coins by 14d return, short bottom K
  3. Hold 7 days, then close all and rebalance
  4. If not bear regime: close any open positions and sit flat

Run modes:
  python3 weekly_mom.py --check      # print regime + rankings, no orders
  python3 weekly_mom.py --rebalance  # close existing + open new shorts (if bear)
  python3 weekly_mom.py --close      # close all open positions
  python3 weekly_mom.py --status     # show current positions + P&L

Schedule (cron on stage):
  0 0 * * 1 cd ~/engine && python3 weekly_mom.py --rebalance >> logs/weekly_mom.log 2>&1

Environment (.env.stage additions):
  WEEKLY_MOM_ENABLED=true      # master switch
  WEEKLY_MOM_USDT=200          # USDT notional per position
  WEEKLY_MOM_K=5               # number of shorts
  WEEKLY_MOM_LOOKBACK=14       # days for momentum ranking
  WEEKLY_MOM_UNIVERSE=BTCUSDT,ETHUSDT,...  # comma-separated
"""

import hashlib
import hmac
import json
import logging
import os
import sys
import time
import urllib.parse
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import requests

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-7s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger('weekly_mom')

# ── Config ────────────────────────────────────────────────────────────────────
def _env_bool(k, d): return os.environ.get(k, str(d)).lower() in ('1','true','yes')
def _env_float(k, d):
    try: return float(os.environ.get(k, d))
    except: return d
def _env_str(k, d): return os.environ.get(k, d)

ENABLED      = _env_bool('WEEKLY_MOM_ENABLED', False)
LIVE_MODE    = _env_bool('LIVE_MODE', False)           # False = demo-fapi
POS_USDT     = _env_float('WEEKLY_MOM_USDT', 200.0)
K            = int(_env_float('WEEKLY_MOM_K', 5))
LOOKBACK     = int(_env_float('WEEKLY_MOM_LOOKBACK', 14))
MA_PERIOD    = int(_env_float('WEEKLY_MOM_MA', 50))
STATE_FILE   = Path(_env_str('WEEKLY_MOM_STATE', 'weekly_mom_state.json'))
LOG_FILE     = Path(_env_str('WEEKLY_MOM_LOG', 'logs/weekly_mom_trades.csv'))

BASE_URL  = 'https://fapi.binance.com'       if LIVE_MODE else 'https://demo-fapi.binance.com'

UNIVERSE = [s.strip() for s in _env_str('WEEKLY_MOM_UNIVERSE', ','.join([
    'BTCUSDT','ETHUSDT','BNBUSDT','SOLUSDT','AVAXUSDT',
    'DOTUSDT','LINKUSDT','AAVEUSDT','UNIUSDT','CRVUSDT',
    'MKRUSDT','COMPUSDT','SUSHIUSDT','BALUSDT',
    'GMXUSDT','LDOUSDT','OPUSDT','ARBUSDT',
    'INJUSDT','APTUSDT','DYDXUSDT',
    'NEARUSDT','FTMUSDT',
    '1000PEPEUSDT','WLDUSDT','ORDIUSDT',
    'PERPUSDT','RDNTUSDT','BLURUSDT',
])).split(',') if s.strip()]

# ── HMAC signing (mirrors live_execution.py) ──────────────────────────────────
def _api_key():    return os.environ.get('BINANCE_API_KEY', '')
def _api_secret(): return os.environ.get('BINANCE_API_SECRET', '')

def _plain(v):
    if isinstance(v, float):
        return format(Decimal(str(v)), 'f')
    return v

def _sign(params: dict) -> str:
    q = urllib.parse.urlencode(params)
    return hmac.new(_api_secret().encode(), q.encode(), hashlib.sha256).hexdigest()

def _prepare(params: dict) -> dict:
    params = {k: _plain(v) for k, v in params.items()}
    params.setdefault('recvWindow', 5000)
    params['timestamp'] = int(time.time() * 1000)
    params['signature'] = _sign(params)
    return params

def _headers(): return {'X-MBX-APIKEY': _api_key()}

def _get(path, params=None):
    p = _prepare(params or {})
    try:
        r = requests.get(BASE_URL + path, headers=_headers(), params=p, timeout=10)
        return r.json()
    except Exception as e:
        log.error(f'GET {path}: {e}'); return {}

def _post(path, params):
    p = _prepare(params)
    try:
        r = requests.post(BASE_URL + path, headers=_headers(), params=p, timeout=10)
        return r.json()
    except Exception as e:
        log.error(f'POST {path}: {e}'); return {}

def _delete(path, params):
    p = _prepare(params)
    try:
        r = requests.delete(BASE_URL + path, headers=_headers(), params=p, timeout=10)
        return r.json()
    except Exception as e:
        log.error(f'DELETE {path}: {e}'); return {}

# ── Exchange info (lot size) ───────────────────────────────────────────────────
_qty_prec: dict = {}
_price_prec: dict = {}

def _load_exchange_info():
    global _qty_prec, _price_prec
    if _qty_prec: return
    try:
        r = requests.get(BASE_URL + '/fapi/v1/exchangeInfo', timeout=15).json()
        for s in r.get('symbols', []):
            sym = s['symbol']
            for f in s.get('filters', []):
                if f['filterType'] == 'LOT_SIZE':
                    step = f['stepSize'].rstrip('0') or '1'
                    _qty_prec[sym] = len(step.split('.')[-1]) if '.' in step else 0
                if f['filterType'] == 'PRICE_FILTER':
                    tick = f['tickSize'].rstrip('0') or '1'
                    _price_prec[sym] = len(tick.split('.')[-1]) if '.' in tick else 0
        log.info(f'Exchange info loaded for {len(_qty_prec)} symbols')
    except Exception as e:
        log.error(f'exchangeInfo failed: {e}')

def _round_qty(sym, qty):
    p = _qty_prec.get(sym, 3)
    f = 10 ** p
    return round(int(qty * f) / f, p)

# ── Market data ───────────────────────────────────────────────────────────────
def fetch_klines(sym, interval='1d', limit=60):
    """Fetch recent daily closes. Returns list of closes or []."""
    try:
        r = requests.get(
            BASE_URL + '/fapi/v1/klines',
            params={'symbol': sym, 'interval': interval, 'limit': limit},
            timeout=10
        ).json()
        return [float(c[4]) for c in r]   # index 4 = close price
    except Exception as e:
        log.warning(f'klines {sym}: {e}'); return []

def get_mark_price(sym):
    try:
        r = requests.get(BASE_URL + '/fapi/v1/premiumIndex',
                         params={'symbol': sym}, timeout=5).json()
        return float(r['markPrice'])
    except: return None

# ── Regime check ──────────────────────────────────────────────────────────────
def check_regime():
    """Returns (is_bear, btc_price, ma50, details_str)."""
    closes = fetch_klines('BTCUSDT', '1d', MA_PERIOD + 5)
    if len(closes) < MA_PERIOD:
        return False, 0, 0, 'insufficient BTC data'
    ma = sum(closes[-MA_PERIOD:]) / MA_PERIOD
    price = closes[-1]
    is_bear = price < ma
    return is_bear, price, ma, f'BTC={price:,.0f}  MA{MA_PERIOD}={ma:,.0f}'

# ── Ranking ───────────────────────────────────────────────────────────────────
def rank_universe():
    """
    Rank UNIVERSE by 14d return. Returns list of (sym, ret_pct) sorted worst first.
    Skips coins with insufficient data.
    """
    results = []
    for sym in UNIVERSE:
        closes = fetch_klines(sym, '1d', LOOKBACK + 3)
        if len(closes) < LOOKBACK + 1:
            log.warning(f'  {sym}: insufficient data ({len(closes)} bars)')
            continue
        ret = (closes[-1] / closes[-LOOKBACK - 1] - 1) * 100
        results.append((sym, ret))
        time.sleep(0.05)   # gentle rate limiting
    results.sort(key=lambda x: x[1])   # worst first
    return results

# ── Position management ───────────────────────────────────────────────────────
def get_open_positions():
    """Returns list of open positions as dicts."""
    r = _get('/fapi/v2/positionRisk')
    if not isinstance(r, list): return []
    return [p for p in r if abs(float(p.get('positionAmt', 0))) > 0]

def open_short(sym, usdt_notional):
    """Open a SHORT position via MARKET order."""
    mark = get_mark_price(sym)
    if not mark:
        log.error(f'  {sym}: cannot get mark price'); return None
    raw_qty = usdt_notional / mark
    qty = _round_qty(sym, raw_qty)
    if qty <= 0:
        log.error(f'  {sym}: qty rounds to 0 (mark={mark}, notional={usdt_notional})')
        return None
    log.info(f'  SHORT {sym}  qty={qty}  mark={mark:.4f}  notional≈${qty*mark:.0f}')
    r = _post('/fapi/v1/order', {
        'symbol':   sym,
        'side':     'SELL',
        'type':     'MARKET',
        'quantity': qty,
    })
    if 'orderId' in r:
        log.info(f'  ✅ {sym} short opened  orderId={r["orderId"]}')
    else:
        log.error(f'  ❌ {sym} open failed: {r}')
    return r

def close_position(sym, pos_amt):
    """Close a position by reversing it (MARKET order)."""
    qty = abs(float(pos_amt))
    qty = _round_qty(sym, qty)
    side = 'BUY' if float(pos_amt) < 0 else 'SELL'   # close short = BUY
    log.info(f'  CLOSE {sym}  qty={qty}  side={side}')
    r = _post('/fapi/v1/order', {
        'symbol':           sym,
        'side':             side,
        'type':             'MARKET',
        'quantity':         qty,
        'reduceOnly':       'true',
    })
    if 'orderId' in r:
        log.info(f'  ✅ {sym} closed  orderId={r["orderId"]}')
    else:
        log.error(f'  ❌ {sym} close failed: {r}')
    return r

def close_all():
    """Close all open positions managed by weekly_mom."""
    positions = get_open_positions()
    state = load_state()
    our_syms = set(state.get('positions', {}).keys())
    closed = []
    for p in positions:
        sym = p['symbol']
        if sym not in our_syms:
            log.info(f'  Skipping {sym} — not in weekly_mom state')
            continue
        r = close_position(sym, p['positionAmt'])
        if r and 'orderId' in r:
            closed.append(sym)
    return closed

# ── State persistence ─────────────────────────────────────────────────────────
def load_state():
    if STATE_FILE.exists():
        try: return json.loads(STATE_FILE.read_text())
        except: pass
    return {'positions': {}, 'rebalance_ts': None, 'regime_at_open': None}

def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, default=str))

# ── Trade log ─────────────────────────────────────────────────────────────────
def log_trade(event, sym, side, qty, price, note=''):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    header = not LOG_FILE.exists()
    with open(LOG_FILE, 'a') as f:
        if header:
            f.write('ts,event,symbol,side,qty,price,note\n')
        ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        f.write(f'{ts},{event},{sym},{side},{qty},{price},{note}\n')

# ── Commands ──────────────────────────────────────────────────────────────────
def cmd_check():
    """Print regime + rankings without placing orders."""
    print(f'\n{"="*60}')
    print(f'  Weekly Momentum — CHECK  ({datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")})')
    print(f'  Mode: {"LIVE" if LIVE_MODE else "DEMO"}  |  URL: {BASE_URL}')
    print(f'{"="*60}')

    is_bear, btc, ma, detail = check_regime()
    regime_str = '🔴 BEAR' if is_bear else '🟢 BULL'
    print(f'\n  Regime: {regime_str}  ({detail})')
    print(f'  Action: {"→ SHORT bottom {K}" if is_bear else "→ FLAT (no trades)"}')

    print(f'\n  Ranking (14d return, {len(UNIVERSE)} coins):')
    rankings = rank_universe()
    for i, (sym, ret) in enumerate(rankings):
        marker = '  ← SHORT' if is_bear and i < K else ''
        flag   = '🔴' if ret < 0 else '🟢'
        print(f'  {i+1:>2}. {flag} {sym:<18} {ret:>+7.2f}%{marker}')

    state = load_state()
    if state.get('positions'):
        print(f'\n  Current positions (from state):')
        for sym, info in state['positions'].items():
            entry  = info.get('entry_price', 0)
            mark   = get_mark_price(sym) or 0
            pnl_pct = (entry / mark - 1) * 100 if mark and entry else 0  # short P&L
            print(f'    {sym:<18} entry={entry:.4f}  mark={mark:.4f}  '
                  f'pnl≈{pnl_pct:+.2f}%')
        opened = state.get('rebalance_ts', 'unknown')
        print(f'  Opened: {opened}')


def cmd_rebalance(dry_run=False):
    """Close existing positions, open new ones if bear regime."""
    print(f'\n{"="*60}')
    print(f'  Weekly Momentum — REBALANCE  '
          f'({"DRY RUN  " if dry_run else ""}{"LIVE" if LIVE_MODE else "DEMO"})')
    print(f'{"="*60}\n')

    if not ENABLED and not dry_run:
        log.warning('WEEKLY_MOM_ENABLED=false — set to true to trade'); return

    _load_exchange_info()

    # Step 1: close existing positions
    state = load_state()
    if state.get('positions'):
        log.info('Step 1: closing existing positions')
        if not dry_run:
            closed = close_all()
            log.info(f'  Closed: {closed}')
        else:
            log.info(f'  [DRY] would close: {list(state["positions"].keys())}')
        state['positions'] = {}
        save_state(state)
    else:
        log.info('Step 1: no existing positions to close')

    # Step 2: check regime
    is_bear, btc, ma, detail = check_regime()
    log.info(f'Step 2: regime check — {"BEAR" if is_bear else "BULL"}  ({detail})')

    if not is_bear:
        log.info('  Regime is BULL — staying flat')
        state['regime_at_open'] = 'bull'
        state['rebalance_ts']   = datetime.now(timezone.utc).isoformat()
        save_state(state)
        return

    # Step 3: rank and select shorts
    log.info(f'Step 3: ranking {len(UNIVERSE)} coins by {LOOKBACK}d return')
    rankings = rank_universe()
    if len(rankings) < K:
        log.error(f'  Only {len(rankings)} coins with data, need {K}'); return

    shorts = rankings[:K]
    log.info(f'  Bottom {K}: {[(s, f"{r:+.2f}%") for s, r in shorts]}')

    # Step 4: open shorts
    log.info(f'Step 4: opening {K} short positions (${POS_USDT} each)')
    new_positions = {}
    for sym, ret in shorts:
        if dry_run:
            mark = get_mark_price(sym) or 0
            log.info(f'  [DRY] SHORT {sym}  ret={ret:+.2f}%  mark={mark:.4f}')
            new_positions[sym] = {'entry_price': mark, 'ret_at_open': ret}
            continue
        r = open_short(sym, POS_USDT)
        if r and 'orderId' in r:
            mark = get_mark_price(sym) or 0
            new_positions[sym] = {
                'entry_price':  mark,
                'ret_at_open':  ret,
                'order_id':     r['orderId'],
                'qty':          abs(float(r.get('executedQty', 0))),
            }
            log_trade('open', sym, 'SHORT',
                      new_positions[sym]['qty'], mark,
                      f'14d_ret={ret:.2f}%')
        time.sleep(0.2)

    state['positions']       = new_positions
    state['rebalance_ts']    = datetime.now(timezone.utc).isoformat()
    state['regime_at_open']  = 'bear'
    state['btc_at_open']     = btc
    state['ma_at_open']      = ma
    save_state(state)
    log.info(f'  State saved — {len(new_positions)} positions open')


def cmd_close():
    """Close all weekly_mom positions."""
    print(f'\n{"="*60}')
    print(f'  Weekly Momentum — CLOSE ALL')
    print(f'{"="*60}\n')
    _load_exchange_info()
    closed = close_all()
    state = load_state()
    state['positions'] = {}
    save_state(state)
    log.info(f'Closed: {closed}')


def cmd_status():
    """Show current positions and estimated P&L."""
    state = load_state()
    positions = state.get('positions', {})
    print(f'\n{"="*60}')
    print(f'  Weekly Momentum — STATUS  '
          f'({"LIVE" if LIVE_MODE else "DEMO"})')
    print(f'{"="*60}')
    print(f'  Last rebalance: {state.get("rebalance_ts", "never")}')
    is_bear, btc, ma, detail = check_regime()
    print(f'  Current regime: {"BEAR" if is_bear else "BULL"}  ({detail})')
    print()
    if not positions:
        print('  No open positions in state.')
        return

    total_pnl = 0
    print(f'  {"symbol":<18} {"entry":>9} {"mark":>9} {"pnl%":>7} {"pnl$":>8}')
    print(f'  {"-"*55}')
    for sym, info in positions.items():
        entry = float(info.get('entry_price', 0))
        mark  = get_mark_price(sym) or entry
        qty   = float(info.get('qty', POS_USDT / entry if entry else 0))
        # Short P&L: profit when price falls
        pnl_pct = (entry / mark - 1) * 100 if mark and entry else 0
        pnl_usd = qty * (entry - mark)
        total_pnl += pnl_usd
        flag = '✅' if pnl_pct > 0 else '❌'
        print(f'  {flag} {sym:<16} {entry:>9.4f} {mark:>9.4f} '
              f'{pnl_pct:>+6.2f}% {pnl_usd:>+7.2f}$')
    print(f'  {"-"*55}')
    print(f'  {"TOTAL":>36} {total_pnl:>+7.2f}$')


# ── Entry ─────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser(description='Weekly bear-regime momentum trader')
    ap.add_argument('--check',     action='store_true', help='Check regime + rankings')
    ap.add_argument('--rebalance', action='store_true', help='Close + reopen positions')
    ap.add_argument('--close',     action='store_true', help='Close all positions')
    ap.add_argument('--status',    action='store_true', help='Show positions + P&L')
    ap.add_argument('--dry-run',   action='store_true', help='Simulate without orders')
    args = ap.parse_args()

    if not any([args.check, args.rebalance, args.close, args.status]):
        ap.print_help(); sys.exit(1)

    if args.check:     cmd_check()
    if args.rebalance: cmd_rebalance(dry_run=args.dry_run)
    if args.close:     cmd_close()
    if args.status:    cmd_status()
