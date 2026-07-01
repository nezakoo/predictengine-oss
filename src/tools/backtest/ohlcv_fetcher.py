"""
PredictEngine — OHLCV + Market Data Fetcher
=============================================
Fetches all Binance USDT-M futures data needed for walk-forward replay.
Stores everything under ohlcv_cache/ as CSV files.

Data fetched per symbol:
  SYMBOL_1m.csv          — 1m klines with taker buy/sell volume (real VPIN input)
  SYMBOL_funding.csv     — funding rates every 8h (full history)
  SYMBOL_oi_5m.csv       — open interest every 5m (30d retention)
  SYMBOL_takerflow_5m.csv — taker buy/sell volume buckets 5m (30d retention)
  SYMBOL_lsr_5m.csv      — long/short account ratio 5m (30d retention)
  SYMBOL_top_lsr_5m.csv  — top-trader long/short ratio 5m (30d retention)

Usage:
  python ohlcv_fetcher.py                      # all default symbols, 30 days
  python ohlcv_fetcher.py --days 7             # last 7 days
  python ohlcv_fetcher.py --symbols WLDUSDT BTCUSDT
  python ohlcv_fetcher.py --refresh            # force re-download
  python ohlcv_fetcher.py --status             # show cache summary
  python ohlcv_fetcher.py --klines-only        # skip auxiliary data
  python ohlcv_fetcher.py --dry-run            # show plan without fetching

Rate budget: 2400 weight/min. Klines=10w, everything else=1w.
Full fetch of 26 symbols ≈ 11 minutes.
Incremental (daily top-up) ≈ 2 minutes.
"""

import csv
import json
import os
import sys
import time
import argparse
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ── Constants ─────────────────────────────────────────────────────────────────

BASE_URL   = 'https://fapi.binance.com'
DEMO_URL   = 'https://testnet.binancefuture.com'
DATA_URL   = 'https://fapi.binance.com'   # /futures/data/* on same host

_HERE = Path(__file__).parent
CACHE_DIR  = _HERE / 'ohlcv_cache'  # tools/backtest/ohlcv_cache

# Rate limiting — conservative to avoid 429
KLINE_DELAY   = 1.0 / 3.5   # klines = 10 weight, budget at 3.5/sec
AUX_DELAY     = 1.0 / 10.0  # aux endpoints = 1 weight, 10/sec safe
REQ_TIMEOUT   = 20

# Kline pagination
KLINE_LIMIT   = 1500         # max per request
KLINE_INTERVAL = '1m'   # overridden by --interval at runtime

# Interval → milliseconds per bar (for pagination cursor step)
_INTERVAL_MS = {
    '1m': 60_000, '3m': 180_000, '5m': 300_000, '15m': 900_000,
    '30m': 1_800_000, '1h': 3_600_000, '2h': 7_200_000, '4h': 14_400_000,
    '6h': 21_600_000, '8h': 28_800_000, '12h': 43_200_000,
    '1d': 86_400_000, '3d': 259_200_000, '1w': 604_800_000,
}
AUX_PERIOD    = '5m'
AUX_LIMIT     = 500          # max per aux request

# Default symbol universe — covers all active strategies
DEFAULT_SYMBOLS = [
    # Core B/W universe
    'WLDUSDT',  'HYPEUSDT', 'NEARUSDT', 'INJUSDT',  'ENAUSDT',
    'OPUSDT',   'APTUSDT',  'JTOUSDT',  'TONUSDT',  'ONDOUSDT',
    'ARBUSDT',  'TIAUSDT',  'SUIUSDT',  'JUPUSDT',  'PENDLEUSDT',
    'DASHUSDT', 'DYDXUSDT', 'NILUSDT',  'NOTUSDT',  '1000PEPEUSDT',
    'AVAXUSDT', 'STRKUSDT', 'WIFUSDT',  'GALAUSDT',
    # Required for W (BTC decorrelation signal)
    'BTCUSDT',
    # Extras for L/CGY
    'SOLUSDT',
]

# ── HTTP ──────────────────────────────────────────────────────────────────────

def _get(url: str, params: dict, timeout: int = REQ_TIMEOUT) -> object:
    full = f'{url}?{urllib.parse.urlencode(params)}'
    try:
        req = urllib.request.Request(full, headers={'User-Agent': 'PredictEngine/2.0'})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='replace')[:300]
        raise RuntimeError(f'HTTP {e.code} {url}: {body}')
    except Exception as e:
        raise RuntimeError(f'Request failed {url}: {e}')


# ── Cache helpers ─────────────────────────────────────────────────────────────

def _cache_path(symbol: str, suffix: str) -> Path:
    return CACHE_DIR / f'{symbol}_{suffix}.csv'

def _load_csv(path: Path, ts_field: str = 'ts_ms') -> list:
    """Load CSV, return list of dicts sorted by ts_field."""
    if not path.exists():
        return []
    rows = []
    try:
        with open(path, newline='') as fh:
            for row in csv.DictReader(fh):
                try:
                    row[ts_field] = int(row[ts_field])
                    rows.append(row)
                except (KeyError, ValueError):
                    pass
    except Exception:
        return []
    rows.sort(key=lambda r: r[ts_field])
    return rows

def _save_csv(path: Path, rows: list, fieldnames: list) -> None:
    CACHE_DIR.mkdir(exist_ok=True)
    rows.sort(key=lambda r: r[fieldnames[0]])
    with open(path, 'w', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)

def _cache_bounds(rows: list, ts_field: str = 'ts_ms') -> tuple:
    """Return (oldest_ts, newest_ts) or (None, None)."""
    if not rows:
        return None, None
    return rows[0][ts_field], rows[-1][ts_field]

def _merge(existing: list, new_rows: list, ts_field: str = 'ts_ms') -> list:
    """Merge two lists, deduplicate by ts_field (new_rows wins), sort."""
    merged = {r[ts_field]: r for r in existing}
    merged.update({r[ts_field]: r for r in new_rows})
    return sorted(merged.values(), key=lambda r: r[ts_field])


# ── Klines (1m OHLCV + taker volume) ─────────────────────────────────────────

KLINE_FIELDS = [
    'ts_ms', 'open', 'high', 'low', 'close', 'volume', 'quote_volume',
    'taker_buy_base_vol', 'taker_buy_quote_vol', 'n_trades',
]

def _parse_klines(raw: list) -> list:
    """
    Binance klines response columns:
    0=open_time 1=open 2=high 3=low 4=close 5=volume 6=close_time
    7=quote_asset_volume 8=num_trades 9=taker_buy_base 10=taker_buy_quote 11=ignore
    """
    out = []
    for k in raw:
        try:
            out.append({
                'ts_ms':               int(k[0]),
                'open':                k[1],
                'high':                k[2],
                'low':                 k[3],
                'close':               k[4],
                'volume':              k[5],
                'quote_volume':        k[7],
                'taker_buy_base_vol':  k[9],
                'taker_buy_quote_vol': k[10],
                'n_trades':            k[8],
            })
        except (IndexError, ValueError, TypeError):
            pass
    return out

def _fetch_klines_range(symbol: str, start_ms: int, end_ms: int,
                         base_url: str) -> list:
    """Paginate klines from start_ms to end_ms."""
    bar_ms  = _INTERVAL_MS.get(KLINE_INTERVAL, 60_000)
    candles = []
    cursor  = start_ms
    while cursor < end_ms:
        params = {
            'symbol':    symbol,
            'interval':  KLINE_INTERVAL,
            'startTime': cursor,
            'endTime':   min(end_ms, cursor + KLINE_LIMIT * bar_ms),
            'limit':     KLINE_LIMIT,
        }
        batch = _parse_klines(_get(base_url + '/fapi/v1/klines', params))
        if not batch:
            break
        candles.extend(batch)
        cursor = batch[-1]['ts_ms'] + bar_ms
        if len(batch) < KLINE_LIMIT:
            break
        time.sleep(KLINE_DELAY)
    return candles

def fetch_klines(symbol: str, days: int, base_url: str,
                  refresh: bool = False) -> tuple:
    """Incremental klines fetch. Returns (candles, n_new, n_cached)."""
    bar_ms   = _INTERVAL_MS.get(KLINE_INTERVAL, 60_000)
    path     = _cache_path(symbol, KLINE_INTERVAL)   # e.g. BTCUSDT_1d.csv
    now_ms   = int(time.time() * 1000)
    start_ms = (now_ms - days * 86_400_000) // bar_ms * bar_ms
    existing = [] if refresh else _load_csv(path)
    _, cache_new = _cache_bounds(existing)

    new_candles = []

    # Tail gap (catch up to now)
    tail_start = (cache_new + bar_ms) if cache_new else start_ms
    if tail_start < now_ms - 2 * bar_ms:
        new_candles.extend(_fetch_klines_range(symbol, tail_start, now_ms, base_url))
        time.sleep(KLINE_DELAY)

    # Head gap (extend lookback)
    cache_old, _ = _cache_bounds(existing)
    if cache_old and cache_old > start_ms:
        head = _fetch_klines_range(symbol, start_ms, cache_old - bar_ms, base_url)
        new_candles.extend(head)
        time.sleep(KLINE_DELAY)

    if not new_candles and existing:
        return existing, 0, len(existing)

    merged = _merge(existing, new_candles)
    # Trim to window
    merged = [c for c in merged if c['ts_ms'] >= start_ms]
    _save_csv(path, merged, KLINE_FIELDS)
    return merged, len(new_candles), len(existing)


# ── Funding rates ─────────────────────────────────────────────────────────────

FUNDING_FIELDS = ['ts_ms', 'fundingRate', 'markPrice']

def _parse_funding(raw: list) -> list:
    out = []
    for r in raw:
        try:
            out.append({
                'ts_ms':       int(r['fundingTime']),
                'fundingRate': r['fundingRate'],
                'markPrice':   r.get('markPrice', ''),
            })
        except (KeyError, ValueError, TypeError):
            pass
    return out

def fetch_funding(symbol: str, days: int, base_url: str,
                   refresh: bool = False) -> tuple:
    """Fetch funding rate history. Full history available."""
    path     = _cache_path(symbol, 'funding')
    now_ms   = int(time.time() * 1000)
    start_ms = now_ms - days * 86_400_000
    existing = [] if refresh else _load_csv(path)
    _, cache_new = _cache_bounds(existing)
    fetch_start  = (cache_new + 1) if cache_new else start_ms

    if fetch_start >= now_ms - 28_800_000:  # less than 8h gap
        return existing, 0, len(existing)

    new_rows = []
    cursor   = fetch_start
    while cursor < now_ms:
        params = {
            'symbol':    symbol,
            'startTime': cursor,
            'endTime':   now_ms,
            'limit':     1000,
        }
        batch = _parse_funding(_get(base_url + '/fapi/v1/fundingRate', params))
        if not batch:
            break
        new_rows.extend(batch)
        cursor = batch[-1]['ts_ms'] + 1
        if len(batch) < 1000:
            break
        time.sleep(AUX_DELAY)

    if not new_rows:
        return existing, 0, len(existing)

    merged = _merge(existing, new_rows)
    _save_csv(path, merged, FUNDING_FIELDS)
    return merged, len(new_rows), len(existing)


# ── Auxiliary 5m data (OI, taker flow, LSR) ──────────────────────────────────

AUX_ENDPOINTS = {
    'oi_5m': {
        'path':   '/futures/data/openInterestHist',
        'fields': ['ts_ms', 'sumOpenInterest', 'sumOpenInterestValue'],
        'parser': lambda r: {
            'ts_ms':              int(r['timestamp']),
            'sumOpenInterest':    r['sumOpenInterest'],
            'sumOpenInterestValue': r['sumOpenInterestValue'],
        },
    },
    'takerflow_5m': {
        'path':   '/futures/data/takervBuySellVol',
        'fields': ['ts_ms', 'buySellRatio', 'buyVol', 'sellVol'],
        'parser': lambda r: {
            'ts_ms':       int(r['timestamp']),
            'buySellRatio': r['buySellRatio'],
            'buyVol':       r['buyVol'],
            'sellVol':      r['sellVol'],
        },
    },
    'lsr_5m': {
        'path':   '/futures/data/globalLongShortAccountRatio',
        'fields': ['ts_ms', 'longShortRatio', 'longAccount', 'shortAccount'],
        'parser': lambda r: {
            'ts_ms':          int(r['timestamp']),
            'longShortRatio': r['longShortRatio'],
            'longAccount':    r['longAccount'],
            'shortAccount':   r['shortAccount'],
        },
    },
    'top_lsr_5m': {
        'path':   '/futures/data/topLongShortPositionRatio',
        'fields': ['ts_ms', 'longShortRatio', 'longAccount', 'shortAccount'],
        'parser': lambda r: {
            'ts_ms':          int(r['timestamp']),
            'longShortRatio': r['longShortRatio'],
            'longAccount':    r['longAccount'],
            'shortAccount':   r['shortAccount'],
        },
    },
}

def fetch_aux(symbol: str, key: str, days: int, base_url: str,
               refresh: bool = False) -> tuple:
    """
    Fetch one auxiliary 5m dataset.
    Returns (rows, n_new, n_cached).
    Note: /futures/data/* endpoints have 30-day retention only.
    """
    ep       = AUX_ENDPOINTS[key]
    path     = _cache_path(symbol, key)
    now_ms   = int(time.time() * 1000)
    # Clamp to 29 days (30d retention limit with safety margin)
    lookback = min(days, 29)
    start_ms = now_ms - lookback * 86_400_000

    existing = [] if refresh else _load_csv(path)
    _, cache_new = _cache_bounds(existing)
    fetch_start  = (cache_new + 1) if cache_new else start_ms

    if fetch_start >= now_ms - 600_000:  # less than 10min gap
        return existing, 0, len(existing)

    new_rows = []
    cursor   = fetch_start
    interval_ms = 300_000   # 5m in ms
    batch_span  = AUX_LIMIT * interval_ms

    while cursor < now_ms:
        params = {
            'symbol':    symbol,
            'period':    AUX_PERIOD,
            'startTime': cursor,
            'endTime':   min(now_ms, cursor + batch_span),
            'limit':     AUX_LIMIT,
        }
        try:
            raw = _get(base_url + ep['path'], params)
        except RuntimeError as e:
            # Some symbols don't have aux data (e.g. newer listings)
            if '400' in str(e) or '404' in str(e):
                break
            raise
        if not raw:
            break
        try:
            batch = [ep['parser'](r) for r in raw]
        except (KeyError, TypeError):
            break
        new_rows.extend(batch)
        if not batch:
            break
        cursor = batch[-1]['ts_ms'] + interval_ms
        if len(batch) < AUX_LIMIT:
            break
        time.sleep(AUX_DELAY)

    if not new_rows:
        return existing, 0, len(existing)

    merged = _merge(existing, new_rows)
    merged = [r for r in merged if r['ts_ms'] >= start_ms]
    _save_csv(path, merged, ep['fields'])
    return merged, len(new_rows), len(existing)


# ── Per-symbol fetch orchestrator ─────────────────────────────────────────────

def fetch_symbol_all(symbol: str, days: int, base_url: str,
                      refresh: bool = False, klines_only: bool = False,
                      verbose: bool = True) -> dict:
    """
    Fetch all data for one symbol. Returns dict of {key: (n_new, n_cached)}.
    """
    results = {}

    # 1. Klines (most important, highest weight)
    try:
        candles, n_new, n_cached = fetch_klines(symbol, days, base_url, refresh)
        results['1m'] = (n_new, n_cached)
    except RuntimeError as e:
        if verbose:
            print(f'  [WARN] {symbol} klines: {e}', file=sys.stderr)
        results['1m'] = (0, 0)

    if klines_only:
        return results

    # 2. Funding rates (very light, full history)
    try:
        _, n_new, n_cached = fetch_funding(symbol, days, base_url, refresh)
        results['funding'] = (n_new, n_cached)
    except RuntimeError as e:
        if verbose:
            print(f'  [WARN] {symbol} funding: {e}', file=sys.stderr)
        results['funding'] = (0, 0)
    time.sleep(AUX_DELAY)

    # 3. Auxiliary 5m data (30d retention)
    for key in AUX_ENDPOINTS:
        try:
            _, n_new, n_cached = fetch_aux(symbol, key, days, base_url, refresh)
            results[key] = (n_new, n_cached)
        except RuntimeError as e:
            if verbose:
                print(f'  [WARN] {symbol} {key}: {e}', file=sys.stderr)
            results[key] = (0, 0)
        time.sleep(AUX_DELAY)

    return results


# ── Status reporter ───────────────────────────────────────────────────────────

def cache_summary(symbols: list) -> None:
    RESET = '\033[0m'; BOLD = '\033[1m'; CYAN = '\033[96m'
    GREEN = '\033[92m'; YELLOW = '\033[93m'; DIM = '\033[2m'

    keys = ['1m', 'funding', 'oi_5m', 'takerflow_5m', 'lsr_5m', 'top_lsr_5m']

    print(f'\n{BOLD}{CYAN}  OHLCV Cache Summary{RESET}')
    print(f'  {"Symbol":<18} {"1m klines":>10} {"funding":>9} '
          f'{"oi_5m":>8} {"taker":>7} {"lsr":>6} {"top_lsr":>8} {"span":>6}')
    print(f'  {"─"*17}  {"─"*9}  {"─"*8}  {"─"*7}  {"─"*6}  {"─"*5}  {"─"*7}  {"─"*5}')

    total = 0
    for sym in sorted(symbols):
        row_data = {}
        span_days = 0
        for key in keys:
            suffix = '1m' if key == '1m' else key
            rows = _load_csv(_cache_path(sym, suffix))
            row_data[key] = len(rows)
            if key == '1m' and rows:
                old_ts, new_ts = rows[0]['ts_ms'], rows[-1]['ts_ms']
                span_days = (new_ts - old_ts) / 86_400_000
            total += len(rows)

        col = GREEN if span_days >= 25 else (YELLOW if span_days > 0 else DIM)
        print(f'  {sym:<18} {row_data["1m"]:>10,} {row_data["funding"]:>9,} '
              f'{row_data["oi_5m"]:>8,} {row_data["takerflow_5m"]:>7,} '
              f'{row_data["lsr_5m"]:>6,} {row_data["top_lsr_5m"]:>8,} '
              f'{col}{span_days:>5.0f}d{RESET}')

    print(f'\n  Total rows cached: {total:,}')



# ── aggTrades fetching (for B strategy microburst signal) ─────────────────────

AGG_FIELDS = ['ts_ms', 'price', 'qty', 'is_buyer_maker']
AGG_LIMIT   = 1000    # max per request
AGG_WEIGHT  = 20      # weight per request — heavier than klines
AGG_DELAY   = 1.0 / 2.0  # conservative: 2 req/sec for 20-weight calls


def _parse_agg_trades(raw: list) -> list:
    """
    Binance aggTrades response:
    {"a": aggId, "p": "price", "q": "qty", "f": firstId, "l": lastId,
     "T": timestamp_ms, "m": isBuyerMaker}
    """
    out = []
    for t in raw:
        try:
            out.append({
                'ts_ms':           int(t['T']),
                'price':           t['p'],
                'qty':             t['q'],
                'is_buyer_maker':  '1' if t['m'] else '0',
            })
        except (KeyError, ValueError, TypeError):
            pass
    return out


def _agg_cache_path(symbol: str, date_str: str) -> Path:
    """Cache path: ohlcv_cache/agg/SYMBOL/YYYYMMDD.csv"""
    d = CACHE_DIR / 'agg' / symbol
    d.mkdir(parents=True, exist_ok=True)
    return d / f'{date_str}.csv'


def _load_agg_day(symbol: str, date_str: str) -> list:
    p = _agg_cache_path(symbol, date_str)
    return _load_csv(p) if p.exists() else []


def _save_agg_day(symbol: str, date_str: str, rows: list) -> None:
    p = _agg_cache_path(symbol, date_str)
    _save_csv(p, rows, AGG_FIELDS)


def fetch_agg_trades(symbol: str, days: int, base_url: str,
                      refresh: bool = False, verbose: bool = True) -> dict:
    """
    Fetch aggTrades for last N days (max 7 due to Binance retention).
    Returns dict of {date_str: [trades]} for each day fetched.
    Saves per-day CSV files for incremental updates.
    """
    days = min(days, 7)   # hard cap at Binance retention limit
    now_ms   = int(time.time() * 1000)
    results  = {}
    n_new    = 0
    n_cached = 0

    for d in range(days):
        day_start_ms = now_ms - (d + 1) * 86_400_000
        day_end_ms   = now_ms - d * 86_400_000
        dt           = datetime.fromtimestamp(day_start_ms / 1000, tz=timezone.utc)
        date_str     = dt.strftime('%Y%m%d')

        existing = [] if refresh else _load_agg_day(symbol, date_str)
        if existing and not refresh:
            results[date_str] = existing
            n_cached += len(existing)
            continue

        day_trades = []
        cursor     = day_start_ms

        while cursor < day_end_ms:
            params = {
                'symbol':    symbol,
                'startTime': cursor,
                'endTime':   min(day_end_ms, cursor + AGG_LIMIT * 1000),  # ~1000 trades span
                'limit':     AGG_LIMIT,
            }
            try:
                raw = _get(base_url + '/fapi/v1/aggTrades', params)
            except RuntimeError as e:
                if verbose:
                    print(f'    [WARN] {symbol} aggTrades {date_str}: {e}', file=sys.stderr)
                break

            batch = _parse_agg_trades(raw)
            if not batch:
                break

            day_trades.extend(batch)
            cursor = batch[-1]['ts_ms'] + 1

            if len(batch) < AGG_LIMIT:
                break
            time.sleep(AGG_DELAY)

        if day_trades:
            _save_agg_day(symbol, date_str, day_trades)
            results[date_str] = day_trades
            n_new += len(day_trades)

    return results, n_new, n_cached


def load_agg_trades(symbol: str, start_ms: int, end_ms: int) -> list:
    """
    Load cached aggTrades for symbol between start_ms and end_ms.
    Returns list sorted by ts_ms asc.
    """
    rows = []
    agg_dir = CACHE_DIR / 'agg' / symbol
    if not agg_dir.exists():
        return []
    for p in sorted(agg_dir.glob('*.csv')):
        date_str = p.stem
        try:
            dt = datetime.strptime(date_str, '%Y%m%d').replace(tzinfo=timezone.utc)
            day_ms = int(dt.timestamp() * 1000)
            # Skip days entirely outside range
            if day_ms + 86_400_000 < start_ms or day_ms > end_ms:
                continue
        except ValueError:
            continue
        for row in _load_csv(p):
            ts = row['ts_ms']
            if start_ms <= ts <= end_ms:
                rows.append(row)
    rows.sort(key=lambda r: r['ts_ms'])
    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Fetch all Binance futures market data for replay',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Data fetched per symbol:
  1m klines with taker buy/sell volume (real VPIN)
  Funding rates (8h, full history)
  Open interest 5m (30d)
  Taker buy/sell volume 5m (30d)
  Long/short account ratio 5m (30d)
  Top-trader long/short ratio 5m (30d)

Examples:
  python ohlcv_fetcher.py
  python ohlcv_fetcher.py --days 7
  python ohlcv_fetcher.py --symbols WLDUSDT BTCUSDT HYPEUSDT
  python ohlcv_fetcher.py --refresh
  python ohlcv_fetcher.py --status
  python ohlcv_fetcher.py --klines-only
        """
    )
    parser.add_argument('--symbols',     nargs='+', default=None)
    parser.add_argument('--days',        type=int,  default=30)
    parser.add_argument('--interval',    default='1m',
                        choices=list(_INTERVAL_MS.keys()),
                        help='Kline interval (default: 1m). Use 1d for xsmom backtest.')
    parser.add_argument('--refresh',     action='store_true')
    parser.add_argument('--klines-only', action='store_true',
                        help='Skip funding/OI/LSR (only klines)')
    parser.add_argument('--agg-trades',  action='store_true',
                        help='Also fetch aggTrades (last 7 days, needed for B strategy replay)')
    parser.add_argument('--agg-days',    type=int, default=7,
                        help='Days of aggTrades to fetch (max 7, default 7)')
    parser.add_argument('--agg-symbols', nargs='+', default=None,
                        help='Symbols for aggTrades (default: same as --symbols)')
    parser.add_argument('--status',      action='store_true')
    parser.add_argument('--dry-run',     action='store_true')
    parser.add_argument('--demo',        action='store_true')
    parser.add_argument('--cache-dir',   default='ohlcv_cache')
    args = parser.parse_args()

    global CACHE_DIR
    CACHE_DIR = Path(args.cache_dir)

    base_url = DEMO_URL if args.demo else BASE_URL
    symbols  = args.symbols or DEFAULT_SYMBOLS

    global KLINE_INTERVAL
    KLINE_INTERVAL = args.interval

    RESET = '\033[0m'; BOLD = '\033[1m'; CYAN = '\033[96m'
    GREEN = '\033[92m'; YELLOW = '\033[93m'; DIM = '\033[2m'

    print(f'\n{BOLD}{CYAN}PredictEngine — Market Data Fetcher{RESET}')
    mode = 'klines only' if args.klines_only else 'full (klines + funding + OI + LSR)'
    print(f'{DIM}  endpoint={base_url}  days={args.days}'
          f'  symbols={len(symbols)}  mode={mode}{RESET}\n')

    if args.status:
        cache_summary(symbols)
        return

    if args.dry_run:
        now_ms   = int(time.time() * 1000)
        start_ms = now_ms - args.days * 86_400_000
        dt_from  = datetime.fromtimestamp(start_ms/1000, tz=timezone.utc).strftime('%Y-%m-%d')
        print(f'  Dry run — {len(symbols)} symbols from {dt_from}')
        keys = [KLINE_INTERVAL] if args.klines_only else [KLINE_INTERVAL, 'funding', 'oi_5m', 'takerflow_5m', 'lsr_5m', 'top_lsr_5m']
        for sym in symbols:
            parts = []
            for key in keys:
                suffix = KLINE_INTERVAL if key == KLINE_INTERVAL else key
                rows = _load_csv(_cache_path(sym, suffix))
                if rows and not args.refresh:
                    _, new_ts = _cache_bounds(rows)
                    gap_min = (now_ms - new_ts) // 60_000
                    parts.append(f'{key}:+{gap_min//60}h')
                else:
                    parts.append(f'{key}:FULL')
            print(f'  {sym:<18} {", ".join(parts)}')
        return

    CACHE_DIR.mkdir(exist_ok=True)
    t0 = time.time()
    failed = []

    for i, sym in enumerate(symbols):
        print(f'  [{i+1:>2}/{len(symbols)}] {sym:<18}', end=' ', flush=True)

        try:
            results = fetch_symbol_all(
                sym, args.days, base_url,
                refresh=args.refresh,
                klines_only=args.klines_only,
                verbose=True,
            )
            # Format summary line
            parts = []
            for key, (n_new, n_cached) in results.items():
                if n_new > 0:
                    parts.append(f'{GREEN}+{n_new:,}{RESET} {key}')
                else:
                    parts.append(f'{DIM}{n_cached:,} {key}{RESET}')
            print('  '.join(parts))

        except Exception as e:
            print(f'{YELLOW}ERROR: {e}{RESET}')
            failed.append(sym)
            time.sleep(5.0)

    # aggTrades fetch (separate loop — heavier, selective symbols)
    if args.agg_trades:
        agg_syms = args.agg_symbols or symbols
        print(f'\n  Fetching aggTrades for {len(agg_syms)} symbols (last {args.agg_days} days)...')
        print(f'  {DIM}Note: aggTrades are large (~50-200MB/sym/day). Only last 7 days available.{RESET}')
        for sym in agg_syms:
            print(f'  [{agg_syms.index(sym)+1:>2}/{len(agg_syms)}] {sym:<18}', end=' ', flush=True)
            try:
                _, n_new, n_cached = fetch_agg_trades(sym, args.agg_days, base_url,
                                                       refresh=args.refresh, verbose=True)
                status = (f'{GREEN}+{n_new:,} new{RESET}' if n_new > 0
                          else f'{DIM}{n_cached:,} cached{RESET}')
                print(f'  {status}')
            except Exception as e:
                print(f'{YELLOW}ERROR: {e}{RESET}')

    elapsed = time.time() - t0
    print(f'\n  Done in {elapsed:.0f}s')
    if failed:
        print(f'  {YELLOW}Failed: {", ".join(failed)}{RESET}')

    cache_summary(symbols)


if __name__ == '__main__':
    main()
