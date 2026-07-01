#!/usr/bin/env python3
"""
weekly_mom_dashboard.py — Web dashboard for weekly momentum strategy
Serves on port 8100. Auto-refreshes every 30s.
Run: python3 weekly_mom_dashboard.py
"""
import hashlib, hmac, json, os, time, urllib.parse
from datetime import datetime, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import requests

# ── Config (same as weekly_mom.py) ───────────────────────────────────────────
def _env_bool(k, d): return os.environ.get(k, str(d)).lower() in ('1','true','yes')
def _env_float(k, d):
    try: return float(os.environ.get(k, d))
    except: return d

LIVE_MODE  = _env_bool('LIVE_MODE', False)
BASE_URL   = 'https://fapi.binance.com' if LIVE_MODE else 'https://demo-fapi.binance.com'
STATE_FILE = Path(os.environ.get('WEEKLY_MOM_STATE', 'weekly_mom_state.json'))
LOG_FILE   = Path(os.environ.get('WEEKLY_MOM_LOG', 'logs/weekly_mom_trades.csv'))
MA_PERIOD  = int(_env_float('WEEKLY_MOM_MA', 50))
LOOKBACK   = int(_env_float('WEEKLY_MOM_LOOKBACK', 14))
PORT       = int(_env_float('WEEKLY_MOM_DASH_PORT', 8100))

UNIVERSE = [s.strip() for s in os.environ.get('WEEKLY_MOM_UNIVERSE', ','.join([
    'BTCUSDT','ETHUSDT','BNBUSDT','SOLUSDT','AVAXUSDT',
    'DOTUSDT','LINKUSDT','AAVEUSDT','UNIUSDT','CRVUSDT',
    'MKRUSDT','COMPUSDT','SUSHIUSDT','GMXUSDT','LDOUSDT',
    'OPUSDT','ARBUSDT','INJUSDT','APTUSDT','DYDXUSDT',
    'NEARUSDT','FTMUSDT','1000PEPEUSDT','WLDUSDT','ORDIUSDT',
    'PERPUSDT','RDNTUSDT','BLURUSDT',
])).split(',') if s.strip()]

# ── Auth ──────────────────────────────────────────────────────────────────────
def _plain(v): return format(Decimal(str(v)), 'f') if isinstance(v, float) else v
def _sign(p): return hmac.new(os.environ.get('BINANCE_API_SECRET','').encode(),
                               urllib.parse.urlencode(p).encode(), hashlib.sha256).hexdigest()
def _prepare(p):
    p = {k: _plain(v) for k,v in p.items()}
    p.setdefault('recvWindow', 5000)
    p['timestamp'] = int(time.time()*1000)
    p['signature'] = _sign(p)
    return p
def _headers(): return {'X-MBX-APIKEY': os.environ.get('BINANCE_API_KEY','')}
def _get(path, params=None):
    try:
        r = requests.get(BASE_URL+path, headers=_headers(),
                         params=_prepare(params or {}), timeout=8)
        return r.json()
    except: return {}

# ── Data fetchers ─────────────────────────────────────────────────────────────
def get_regime():
    try:
        r = requests.get(BASE_URL+'/fapi/v1/klines',
            params={'symbol':'BTCUSDT','interval':'1d','limit': MA_PERIOD+2},
            timeout=8).json()
        closes = [float(c[4]) for c in r]
        if len(closes) < MA_PERIOD: return None, 0, 0
        ma = sum(closes[-MA_PERIOD:]) / MA_PERIOD
        return closes[-1] < ma, closes[-1], ma
    except: return None, 0, 0

def get_rankings():
    results = []
    for sym in UNIVERSE:
        try:
            r = requests.get(BASE_URL+'/fapi/v1/klines',
                params={'symbol':sym,'interval':'1d','limit':LOOKBACK+3},
                timeout=6).json()
            if not isinstance(r, list) or len(r) < LOOKBACK+1: continue
            closes = [float(c[4]) for c in r]
            ret = (closes[-1]/closes[-LOOKBACK-1]-1)*100
            results.append((sym, ret, closes[-1]))
        except: continue
        time.sleep(0.03)
    results.sort(key=lambda x: x[1])
    return results

def get_mark_prices(syms):
    prices = {}
    for sym in syms:
        try:
            r = requests.get(BASE_URL+'/fapi/v1/premiumIndex',
                             params={'symbol':sym}, timeout=4).json()
            prices[sym] = float(r['markPrice'])
        except: pass
    return prices

def load_state():
    if STATE_FILE.exists():
        try: return json.loads(STATE_FILE.read_text())
        except: pass
    return {'positions': {}, 'rebalance_ts': None}

def load_trade_log():
    if not LOG_FILE.exists(): return []
    try:
        lines = LOG_FILE.read_text().strip().split('\n')
        if len(lines) < 2: return []
        trades = []
        for line in lines[1:]:  # skip header
            parts = line.split(',')
            if len(parts) >= 6:
                trades.append({'ts': parts[0], 'event': parts[1], 'symbol': parts[2],
                               'side': parts[3], 'qty': parts[4], 'price': parts[5],
                               'note': parts[6] if len(parts)>6 else ''})
        return list(reversed(trades))  # newest first
    except: return []

def get_account_balance():
    r = _get('/fapi/v2/account')
    if isinstance(r, dict):
        return float(r.get('totalWalletBalance', 0)), float(r.get('totalUnrealizedProfit', 0))
    return 0, 0

# ── HTML generation ───────────────────────────────────────────────────────────
def build_html():
    is_bear, btc_price, ma50 = get_regime()
    state = load_state()
    positions = state.get('positions', {})
    rebal_ts  = state.get('rebalance_ts', '')
    marks = get_mark_prices(list(positions.keys())) if positions else {}
    balance, unrealised = get_account_balance()
    rankings = get_rankings()
    trades = load_trade_log()
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

    # Regime
    regime_color = '#ef4444' if is_bear else '#22c55e'
    regime_label = 'BEAR' if is_bear else 'BULL' if is_bear is False else '—'
    regime_action = f'SHORT bottom 5 coins' if is_bear else 'FLAT — no positions'

    # Positions P&L
    pos_rows = ''
    total_pnl = 0
    for sym, info in positions.items():
        entry = float(info.get('entry_price', 0))
        mark  = marks.get(sym, entry)
        qty   = float(info.get('qty', 0))
        ret_open = float(info.get('ret_at_open', 0))
        pnl_pct = (entry/mark - 1)*100 if mark and entry else 0
        pnl_usd = qty*(entry - mark) if qty else 0
        total_pnl += pnl_usd
        color = '#22c55e' if pnl_pct > 0 else '#ef4444'
        pos_rows += f'''<tr>
          <td class="sym">{sym.replace("USDT","")}</td>
          <td>{entry:.4f}</td>
          <td>{mark:.4f}</td>
          <td style="color:{color};font-weight:600">{pnl_pct:+.2f}%</td>
          <td style="color:{color};font-weight:600">{pnl_usd:+.2f}</td>
          <td class="dim">{ret_open:+.1f}%</td>
        </tr>'''

    if not pos_rows:
        pos_rows = '<tr><td colspan="6" class="empty">No open positions</td></tr>'

    # Rankings
    rank_rows = ''
    our_syms = set(positions.keys())
    for i, (sym, ret, price) in enumerate(rankings):
        is_short = sym in our_syms
        ret_color = '#ef4444' if ret < 0 else '#22c55e'
        marker = '<span class="badge">SHORT</span>' if is_short else ''
        rank_rows += f'''<tr class="{'active-row' if is_short else ''}">
          <td class="dim">{i+1}</td>
          <td class="sym">{sym.replace("USDT","")} {marker}</td>
          <td style="color:{ret_color};font-weight:600">{ret:+.2f}%</td>
          <td class="dim">{price:.4f}</td>
        </tr>'''

    # Trade log
    trade_rows = ''
    for t in trades[:20]:
        ev_color = '#22c55e' if t['event']=='open' else '#94a3b8'
        trade_rows += f'''<tr>
          <td class="dim">{t['ts']}</td>
          <td style="color:{ev_color}">{t['event'].upper()}</td>
          <td class="sym">{t['symbol'].replace("USDT","")}</td>
          <td class="dim">{t['note']}</td>
        </tr>'''
    if not trade_rows:
        trade_rows = '<tr><td colspan="4" class="empty">No trades yet</td></tr>'

    pnl_color = '#22c55e' if total_pnl >= 0 else '#ef4444'
    mode_badge = 'DEMO' if not LIVE_MODE else 'LIVE'
    mode_color = '#f59e0b' if not LIVE_MODE else '#22c55e'

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="30">
<title>Weekly Momentum</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@400;500;600&display=swap');

  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

  :root {{
    --bg:      #0a0e1a;
    --surface: #111827;
    --border:  #1e2d40;
    --text:    #e2e8f0;
    --dim:     #64748b;
    --accent:  #3b82f6;
    --red:     #ef4444;
    --green:   #22c55e;
    --mono:    'IBM Plex Mono', monospace;
    --sans:    'IBM Plex Sans', sans-serif;
  }}

  body {{ background: var(--bg); color: var(--text); font-family: var(--sans);
          font-size: 14px; line-height: 1.5; }}

  header {{ border-bottom: 1px solid var(--border); padding: 16px 24px;
             display: flex; align-items: center; gap: 16px; }}
  header h1 {{ font-family: var(--mono); font-size: 15px; font-weight: 600;
                letter-spacing: 0.05em; color: var(--accent); }}
  .mode-badge {{ font-family: var(--mono); font-size: 11px; font-weight: 600;
                  padding: 2px 8px; border-radius: 3px;
                  background: {mode_color}22; color: {mode_color};
                  border: 1px solid {mode_color}44; }}
  .ts {{ font-family: var(--mono); font-size: 11px; color: var(--dim); margin-left: auto; }}

  .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1px;
            background: var(--border); }}
  .grid-3 {{ grid-template-columns: 1fr 1fr 1fr; }}
  @media (max-width: 900px) {{ .grid, .grid-3 {{ grid-template-columns: 1fr; }} }}

  .card {{ background: var(--surface); padding: 20px 24px; }}
  .card-title {{ font-family: var(--mono); font-size: 10px; font-weight: 600;
                  letter-spacing: 0.12em; color: var(--dim);
                  text-transform: uppercase; margin-bottom: 16px; }}

  /* Regime card */
  .regime-val {{ font-family: var(--mono); font-size: 32px; font-weight: 600;
                  color: {regime_color}; line-height: 1; }}
  .regime-sub {{ font-family: var(--mono); font-size: 12px; color: var(--dim);
                  margin-top: 6px; }}
  .regime-action {{ font-size: 12px; color: var(--dim); margin-top: 12px;
                     padding: 8px 12px; background: {regime_color}11;
                     border-left: 2px solid {regime_color}; border-radius: 2px; }}

  /* Stat cards */
  .stat-val {{ font-family: var(--mono); font-size: 24px; font-weight: 600; }}
  .stat-sub {{ font-family: var(--mono); font-size: 11px; color: var(--dim); margin-top: 4px; }}

  /* Tables */
  table {{ width: 100%; border-collapse: collapse; font-family: var(--mono); font-size: 12px; }}
  th {{ text-align: left; color: var(--dim); font-size: 10px; letter-spacing: 0.08em;
        text-transform: uppercase; padding: 0 8px 10px; font-weight: 600; }}
  td {{ padding: 7px 8px; border-top: 1px solid var(--border); }}
  tr:first-child td {{ border-top: none; }}
  .sym {{ font-weight: 600; color: var(--text); }}
  .dim {{ color: var(--dim); }}
  .empty {{ color: var(--dim); text-align: center; padding: 20px; }}
  .active-row {{ background: #3b82f611; }}
  .badge {{ font-size: 9px; font-weight: 600; padding: 1px 5px;
             background: #ef444422; color: #ef4444;
             border: 1px solid #ef444444; border-radius: 2px;
             vertical-align: middle; margin-left: 4px; }}

  .full-width {{ grid-column: 1 / -1; }}
  .refresh-note {{ font-family: var(--mono); font-size: 10px; color: var(--dim);
                    text-align: center; padding: 12px; }}
</style>
</head>
<body>

<header>
  <h1>WEEKLY MOMENTUM</h1>
  <span class="mode-badge">{mode_badge}</span>
  <span class="ts">auto-refresh 30s &nbsp;·&nbsp; {now}</span>
</header>

<div class="grid grid-3" style="margin-bottom:1px">

  <div class="card">
    <div class="card-title">Regime</div>
    <div class="regime-val">{regime_label}</div>
    <div class="regime-sub">BTC {btc_price:,.0f} &nbsp;/&nbsp; MA{MA_PERIOD} {ma50:,.0f}</div>
    <div class="regime-action">{regime_action}</div>
  </div>

  <div class="card">
    <div class="card-title">Positions P&L</div>
    <div class="stat-val" style="color:{pnl_color}">{total_pnl:+.2f} USDT</div>
    <div class="stat-sub">{len(positions)} open &nbsp;·&nbsp; unrealised</div>
    <div class="stat-sub" style="margin-top:8px">last rebalance<br>{rebal_ts[:19] if rebal_ts else '—'}</div>
  </div>

  <div class="card">
    <div class="card-title">Account</div>
    <div class="stat-val">{balance:,.2f} USDT</div>
    <div class="stat-sub">wallet balance</div>
    <div class="stat-val" style="font-size:16px;margin-top:12px;
         color:{'#22c55e' if unrealised>=0 else '#ef4444'}">{unrealised:+.2f} USDT</div>
    <div class="stat-sub">total unrealised</div>
  </div>

</div>

<div class="grid" style="margin-bottom:1px">

  <div class="card">
    <div class="card-title">Open Positions</div>
    <table>
      <thead><tr>
        <th>Symbol</th><th>Entry</th><th>Mark</th><th>P&L %</th><th>P&L $</th><th>14d ret</th>
      </tr></thead>
      <tbody>{pos_rows}</tbody>
    </table>
  </div>

  <div class="card">
    <div class="card-title">Universe Ranking ({LOOKBACK}d return)</div>
    <table>
      <thead><tr><th>#</th><th>Symbol</th><th>Return</th><th>Price</th></tr></thead>
      <tbody>{rank_rows}</tbody>
    </table>
  </div>

</div>

<div class="grid">
  <div class="card full-width">
    <div class="card-title">Trade Log</div>
    <table>
      <thead><tr><th>Time</th><th>Event</th><th>Symbol</th><th>Note</th></tr></thead>
      <tbody>{trade_rows}</tbody>
    </table>
  </div>
</div>

<div class="refresh-note">auto-refreshes every 30s &nbsp;·&nbsp; weekly_mom_dashboard.py</div>
</body>
</html>'''

# ── HTTP Server ───────────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ('/', '/index.html'):
            self.send_response(404); self.end_headers(); return
        try:
            html = build_html().encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', len(html))
            self.end_headers()
            self.wfile.write(html)
        except Exception as e:
            self.send_response(500); self.end_headers()
            self.wfile.write(str(e).encode())

    def log_message(self, fmt, *args):
        pass  # suppress per-request logs

if __name__ == '__main__':
    print(f'Weekly Momentum Dashboard → http://<REDACTED_IP>:{PORT}')
    print(f'Mode: {"LIVE" if LIVE_MODE else "DEMO"}  |  {BASE_URL}')
    HTTPServer(('0.0.0.0', PORT), Handler).serve_forever()
