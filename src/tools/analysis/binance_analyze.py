#!/usr/bin/env python3
"""
binance_analyze.py — Pull trade history from Binance demo account and analyze.

Complements analyze.sh (which reads engine CSV logs) by pulling ground-truth
data directly from Binance: actual fills, realized PnL, commissions, slippage.

Usage:
  python3 binance_analyze.py                    # last 24h, JSON + HTML
  python3 binance_analyze.py --since 6h         # last 6 hours
  python3 binance_analyze.py --since 2h         # last 2 hours
  python3 binance_analyze.py --json-only        # skip HTML
  python3 binance_analyze.py --sym BTCUSDT      # single symbol
  python3 binance_analyze.py --compare logs/    # cross-ref with engine CSV logs dir

Reads from .env:
  BINANCE_API_KEY=...
  BINANCE_API_SECRET=...
  LIVE_MODE=false    # false = demo-fapi.binance.com, true = fapi.binance.com
"""

import os, sys, json, csv, hashlib, hmac, time, argparse, re, urllib.parse
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from pathlib import Path

# ── Load .env ─────────────────────────────────────────────────────────────────
def _load_env(path=".env"):
    env_path = Path(path)
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

_load_env()

API_KEY    = os.environ.get("BINANCE_API_KEY", "")
API_SECRET = os.environ.get("BINANCE_API_SECRET", "")
LIVE_MODE  = os.environ.get("LIVE_MODE", "false").lower() in ("1", "true", "yes")
BASE_URL   = "https://fapi.binance.com" if LIVE_MODE else "https://demo-fapi.binance.com"

if not API_KEY or not API_SECRET:
    print("❌  BINANCE_API_KEY / BINANCE_API_SECRET not set in .env or environment", file=sys.stderr)
    sys.exit(1)

# ── Signed REST ───────────────────────────────────────────────────────────────
import requests

def _sign(params: dict) -> str:
    qs = urllib.parse.urlencode(params)
    return hmac.new(API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()

def _get(path: str, params: dict | None = None) -> any:
    params = params or {}
    params["timestamp"] = int(time.time() * 1000)
    params["signature"] = _sign(params)
    r = requests.get(BASE_URL + path,
                     headers={"X-MBX-APIKEY": API_KEY},
                     params=params, timeout=10)
    return r.json()

# ── Since parsing ─────────────────────────────────────────────────────────────
def parse_since(s: str) -> datetime:
    s = s.strip().lower()
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*h(?:ours?)?", s)
    if m:
        return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=float(m.group(1)))
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*m(?:in(?:utes?)?)?", s)
    if m:
        return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=float(m.group(1)))
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    print(f"❌  Cannot parse --since: {s!r}", file=sys.stderr)
    sys.exit(1)

# ── Binance data fetchers ──────────────────────────────────────────────────────
def fetch_account_info() -> dict:
    return _get("/fapi/v2/account")

def fetch_balance() -> dict:
    """Return USDT balance dict."""
    data = _get("/fapi/v2/balance")
    if isinstance(data, list):
        for a in data:
            if a.get("asset") == "USDT":
                return a
    return {}

def fetch_income_history(start_ms: int, end_ms: int, income_type: str = None) -> list:
    """
    Pull income history (realized PnL, commission, funding).
    income_type: REALIZED_PNL | COMMISSION | FUNDING_FEE | None (all)
    Binance returns max 1000 rows per call — paginates automatically.
    """
    results = []
    params = {"startTime": start_ms, "endTime": end_ms, "limit": 1000}
    if income_type:
        params["incomeType"] = income_type
    while True:
        data = _get("/fapi/v1/income", params)
        if not isinstance(data, list) or not data:
            break
        results.extend(data)
        if len(data) < 1000:
            break
        # paginate: next batch starts after last item
        params["startTime"] = data[-1]["time"] + 1
        if params["startTime"] >= end_ms:
            break
    return results

def fetch_trade_history(sym: str, start_ms: int, end_ms: int) -> list:
    """Pull all user trades for a symbol in the time window."""
    results = []
    params = {"symbol": sym, "startTime": start_ms, "endTime": end_ms, "limit": 1000}
    while True:
        data = _get("/fapi/v1/userTrades", params)
        if not isinstance(data, list) or not data:
            break
        results.extend(data)
        if len(data) < 1000:
            break
        params["startTime"] = data[-1]["time"] + 1
        if params["startTime"] >= end_ms:
            break
    return results

def fetch_all_symbols_with_trades(start_ms: int, end_ms: int) -> list[str]:
    """
    Find symbols that had activity in the window.

    Strategy:
      1. Try /fapi/v1/income (REALIZED_PNL) — works on live, often empty on demo.
      2. Fallback: scan /fapi/v1/userTrades on every USDT-M symbol from exchangeInfo.
         Filters to symbols where the exchange actually returns fills — fast because
         Binance returns [] immediately for symbols with no trades.
    """
    # Method 1: income history (fast, works on live)
    income = fetch_income_history(start_ms, end_ms, "REALIZED_PNL")
    syms_from_income = list({row["symbol"] for row in income if row.get("symbol")})
    if syms_from_income:
        return syms_from_income

    # Method 2: demo fallback — use ENGINE coin list + BTC/ETH/SOL as universe
    # Much faster than scanning all 300+ Binance symbols
    print("   ℹ️  income API returned 0 (demo limitation) — scanning engine coin universe...", file=sys.stderr)
    engine_coins = _get_engine_coin_universe()
    found = []
    for sym in engine_coins:
        try:
            data = _get("/fapi/v1/userTrades", {
                "symbol": sym, "startTime": start_ms, "endTime": end_ms, "limit": 1
            })
            if isinstance(data, list) and len(data) > 0:
                found.append(sym)
        except Exception:
            pass
    return found

def _get_engine_coin_universe() -> list[str]:
    """
    Return the coin universe to scan when income API is unavailable.
    Reads DEFAULT_COINS from config.py if on the VPS, otherwise uses a
    hardcoded list of the most commonly traded symbols.
    """
    # Try reading from engine config.py in the same dir or parent
    for config_path in ["config.py", "../engine/config.py", "./engine/config.py"]:
        try:
            src = Path(config_path).read_text()
            m = re.search(r"DEFAULT_COINS\s*=\s*\[(.*?)\]", src, re.DOTALL)
            if m:
                coins = re.findall(r"'([A-Z0-9]+USDT)'", m.group(1))
                if coins:
                    return coins
        except Exception:
            pass
    # Hardcoded fallback: engine's active coin list
    return [
        "BTCUSDT","ETHUSDT","SOLUSDT","JTOUSDT","PENDLEUSDT","ONDOUSDT","SUIUSDT",
        "NEARUSDT","TONUSDT","NILUSDT","STRKUSDT","DYDXUSDT","OPUSDT","GALAUSDT",
        "JUPUSDT","FILUSDT","NOTUSDT","TIAUSDT","ARBUSDT","ENAUSDT","INJUSDT",
        "DASHUSDT","APTUSDT","WLDUSDT","1000BONKUSDT","DOTUSDT","HYPEUSDT",
        "WIFUSDT","MEWUSDT","TRUMPUSDT","1000PEPEUSDT",
    ]

def fetch_order_history(sym: str, start_ms: int, end_ms: int) -> list:
    """Pull all orders (filled, cancelled etc) for a symbol."""
    params = {"symbol": sym, "startTime": start_ms, "endTime": end_ms, "limit": 1000}
    data = _get("/fapi/v1/allOrders", params)
    return data if isinstance(data, list) else []

# ── Analysis ───────────────────────────────────────────────────────────────────
def ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)

def analyze_income(income_rows: list) -> dict:
    """
    Summarize income by type: realized PnL, commission, funding fees.
    Returns per-type totals and per-symbol breakdown.
    """
    by_type   = defaultdict(float)
    by_symbol = defaultdict(lambda: defaultdict(float))

    for row in income_rows:
        t   = row.get("incomeType", "OTHER")
        sym = row.get("symbol", "UNKNOWN")
        val = float(row.get("income", 0))
        by_type[t]          += val
        by_symbol[sym][t]   += val

    return {
        "totals":     {k: round(v, 6) for k, v in sorted(by_type.items())},
        "by_symbol":  {
            sym: {k: round(v, 6) for k, v in data.items()}
            for sym, data in sorted(by_symbol.items())
        },
    }

def build_trades_table(all_trades: dict[str, list], income_by_sym: dict) -> list[dict]:
    """
    Build a unified trade table from per-symbol trade lists.
    Groups fills into round-trip trades (entry fill → exit fill).
    Each row: sym, dir, open_time, close_time, entry_px, exit_px,
              qty, realized_pnl, commission, slippage_pct, dur_sec
    """
    table = []

    for sym, trades in all_trades.items():
        # Sort by time
        trades = sorted(trades, key=lambda t: t["time"])

        # Walk through fills, tracking running position
        pos_qty   = 0.0
        pos_dir   = None
        pos_entry_price = 0.0
        pos_open_time   = None
        pos_cost        = 0.0

        for fill in trades:
            qty    = float(fill["qty"])
            price  = float(fill["price"])
            side   = fill["side"]          # BUY or SELL
            reduce = fill.get("reduceOnly", False)
            t_ms   = fill["time"]
            pnl    = float(fill.get("realizedPnl", 0))
            comm   = float(fill.get("commission", 0))

            if reduce or (pos_dir == "long" and side == "SELL") or (pos_dir == "short" and side == "BUY"):
                # Closing fill
                if pos_qty > 0 and pos_open_time is not None:
                    dur = (ms_to_dt(t_ms) - pos_open_time).total_seconds()
                    slippage = ((price - pos_entry_price) / pos_entry_price * 100
                                if pos_dir == "long"
                                else (pos_entry_price - price) / pos_entry_price * 100)
                    table.append({
                        "sym":          sym,
                        "dir":          pos_dir,
                        "open_time":    pos_open_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "close_time":   ms_to_dt(t_ms).strftime("%Y-%m-%d %H:%M:%S"),
                        "entry_px":     round(pos_entry_price, 6),
                        "exit_px":      round(price, 6),
                        "qty":          round(qty, 6),
                        "realized_pnl": round(pnl, 6),
                        "commission":   round(comm, 6),
                        "slippage_pct": round(slippage, 4),
                        "dur_sec":      round(dur),
                        "pnl_pct":      round(pnl / (pos_entry_price * qty) * 100, 4) if pos_entry_price * qty > 0 else 0,
                    })
                pos_qty         = 0.0
                pos_dir         = None
                pos_entry_price = 0.0
                pos_open_time   = None
            else:
                # Opening fill
                if pos_qty == 0:
                    pos_dir         = "long" if side == "BUY" else "short"
                    pos_entry_price = price
                    pos_open_time   = ms_to_dt(t_ms)
                pos_qty += qty

    return table

def analyze_trades_table(table: list[dict]) -> dict:
    """Per-symbol and overall stats from the trades table."""
    overall = {
        "total_trades": 0, "wins": 0, "losses": 0,
        "total_pnl": 0.0, "total_commission": 0.0,
        "avg_dur_sec": 0.0, "avg_slippage_pct": 0.0,
        "best_trade": None, "worst_trade": None,
    }
    by_sym = defaultdict(lambda: {
        "trades": 0, "wins": 0, "losses": 0,
        "pnl": 0.0, "commission": 0.0,
        "avg_slippage_pct": 0.0, "_slippages": [],
    })

    durs = []
    slippages = []

    for t in table:
        sym  = t["sym"]
        pnl  = t["realized_pnl"]
        comm = t["commission"]
        slip = t["slippage_pct"]
        dur  = t["dur_sec"]

        overall["total_trades"]      += 1
        overall["total_pnl"]         += pnl
        overall["total_commission"]  += comm
        if pnl > 0: overall["wins"]   += 1
        if pnl < 0: overall["losses"] += 1
        durs.append(dur)
        slippages.append(slip)

        if overall["best_trade"]  is None or pnl > overall["best_trade"]["realized_pnl"]:
            overall["best_trade"]  = t
        if overall["worst_trade"] is None or pnl < overall["worst_trade"]["realized_pnl"]:
            overall["worst_trade"] = t

        by_sym[sym]["trades"]     += 1
        by_sym[sym]["pnl"]        += pnl
        by_sym[sym]["commission"] += comm
        if pnl > 0: by_sym[sym]["wins"]   += 1
        if pnl < 0: by_sym[sym]["losses"] += 1
        by_sym[sym]["_slippages"].append(slip)

    if durs:      overall["avg_dur_sec"]      = round(sum(durs) / len(durs))
    if slippages: overall["avg_slippage_pct"] = round(sum(slippages) / len(slippages), 4)

    overall["total_pnl"]        = round(overall["total_pnl"], 6)
    overall["total_commission"] = round(overall["total_commission"], 6)
    overall["wr_pct"]           = round(overall["wins"] / overall["total_trades"] * 100, 1) if overall["total_trades"] else 0

    sym_stats = {}
    for sym, d in by_sym.items():
        slips = d.pop("_slippages")
        d["pnl"]              = round(d["pnl"], 6)
        d["commission"]       = round(d["commission"], 6)
        d["avg_slippage_pct"] = round(sum(slips) / len(slips), 4) if slips else 0
        d["wr_pct"]           = round(d["wins"] / d["trades"] * 100, 1) if d["trades"] else 0
        sym_stats[sym] = d

    return {"overall": overall, "by_symbol": sym_stats}

# ── CSV log cross-reference ────────────────────────────────────────────────────
def load_engine_trades(logs_dir: str) -> list[dict]:
    """Load preds CSVs from engine logs dir for slippage cross-ref."""
    trades = []
    for f in Path(logs_dir).glob("preds*.csv"):
        try:
            lines = [l for l in Path(f).read_text().splitlines() if not l.startswith("# STRATEGY")]
            reader = csv.DictReader(lines)
            for row in reader:
                trades.append(row)
        except Exception:
            pass
    return trades

def cross_reference_slippage(binance_trades: list[dict], engine_trades: list[dict]) -> list[dict]:
    """
    Match Binance fills to engine preds by symbol + approximate time.
    Returns list of {sym, dir, engine_entry, binance_entry, slippage_pct, engine_exit, binance_exit}.
    """
    results = []
    for bt in binance_trades:
        sym      = bt["sym"]
        b_entry  = bt["entry_px"]
        b_exit   = bt["exit_px"]
        open_dt  = datetime.strptime(bt["open_time"], "%Y-%m-%d %H:%M:%S")

        # Find engine pred: same symbol, within 5s of Binance open
        best = None
        best_delta = timedelta(seconds=5)
        for et in engine_trades:
            if et.get("sym") != sym:
                continue
            try:
                e_ts = datetime.fromtimestamp(float(et.get("ts", 0)))
            except (ValueError, TypeError):
                continue
            delta = abs(e_ts - open_dt)
            if delta < best_delta:
                best = et
                best_delta = delta

        if best:
            try:
                e_entry = float(best.get("entry_px") or best.get("entry") or 0)
                e_exit  = float(best.get("exit_price") or 0)
                entry_slip = round((b_entry - e_entry) / e_entry * 100, 4) if e_entry else None
                exit_slip  = round((b_exit  - e_exit)  / e_exit  * 100, 4) if e_exit  else None
                results.append({
                    "sym":           sym,
                    "dir":           bt["dir"],
                    "open_time":     bt["open_time"],
                    "engine_entry":  e_entry,
                    "binance_entry": b_entry,
                    "entry_slip_pct": entry_slip,
                    "engine_exit":   e_exit,
                    "binance_exit":  b_exit,
                    "exit_slip_pct": exit_slip,
                    "realized_pnl":  bt["realized_pnl"],
                    "engine_match_delta_ms": round(best_delta.total_seconds() * 1000),
                })
            except Exception:
                pass

    return results

# ── HTML output ───────────────────────────────────────────────────────────────
def format_html(income: dict, trades_analysis: dict, trades_table: list,
                slippage_xref: list, since_label: str, balance: dict) -> str:
    overall = trades_analysis["overall"]
    by_sym  = trades_analysis["by_symbol"]

    pnl_color = "#7cfc00" if overall["total_pnl"] >= 0 else "#ff6b6b"

    rows_sym = ""
    for sym, d in sorted(by_sym.items(), key=lambda x: -abs(x[1]["pnl"])):
        c = "#7cfc00" if d["pnl"] >= 0 else "#ff6b6b"
        rows_sym += f"""<tr>
            <td>{sym.replace('USDT','')}</td>
            <td>{d['trades']}</td>
            <td style="color:{c}">{d['pnl']:+.4f}</td>
            <td>{d['wins']}/{d['losses']}</td>
            <td>{d['wr_pct']:.0f}%</td>
            <td>{d['commission']:.4f}</td>
            <td>{d['avg_slippage_pct']:+.4f}%</td>
        </tr>"""

    rows_trades = ""
    for t in sorted(trades_table, key=lambda x: x["open_time"], reverse=True)[:50]:
        c = "#7cfc00" if t["realized_pnl"] >= 0 else "#ff6b6b"
        dur = f"{t['dur_sec']//60}m{t['dur_sec']%60}s"
        rows_trades += f"""<tr>
            <td>{t['open_time'][11:]}</td>
            <td>{t['sym'].replace('USDT','')}</td>
            <td style="color:{'#7cfc00' if t['dir']=='long' else '#ff6b6b'}">{t['dir']}</td>
            <td>{t['entry_px']}</td>
            <td>{t['exit_px']}</td>
            <td>{t['qty']}</td>
            <td style="color:{c}">{t['realized_pnl']:+.4f}</td>
            <td>{t['commission']:.4f}</td>
            <td>{t['slippage_pct']:+.4f}%</td>
            <td>{dur}</td>
        </tr>"""

    rows_xref = ""
    for x in slippage_xref[:30]:
        es = f"{x['entry_slip_pct']:+.4f}%" if x['entry_slip_pct'] is not None else "—"
        xs = f"{x['exit_slip_pct']:+.4f}%"  if x['exit_slip_pct']  is not None else "—"
        rows_xref += f"""<tr>
            <td>{x['open_time'][11:]}</td>
            <td>{x['sym'].replace('USDT','')}</td>
            <td>{x['dir']}</td>
            <td>{x['engine_entry']}</td>
            <td>{x['binance_entry']}</td>
            <td>{es}</td>
            <td>{x['engine_exit'] or '—'}</td>
            <td>{x['binance_exit']}</td>
            <td>{xs}</td>
            <td>{x['engine_match_delta_ms']}ms</td>
        </tr>"""

    income_rows = ""
    for t, v in income["totals"].items():
        c = "#7cfc00" if v >= 0 else "#ff6b6b"
        income_rows += f"<tr><td>{t}</td><td style='color:{c}'>{v:+.6f} USDT</td></tr>"

    bal_wallet   = balance.get("balance", "N/A")
    bal_avail    = balance.get("availableBalance", "N/A")
    bal_unreal   = balance.get("unrealizedProfit", "N/A")

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Binance Demo Analysis</title>
<style>
  body {{ font-family: monospace; background: #0a0e27; color: #e0e0e0; padding: 20px; }}
  h1 {{ color: #00ff88; }} h2 {{ color: #88ccff; margin-top:2em; border-bottom:1px solid #333; padding-bottom:4px; }}
  table {{ width:100%; border-collapse:collapse; background:#1a1f3a; margin-bottom:1.5em; }}
  td,th {{ padding:7px 10px; border-bottom:1px solid #2a2f4a; text-align:left; font-size:0.9em; }}
  th {{ background:#0f1428; color:#88ccff; }}
  tr:nth-child(even) {{ background:#0f1428; }}
  .stat {{ display:inline-block; background:#1a1f3a; border:1px solid #2a2f4a;
           padding:12px 20px; margin:6px; border-radius:6px; min-width:140px; }}
  .stat-label {{ color:#888; font-size:0.8em; }}
  .stat-val {{ font-size:1.4em; font-weight:bold; }}
</style>
</head>
<body>
<h1>📊 Binance Demo Account Analysis</h1>
<p>Generated: {datetime.now(timezone.utc).replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S')} UTC &nbsp;|&nbsp;
   Window: since <strong>{since_label}</strong> &nbsp;|&nbsp;
   Mode: <strong>{'LIVE' if LIVE_MODE else 'DEMO'}</strong></p>

<h2>Account Balance</h2>
<div>
  <div class="stat"><div class="stat-label">Wallet Balance</div><div class="stat-val">{bal_wallet} USDT</div></div>
  <div class="stat"><div class="stat-label">Available</div><div class="stat-val">{bal_avail} USDT</div></div>
  <div class="stat"><div class="stat-label">Unrealized PnL</div><div class="stat-val">{bal_unreal} USDT</div></div>
</div>

<h2>Session Summary</h2>
<div>
  <div class="stat"><div class="stat-label">Total Trades</div><div class="stat-val">{overall['total_trades']}</div></div>
  <div class="stat"><div class="stat-label">Win Rate</div><div class="stat-val">{overall['wr_pct']:.0f}%</div></div>
  <div class="stat"><div class="stat-label">Realized PnL</div><div class="stat-val" style="color:{pnl_color}">{overall['total_pnl']:+.4f} USDT</div></div>
  <div class="stat"><div class="stat-label">Commission Paid</div><div class="stat-val" style="color:#ff6b6b">{overall['total_commission']:.4f} USDT</div></div>
  <div class="stat"><div class="stat-label">Avg Duration</div><div class="stat-val">{overall['avg_dur_sec']//60}m {overall['avg_dur_sec']%60}s</div></div>
  <div class="stat"><div class="stat-label">Avg Slippage</div><div class="stat-val">{overall['avg_slippage_pct']:+.4f}%</div></div>
</div>

<h2>Income Breakdown</h2>
<table><tr><th>Type</th><th>Total (USDT)</th></tr>
{income_rows}
</table>

<h2>Per-Symbol Performance</h2>
<table>
  <tr><th>Symbol</th><th>Trades</th><th>PnL (USDT)</th><th>W/L</th><th>WR%</th><th>Commission</th><th>Avg Slippage</th></tr>
  {rows_sym}
</table>

<h2>Trade Log (last 50)</h2>
<table>
  <tr><th>Time (UTC)</th><th>Sym</th><th>Dir</th><th>Entry</th><th>Exit</th><th>Qty</th><th>PnL</th><th>Comm</th><th>Slippage</th><th>Dur</th></tr>
  {rows_trades}
</table>

{'<h2>Engine vs Binance Slippage Cross-Reference</h2><table><tr><th>Time</th><th>Sym</th><th>Dir</th><th>Eng Entry</th><th>BNB Entry</th><th>Entry Slip</th><th>Eng Exit</th><th>BNB Exit</th><th>Exit Slip</th><th>Match Δ</th></tr>' + rows_xref + '</table>' if rows_xref else ''}

</body>
</html>"""

# ── JSON output ───────────────────────────────────────────────────────────────
def format_json(income, trades_analysis, trades_table, slippage_xref, since_label, balance) -> str:
    return json.dumps({
        "generated":       datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        "since":           since_label,
        "mode":            "LIVE" if LIVE_MODE else "DEMO",
        "balance":         balance,
        "income":          income,
        "trades_analysis": trades_analysis,
        "trades_table":    trades_table,
        "slippage_xref":   slippage_xref,
    }, indent=2, default=str)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Binance demo account trade analyzer")
    parser.add_argument("--since",     default="24h",  help="Time window: 1h / 6h / 24h / 2025-01-15T09:30")
    parser.add_argument("--sym",       default=None,   help="Filter to a single symbol e.g. BTCUSDT")
    parser.add_argument("--compare",   default=None,   metavar="LOGS_DIR", help="Engine CSV logs dir for slippage cross-ref")
    parser.add_argument("--json-only", action="store_true")
    args = parser.parse_args()

    since_dt  = parse_since(args.since)
    since_ms  = int(since_dt.timestamp() * 1000)
    now_ms    = int(time.time() * 1000)
    since_label = since_dt.strftime("%Y-%m-%d %H:%M:%S UTC")

    print(f"📡 Binance {'LIVE' if LIVE_MODE else 'DEMO'} — pulling since {since_label}", file=sys.stderr)

    # ── Balance ───────────────────────────────────────────────────
    print("   Fetching balance...", file=sys.stderr)
    balance = fetch_balance()

    # ── Income history ────────────────────────────────────────────
    print("   Fetching income history...", file=sys.stderr)
    income_rows = fetch_income_history(since_ms, now_ms)
    income      = analyze_income(income_rows)
    print(f"   Income rows: {len(income_rows)}", file=sys.stderr)

    # ── Trade fills ───────────────────────────────────────────────
    if args.sym:
        symbols = [args.sym.upper()]
    else:
        symbols = fetch_all_symbols_with_trades(since_ms, now_ms)
    print(f"   Symbols with trades: {len(symbols)} — {', '.join(symbols[:10])}{'...' if len(symbols) > 10 else ''}", file=sys.stderr)

    all_trades = {}
    for sym in symbols:
        fills = fetch_trade_history(sym, since_ms, now_ms)
        if fills:
            all_trades[sym] = fills

    total_fills = sum(len(v) for v in all_trades.values())
    print(f"   Total fills: {total_fills}", file=sys.stderr)

    trades_table    = build_trades_table(all_trades, income["by_symbol"])
    trades_analysis = analyze_trades_table(trades_table)

    # ── Slippage cross-reference ──────────────────────────────────
    slippage_xref = []
    if args.compare:
        print(f"   Cross-referencing with engine logs in {args.compare}...", file=sys.stderr)
        engine_trades = load_engine_trades(args.compare)
        slippage_xref = cross_reference_slippage(trades_table, engine_trades)
        print(f"   Matched {len(slippage_xref)} trades", file=sys.stderr)

    # ── Output ────────────────────────────────────────────────────
    ts = datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y%m%d_%H%M%S")

    json_str  = format_json(income, trades_analysis, trades_table, slippage_xref, since_label, balance)
    json_file = f"binance_analysis_{ts}.json"
    Path(json_file).write_text(json_str)
    print(f"✅  {json_file}", file=sys.stderr)

    if not args.json_only:
        html_str  = format_html(income, trades_analysis, trades_table, slippage_xref, since_label, balance)
        html_file = f"binance_analysis_{ts}.html"
        Path(html_file).write_text(html_str)
        print(f"✅  {html_file}", file=sys.stderr)

    # Quick console summary
    ov = trades_analysis["overall"]
    print(f"\n{'─'*50}", file=sys.stderr)
    print(f"  Trades : {ov['total_trades']}  WR: {ov['wr_pct']:.0f}%", file=sys.stderr)
    print(f"  PnL    : {ov['total_pnl']:+.4f} USDT", file=sys.stderr)
    print(f"  Fees   : {ov['total_commission']:.4f} USDT", file=sys.stderr)
    print(f"  Avg slip: {ov['avg_slippage_pct']:+.4f}%", file=sys.stderr)
    print(f"  Avg dur : {ov['avg_dur_sec']//60}m {ov['avg_dur_sec']%60}s", file=sys.stderr)
    print(f"{'─'*50}\n", file=sys.stderr)
    print(f"Share JSON with Claude:  cat {json_file} | pbcopy", file=sys.stderr)

if __name__ == "__main__":
    main()
