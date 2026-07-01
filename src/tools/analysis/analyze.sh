#!/usr/bin/env python3
"""
analyze.sh — Unified PredictEngine analysis: engine logs + Binance demo ground truth.

Usage:
  python3 analyze.sh                        # pull server logs + pull Binance + unified report
  python3 analyze.sh --since 2h             # last 2 hours only
  python3 analyze.sh --since deploy         # since last deploy
  python3 analyze.sh --local                # use local cached logs (no server pull)
  python3 analyze.sh --no-binance           # skip Binance pull (engine logs only)
  python3 analyze.sh --local --no-binance   # fully offline
  python3 analyze.sh --json-only            # skip HTML output

Default behaviour (no flags):
  1. Pull engine CSVs from VPS via SSH
  2. Pull Binance demo trade history via API
  3. Reconcile engine sim vs Binance fills
  4. Output unified HTML + JSON

Reads .env for: BINANCE_API_KEY, BINANCE_API_SECRET, LIVE_MODE
"""

import os, sys, json, csv, glob, subprocess, argparse, re, hashlib, hmac, time, urllib.parse
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from pathlib import Path

# ══════════════════════════════════════════════════════════════════
# .ENV + BINANCE CONFIG
# ══════════════════════════════════════════════════════════════════

def _load_env(path=".env"):
    for line in Path(path).read_text().splitlines() if Path(path).exists() else []:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        v = v.strip().strip('"').strip("'")
        os.environ.setdefault(k.strip(), v)

# Load env — MOST-SPECIFIC FIRST.
# _load_env uses os.environ.setdefault, so the FIRST value loaded wins.
# Previously .env (generic) loaded first and silently shadowed .env.prod,
# which could make a "prod" run use stage keys / wrong LIVE_MODE. Load the
# env that matches the requested mode first, then generic files as fallback.
if "--stage" in sys.argv:
    _load_env(".env.stage")     # stage keys win for stage runs
else:
    _load_env(".env.prod")      # prod keys win for prod runs
_load_env(os.path.expanduser("~/engine/.env"))  # server path (prod box) fallback
_load_env(".env")                               # generic legacy fallback (lowest priority)

BINANCE_API_KEY    = os.environ.get("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET", "")
LIVE_MODE          = os.environ.get("LIVE_MODE", "false").lower() in ("1","true","yes")
if "--live" in sys.argv:
    LIVE_MODE = True
BINANCE_BASE       = "https://fapi.binance.com" if LIVE_MODE else "https://demo-fapi.binance.com"
if LIVE_MODE:
    print(f"[analyze] Using LIVE endpoint: {BINANCE_BASE}", file=sys.stderr)

# ══════════════════════════════════════════════════════════════════
# DEPLOYMENTS LOG
# ══════════════════════════════════════════════════════════════════

def _deployments_log(is_stage=False):
    tag = "stage" if is_stage else "prod"
    return f"./data_backup/deployments_{tag}.log"

DEPLOYMENTS_LOG = "./data_backup/deployments_prod.log"  # default, overridden after arg parse

def load_deployments():
    deploys = []
    try:
        with open(DEPLOYMENTS_LOG) as f:
            for line in f:
                line = line.strip()
                if not line: continue
                parts = [p.strip() for p in line.split('|', 2)]
                if len(parts) < 1: continue
                try:
                    ts = datetime.strptime(parts[0], '%Y%m%d_%H%M%S')
                except ValueError:
                    continue
                kind = parts[1] if len(parts) > 1 else ''
                note = parts[2] if len(parts) > 2 else ''
                deploys.append((ts, kind, note))
    except FileNotFoundError:
        pass
    return deploys

def get_deploy_cutoff(n=1):
    deploys = load_deployments()
    if not deploys:
        print(f"❌ No entries in {DEPLOYMENTS_LOG}", file=sys.stderr); sys.exit(1)
    if n > len(deploys):
        print(f"❌ Requested deploy:{n} but only {len(deploys)} entries", file=sys.stderr); sys.exit(1)
    ts, kind, note = deploys[-n]
    label = "last" if n == 1 else f"{n}th-to-last"
    print(f"⏱  Since {label} deploy [{ts.strftime('%Y-%m-%d %H:%M:%S')}]: {note}", file=sys.stderr)
    return ts

# ══════════════════════════════════════════════════════════════════
# SINCE PARSING
# ══════════════════════════════════════════════════════════════════

def parse_since(since_str):
    s = since_str.strip().lower()
    m = re.fullmatch(r'deploy(?::(\d+))?', s)
    if m:
        return get_deploy_cutoff(int(m.group(1)) if m.group(1) else 1)
    m = re.fullmatch(r'(\d+(?:\.\d+)?)\s*h(?:ours?)?', s)
    if m:
        cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=float(m.group(1)))
        print(f"⏱  Since {m.group(1)}h ago: {cutoff.strftime('%Y-%m-%d %H:%M:%S')}", file=sys.stderr)
        return cutoff
    m = re.fullmatch(r'(\d+(?:\.\d+)?)\s*m(?:in(?:utes?)?)?', s)
    if m:
        cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=float(m.group(1)))
        print(f"⏱  Since {m.group(1)}m ago: {cutoff.strftime('%Y-%m-%d %H:%M:%S')}", file=sys.stderr)
        return cutoff
    for fmt in ('%Y-%m-%dT%H:%M:%S','%Y-%m-%dT%H:%M','%Y-%m-%d %H:%M:%S','%Y-%m-%d %H:%M','%Y-%m-%d'):
        try:
            cutoff = datetime.strptime(since_str.strip(), fmt)
            print(f"⏱  Since {cutoff.strftime('%Y-%m-%d %H:%M:%S')}", file=sys.stderr)
            return cutoff
        except ValueError:
            pass
    print(f"❌ Cannot parse --since: {since_str!r}", file=sys.stderr); sys.exit(1)

# ══════════════════════════════════════════════════════════════════
# FILE TIMESTAMP HELPERS
# ══════════════════════════════════════════════════════════════════

def preds_file_timestamp(filepath):
    fname = os.path.basename(filepath)
    m = re.match(r'preds_(b_)?(\d{8})_(\d{4,6})', fname)
    if not m: return None
    date_str, time_str = m.group(2), m.group(3)
    fmt = '%Y%m%d_%H%M%S' if len(time_str) == 6 else '%Y%m%d_%H%M'
    try: return datetime.strptime(f"{date_str}_{time_str}", fmt)
    except ValueError: return None

def signals_file_timestamp(filepath):
    fname = os.path.basename(filepath)
    m = re.match(r'signals_(\d{8})_(\d{4,6})', fname)
    if not m: return None
    date_str, time_str = m.group(1), m.group(2)
    fmt = '%Y%m%d_%H%M%S' if len(time_str) == 6 else '%Y%m%d_%H%M'
    try: return datetime.strptime(f"{date_str}_{time_str}", fmt)
    except ValueError: return None

def session_dir_timestamp(session_dir):
    name = os.path.basename(session_dir.rstrip('/'))
    name = re.sub(r'_(stage|prod)$', '', name)   # ← add this
    try: return datetime.strptime(name, '%Y%m%d_%H%M%S')
    except ValueError: return None

def parse_row_time(row, time_keys=('time','timestamp','ts','datetime')):
    for key in time_keys:
        raw = row.get(key,'').strip()
        if not raw: continue
        try: return datetime.fromtimestamp(float(raw), tz=timezone.utc).replace(tzinfo=None)
        except (ValueError, OSError): pass
        for fmt in ('%Y-%m-%dT%H:%M:%S','%Y-%m-%dT%H:%M','%Y-%m-%d %H:%M:%S','%Y-%m-%d %H:%M'):
            try: return datetime.strptime(raw, fmt)
            except ValueError: pass
    raw = row.get('_file_ts','').strip()
    if raw:
        try: return datetime.fromisoformat(raw)
        except ValueError: pass
    return None

def filter_by_since(rows, cutoff, label='rows'):
    if cutoff is None: return rows
    kept, skipped, unparseable = [], 0, 0
    for row in rows:
        t = parse_row_time(row)
        if t is None: unparseable += 1; kept.append(row)
        elif t >= cutoff: kept.append(row)
        else: skipped += 1
    if skipped or unparseable:
        print(f"   ⏱  {label}: kept {len(kept)-unparseable}, skipped {skipped}"
              + (f", {unparseable} no-time (kept)" if unparseable else ""), file=sys.stderr)
    return kept

# ══════════════════════════════════════════════════════════════════
# SERVER PULL
# ══════════════════════════════════════════════════════════════════

def pull_data_from_server(clean_remote=True, is_stage=False):
    SERVER    = "${DEPLOY_HOST:-user@host.example.com}" if is_stage else "${DEPLOY_HOST:-user@host.example.com}"
    KEY       = os.path.expanduser("~/.ssh/oracle_key")
    REMOTE    = "~/engine"
    BACKUP    = "./data_backup"
    ENV_TAG   = "stage" if is_stage else "prod"
    CTRL      = f"{os.path.expanduser('~')}/.ssh/ctl-oracle-%r@%h:%p"
    SSH       = f"ssh -i {KEY} -o ControlMaster=auto -o ControlPath={CTRL} -o ControlPersist=60s {SERVER}"

    print(f"📥 Pulling engine logs from {ENV_TAG} server ({SERVER})...", file=sys.stderr)
    session_dir = f"{BACKUP}/{datetime.now().strftime('%Y%m%d_%H%M%S')}_{ENV_TAG}"
    os.makedirs(f"{session_dir}/prod", exist_ok=True)
    os.makedirs(f"{session_dir}/logs", exist_ok=True)

    # Trade CSVs (preds_*.csv in engine root)
    r = subprocess.run(f"{SSH} 'ls {REMOTE}/preds_*.csv {REMOTE}/preds_b_*.csv 2>/dev/null | wc -l'",
                       shell=True, capture_output=True, text=True)
    if int(r.stdout.strip() or "0") > 0:
        subprocess.run(f"{SSH} 'cd {REMOTE} && zip -q /tmp/csvs.zip preds_*.csv preds_b_*.csv 2>/dev/null || zip -q /tmp/csvs.zip preds_*.csv'",
                       shell=True)
        subprocess.run(f"scp -i {KEY} -o ControlPath={CTRL} "
                       f"{SERVER}:/tmp/csvs.zip {session_dir}/prod/csvs.zip",
                       shell=True)
        subprocess.run(f"unzip -q {session_dir}/prod/csvs.zip -d {session_dir}/prod 2>/dev/null || true", shell=True)
        print("   ✅ Trade CSVs", file=sys.stderr)

    # Positions CSV (real Binance opens/closes)
    r = subprocess.run(f"{SSH} 'ls {REMOTE}/positions_*.csv 2>/dev/null | wc -l'",
                       shell=True, capture_output=True, text=True)
    if int(r.stdout.strip() or "0") > 0:
        r2 = subprocess.run(f"{SSH} 'cat {REMOTE}/positions_*.csv'",
                            shell=True, capture_output=True, text=True)
        with open(f"{session_dir}/prod/positions_combined.csv","w") as f: f.write(r2.stdout)
        print("   ✅ Positions CSV", file=sys.stderr)

    # Signal CSVs
    r = subprocess.run(f"{SSH} 'ls {REMOTE}/logs/signals_*.csv 2>/dev/null | wc -l'",
                       shell=True, capture_output=True, text=True)
    if int(r.stdout.strip() or "0") > 0:
        r2 = subprocess.run(f"{SSH} 'cd {REMOTE}/logs && for f in signals_*.csv; do cat \"$f\"; done'",
                            shell=True, capture_output=True, text=True)
        with open(f"{session_dir}/logs/signals_combined.csv","w") as f: f.write(r2.stdout)
        print("   ✅ Signal CSVs", file=sys.stderr)

    if clean_remote:
        subprocess.run(f"{SSH} 'rm -f {REMOTE}/preds_*.csv {REMOTE}/preds_b_*.csv {REMOTE}/logs/signals_*.csv {REMOTE}/positions_*.csv'", shell=True)
        print("   ✅ Remote cleaned", file=sys.stderr)

    print(f"   Session: {session_dir}", file=sys.stderr)
    return [session_dir]

# ══════════════════════════════════════════════════════════════════
# LOAD ENGINE DATA
# ══════════════════════════════════════════════════════════════════

def load_signals(logs_dirs, cutoff=None):
    if isinstance(logs_dirs, str): logs_dirs = [logs_dirs]
    signals = []
    for logs_dir in logs_dirs:
        session_ts = session_dir_timestamp(os.path.dirname(logs_dir.rstrip('/')))
        for f in glob.glob(f"{logs_dir}/signals_*.csv"):
            file_ts = signals_file_timestamp(f)
            effective_ts = file_ts or session_ts
            if cutoff and effective_ts and effective_ts < cutoff: continue
            try:
                with open(f) as fh:
                    reader = csv.DictReader(fh)
                    for row in reader:
                        if effective_ts: row.setdefault('_file_ts', effective_ts.isoformat())
                        signals.append(row)
            except Exception: pass
    return signals

def load_trades(session_dirs, cutoff=None):
    if isinstance(session_dirs, str): session_dirs = [session_dirs]
    trades = []
    for session_dir in session_dirs:
        for files, is_b in [(glob.glob(f"{session_dir}/prod/preds_*.csv"), False),
                            (glob.glob(f"{session_dir}/prod/preds_b_*.csv"), True)]:
            for f in files:
                if not is_b and os.path.basename(f).startswith('preds_b_'): continue
                file_ts = preds_file_timestamp(f)
                if cutoff and file_ts and file_ts < cutoff: continue
                fname = os.path.basename(f).replace('.csv','')
                parts = re.sub(r'^preds_(b_)?','',fname).split('_')
                strat_label = next((p for i,p in enumerate(parts) if i>=1 and p.isupper() and len(p)<=3), '?')
                if is_b: strat_label += '_B'
                try:
                    raw_lines = Path(f).read_text().splitlines()
                    # Separate OPEN rows and OUT_ rows
                    # OUT_HH:MM:SS rows contain the close data (outcome, pct_exit etc.)
                    # Match them to their open row by sym+dir+entry_px
                    header_lines = [l for l in raw_lines if not l.startswith('# STRATEGY')]
                    open_rows = {}   # key → row dict
                    closed_rows = []
                    for row in csv.DictReader(header_lines):
                        t = row.get('time','')
                        if t.startswith('OUT_'):
                            # Exit row — merge into corresponding open row
                            _id = row.get('id','')
                            key  = (row.get('sym',''), row.get('dir',''), row.get('entry_px',''), _id)
                            # Fallback key without id for old CSVs where id column is absent
                            key2 = (row.get('sym',''), row.get('dir',''), row.get('entry_px',''), '')
                            if key not in open_rows and key2 in open_rows: key = key2
                            if key in open_rows:
                                open_rows[key].update({
                                    'outcome': row.get('outcome',''),
                                    'pct_exit': row.get('pct_exit',''),
                                    'net_exit': row.get('net_exit',''),
                                    'reason':   row.get('reason',''),
                                    'dur_sec':  row.get('dur_sec',''),
                                    'exit_px':  row.get('exit_px',''),
                                    'max_dp':   row.get('max_dp',''),
                                    'min_dp':   row.get('min_dp',''),
                                })
                                closed_rows.append(open_rows.pop(key))
                        else:
                            # Reconstruct UTC timestamp for reconciliation matching.
                            # Priority 1: ts_epoch column (epoch-s, written by _log_pred since v18+)
                            # Priority 2: reconstruct from file date + HH:MM:SS, handling midnight crossing
                            raw_epoch = row.get('ts_epoch','').strip()
                            if raw_epoch:
                                try:
                                    row['ts'] = str(float(raw_epoch))
                                except (ValueError, TypeError):
                                    pass
                            if not row.get('ts') and file_ts and t and len(t) >= 8:
                                try:
                                    h, m, s = int(t[0:2]), int(t[3:5]), int(t[6:8])
                                    full_dt = file_ts.replace(hour=h, minute=m, second=s)
                                    # Handle midnight crossing: trade time << file start time → next day
                                    if (file_ts - full_dt).total_seconds() > 3600:
                                        full_dt += timedelta(days=1)
                                    row['ts'] = str(full_dt.replace(tzinfo=timezone.utc).timestamp())
                                except (ValueError, AttributeError):
                                    pass
                            row['strategy'] = strat_label
                            if file_ts: row.setdefault('_file_ts', file_ts.isoformat())
                            key = (row.get('sym',''), row.get('dir',''), row.get('entry_px',''), row.get('id',''))
                            open_rows[key] = row
                    # Add any still-open trades
                    all_rows = closed_rows + list(open_rows.values())
                    trades.extend(all_rows)
                except Exception:
                    pass
    return trades

# ══════════════════════════════════════════════════════════════════
# BINANCE REST
# ══════════════════════════════════════════════════════════════════

import requests as _requests

def _bnb_sign(params):
    qs = urllib.parse.urlencode(params)
    return hmac.new(BINANCE_API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()

def _bnb_get(path, params=None):
    params = params or {}
    params["timestamp"] = int(time.time() * 1000)
    params["signature"] = _bnb_sign(params)
    try:
        r = _requests.get(BINANCE_BASE + path,
                          headers={"X-MBX-APIKEY": BINANCE_API_KEY},
                          params=params, timeout=10)
        return r.json()
    except Exception as e:
        return {"error": str(e)}

def _bnb_fetch_balance():
    data = _bnb_get("/fapi/v2/balance")
    if isinstance(data, list):
        for a in data:
            if a.get("asset") == "USDT": return a
    return {}

def _bnb_fetch_income(start_ms, end_ms, income_type=None):
    results = []
    params = {"startTime": start_ms, "endTime": end_ms, "limit": 1000}
    if income_type: params["incomeType"] = income_type
    while True:
        data = _bnb_get("/fapi/v1/income", params)
        if not isinstance(data, list) or not data: break
        results.extend(data)
        if len(data) < 1000: break
        params["startTime"] = data[-1]["time"] + 1
        if params["startTime"] >= end_ms: break
    return results

def _bnb_fetch_trades(sym, start_ms, end_ms):
    results = []
    params = {"symbol": sym, "startTime": start_ms, "endTime": end_ms, "limit": 1000}
    while True:
        data = _bnb_get("/fapi/v1/userTrades", params)
        if not isinstance(data, list) or not data: break
        results.extend(data)
        if len(data) < 1000: break
        params["startTime"] = data[-1]["time"] + 1
        if params["startTime"] >= end_ms: break
    return results

def _bnb_get_engine_coins():
    for config_path in ["config.py","../engine/config.py","./engine/config.py"]:
        try:
            src = Path(config_path).read_text()
            m = re.search(r'DEFAULT_COINS\s*=\s*\[(.*?)\]', src, re.DOTALL)
            if m:
                coins = re.findall(r"'([A-Z0-9]+USDT)'", m.group(1))
                if coins: return coins
        except Exception: pass
    return ["BTCUSDT","ETHUSDT","SOLUSDT","JTOUSDT","PENDLEUSDT","ONDOUSDT","SUIUSDT",
            "NEARUSDT","TONUSDT","NILUSDT","STRKUSDT","DYDXUSDT","OPUSDT","GALAUSDT",
            "JUPUSDT","FILUSDT","NOTUSDT","TIAUSDT","ARBUSDT","ENAUSDT","INJUSDT",
            "DASHUSDT","APTUSDT","WLDUSDT","1000BONKUSDT","DOTUSDT","HYPEUSDT",
            "WIFUSDT","MEWUSDT","TRUMPUSDT","1000PEPEUSDT"]

def pull_binance_data(since_dt):
    """Pull balance + income + fills from Binance. Returns structured dict."""
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        print("⚠️  BINANCE_API_KEY/SECRET not set — skipping Binance pull", file=sys.stderr)
        return None

    start_ms = int(since_dt.timestamp() * 1000)
    now_ms   = int(time.time() * 1000)

    print(f"📡 Pulling Binance {'LIVE' if LIVE_MODE else 'DEMO'} data...", file=sys.stderr)

    balance     = _bnb_fetch_balance()
    income_rows = _bnb_fetch_income(start_ms, now_ms)
    print(f"   Income rows: {len(income_rows)}", file=sys.stderr)

    # Symbol discovery: income first, fallback to coin universe scan
    symbols = list({r["symbol"] for r in income_rows if r.get("symbol")})
    if not symbols:
        print("   ℹ️  Income API empty — scanning coin universe for fills...", file=sys.stderr)
        universe = _bnb_get_engine_coins()
        symbols = []
        for sym in universe:
            data = _bnb_get("/fapi/v1/userTrades",
                            {"symbol": sym, "startTime": start_ms, "endTime": now_ms, "limit": 1})
            if isinstance(data, list) and data:
                symbols.append(sym)

    print(f"   Symbols with fills: {len(symbols)} — {', '.join(symbols[:8])}{'...' if len(symbols)>8 else ''}", file=sys.stderr)

    all_fills = {}
    for sym in symbols:
        fills = _bnb_fetch_trades(sym, start_ms, now_ms)
        if fills: all_fills[sym] = fills

    total_fills = sum(len(v) for v in all_fills.values())
    print(f"   Total fills: {total_fills}", file=sys.stderr)

    # Build income summary
    income_totals    = defaultdict(float)
    income_by_symbol = defaultdict(lambda: defaultdict(float))
    for r in income_rows:
        t = r.get("incomeType","OTHER"); sym = r.get("symbol",""); val = float(r.get("income",0))
        income_totals[t] += val; income_by_symbol[sym][t] += val

    # Build round-trip trades table
    trades_table = _build_bnb_trades_table(all_fills)
    trades_stats = _analyze_bnb_trades(trades_table)

    return {
        "balance":        balance,
        "income_totals":  {k: round(v,6) for k,v in income_totals.items()},
        "income_by_sym":  {s: {k: round(v,6) for k,v in d.items()} for s,d in income_by_symbol.items()},
        "trades_table":   trades_table,
        "trades_stats":   trades_stats,
    }

def _dt_from_ms(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)

def _build_bnb_trades_table(all_fills):
    table = []
    for sym, fills in all_fills.items():
        fills = sorted(fills, key=lambda f: f["time"])
        pos_qty = 0.0; pos_dir = None; pos_entry = 0.0; pos_open = None
        for fill in fills:
            qty   = float(fill["qty"]); price = float(fill["price"])
            side  = fill["side"]; t_ms = fill["time"]
            pnl   = float(fill.get("realizedPnl",0))
            comm  = float(fill.get("commission",0))
            closing = (fill.get("reduceOnly") or
                       (pos_dir=="long" and side=="SELL") or
                       (pos_dir=="short" and side=="BUY"))
            if closing and pos_qty > 0 and pos_open:
                dur  = (_dt_from_ms(t_ms) - pos_open).total_seconds()
                slip = ((price-pos_entry)/pos_entry*100 if pos_dir=="long"
                        else (pos_entry-price)/pos_entry*100)
                notional = pos_entry * qty
                table.append({"sym":sym,"dir":pos_dir,
                    "open_time":  pos_open.strftime("%Y-%m-%d %H:%M:%S"),
                    "close_time": _dt_from_ms(t_ms).strftime("%Y-%m-%d %H:%M:%S"),
                    "entry_px":   round(pos_entry,6), "exit_px": round(price,6),
                    "qty":        round(qty,6),
                    "realized_pnl": round(pnl,6), "commission": round(comm,6),
                    "slippage_pct": round(slip,4),
                    "dur_sec":    round(dur),
                    "pnl_pct":    round(pnl/notional*100,4) if notional>0 else 0})
                pos_qty=0.0; pos_dir=None; pos_entry=0.0; pos_open=None
            elif not closing:
                if pos_qty == 0:
                    pos_dir=("long" if side=="BUY" else "short"); pos_entry=price; pos_open=_dt_from_ms(t_ms)
                pos_qty += qty
    return table

def _analyze_bnb_trades(table):
    if not table:
        return {"overall":{"total_trades":0,"wins":0,"losses":0,"total_pnl":0,
                           "total_commission":0,"wr_pct":0,"avg_dur_sec":0,
                           "avg_slippage_pct":0,"best_trade":None,"worst_trade":None},
                "by_symbol":{}}
    overall = {"total_trades":0,"wins":0,"losses":0,"total_pnl":0.0,
               "total_commission":0.0,"avg_dur_sec":0,"avg_slippage_pct":0,
               "best_trade":None,"worst_trade":None}
    by_sym  = defaultdict(lambda:{"trades":0,"wins":0,"losses":0,"pnl":0.0,
                                   "commission":0.0,"_slips":[]})
    durs=[]; slips=[]
    for t in table:
        pnl=t["realized_pnl"]; comm=t["commission"]; slip=t["slippage_pct"]
        overall["total_trades"]+=1; overall["total_pnl"]+=pnl; overall["total_commission"]+=comm
        if pnl>0: overall["wins"]+=1
        if pnl<0: overall["losses"]+=1
        durs.append(t["dur_sec"]); slips.append(slip)
        if not overall["best_trade"]  or pnl>overall["best_trade"]["realized_pnl"]:  overall["best_trade"]=t
        if not overall["worst_trade"] or pnl<overall["worst_trade"]["realized_pnl"]: overall["worst_trade"]=t
        s=by_sym[t["sym"]]; s["trades"]+=1; s["pnl"]+=pnl; s["commission"]+=comm; s["_slips"].append(slip)
        if pnl>0: s["wins"]+=1
        if pnl<0: s["losses"]+=1
    overall["avg_dur_sec"]      = round(sum(durs)/len(durs)) if durs else 0
    overall["avg_slippage_pct"] = round(sum(slips)/len(slips),4) if slips else 0
    overall["total_pnl"]        = round(overall["total_pnl"],6)
    overall["total_commission"] = round(overall["total_commission"],6)
    overall["wr_pct"]           = round(overall["wins"]/overall["total_trades"]*100,1) if overall["total_trades"] else 0
    sym_stats={}
    for sym,d in by_sym.items():
        slp=d.pop("_slips")
        d["pnl"]=round(d["pnl"],6); d["commission"]=round(d["commission"],6)
        d["avg_slippage_pct"]=round(sum(slp)/len(slp),4) if slp else 0
        d["wr_pct"]=round(d["wins"]/d["trades"]*100,1) if d["trades"] else 0
        sym_stats[sym]=d
    return {"overall":overall,"by_symbol":sym_stats}

# ══════════════════════════════════════════════════════════════════
# ENGINE LOG ANALYSIS
# ══════════════════════════════════════════════════════════════════

EXIT_TYPE_KEYS = ('exit_type','exit','reason','exit_reason')

def get_exit_type(row):
    for key in EXIT_TYPE_KEYS:
        val = row.get(key,'').strip().lower()
        if val:
            if val in ('trail','trailing','trail_stop'):        return 'trail'
            if val in ('sl','stop','stop_loss','stoploss'):     return 'sl'
            if val in ('time','timeout','timed_out','max_time'): return 'time'
            if val in ('inertia','no_momentum','flat'):         return 'inertia'
            if val in ('tp','take_profit','takeprofit'):        return 'tp'
            return val
    return 'unknown'

def analyze_signals(signals):
    funnel=defaultdict(lambda:{'detected':0,'blocked':0,'fired':0,'closed':0})
    block_reasons=defaultdict(lambda:defaultdict(int))
    fired_detail=defaultdict(lambda:defaultdict(int))
    symbol_hits=defaultdict(lambda:defaultdict(int))
    vpin_fire=defaultdict(list); vpin_block=defaultdict(list)
    conf_fire=defaultdict(list); score_fire=defaultdict(list)
    for sig in signals:
        strat=sig.get('strategy','?'); event=sig.get('event','').strip().lower()
        detail=sig.get('detail','').strip(); symbol=sig.get('symbol','').strip()
        def _f(k):
            try: return float(sig.get(k) or '')
            except: return None
        vpin=_f('vpin'); conf=_f('conf'); score=_f('score')
        if symbol: symbol_hits[strat][symbol]+=1
        if event in ('impulse','pattern','detected'):    funnel[strat]['detected']+=1
        elif event=='blocked':
            funnel[strat]['blocked']+=1
            dl=detail.lower()
            if 'vpin' in dl:                             block_reasons[strat]['vpin_too_low']+=1
            elif 'spread' in dl:                         block_reasons[strat]['spread_too_wide']+=1
            elif 'bounce' in dl or 'already' in dl:      block_reasons[strat]['already_moved']+=1
            elif 'conflict' in dl or 'open' in dl:       block_reasons[strat]['position_conflict']+=1
            elif 'cooldown' in dl or 'cd' in dl:         block_reasons[strat]['cooldown']+=1
            else:                                        block_reasons[strat]['other']+=1
            if vpin: vpin_block[strat].append(vpin)
        elif event=='fired':
            funnel[strat]['fired']+=1
            if detail: fired_detail[strat][detail]+=1
            if vpin:  vpin_fire[strat].append(vpin)
            if conf:  conf_fire[strat].append(conf)
            if score: score_fire[strat].append(score)
        elif event=='closed': funnel[strat]['closed']+=1
    for strat in funnel:
        det = funnel[strat]['detected']
        fir = funnel[strat]['fired']
        
        # --- Update the logic to ensure keys are created ---
        funnel[strat]['d2f_pct'] = round(fir / det * 100, 1) if det > 0 else 0.0
        funnel[strat]['f2c_pct'] = round(funnel[strat]['closed'] / fir * 100, 1) if fir > 0 else 0.0
    def avg(l): return round(sum(l)/len(l),3) if l else None
    def top(d,n=5): return dict(sorted(d.items(),key=lambda x:-x[1])[:n])
    signals_detail={}
    for strat in set(list(funnel)+list(fired_detail)+list(symbol_hits)):
        vf=vpin_fire.get(strat,[]); vb=vpin_block.get(strat,[])
        cf=conf_fire.get(strat,[]); sf=score_fire.get(strat,[])
        signals_detail[strat]={
            'fired_count':       funnel[strat]['fired'],
            'top_fired_details': top(fired_detail.get(strat,{})),
            'top_symbols':       top(symbol_hits.get(strat,{})),
            'vpin_avg_at_fire':  avg(vf),
            'vpin_avg_at_block': avg(vb),
            'vpin_min_at_fire':  round(min(vf),3) if vf else None,
            'vpin_max_at_fire':  round(max(vf),3) if vf else None,
            'conf_avg_at_fire':  avg(cf),
            'score_avg_at_fire': avg(sf),
            'note': 'vpin/conf/score only available for strategies with log_signal(fired) calls (K, E, Y)' if not vf else None,
        }
    return dict(funnel), dict(block_reasons), signals_detail

def analyze_engine_trades(trades):
    by_strat=defaultdict(lambda:{'trades':0,'wins':0,'losses':0,'net':0.0,'exits':defaultdict(int)})
    for trade in trades:
        strat=trade.get('strategy','?')
        # Engine CSV uses 'outcome' (win/lose/flat) and 'pct_exit'
        outcome=(trade.get('outcome') or trade.get('out3','')).strip().lower()
        exit_t=get_exit_type(trade)
        try: net=float(trade.get('pct_exit') or trade.get('pct3') or trade.get('net_exit') or 0)
        except: net=0.0
        by_strat[strat]['trades']+=1; by_strat[strat]['net']+=net; by_strat[strat]['exits'][exit_t]+=1
        if outcome=='win':              by_strat[strat]['wins']+=1
        elif outcome in ('lose','loss'):        by_strat[strat]['losses']+=1
    analysis={}
    for strat,data in by_strat.items():
        decided=data['wins']+data['losses']
        wr=(data['wins']/decided*100) if decided>0 else 0
        analysis[strat]={'trades':data['trades'],'wins':data['wins'],'wr':round(wr,1),
            'net':round(data['net'],4),
            'avg_per_trade':round(data['net']/data['trades'],4) if data['trades']>0 else 0,
            'exits':dict(sorted(data['exits'].items(),key=lambda x:-x[1]))}
    return analysis

# ══════════════════════════════════════════════════════════════════
# RECONCILIATION
# ══════════════════════════════════════════════════════════════════

def load_positions_csv(data_dir: str = ".") -> list:
    """Load positions CSV files from session dir (pulled from server or local)."""
    import glob
    rows = []
    # Check both prod subdir (pulled from server) and root
    for pattern in [f"{data_dir}/prod/positions_*.csv",
                    f"{data_dir}/positions_*.csv",
                    f"{data_dir}/**/positions_*.csv"]:
        for path in sorted(glob.glob(pattern, recursive=True)):
            try:
                with open(path) as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        rows.append(row)
            except Exception as e:
                print(f"   [positions] failed to load {path}: {e}", file=sys.stderr)
        if rows: break  # found in this pattern, stop searching
    if rows:
        print(f"   [positions] loaded {len(rows)} rows from positions CSV", file=sys.stderr)
    return rows


def reconcile(engine_trades, bnb_data):
    """Match engine trades to Binance fills by symbol + entry price proximity."""
    WINDOW     = timedelta(seconds=3600)  # wide window — match by price, not just time
    PRICE_TOL  = 0.005                    # 0.5% price tolerance for entry matching
    bnb_trades = bnb_data.get("trades_table", []) if bnb_data else []

    # Index engine trades by symbol — only include live trades (is_live=1)
    # Sim trades (is_live=0 or missing) have no Binance counterpart
    eng_by_sym = defaultdict(list)
    _dbg_sim_skipped = 0
    _dbg_total = 0; _dbg_no_sym = 0; _dbg_no_dt = 0
    for t in engine_trades:
        _dbg_total += 1
        sym = t.get('sym','').strip()
        if not sym: _dbg_no_sym += 1; continue
        # Skip sim trades — they have no Binance counterpart
        is_live = t.get('is_live', '')
        if is_live == '0': _dbg_sim_skipped += 1; continue
        try:
            ts = float(t.get('ts') or t.get('timestamp') or 0)
            dt = datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None) if ts > 1e9 else None
        except: dt = None
        if dt is None: _dbg_no_dt += 1; continue
        eng_by_sym[sym].append({'dt':dt,'row':t})
    if _dbg_total > 0:
        indexed = len(engine_trades)-_dbg_no_sym-_dbg_no_dt
        sample_dts = [e['dt'] for entries in list(eng_by_sym.values())[:3] for e in entries[:2]]
        print(f"   [reconcile] {_dbg_total} engine rows: {indexed} indexed, "
              f"{_dbg_no_sym} no-sym, {_dbg_no_dt} no-ts, {_dbg_sim_skipped} sim-skipped  syms={list(eng_by_sym.keys())[:5]}", file=sys.stderr)
        if sample_dts:
            print(f"   [reconcile] sample eng timestamps: {[str(d) for d in sample_dts[:3]]}", file=sys.stderr)

    matched=[]; unmatched_bnb=[]; used_eng=set()

    for bt in bnb_trades:
        sym=bt['sym']
        try: b_dt=datetime.strptime(bt['open_time'],'%Y-%m-%d %H:%M:%S')
        except: unmatched_bnb.append(bt); continue
        b_entry = float(bt.get('entry_px') or 0)
        best=None; best_idx=None; best_score=float('inf'); best_dt_delta=0
        for i,e in enumerate(eng_by_sym.get(sym,[])):
            if (sym,i) in used_eng: continue
            dt_delta = abs((e['dt']-b_dt).total_seconds())
            if dt_delta > WINDOW.total_seconds(): continue
            # Score by entry price proximity first, time second
            e_entry = float(e['row'].get('entry_px') or e['row'].get('entry') or 0)
            if e_entry > 0 and b_entry > 0:
                price_diff = abs(e_entry - b_entry) / b_entry
                if price_diff > PRICE_TOL: continue
                score = price_diff * 1000 + dt_delta / 3600  # price weighted higher
            else:
                score = dt_delta  # fallback to time-only
            if score < best_score: best, best_idx, best_score, best_dt_delta = e['row'], i, score, dt_delta
        if best is None: unmatched_bnb.append(bt); continue
        used_eng.add((sym,best_idx))
        try:    eng_entry=float(best.get('entry_px') or best.get('entry') or 0)
        except: eng_entry=0
        try:    eng_exit=float(best.get('exit_px') or best.get('exit_price') or 0)
        except: eng_exit=0
        try:    eng_pnl=float(best.get('pct_exit') or best.get('pct3') or best.get('net_exit') or 0)
        except: eng_pnl=0
        entry_slip=round((bt['entry_px']-eng_entry)/eng_entry*100,4) if eng_entry else None
        exit_slip =round((bt['exit_px'] -eng_exit) /eng_exit *100,4) if eng_exit  else None
        matched.append({'sym':sym,'dir':bt['dir'],'open_time':bt['open_time'],
            'match_delta_ms':round(best_dt_delta*1000),
            'eng_entry':eng_entry,'eng_exit':eng_exit,'eng_pnl_pct':eng_pnl,
            'eng_reason':best.get('reason','?'),
            'bnb_entry':bt['entry_px'],'bnb_exit':bt['exit_px'],
            'bnb_pnl_pct':bt['pnl_pct'],'bnb_pnl_usdt':bt['realized_pnl'],
            'bnb_commission':bt['commission'],'dur_sec':bt['dur_sec'],
            'entry_slip_pct':entry_slip,'exit_slip_pct':exit_slip,
            'pnl_diff_pct':round(bt['pnl_pct']-eng_pnl,4)})

    def avg(l): return round(sum(l)/len(l),4) if l else None
    slips_e=[r['entry_slip_pct'] for r in matched if r['entry_slip_pct'] is not None]
    slips_x=[r['exit_slip_pct']  for r in matched if r['exit_slip_pct']  is not None]
    pdiffs =[r['pnl_diff_pct']   for r in matched]
    # Classify unmatched engine trades: shared-position duplicates vs truly missing
    unmatched_eng_raw=[e for sym,entries in eng_by_sym.items()
                       for i,e in enumerate(entries) if (sym,i) not in used_eng]
    # Group by sym+dir+approx_minute — entries within 60s on same sym/dir are likely shared-position
    shared_groups=defaultdict(list)
    for e in unmatched_eng_raw:
        bucket = (e['row'].get('sym',''), e['row'].get('dir',''), int(e['dt'].timestamp()//60))
        shared_groups[bucket].append(e)
    unmatched_eng=[e['row'] for e in unmatched_eng_raw]
    shared_count=sum(len(g)-1 for g in shared_groups.values() if len(g)>1)
    truly_missing=len(unmatched_eng)-shared_count
    deltas = [r['match_delta_ms'] for r in matched]
    return {'matched':matched,'unmatched_binance':unmatched_bnb,'unmatched_engine':unmatched_eng,
            'stats':{'matched_count':len(matched),
                     'unmatched_bnb_count':len(unmatched_bnb),
                     'unmatched_eng_count':len(unmatched_eng),
                     'unmatched_eng_shared':shared_count,
                     'unmatched_eng_truly_missing':truly_missing,
                     'avg_match_delta_ms': avg(deltas),
                     'max_match_delta_ms': max(deltas) if deltas else None,
                     'avg_entry_slip_pct':avg(slips_e),
                     'avg_exit_slip_pct': avg(slips_x),
                     'avg_pnl_diff_pct':  avg(pdiffs)}}

# ══════════════════════════════════════════════════════════════════
# HTML OUTPUT
# ══════════════════════════════════════════════════════════════════

CSS = """
body{font-family:monospace;background:#0a0e27;color:#e0e0e0;padding:20px}
h1{color:#00ff88}
h2{color:#88ccff;margin-top:2em;border-bottom:1px solid #333;padding-bottom:4px}
h3{color:#aaffaa;margin-top:1.2em}
table{width:100%;border-collapse:collapse;background:#1a1f3a;margin-bottom:1.5em}
td,th{padding:7px 10px;border-bottom:1px solid #2a2f4a;text-align:left;font-size:0.88em}
th{background:#0f1428;color:#88ccff}
tr:nth-child(even){background:#0f1428}
.pos{color:#7cfc00}.neg{color:#ff6b6b}.dim{color:#888;font-size:0.85em}
.warn{color:#ffd700}
.badge{display:inline-block;padding:1px 6px;border-radius:3px;font-size:0.8em;margin:1px}
.b-trail{background:#1a3a1a;color:#7cfc00}.b-sl{background:#3a1a1a;color:#ff6b6b}
.b-time{background:#2a2a1a;color:#ffd700}.b-inertia{background:#1a1a3a;color:#88ccff}
.b-tp{background:#1a3a3a;color:#00ffcc}.b-unknown{background:#2a2a2a;color:#888}
.stat{display:inline-block;background:#1a1f3a;border:1px solid #2a2f4a;
      padding:10px 16px;margin:4px;border-radius:6px;min-width:130px;vertical-align:top}
.stat-label{color:#888;font-size:0.8em}.stat-val{font-size:1.3em;font-weight:bold}
.section-eng{border-left:3px solid #7cfc00;padding-left:8px}
.section-bnb{border-left:3px solid #88ccff;padding-left:8px}
.section-rec{border-left:3px solid #cc88ff;padding-left:8px}
"""

def _stat(label, val, color=None):
    c = f'style="color:{color}"' if color else ''
    return f'<div class="stat"><div class="stat-label">{label}</div><div class="stat-val" {c}>{val}</div></div>'

def _pcolor(v): return '#7cfc00' if v >= 0 else '#ff6b6b'

def render_html(signal_funnel, block_reasons, signals_detail, engine_trade_analysis,
                bnb_data, recon, since_label):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    mode_badge = f'<span style="color:#ffd700">{"🔴 LIVE" if LIVE_MODE else "🟡 DEMO"}</span>'

    # ── Section 1: Signal Funnel ──────────────────────────────────
    funnel_rows = ''
    for strat in sorted(signal_funnel):
        d = signal_funnel[strat]
        # Use .get(key, default) to prevent KeyError if the key is missing
        d2f = d.get('d2f_pct', 0.0)
        f2c = d.get('f2c_pct', 0.0)

        det_str = str(d['detected']) if d['detected'] > 0 else '<span class="dim">—</span>'
        blk_str = str(d['blocked'])  if d['detected'] > 0 else '<span class="dim">—</span>'
        funnel_rows += f"""<tr><td>{strat}</td><td>{det_str}</td>
          <td>{blk_str}</td><td>{d['fired']}</td><td>{d['closed']}</td>
          <td>{d2f:.1f}%</td><td>{f2c:.1f}%</td></tr>"""

    block_rows = ''
    for strat in sorted(set(list(block_reasons)+list(signal_funnel))):
        br = block_reasons.get(strat,{})
        if not any(br.values()): continue
        def bc(k): return br.get(k,0) or ''
        block_rows += f"""<tr><td>{strat}</td><td>{bc('vpin_too_low')}</td>
          <td>{bc('spread_too_wide')}</td><td>{bc('already_moved')}</td>
          <td>{bc('position_conflict')}</td><td>{bc('cooldown')}</td><td>{bc('other')}</td></tr>"""

    sig_detail_html = ''
    for strat in sorted(signals_detail):
        sd = signals_detail[strat]
        parts=[]
        for k,label in [('vpin_avg_at_fire','vpin@fire'),('vpin_avg_at_block','vpin@block'),
                         ('conf_avg_at_fire','conf@fire'),('score_avg_at_fire','score@fire')]:
            if sd.get(k) is not None: parts.append(f"{label}={sd[k]}")
        sig_detail_html += f'<h3>{strat}</h3>'
        if parts: sig_detail_html += f'<p class="dim">{" &nbsp;|&nbsp; ".join(parts)}</p>'
        if sd.get('top_fired_details'):
            sig_detail_html += '<p><strong>Top fired:</strong> '
            sig_detail_html += ', '.join(f'{d} <span class="dim">×{n}</span>'
                                         for d,n in sd['top_fired_details'].items()) + '</p>'
        if sd.get('top_symbols'):
            sig_detail_html += '<p><strong>Top symbols:</strong> '
            sig_detail_html += ', '.join(f'{s} <span class="dim">×{n}</span>'
                                         for s,n in sd['top_symbols'].items()) + '</p>'

    # ── Section 2: Engine Sim Trades ─────────────────────────────
    all_exit_types=[]
    for d in engine_trade_analysis.values():
        for k in d.get('exits',{}):
            if k not in all_exit_types: all_exit_types.append(k)
    order=['trail','sl','time','inertia','tp','unknown']
    all_exit_types=[k for k in order if k in all_exit_types]+[k for k in all_exit_types if k not in order]

    eng_rows=''
    for strat in sorted(engine_trade_analysis):
        d=engine_trade_analysis[strat]
        nc=_pcolor(d['net'])
        eng_rows+=f"""<tr><td>{strat}</td><td>{d['trades']}</td><td>{d['wins']}</td>
          <td>{d['wr']:.1f}%</td>
          <td style="color:{nc}">{d['net']:+.2f}%</td>
          <td>{d['avg_per_trade']:+.4f}%</td></tr>"""

    exit_headers=''.join(f'<th>{et}</th>' for et in all_exit_types)
    exit_rows=''
    for strat in sorted(engine_trade_analysis):
        exits=engine_trade_analysis[strat].get('exits',{}); total=engine_trade_analysis[strat]['trades']
        cells=[]
        for et in all_exit_types:
            n=exits.get(et,0); pct=n/total*100 if total else 0
            css=f'b-{et}' if et in order else ''
            cells.append(f'<td><span class="badge {css}">{n}({pct:.0f}%)</span></td>' if n else '<td class="dim">—</td>')
        exit_rows+=f'<tr><td>{strat}</td><td>{total}</td>{"".join(cells)}</tr>'

    # ── Section 3: Binance Ground Truth ──────────────────────────
    bnb_section = ''
    if bnb_data:
        ov  = bnb_data['trades_stats']['overall']
        bal = bnb_data['balance']
        inc = bnb_data['income_totals']
        bsym= bnb_data['trades_stats']['by_symbol']
        avg_dur=int(ov['avg_dur_sec'])

        inc_rows=''.join(f"<tr><td>{k}</td><td style='color:{_pcolor(v)}'>{v:+.6f} USDT</td></tr>"
                         for k,v in inc.items())
        sym_rows=''
        for sym,d in sorted(bsym.items(),key=lambda x:-abs(x[1]['pnl'])):
            sym_rows+=f"""<tr><td>{sym.replace('USDT','')}</td><td>{d['trades']}</td>
              <td style="color:{_pcolor(d['pnl'])}">{d['pnl']:+.4f}</td>
              <td>{d['wins']}/{d['losses']}</td><td>{d['wr_pct']:.0f}%</td>
              <td class="neg">{d['commission']:.4f}</td>
              <td style="color:{'#ff6b6b' if d['avg_slippage_pct']<-0.1 else '#888'}">{d['avg_slippage_pct']:+.4f}%</td></tr>"""

        trade_rows=''
        for t in sorted(bnb_data['trades_table'],key=lambda x:x['open_time'],reverse=True)[:40]:
            dur=f"{t['dur_sec']//60}m{t['dur_sec']%60}s"
            trade_rows+=f"""<tr><td>{t['open_time'][11:]}</td>
              <td>{t['sym'].replace('USDT','')}</td>
              <td style="color:{'#7cfc00' if t['dir']=='long' else '#ff6b6b'}">{t['dir']}</td>
              <td>{t['entry_px']}</td><td>{t['exit_px']}</td><td>{t['qty']}</td>
              <td style="color:{_pcolor(t['realized_pnl'])}">{t['realized_pnl']:+.4f}</td>
              <td class="neg">{t['commission']:.4f}</td>
              <td style="color:{'#ff6b6b' if t['slippage_pct']<-0.2 else '#888'}">{t['slippage_pct']:+.4f}%</td>
              <td>{dur}</td></tr>"""

        bnb_section = f"""
<h2 class="section-bnb">📡 Binance {('LIVE' if LIVE_MODE else 'DEMO')} — Ground Truth</h2>
<div>
  {_stat('Balance', f"{bal.get('balance','?')} USDT")}
  {_stat('Available', f"{bal.get('availableBalance','?')} USDT")}
  {_stat('Trades', ov['total_trades'])}
  {_stat('Win Rate', f"{ov['wr_pct']:.0f}%")}
  {_stat('Realized PnL', f"{ov['total_pnl']:+.4f} USDT", _pcolor(ov['total_pnl']))}
  {_stat('Commission', f"{ov['total_commission']:.4f} USDT", '#ff6b6b')}
  {_stat('Avg Duration', f"{avg_dur//60}m {avg_dur%60}s")}
  {_stat('Avg Slippage', f"{ov['avg_slippage_pct']:+.4f}%", '#ff6b6b' if ov['avg_slippage_pct']<-0.1 else '#888')}
</div>
<h3>Income Breakdown</h3>
<table style="width:320px"><tr><th>Type</th><th>USDT</th></tr>{inc_rows}</table>
<h3>Per-Symbol Performance (Binance)</h3>
<table><tr><th>Symbol</th><th>Trades</th><th>PnL USDT</th><th>W/L</th><th>WR%</th><th>Commission</th><th>Avg Slip</th></tr>
{sym_rows}</table>
<h3>Fill Log (last 40)</h3>
<table><tr><th>Time UTC</th><th>Sym</th><th>Dir</th><th>Entry</th><th>Exit</th><th>Qty</th>
  <th>PnL USDT</th><th>Comm</th><th>Slippage</th><th>Dur</th></tr>
{trade_rows}</table>"""

    # ── Section 4: Reconciliation ─────────────────────────────────
    recon_section = ''
    if recon:
        st = recon['stats']
        matched = recon['matched']
        unmatched_bnb = recon['unmatched_binance']

        recon_stats = f"""<div>
  {_stat('Matched', st['matched_count'])}
  {_stat('Unmatched Binance', st['unmatched_bnb_count'], '#ffd700' if st['unmatched_bnb_count'] else None)}
  {_stat('Unmatched Engine', st['unmatched_eng_count'], '#ffd700' if st['unmatched_eng_count'] else None)}
  {_stat('Avg Entry Slip', f"{(st['avg_entry_slip_pct'] or 0):+.4f}%", _pcolor(st['avg_entry_slip_pct'] or 0))}
  {_stat('Avg Exit Slip',  f"{(st['avg_exit_slip_pct'] or 0):+.4f}%",  _pcolor(st['avg_exit_slip_pct'] or 0))}
  {_stat('Avg PnL Diff (real−sim)', f"{(st['avg_pnl_diff_pct'] or 0):+.4f}%", _pcolor(st['avg_pnl_diff_pct'] or 0))}
</div>"""

        recon_rows = ''
        for r in sorted(matched, key=lambda x: x['open_time'], reverse=True):
            es = f"{r['entry_slip_pct']:+.4f}%" if r['entry_slip_pct'] is not None else '—'
            xs = f"{r['exit_slip_pct']:+.4f}%"  if r['exit_slip_pct']  is not None else '—'
            esc = '#ff6b6b' if (r['entry_slip_pct'] or 0) < -0.2 else '#888'
            xsc = '#ff6b6b' if (r['exit_slip_pct']  or 0) < -0.2 else '#888'
            dur = f"{r['dur_sec']//60}m{r['dur_sec']%60}s"
            recon_rows += f"""<tr>
              <td>{r['open_time'][11:]}</td>
              <td>{r['sym'].replace('USDT','')}</td>
              <td style="color:{'#7cfc00' if r['dir']=='long' else '#ff6b6b'}">{r['dir']}</td>
              <td>{r['eng_entry'] or '—'}</td>
              <td style="color:{_pcolor(r['eng_pnl_pct'])}">{r['eng_pnl_pct']:+.4f}%</td>
              <td class="dim">{r['eng_reason']}</td>
              <td>{r['bnb_entry']}</td>
              <td style="color:{_pcolor(r['bnb_pnl_pct'])}">{r['bnb_pnl_pct']:+.4f}%</td>
              <td style="color:{_pcolor(r['bnb_pnl_usdt'])}">{r['bnb_pnl_usdt']:+.4f}</td>
              <td style="color:{esc}">{es}</td>
              <td style="color:{xsc}">{xs}</td>
              <td style="color:{_pcolor(r['pnl_diff_pct'])}">{r['pnl_diff_pct']:+.4f}%</td>
              <td>{dur}</td>
              <td class="dim">{r['match_delta_ms']}ms</td></tr>"""

        unmatched_rows = ''
        for t in unmatched_bnb:
            unmatched_rows += f"""<tr>
              <td>{t['open_time'][11:]}</td><td>{t['sym'].replace('USDT','')}</td>
              <td style="color:{'#7cfc00' if t['dir']=='long' else '#ff6b6b'}">{t['dir']}</td>
              <td style="color:{_pcolor(t['realized_pnl'])}">{t['realized_pnl']:+.4f} USDT</td>
              <td>{t['pnl_pct']:+.4f}%</td>
              <td class="dim">no engine match within 60s</td></tr>"""

        unmatched_section = f"""
<h3 class="warn">⚠️ Binance fills with no engine match ({len(unmatched_bnb)})</h3>
<table><tr><th>Time</th><th>Sym</th><th>Dir</th><th>PnL USDT</th><th>PnL%</th><th>Note</th></tr>
{unmatched_rows}</table>""" if unmatched_bnb else ''

        recon_section = f"""
<h2 class="section-rec">🔗 Reconciliation — Engine Sim vs Binance Fills</h2>
{recon_stats}
<table>
  <tr>
    <th>Time UTC</th><th>Sym</th><th>Dir</th>
    <th style="background:#0f2010;color:#7cfc00">Eng Entry</th>
    <th style="background:#0f2010;color:#7cfc00">Eng PnL%</th>
    <th style="background:#0f2010;color:#7cfc00">Exit</th>
    <th style="background:#0f1020;color:#88ccff">BNB Entry</th>
    <th style="background:#0f1020;color:#88ccff">BNB PnL%</th>
    <th style="background:#0f1020;color:#88ccff">PnL USDT</th>
    <th style="background:#1a0f20;color:#cc88ff">Entry Slip</th>
    <th style="background:#1a0f20;color:#cc88ff">Exit Slip</th>
    <th style="background:#1a0f20;color:#cc88ff">PnL Diff</th>
    <th>Dur</th><th class="dim">Match Δ</th>
  </tr>
  {recon_rows}
</table>
{unmatched_section}"""

    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<title>PredictEngine Unified Analysis</title>
<style>{CSS}</style>
</head><body>
<h1>⚡ PredictEngine — Unified Analysis</h1>
<p>Generated: {now} &nbsp;|&nbsp; Window: <strong>{since_label or 'all'}</strong> &nbsp;|&nbsp; {mode_badge}</p>

<h2 class="section-eng">📶 Signal Funnel</h2>
<table><tr><th>Strategy</th><th>Detected</th><th>Blocked</th><th>Fired</th><th>Closed</th><th>D→F%</th><th>F→C%</th></tr>
{funnel_rows}</table>

<h2 class="section-eng">🚫 Block Reasons</h2>
<table><tr><th>Strategy</th><th>vpin_too_low</th><th>spread_too_wide</th><th>already_moved</th>
  <th>position_conflict</th><th>cooldown</th><th>other</th></tr>
{block_rows}</table>

<h2 class="section-eng">🔍 Signal Detail</h2>
{sig_detail_html}

<h2 class="section-eng">📊 Engine Sim — Trade Performance</h2>
<table><tr><th>Strategy</th><th>Trades</th><th>Wins</th><th>WR%</th><th>Net%</th><th>Avg/Trade</th></tr>
{eng_rows}</table>
<h3>Exit Distribution</h3>
<table><tr><th>Strategy</th><th>Total</th>{exit_headers}</tr>{exit_rows}</table>

{bnb_section}
{recon_section}
</body></html>"""

# ══════════════════════════════════════════════════════════════════
# JSON OUTPUT
# ══════════════════════════════════════════════════════════════════

def format_json(signal_funnel, block_reasons, signals_detail, engine_trades,
                bnb_data, recon, since_label):
    return json.dumps({
        'generated':    datetime.now().isoformat(),
        'since':        since_label,
        'mode':         'LIVE' if LIVE_MODE else 'DEMO',
        'signals':      {'funnel':signal_funnel,'block_reasons':block_reasons,'detail':signals_detail},
        'engine_trades': engine_trades,
        'binance':       bnb_data,
        'reconciliation': recon,
    }, indent=2, default=str)

# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="PredictEngine unified analysis — engine logs + Binance demo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 analyze.sh                        pull server + Binance, full report
  python3 analyze.sh --since 2h             last 2 hours
  python3 analyze.sh --since deploy         since last deploy
  python3 analyze.sh --local                use cached logs, still pull Binance
  python3 analyze.sh --local --no-binance   fully offline
  python3 analyze.sh --no-binance           pull server only, no Binance
  python3 analyze.sh --since 6h --json-only JSON only, no HTML
  python3 analyze.sh --stage                pull stage server, skip Binance
  python3 analyze.sh --stage --since 2h     stage last 2h
  python3 analyze.sh --pull-all             pull BOTH servers, analyze combined
""")
    parser.add_argument('--stage',      action='store_true', help='Pull from stage server (0.0.0.0), no Binance')
    parser.add_argument('--pull-all',   action='store_true', help='Pull from BOTH prod and stage servers, then analyze combined')
    parser.add_argument('--clean',      action='store_true', help='Wipe remote CSVs after pulling (avoids duplicates on next pull)')
    parser.add_argument('--local',      action='store_true', help='Use cached local logs (skip server pull)')
    parser.add_argument('--no-binance', action='store_true', help='Skip Binance API pull')
    parser.add_argument('--live',       action='store_true', help='Force LIVE endpoint (prod). Consumed at import for LIVE_MODE; registered here so argparse accepts it.')
    parser.add_argument('--json-only',  action='store_true', help='Output JSON only, no HTML')
    parser.add_argument('--since', metavar='WINDOW',
                        help='Time window: 2h / 6h / 24h / deploy / 2026-06-04T12:00')
    args = parser.parse_args()

    # ── Stage override ────────────────────────────────────────────
    if args.stage:
        args.no_binance = True   # stage has no real fills

    # Set correct deployments log for this env
    global DEPLOYMENTS_LOG
    DEPLOYMENTS_LOG = _deployments_log(is_stage=getattr(args, 'stage', False))

    # ── Pull-all handled below after local/remote branch ───────────

    # ── Since cutoff ──────────────────────────────────────────────
    cutoff      = parse_since(args.since) if args.since else None
    since_label = cutoff.strftime('%Y-%m-%d %H:%M:%S') if cutoff else None
    # For Binance: default to 24h if no --since given
    bnb_since   = cutoff if cutoff else (datetime.now() - timedelta(hours=24))
    # NOTE: bnb_since is re-adjusted after trades load to cover earliest engine trade

    # ── Engine logs ───────────────────────────────────────────────
    if getattr(args, 'pull_all', False):
        print("📥 Pulling from BOTH prod and stage servers...", file=sys.stderr)
        args.no_binance = True
        _clean = getattr(args, 'clean', False)
        prod_dirs  = pull_data_from_server(clean_remote=_clean, is_stage=False)
        stage_dirs = pull_data_from_server(clean_remote=_clean, is_stage=True)
        session_dirs = [d for d in prod_dirs + stage_dirs if d]
        print(f"   ✅ Combined: {len(prod_dirs)} prod + {len(stage_dirs)} stage sessions", file=sys.stderr)
    elif args.local:
        _sfx = "_stage" if getattr(args, "stage", False) else "_prod"
        all_sessions = sorted(glob.glob(f"./data_backup/20??????_??????{_sfx}/"))
        if not all_sessions:  # fallback: old sessions without suffix
            all_sessions = sorted(glob.glob("./data_backup/20??????_??????/"))
        if not all_sessions:
            print("❌ No local sessions in ./data_backup/", file=sys.stderr); sys.exit(1)
        if cutoff:
            after  = [s for s in all_sessions if (session_dir_timestamp(s) or datetime.min) >= cutoff]
            before = [s for s in all_sessions if (session_dir_timestamp(s) or datetime.min) <  cutoff]
            session_dirs = (before[-1:] + after) if before else after
            print(f"📂 Local: {len(session_dirs)} session(s) since {since_label}", file=sys.stderr)
        else:
            session_dirs = [all_sessions[-1]]
            print(f"📂 Local: {session_dirs[0]}", file=sys.stderr)
    else:
        if args.stage:
            print("⚠️  STAGE mode — pulling from 0.0.0.0, skipping Binance", file=sys.stderr)
        _clean = getattr(args, 'clean', True)  # default True for single-server pull
        session_dirs = pull_data_from_server(clean_remote=_clean, is_stage=getattr(args, 'stage', False))
        if not session_dirs:
            print("   ⚠️  No new data from server", file=sys.stderr)
            sys.exit(0)

    print("📊 Analyzing engine logs...", file=sys.stderr)
    logs_dirs = [os.path.join(s,'logs') for s in session_dirs]
    signals   = load_signals(logs_dirs, cutoff=None)   # filter by row-time below, not file-time
    trades    = load_trades(session_dirs, cutoff=None)    # filter by row-time below, not file-time
    if cutoff:
        signals = filter_by_since(signals, cutoff, label='signals')
        trades  = filter_by_since(trades,  cutoff, label='trades')

    signal_funnel, block_reasons, signals_detail = analyze_signals(signals)
    engine_trade_analysis = analyze_engine_trades(trades)

    # Load positions CSV (real Binance opens/closes logged by live_execution.py)
    positions_rows = []
    for sd in session_dirs:
        positions_rows += load_positions_csv(sd)
    if positions_rows:
        # Tag engine trades with is_live=1 where we have a matching positions row
        # Match by sym + ts within 2s
        pos_by_sym = defaultdict(list)
        for p in positions_rows:
            if p.get('event') == 'open' and p.get('sym'):
                try:
                    ts = datetime.fromisoformat(p['ts'].replace('Z','+00:00')).replace(tzinfo=None)
                    pos_by_sym[p['sym']].append((ts, p))
                except Exception: pass
        tagged = 0
        for t in trades:
            sym = t.get('sym','')
            try:
                e_ts = datetime.fromtimestamp(float(t.get('ts_epoch') or t.get('ts') or 0),
                                              tz=timezone.utc).replace(tzinfo=None)
            except Exception: continue
            for p_ts, p in pos_by_sym.get(sym, []):
                if abs((e_ts - p_ts).total_seconds()) < 5:
                    t['is_live'] = '1'
                    t['_strategy_from_pos'] = p.get('strategy','')
                    tagged += 1
                    break
        if tagged:
            print(f"   [positions] tagged {tagged} engine trades as live", file=sys.stderr)

    # Extend bnb_since to cover earliest engine trade timestamp
    # so Binance fills for those trades are included in reconciliation
    if trades:
        from datetime import timezone as _tz
        eng_ts = []
        for t in trades:
            try:
                ts = float(t.get('ts_epoch') or t.get('ts') or 0)
                if ts > 1e9: eng_ts.append(datetime.fromtimestamp(ts, tz=_tz.utc).replace(tzinfo=None))
            except: pass
        if eng_ts:
            earliest = min(eng_ts) - timedelta(minutes=5)  # 5min buffer before first fire
            if earliest < bnb_since:
                bnb_since = earliest
                print(f"   ⏱  bnb_since extended to {bnb_since} to cover engine trades", file=sys.stderr)

    # ── Binance ───────────────────────────────────────────────────
    bnb_data = None
    recon    = None
    if not args.no_binance:
        bnb_data = pull_binance_data(bnb_since)
        if not trades:
            print("⚠️  No engine trades found — skipping reconciliation", file=sys.stderr)
        elif bnb_data and trades:
            recon = reconcile(trades, bnb_data)
            st = recon['stats']
            shared = st.get('unmatched_eng_shared', 0)
            missing = st.get('unmatched_eng_truly_missing', 0)
            print(f"🔗 Reconciliation: {st['matched_count']} matched  "
                  f"{st['unmatched_bnb_count']} unmatched-BNB  "
                  f"{st['unmatched_eng_count']} unmatched-eng "
                  f"(shared={shared} missing={missing})", file=sys.stderr)

    # ── Output ────────────────────────────────────────────────────
    ts        = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_str  = format_json(signal_funnel, block_reasons, signals_detail,
                            engine_trade_analysis, bnb_data, recon, since_label)
    json_file = f"analysis_{ts}.json"
    Path(json_file).write_text(json_str)
    print(f"✅  {json_file}", file=sys.stderr)

    if not args.json_only:
        html_str  = render_html(signal_funnel, block_reasons, signals_detail,
                                engine_trade_analysis, bnb_data, recon, since_label)
        html_file = f"analysis_{ts}.html"
        Path(html_file).write_text(html_str)
        print(f"✅  {html_file}", file=sys.stderr)

    # ── Correlation analysis ──────────────────────────────────────
    corr_script = Path(__file__).parent / "analyze_correlation.py"
    if corr_script.exists() and not args.json_only:
        import subprocess as _sp
        r = _sp.run([sys.executable, str(corr_script), json_file],
                    capture_output=True, text=True)
        if r.returncode == 0:
            for line in r.stderr.strip().splitlines():
                print(line, file=sys.stderr)
        else:
            print(f"   [corr] warning: {r.stderr}", file=sys.stderr)
        corr_json = json_file.replace('.json', '_corr.json')
        print(f"\nShare with Claude:  cat {corr_json} | pbcopy", file=sys.stderr)
    else:
        print(f"\nShare with Claude:  cat {json_file} | pbcopy", file=sys.stderr)

if __name__ == '__main__':
    main()
