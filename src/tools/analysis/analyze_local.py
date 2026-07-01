#!/usr/bin/env python3
"""
PredictEngine — analyze.py  v4.0
Pull from remote + analyze local archive → JSON + HTML report with fees.

Usage:
  python3 analyze.py                        # pull from server + analyze latest session
  python3 analyze.py --local                # skip pull, analyze latest local session
  python3 analyze.py --all                  # analyze ALL local sessions (full archive)
  python3 analyze.py --days 3               # analyze sessions from last N days
  python3 analyze.py --since 20260525       # analyze sessions from YYYYMMDD onward
  python3 analyze.py --since-deploy         # analyze since last deploy stamp
  python3 analyze.py --strategy K --strategy Z  # filter by strategy label
  python3 analyze.py --fee 0.08             # override round-trip fee (default 0.08)
  python3 analyze.py --no-pull              # alias for --local
  python3 analyze.py --json-only            # skip HTML
  python3 analyze.py --html-only            # skip JSON
  python3 analyze.py --out report           # base output name

Default behaviour (no flags):
  1. Pull CSVs + signals from server → save to data_backup/TIMESTAMP/
  2. Clean remote files after pull
  3. Analyze that session only
  4. Output analysis_TIMESTAMP.json + analysis_TIMESTAMP.html

Handles:
  - New layout:  session/prod/preds_*.csv  +  session/logs/signals_*.csv
  - Old layout:  session/preds_*.csv  (no prod/ subdir)
  - New schema (28 cols): entry_px, atr_entry, vpin_entry, max_dp, min_dp, snap30, snap60
  - Old schema (14 cols): entry, pct_exit, outcome, reason, dur_sec
  - OUT_ rows:   engine writes exit data as OUT_<time> rows — matched to entries
  - Inline rows: old schema writes outcome/pct_exit on the same row as entry
  - Open rows:   no outcome + no pct_exit → skipped (unfired snapshots)
  - AGG files:   v13, v16-test, Baseline, FreqAI-Adaptive etc → grouped as AGG
  - QQ and other 2-letter labels → handled correctly
  - Fees:        FEE_RT deducted from every pct_exit before aggregation
"""

import os, sys, re, json, csv, glob, subprocess, argparse
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path

VERSION = "4.0"

# ── Fee config ────────────────────────────────────────────────────
# Binance USDT-M Futures VIP0:
#   Maker: 0.02% per side → round-trip 0.04%
#   Taker: 0.04% per side → round-trip 0.08%
# Algo engines use market orders → taker both sides → 0.08%
# Override with --fee or by editing this line.
FEE_RT = 0.08

# ── Server config ─────────────────────────────────────────────────
SERVER     = "${DEPLOY_HOST:-user@host.example.com}"
SSH_KEY    = os.path.expanduser("~/.ssh/oracle_key")
REMOTE_DIR = "~/engine"
BACKUP_DIR = "./data_backup"

# ── Deploy stamp ──────────────────────────────────────────────────
DEPLOY_STAMP = Path(__file__).parent / "data_backup" / ".deploy_stamp"

# ── Known header fields (truncation detection) ───────────────────
_KNOWN_HEADER = {"time", "sym", "dir", "conf", "score", "outcome",
                 "entry", "entry_px", "pct_exit", "reason", "dur_sec"}


# ══════════════════════════════════════════════════════════════════
# REMOTE PULL
# ══════════════════════════════════════════════════════════════════

def pull_from_server(clean_remote=True):
    """
    SSH into server, zip + pull all CSVs and signal logs,
    save into a timestamped session folder.
    Returns the session path.
    """
    ctrl = f"{os.path.expanduser('~')}/.ssh/ctl-oracle-%r@%h:%p"
    ssh  = (f"ssh -i {SSH_KEY} -o ControlMaster=auto "
            f"-o ControlPath={ctrl} -o ControlPersist=60s {SERVER}")

    session_dir = f"{BACKUP_DIR}/{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    os.makedirs(f"{session_dir}/prod", exist_ok=True)
    os.makedirs(f"{session_dir}/logs", exist_ok=True)

    print("📡 Connecting to server...", file=sys.stderr)

    # ── Trade CSVs ──
    r = subprocess.run(f"{ssh} 'ls {REMOTE_DIR}/*.csv 2>/dev/null | wc -l'",
                       shell=True, capture_output=True, text=True)
    csv_count = int(r.stdout.strip() or "0")

    if csv_count > 0:
        print(f"   Trade CSVs : {csv_count}", file=sys.stderr)
        r = subprocess.run(
            f"{ssh} 'cd {REMOTE_DIR} && zip -q /tmp/pe_csvs.zip *.csv && cat /tmp/pe_csvs.zip'",
            shell=True, capture_output=True)
        zip_path = f"{session_dir}/prod/csvs.zip"
        with open(zip_path, "wb") as f:
            f.write(r.stdout)
        subprocess.run(f"unzip -q -o {zip_path} -d {session_dir}/prod",
                       shell=True, capture_output=True)
        os.remove(zip_path)
        pulled = len(glob.glob(f"{session_dir}/prod/preds_*.csv"))
        print(f"   Extracted  : {pulled} preds_*.csv", file=sys.stderr)
    else:
        print("   Trade CSVs : 0 (nothing to pull)", file=sys.stderr)

    # ── Signal CSVs ──
    r = subprocess.run(
        f"{ssh} 'ls {REMOTE_DIR}/logs/signals_*.csv 2>/dev/null | wc -l'",
        shell=True, capture_output=True, text=True)
    sig_count = int(r.stdout.strip() or "0")

    if sig_count > 0:
        print(f"   Signal CSVs: {sig_count}", file=sys.stderr)
        r = subprocess.run(
            f"{ssh} 'cd {REMOTE_DIR}/logs && "
            f"head -1 $(ls signals_*.csv | head -1) && "
            f"for f in signals_*.csv; do tail -n +2 \"$f\"; done'",
            shell=True, capture_output=True, text=True)
        sig_path = f"{session_dir}/logs/signals_combined.csv"
        with open(sig_path, "w") as f:
            f.write(r.stdout)
        rows = r.stdout.count("\n")
        print(f"   Signal rows: {rows}", file=sys.stderr)
    else:
        print("   Signal CSVs: 0", file=sys.stderr)

    # ── Clean remote ──
    if clean_remote and (csv_count > 0 or sig_count > 0):
        subprocess.run(
            f"{ssh} 'rm -f {REMOTE_DIR}/*.csv {REMOTE_DIR}/logs/signals_*.csv'",
            shell=True, capture_output=True)
        print("   Remote     : cleaned ✓", file=sys.stderr)

    print(f"   Session    : {session_dir}", file=sys.stderr)
    return session_dir


# ══════════════════════════════════════════════════════════════════
# SESSION DISCOVERY
# ══════════════════════════════════════════════════════════════════

def iter_sessions(backup_dir, since=None, since_dt=None, session_filter=None):
    """
    Yield (session_name, session_path, prod_dir).

    Layouts:
      New: session/prod/preds_*.csv  +  session/logs/
      Old: session/preds_*.csv  (no prod/ subdir)

    session_filter: if set, only yield sessions whose name matches this list.
    """
    for s in sorted(glob.glob(os.path.join(backup_dir, "2*"))):
        if not os.path.isdir(s):
            continue
        name = os.path.basename(s)

        if session_filter and name not in session_filter:
            continue
        if since and name < since:
            continue
        if since_dt:
            try:
                if datetime.strptime(name[:15], "%Y%m%d_%H%M%S") < since_dt:
                    continue
            except ValueError:
                pass

        prod = os.path.join(s, "prod")
        if os.path.isdir(prod):
            yield name, s, prod
        elif glob.glob(os.path.join(s, "preds_*.csv")):
            yield name, s, s


# ══════════════════════════════════════════════════════════════════
# FILENAME HELPERS
# ══════════════════════════════════════════════════════════════════

def extract_label(fname):
    """
    preds_YYYYMMDD_HHMM_K_...      → 'K'
    preds_YYYYMMDD_HHMM_QQ_...     → 'QQ'
    preds_YYYYMMDD_HHMM_v13_...    → 'AGG'
    preds_YYYYMMDD_HHMM_Baseline_  → 'AGG'
    """
    m = re.match(r'preds_\d{8}_\d{4}_([A-Z]{1,2})(?:_|$)', fname)
    return m.group(1) if m else "AGG"


def extract_date(fname):
    m = re.match(r'preds_(\d{8})_\d{4}', fname)
    return m.group(1) if m else "unknown"


# ══════════════════════════════════════════════════════════════════
# CSV LOADER — both schemas, OUT_ rows, inline rows
# ══════════════════════════════════════════════════════════════════

def load_csv_file(path):
    """
    Returns (list_of_closed_trade_dicts, error_string_or_None).

    Two formats handled:
      OUT_ rows  — engine writes entry row normally, then OUT_<time> row with exit data.
                   Match by sym+time key.
      Inline rows — old schema: outcome + pct_exit on the same row as entry.
                   Returned directly if outcome is present.
    """
    try:
        with open(path, newline="", encoding="utf-8-sig", errors="replace") as f:
            raw = f.readlines()
    except Exception as e:
        return [], str(e)

    data_lines = []
    for line in raw:
        line = line.replace("\r\n", "\n").replace("\r", "\n")
        if not line.startswith("#") and line.strip():
            data_lines.append(line)

    if not data_lines:
        return [], None

    try:
        reader     = csv.DictReader(data_lines)
        fieldnames = reader.fieldnames or []
    except Exception:
        return [], None

    if not any(f in _KNOWN_HEADER for f in fieldnames):
        return [], "truncated"

    new_schema = "entry_px" in fieldnames
    entry_key  = "entry_px" if new_schema else "entry"

    entries, exits, inline = {}, {}, []

    for row in reader:
        t = (row.get("time") or "").strip()
        if not t:
            continue

        sym     = (row.get("sym") or "").strip()
        outcome = (row.get("outcome") or "").strip().lower()
        pct_raw = (row.get("pct_exit") or row.get("net_exit") or "").strip()
        is_out  = t.startswith("OUT_")
        clean_t = t[4:] if is_out else t
        key     = f"{sym}_{clean_t}"

        rec = {
            "time":        clean_t,
            "sym":         sym,
            "dir":         (row.get("dir") or "").strip().lower(),
            "conf":        _f(row, "conf"),
            "score":       _f(row, "score"),
            "entry":       _f(row, entry_key),
            "dyn_tp":      _f(row, "dyn_tp"),
            "dyn_sl":      _f(row, "dyn_sl"),
            "version":     (row.get("version") or "").strip(),
            "atr_entry":   _f(row, "atr_entry"),
            "vpin_entry":  _f(row, "vpin_entry"),
            "spread":      _f(row, "spread_entry") or _f(row, "spread_pct"),
            "n_agree":     _i(row, "n_agree"),
            "n_avail":     _i(row, "n_avail"),
        }

        if is_out or (outcome and pct_raw):
            rec.update({
                "pct_exit":    _f(row, "pct_exit"),
                "net_exit":    _f(row, "net_exit"),
                "outcome":     outcome,
                "reason":      (row.get("reason") or "").strip().lower(),
                "dur_sec":     _f(row, "dur_sec"),
                "max_dp":      _f(row, "max_dp"),
                "min_dp":      _f(row, "min_dp"),
                "snap30":      _f(row, "snap30"),
                "snap60":      _f(row, "snap60"),
                "be_activated":str(row.get("be_activated") or "").strip(),
                "be_at_sec":   _f(row, "be_at_sec"),
                "tp_extended": str(row.get("tp_extended") or "").strip(),
                "tp_touches":  _i(row, "tp_touches"),
            })
            if is_out:
                exits[key] = rec
            else:
                inline.append(rec)
        else:
            entries[key] = rec

    resolved = list(inline)
    for key, ex in exits.items():
        resolved.append({**entries.get(key, {}), **ex})

    return resolved, None


def _f(row, key, default=None):
    v = (row.get(key) or "").strip()
    if not v: return default
    try: return float(v)
    except: return default

def _i(row, key, default=None):
    v = _f(row, key)
    return int(v) if v is not None else default


# ══════════════════════════════════════════════════════════════════
# BULK LOADERS
# ══════════════════════════════════════════════════════════════════

def load_all_trades(backup_dir, since=None, since_dt=None,
                    labels=None, session_filter=None):
    trades, seen = [], set()
    n_files = 0
    for sess_name, _, prod_dir in iter_sessions(backup_dir, since, since_dt, session_filter):
        for fpath in sorted(glob.glob(os.path.join(prod_dir, "preds_*.csv"))):
            if fpath in seen: continue
            seen.add(fpath)
            fname = os.path.basename(fpath).replace(".csv", "")
            label = extract_label(fname)
            date  = extract_date(fname)
            if labels and label not in labels and label != "AGG":
                continue
            resolved, err = load_csv_file(fpath)
            if err and err != "truncated":
                print(f"  ⚠ {os.path.basename(fpath)}: {err}", file=sys.stderr)
                continue
            n_files += 1
            for t in resolved:
                t["_label"]   = label
                t["_session"] = sess_name
                t["_date"]    = date
                trades.append(t)
    print(f"   {n_files} files → {len(trades)} closed trades", file=sys.stderr)
    return trades


def load_all_signals(backup_dir, since=None, since_dt=None, session_filter=None):
    signals, seen = [], set()
    for sess_name, sess_path, _ in iter_sessions(backup_dir, since, since_dt, session_filter):
        logs_dir = os.path.join(sess_path, "logs")
        if not os.path.isdir(logs_dir): continue
        for fpath in sorted(glob.glob(os.path.join(logs_dir, "signals_*.csv"))):
            if fpath in seen: continue
            seen.add(fpath)
            try:
                with open(fpath, newline="", encoding="utf-8", errors="replace") as f:
                    for row in csv.DictReader(f):
                        row["_session"] = sess_name
                        signals.append(row)
            except Exception as e:
                print(f"  ⚠ signals {os.path.basename(fpath)}: {e}", file=sys.stderr)
    print(f"   {len(signals)} signal rows", file=sys.stderr)
    return signals


# ══════════════════════════════════════════════════════════════════
# ANALYSIS
# ══════════════════════════════════════════════════════════════════

def analyze_trades(trades, fee_rt=FEE_RT):
    by = defaultdict(lambda: {
        "wins": 0, "losses": 0, "flats": 0,
        "gross": 0.0, "net": 0.0,
        "win_pcts": [], "loss_pcts": [],
        "durs": [], "confs": [], "scores": [],
        "max_dps": [], "snap30s": [], "snap60s": [],
        "coins": set(), "longs": 0, "shorts": 0,
        "daily_net": defaultdict(float),
        "by_reason": defaultdict(lambda: {"n":0,"wins":0,"losses":0,"net":0.0}),
        "by_sym":    defaultdict(lambda: {"n":0,"wins":0,"losses":0,"net":0.0}),
        "be_count": 0, "tp_ext_count": 0,
    })

    for t in trades:
        label   = t.get("_label", "?")
        outcome = (t.get("outcome") or "").strip().lower()
        reason  = (t.get("reason")  or "?").strip().lower() or "?"
        sym     = (t.get("sym")     or "?").strip().upper().replace("USDT","")
        dirn    = (t.get("dir")     or "").strip().lower()
        date    = t.get("_date", "unknown")

        pct = t.get("pct_exit") if t.get("pct_exit") is not None else t.get("net_exit")
        if pct is None: continue

        gross = float(pct)
        net   = gross - fee_rt
        s     = by[label]

        s["gross"] += gross
        s["net"]   += net
        s["coins"].add(sym)

        if dirn == "long":   s["longs"]  += 1
        elif dirn == "short": s["shorts"] += 1

        if outcome == "win":
            s["wins"] += 1; s["win_pcts"].append(gross)
        elif outcome in ("lose","loss"):
            s["losses"] += 1; s["loss_pcts"].append(gross)
        elif outcome == "flat":
            s["flats"] += 1

        br = s["by_reason"][reason]
        br["n"] += 1; br["net"] += net
        if outcome == "win":             br["wins"]   += 1
        elif outcome in ("lose","loss"): br["losses"] += 1

        bs = s["by_sym"][sym]
        bs["n"] += 1; bs["net"] += net
        if outcome == "win":             bs["wins"]   += 1
        elif outcome in ("lose","loss"): bs["losses"] += 1

        for attr, key in [("durs","dur_sec"),("confs","conf"),("scores","score"),
                           ("max_dps","max_dp"),("snap30s","snap30"),("snap60s","snap60")]:
            v = t.get(key)
            if v is not None:
                fv = float(v)
                if attr in ("confs","scores") and fv <= 0: continue
                s[attr].append(fv)

        if str(t.get("be_activated","")).lower() in ("true","1","yes"): s["be_count"] += 1
        if str(t.get("tp_extended","")).lower()  in ("true","1","yes"): s["tp_ext_count"] += 1
        if date != "unknown": s["daily_net"][date] += net

    result = {}
    for label, s in by.items():
        total   = s["wins"] + s["losses"] + s["flats"]
        decided = s["wins"] + s["losses"]
        wr      = s["wins"] / decided * 100 if decided > 0 else 0

        def avg(lst): return sum(lst)/len(lst) if lst else None

        by_reason_out = {}
        for r, v in sorted(s["by_reason"].items(), key=lambda x: -x[1]["n"]):
            n = v["n"]
            by_reason_out[r] = {
                "n": n, "net": round(v["net"],4),
                "wr": round(v["wins"]/n*100,1) if n else 0,
            }

        by_sym_out = {}
        for sym, v in sorted(s["by_sym"].items(), key=lambda x: -x[1]["n"])[:20]:
            n = v["n"]
            by_sym_out[sym] = {
                "n": n, "net": round(v["net"],4),
                "wr": round(v["wins"]/n*100,1) if n else 0,
            }

        result[label] = {
            "trades":        total,
            "wins":          s["wins"],
            "losses":        s["losses"],
            "flats":         s["flats"],
            "wr":            round(wr, 1),
            "gross_pct":     round(s["gross"], 4),
            "net_pct":       round(s["net"],   4),
            "fee_total":     round(total * fee_rt, 4),
            "avg_per_trade": round(s["net"]/total, 5) if total else 0,
            "avg_win_pct":   round(avg(s["win_pcts"])  or 0, 4),
            "avg_loss_pct":  round(avg(s["loss_pcts"]) or 0, 4),
            "avg_dur_sec":   round(avg(s["durs"]),   1) if s["durs"]   else None,
            "avg_conf":      round(avg(s["confs"]),  1) if s["confs"]  else None,
            "avg_score":     round(avg(s["scores"]), 1) if s["scores"] else None,
            "avg_max_dp":    round(avg(s["max_dps"]),4) if s["max_dps"] else None,
            "avg_snap30":    round(avg(s["snap30s"]),4) if s["snap30s"] else None,
            "avg_snap60":    round(avg(s["snap60s"]),4) if s["snap60s"] else None,
            "n_coins":       len(s["coins"]),
            "longs":         s["longs"],
            "shorts":        s["shorts"],
            "tp_hits":       s["by_reason"].get("tp",{}).get("n",0),
            "inertia_n":     s["by_reason"].get("inertia",{}).get("n",0),
            "inertia_pct":   round(s["by_reason"].get("inertia",{}).get("n",0)/total*100,1) if total else 0,
            "be_activated":  s["be_count"],
            "tp_extended":   s["tp_ext_count"],
            "by_reason":     by_reason_out,
            "by_sym":        by_sym_out,
            "daily_net":     {k: round(v,4) for k,v in sorted(s["daily_net"].items())},
        }
    return result


def analyze_signals(signals):
    funnel = defaultdict(lambda: {
        "detected":0,"blocked":0,"fired":0,"closed":0,
        "block_reasons": defaultdict(int),
    })
    for sig in signals:
        strat  = (sig.get("strategy") or "?").strip()
        event  = (sig.get("event")    or "").strip().lower()
        detail = (sig.get("detail")   or "").strip().lower()

        if event in ("impulse","pattern","detected","signal"):
            funnel[strat]["detected"] += 1
        elif event == "blocked":
            funnel[strat]["blocked"] += 1
            if   "vpin"   in detail: funnel[strat]["block_reasons"]["vpin"]     += 1
            elif "spread" in detail: funnel[strat]["block_reasons"]["spread"]   += 1
            elif "conf"   in detail: funnel[strat]["block_reasons"]["conf"]     += 1
            elif "score"  in detail: funnel[strat]["block_reasons"]["score"]    += 1
            elif "open"   in detail or "position" in detail:
                                     funnel[strat]["block_reasons"]["has_open"] += 1
            elif "cooldown" in detail: funnel[strat]["block_reasons"]["cooldown"] += 1
            else:                    funnel[strat]["block_reasons"]["other"]    += 1
        elif event == "fired":  funnel[strat]["fired"]  += 1
        elif event == "closed": funnel[strat]["closed"] += 1

    result = {}
    for strat, v in funnel.items():
        det = v["detected"]; fir = v["fired"]; blk = v["blocked"]
        result[strat] = {
            "detected":      det,
            "blocked":       blk,
            "fired":         fir,
            "closed":        v["closed"],
            "d2f_pct":       round(fir/det*100,1) if det else 0,
            "block_pct":     round(blk/(det+blk)*100,1) if (det+blk) else 0,
            "block_reasons": dict(v["block_reasons"]),
        }
    return result


# ══════════════════════════════════════════════════════════════════
# HTML DASHBOARD
# ══════════════════════════════════════════════════════════════════

def generate_html(report):
    data_js   = json.dumps(report, ensure_ascii=False, default=str)
    summary   = report["summary"]
    generated = summary.get("generated_at","")
    mode      = summary.get("mode","")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PredictEngine — {generated}</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;600&family=IBM+Plex+Sans:wght@400;600;700&display=swap" rel="stylesheet">
<style>
:root{{
  --bg:#07090f;--bg2:#0d1017;--bg3:#131a24;--bg4:#1a2233;
  --border:#1e2a3a;--text:#c8d4e0;--dim:#4a5a6a;--dim2:#6a7a8a;
  --accent:#00d4ff;--pos:#2ecc71;--neg:#e74c3c;--neu:#7f8c9a;--warn:#f39c12;
  --mono:'IBM Plex Mono',monospace;--sans:'IBM Plex Sans',sans-serif;
}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--text);font-family:var(--mono);font-size:12px;line-height:1.5}}
.layout{{display:flex;height:100vh}}
.sidebar{{width:200px;min-width:200px;background:var(--bg2);border-right:1px solid var(--border);overflow-y:auto;display:flex;flex-direction:column}}
.main{{flex:1;overflow-y:auto;padding:24px 28px}}
.hdr{{background:var(--bg2);border-bottom:1px solid var(--border);padding:12px 16px;position:sticky;top:0;z-index:10}}
.hdr-title{{font-family:var(--sans);font-size:11px;font-weight:700;color:var(--accent);letter-spacing:.15em;text-transform:uppercase}}
.hdr-sub{{font-size:10px;color:var(--dim2);margin-top:2px}}
.sb-section{{font-size:9px;font-weight:600;letter-spacing:.15em;text-transform:uppercase;color:var(--dim);padding:12px 12px 4px}}
.sb-item{{padding:7px 12px;cursor:pointer;border-left:2px solid transparent;display:flex;align-items:center;gap:8px;transition:.1s}}
.sb-item:hover{{background:var(--bg3)}}
.sb-item.active{{background:var(--bg3);border-left-color:var(--accent)}}
.sb-lbl{{font-family:var(--sans);font-size:12px;font-weight:700;color:var(--accent);min-width:24px}}
.sb-name{{font-size:10px;color:var(--dim2);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:110px}}
.sb-badge{{margin-left:auto;font-size:9px;padding:1px 5px;border-radius:2px;white-space:nowrap}}
.bdg-pos{{background:rgba(46,204,113,.15);color:var(--pos)}}
.bdg-neg{{background:rgba(231,76,60,.15);color:var(--neg)}}
.bdg-neu{{background:rgba(127,140,154,.15);color:var(--neu)}}
.cards{{display:grid;gap:10px;margin-bottom:16px}}
.c5{{grid-template-columns:repeat(5,1fr)}}.c4{{grid-template-columns:repeat(4,1fr)}}.c3{{grid-template-columns:repeat(3,1fr)}}
.card{{background:var(--bg2);border:1px solid var(--border);border-radius:4px;padding:12px 14px}}
.card-lbl{{font-size:9px;color:var(--dim2);text-transform:uppercase;letter-spacing:.1em;margin-bottom:5px}}
.card-val{{font-family:var(--sans);font-size:20px;font-weight:700;line-height:1}}
.card-sub{{font-size:10px;color:var(--dim2);margin-top:3px}}
.pos{{color:var(--pos)}}.neg{{color:var(--neg)}}.neu{{color:var(--neu)}}.warn{{color:var(--warn)}}
.stitle{{font-size:9px;font-weight:600;letter-spacing:.15em;text-transform:uppercase;color:var(--dim);border-bottom:1px solid var(--border);padding-bottom:6px;margin:20px 0 12px}}
.tbl-wrap{{overflow-x:auto;border:1px solid var(--border);border-radius:4px;background:var(--bg2);margin-bottom:16px}}
table{{width:100%;border-collapse:collapse}}
th{{padding:7px 10px;text-align:left;font-size:9px;font-weight:600;letter-spacing:.1em;text-transform:uppercase;color:var(--dim);border-bottom:1px solid var(--border);background:var(--bg3);white-space:nowrap}}
td{{padding:6px 10px;border-bottom:1px solid rgba(255,255,255,.04);font-size:11px;white-space:nowrap}}
tr:last-child td{{border-bottom:none}}
tr.click{{cursor:pointer}}
tr.click:hover td{{background:rgba(0,212,255,.03)}}
.chart-wrap{{background:var(--bg2);border:1px solid var(--border);border-radius:4px;padding:14px;margin-bottom:16px}}
.rbar-row{{display:flex;align-items:center;gap:8px;margin-bottom:6px}}
.rbar-lbl{{width:70px;font-size:10px;color:var(--dim2)}}
.rbar-track{{flex:1;height:6px;background:var(--bg3);border-radius:3px;overflow:hidden}}
.rbar-fill{{height:100%;border-radius:3px}}
.rbar-stat{{width:160px;text-align:right;font-size:10px}}
.fee-note{{background:var(--bg3);border:1px solid var(--border);border-radius:4px;padding:8px 12px;font-size:10px;color:var(--dim2);margin-bottom:16px}}
.fee-note b{{color:var(--text)}}
.mode-badge{{display:inline-block;padding:2px 8px;border-radius:3px;font-size:10px;font-weight:600;
             background:rgba(0,212,255,.12);color:var(--accent);margin-left:8px}}
.strat-hdr{{display:flex;align-items:baseline;gap:12px;margin-bottom:16px}}
.strat-lbl{{font-family:var(--sans);font-size:32px;font-weight:700;color:var(--accent);line-height:1}}
</style>
</head>
<body>
<div class="layout">
  <div class="sidebar" id="sidebar">
    <div class="hdr">
      <div class="hdr-title">⚡ PredictEngine <span class="mode-badge">{mode}</span></div>
      <div class="hdr-sub">{generated}</div>
    </div>
    <div id="sb-content"></div>
  </div>
  <div class="main" id="main"></div>
</div>
<script>
const R=JSON.parse({json.dumps(data_js)});
const T=R.trades, SIG=R.signals||{{}}, SUM=R.summary, FEE=SUM.fee_rt||0.08;
const RC={{trail:'#2ecc71',tp:'#3498db',sl:'#e74c3c',rev:'#f39c12',inertia:'#9b59b6',time:'#7f8c9a','?':'#4a5a6a'}};

function pct(v,d=2){{if(v==null)return'—';return(v>=0?'+':'')+v.toFixed(d)+'%'}}
function col(v){{return v>0?'var(--pos)':v<0?'var(--neg)':'var(--neu)'}}
function wrc(v){{return v>=55?'var(--pos)':v>=45?'var(--warn)':'var(--neg)'}}
function topBR(br){{if(!br)return'—';const e=Object.entries(br).sort((a,b)=>b[1]-a[1]);return e.length?e[0][0]:'—'}}

// sidebar
const sb=document.getElementById('sb-content');
sb.innerHTML+=`<div class="sb-section">Overview</div>
<div class="sb-item active" data-id="_ov" onclick="nav('_ov',this)">
  <span class="sb-lbl">∑</span><span class="sb-name">All strategies</span></div>`;

const strats=Object.keys(T).filter(k=>k!=='AGG').sort((a,b)=>(T[b].net_pct||0)-(T[a].net_pct||0));
sb.innerHTML+=`<div class="sb-section">Strategies</div>`;
[...strats,...(T.AGG?['AGG']:[])].forEach(l=>{{
  const t=T[l];
  const bc=t.net_pct>0?'bdg-pos':t.net_pct<0?'bdg-neg':'bdg-neu';
  sb.innerHTML+=`<div class="sb-item" data-id="${{l}}" onclick="nav('${{l}}',this)">
    <span class="sb-lbl">${{l}}</span><span class="sb-name">${{l}}</span>
    <span class="sb-badge ${{bc}}">${{pct(t.net_pct,1)}}</span></div>`;
}});
if(Object.keys(SIG).length){{
  sb.innerHTML+=`<div class="sb-section">Signals</div>
  <div class="sb-item" data-id="_sig" onclick="nav('_sig',this)">
    <span class="sb-lbl">⚡</span><span class="sb-name">Signal funnel</span></div>`;
}}

// overview
function showOv(){{
  const g=SUM;
  let h=`<div class="fee-note">
    <b>Fee: ${{FEE.toFixed(2)}}% round-trip per trade</b> (VIP0 taker 0.04%×2).
    All net figures are after fees. Gross = raw price move.</div>`;

  h+=`<div class="cards c5">
    <div class="card"><div class="card-lbl">Trades</div>
      <div class="card-val neu">${{g.total_trades.toLocaleString()}}</div></div>
    <div class="card"><div class="card-lbl">Win rate</div>
      <div class="card-val warn">${{g.overall_wr}}%</div></div>
    <div class="card"><div class="card-lbl">Gross P&L</div>
      <div class="card-val ${{g.total_gross_pct>=0?'pos':'neg'}}">${{pct(g.total_gross_pct)}}</div></div>
    <div class="card"><div class="card-lbl">Net P&L (after fees)</div>
      <div class="card-val ${{g.total_net_pct>=0?'pos':'neg'}}">${{pct(g.total_net_pct)}}</div></div>
    <div class="card"><div class="card-lbl">Sessions / Files</div>
      <div class="card-val neu">${{g.n_sessions}}<span style="font-size:12px;color:var(--dim2)"> / ${{g.n_files}}</span></div></div>
  </div>`;

  const ranked=[...strats,...(T.AGG?['AGG']:[])].sort((a,b)=>(T[b].net_pct||0)-(T[a].net_pct||0));
  h+=`<div class="stitle">Strategy ranking — net after fees</div>
  <div class="tbl-wrap"><table><thead><tr>
    <th>#</th><th>Label</th><th>Trades</th><th>WR</th>
    <th>Gross</th><th>Fees</th><th>Net</th><th>Avg/T</th>
    <th>Avg win</th><th>Avg loss</th><th>Break-even need</th>
  </tr></thead><tbody>`;
  ranked.forEach((l,i)=>{{
    const t=T[l];
    const need=FEE-t.avg_win_pct; // how much more avg win needs to grow
    const nc=t.net_pct>=0?'pos':'neg';
    h+=`<tr class="click" onclick="nav('${{l}}',document.querySelector('[data-id=${{l}}]'))">
      <td style="color:var(--dim)">${{i+1}}</td>
      <td style="font-family:var(--sans);font-weight:700;color:var(--accent);font-size:13px">${{l}}</td>
      <td>${{t.trades.toLocaleString()}}</td>
      <td style="color:${{wrc(t.wr)}}">${{t.wr}}%</td>
      <td class="${{t.gross_pct>=0?'pos':'neg'}}">${{pct(t.gross_pct)}}</td>
      <td class="neg">-${{(t.trades*FEE).toFixed(1)}}%</td>
      <td class="${{nc}}">${{pct(t.net_pct)}}</td>
      <td style="color:${{col(t.avg_per_trade)}}">${{pct(t.avg_per_trade,4)}}</td>
      <td class="pos">+${{(t.avg_win_pct||0).toFixed(3)}}%</td>
      <td class="neg">${{(t.avg_loss_pct||0).toFixed(3)}}%</td>
      <td style="color:${{t.avg_per_trade>=-FEE?'var(--pos)':'var(--neg)'}}">
        ${{t.avg_per_trade>=-FEE?'✓ OK':'needs +'+(FEE+t.avg_per_trade).toFixed(4)+'%/t'}}</td>
    </tr>`;
  }});
  h+=`</tbody></table></div>`;

  // daily chart
  const allD=new Set();
  Object.values(T).forEach(t=>Object.keys(t.daily_net||{{}}).forEach(d=>allD.add(d)));
  const dates=[...allD].sort();
  const totals=dates.map(d=>Object.values(T).reduce((s,t)=>s+(t.daily_net?.[d]||0),0));
  h+=`<div class="stitle">Daily net P&L — all strategies combined</div>
  <div class="chart-wrap" style="height:160px"><canvas id="dc-ov" style="width:100%;height:140px"></canvas></div>`;

  document.getElementById('main').innerHTML=h;
  drawBars('dc-ov', dates.map(d=>d.slice(4)), totals);
}}

// strategy detail
function showStrat(l){{
  const t=T[l]; if(!t) return;
  const sig=SIG[l]||{{}};
  let h=`<div class="strat-hdr"><span class="strat-lbl">${{l}}</span></div>`;

  h+=`<div class="fee-note">
    Net = gross − <b>${{FEE.toFixed(2)}}%</b> fee per trade.
    Break-even avg/trade: <b>≥ +${{FEE.toFixed(2)}}%</b>.
    Current: <b style="color:${{col(t.avg_per_trade)}}">${{pct(t.avg_per_trade,4)}}</b>
    ${{t.avg_per_trade>=-FEE?'✓ covers fees':'← needs +'+(FEE+t.avg_per_trade).toFixed(4)+'% more'}}</div>`;

  h+=`<div class="cards c5">
    <div class="card"><div class="card-lbl">Trades</div>
      <div class="card-val neu">${{t.trades.toLocaleString()}}</div>
      <div class="card-sub">${{t.wins}}W / ${{t.losses}}L / ${{t.flats}}F</div></div>
    <div class="card"><div class="card-lbl">Win rate</div>
      <div class="card-val" style="color:${{wrc(t.wr)}}">${{t.wr}}%</div></div>
    <div class="card"><div class="card-lbl">Gross P&L</div>
      <div class="card-val ${{t.gross_pct>=0?'pos':'neg'}}">${{pct(t.gross_pct)}}</div></div>
    <div class="card"><div class="card-lbl">Net P&L</div>
      <div class="card-val ${{t.net_pct>=0?'pos':'neg'}}">${{pct(t.net_pct)}}</div>
      <div class="card-sub neg">fees: -${{t.fee_total.toFixed(1)}}%</div></div>
    <div class="card"><div class="card-lbl">Avg/trade</div>
      <div class="card-val" style="color:${{col(t.avg_per_trade)}};font-size:15px">${{pct(t.avg_per_trade,4)}}</div></div>
  </div>`;

  h+=`<div class="cards c5">
    <div class="card"><div class="card-lbl">Avg win</div>
      <div class="card-val pos" style="font-size:16px">+${{(t.avg_win_pct||0).toFixed(3)}}%</div></div>
    <div class="card"><div class="card-lbl">Avg loss</div>
      <div class="card-val neg" style="font-size:16px">${{(t.avg_loss_pct||0).toFixed(3)}}%</div></div>
    <div class="card"><div class="card-lbl">Avg duration</div>
      <div class="card-val neu" style="font-size:15px">${{t.avg_dur_sec!=null?(t.avg_dur_sec/60).toFixed(1)+'m':'—'}}</div></div>
    <div class="card"><div class="card-lbl">Conf / Score</div>
      <div class="card-val neu" style="font-size:15px">${{t.avg_conf??'—'}} / ${{t.avg_score??'—'}}</div></div>
    <div class="card"><div class="card-lbl">L / S</div>
      <div class="card-val neu" style="font-size:15px">${{t.longs}}L / ${{t.shorts}}S</div></div>
  </div>`;

  if(t.avg_snap30!=null||t.avg_max_dp!=null){{
    h+=`<div class="cards c4">
      <div class="card"><div class="card-lbl">Avg snap30</div>
        <div class="card-val ${{(t.avg_snap30||0)>=0?'pos':'neg'}}" style="font-size:15px">${{pct(t.avg_snap30)}}</div></div>
      <div class="card"><div class="card-lbl">Avg snap60</div>
        <div class="card-val ${{(t.avg_snap60||0)>=0?'pos':'neg'}}" style="font-size:15px">${{pct(t.avg_snap60)}}</div></div>
      <div class="card"><div class="card-lbl">Avg max_dp</div>
        <div class="card-val neu" style="font-size:15px">${{pct(t.avg_max_dp)}}</div></div>
      <div class="card"><div class="card-lbl">BE activated</div>
        <div class="card-val neu" style="font-size:15px">${{t.be_activated||0}}</div></div>
    </div>`;
  }}

  if(sig.detected){{
    h+=`<div class="cards c4">
      <div class="card"><div class="card-lbl">Detected</div>
        <div class="card-val neu" style="font-size:15px">${{sig.detected.toLocaleString()}}</div></div>
      <div class="card"><div class="card-lbl">Blocked (${{sig.block_pct}}%)</div>
        <div class="card-val warn" style="font-size:15px">${{sig.blocked.toLocaleString()}}</div></div>
      <div class="card"><div class="card-lbl">Fired D→F ${{sig.d2f_pct}}%</div>
        <div class="card-val neu" style="font-size:15px">${{sig.fired.toLocaleString()}}</div></div>
      <div class="card"><div class="card-lbl">Top block</div>
        <div class="card-val neu" style="font-size:13px">${{topBR(sig.block_reasons)}}</div></div>
    </div>`;
  }}

  h+=`<div class="stitle">Exit reasons</div>
  <div class="chart-wrap" id="reasons-${{l}}"></div>`;

  h+=`<div class="stitle">Top symbols</div>
  <div class="tbl-wrap"><table>
  <thead><tr><th>Symbol</th><th>Trades</th><th>WR</th><th>Net (after fee)</th></tr></thead><tbody>`;
  Object.entries(t.by_sym||{{}}).forEach(([sym,v])=>{{
    h+=`<tr><td style="color:var(--accent);font-weight:600">${{sym}}</td>
      <td>${{v.n}}</td><td style="color:${{wrc(v.wr)}}">${{v.wr}}%</td>
      <td style="color:${{col(v.net)}}">${{pct(v.net)}}</td></tr>`;
  }});
  h+=`</tbody></table></div>`;

  const dn=t.daily_net||{{}};
  if(Object.keys(dn).length>1){{
    h+=`<div class="stitle">Daily net P&L</div>
    <div class="chart-wrap" style="height:120px">
      <canvas id="dc-${{l}}" style="width:100%;height:100px"></canvas></div>`;
  }}

  document.getElementById('main').innerHTML=h;

  // exit reason bars
  const rd=document.getElementById('reasons-'+l);
  const totalR=Object.values(t.by_reason||{{}}).reduce((a,b)=>a+b.n,0);
  Object.entries(t.by_reason||{{}}).forEach(([r,v])=>{{
    const w=totalR?(v.n/totalR*100).toFixed(1):0;
    rd.innerHTML+=`<div class="rbar-row">
      <span class="rbar-lbl">${{r}}</span>
      <div class="rbar-track"><div class="rbar-fill" style="width:${{w}}%;background:${{RC[r]||RC['?']}}"></div></div>
      <span class="rbar-stat" style="color:${{col(v.net)}}">${{w}}% · ${{v.n}} · ${{pct(v.net)}}</span>
    </div>`;
  }});

  if(Object.keys(dn).length>1){{
    const dates=Object.keys(dn).sort();
    drawBars('dc-'+l, dates.map(d=>d.slice(4)), dates.map(d=>dn[d]));
  }}
}}

// signal funnel view
function showSig(){{
  let h=`<div class="stitle">Signal funnel — all strategies</div>
  <div class="tbl-wrap"><table><thead><tr>
    <th>Strategy</th><th>Detected</th><th>Blocked</th><th>Fired</th>
    <th>D→F%</th><th>Block%</th><th>Top block reason</th>
  </tr></thead><tbody>`;
  Object.entries(SIG).sort((a,b)=>(b[1].fired||0)-(a[1].fired||0)).forEach(([s,v])=>{{
    h+=`<tr><td style="color:var(--accent);font-weight:700">${{s}}</td>
      <td>${{v.detected||0}}</td><td class="warn">${{v.blocked||0}}</td>
      <td>${{v.fired||0}}</td><td>${{v.d2f_pct||0}}%</td>
      <td>${{v.block_pct||0}}%</td>
      <td style="color:var(--dim2)">${{topBR(v.block_reasons)}}</td></tr>`;
  }});
  h+=`</tbody></table></div>`;
  document.getElementById('main').innerHTML=h;
}}

// canvas bar chart
function drawBars(id, labels, values){{
  const cv=document.getElementById(id); if(!cv) return;
  const W=cv.offsetWidth||600, H=cv.offsetHeight||120;
  cv.width=W*2; cv.height=H*2; cv.style.width=W+'px'; cv.style.height=H+'px';
  const ctx=cv.getContext('2d'); ctx.scale(2,2);
  const pad={{t:6,r:8,b:20,l:44}};
  const pw=W-pad.l-pad.r, ph=H-pad.t-pad.b;
  const maxV=Math.max(...values.map(Math.abs),0.01);
  const sy=v=>pad.t+ph*(1-(v+maxV)/(2*maxV));
  const sx=i=>pad.l+(i/(labels.length-1||1))*pw;
  ctx.strokeStyle='#1e2a3a'; ctx.lineWidth=1;
  ctx.beginPath(); ctx.moveTo(pad.l,sy(0)); ctx.lineTo(pad.l+pw,sy(0)); ctx.stroke();
  const bw=Math.max(2,pw/labels.length-1);
  values.forEach((v,i)=>{{
    ctx.fillStyle=v>=0?'rgba(46,204,113,.8)':'rgba(231,76,60,.8)';
    const x=sx(i)-bw/2,y0=sy(0),y1=sy(v);
    ctx.fillRect(x,Math.min(y0,y1),bw,Math.abs(y1-y0)||1);
  }});
  ctx.fillStyle='#4a5a6a'; ctx.font='8px IBM Plex Mono,monospace'; ctx.textAlign='center';
  const step=Math.max(1,Math.floor(labels.length/8));
  labels.forEach((l,i)=>{{ if(i%step===0) ctx.fillText(l,sx(i),H-4); }});
  ctx.textAlign='right';
  [maxV,0,-maxV].forEach(v=>ctx.fillText(v.toFixed(2)+'%',pad.l-3,sy(v)+3));
}}

function nav(id,el){{
  document.querySelectorAll('.sb-item').forEach(e=>e.classList.remove('active'));
  if(el) el.classList.add('active');
  if(id==='_ov') showOv();
  else if(id==='_sig') showSig();
  else if(T[id]) showStrat(id);
  else showOv();
}}
nav('_ov',document.querySelector('[data-id="_ov"]'));
</script>
</body>
</html>"""


# ══════════════════════════════════════════════════════════════════
# DEPLOY STAMP
# ══════════════════════════════════════════════════════════════════

def _find_last_deploy_from_log(log_path):
    """
    Parse deployments.log and return (datetime, label) for the last entry.
    Log format:  YYYYMMDD_HHMMSS | <type> | <description>
    Returns (None, None) if the file cannot be read or has no valid entries.
    """
    try:
        lines = Path(log_path).read_text().strip().splitlines()
    except Exception:
        return None, None

    last_dt, last_lbl = None, None
    for line in lines:
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 3:
            continue
        ts_raw = parts[0]          # e.g. 20260601_231710
        description = parts[2]     # e.g. "fix dashboard visibility"
        try:
            dt = datetime.strptime(ts_raw, "%Y%m%d_%H%M%S")
            last_dt, last_lbl = dt, description
        except ValueError:
            continue

    return last_dt, last_lbl


def read_deploy_stamp(path):
    raw = Path(path).read_text().strip().split("\n")[0].strip()
    label = None
    if " | " in raw:
        ts_part, label = raw.split(" | ", 1)
        raw = ts_part.strip(); label = label.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try: return datetime.strptime(raw, fmt), label
        except ValueError: pass
    try: return datetime.fromtimestamp(float(raw)), label
    except: pass
    raise ValueError(f"Cannot parse deploy stamp: {raw!r}")


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description=f"PredictEngine Analyzer v{VERSION}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 analyze.py                   # pull + analyze latest session
  python3 analyze.py --local           # analyze latest local session (no pull)
  python3 analyze.py --all             # analyze entire archive
  python3 analyze.py --days 3          # last 3 days
  python3 analyze.py --since-deploy    # since last deploy stamp
  python3 analyze.py --fee 0.08        # explicit fee override
        """)

    parser.add_argument("--local",        action="store_true", help="Skip pull, use latest local session")
    parser.add_argument("--no-pull",      action="store_true", help="Alias for --local")
    parser.add_argument("--all",          action="store_true", help="Analyze entire archive (all sessions)")
    parser.add_argument("--days",         type=float, default=None, help="Last N days")
    parser.add_argument("--since",        default=None, help="YYYYMMDD folder filter")
    parser.add_argument("--since-deploy", action="store_true", help="Since .deploy_stamp")
    parser.add_argument("--strategy",     action="append", dest="labels",
                        help="Filter by label (repeatable)")
    parser.add_argument("--fee",          type=float, default=FEE_RT,
                        help=f"Round-trip fee %% (default {FEE_RT})")
    parser.add_argument("--backup-dir",   default=BACKUP_DIR)
    parser.add_argument("--out-dir",      default=".")
    parser.add_argument("--out",          default=None, help="Output base name")
    parser.add_argument("--json-only",    action="store_true")
    parser.add_argument("--html-only",    action="store_true")
    parser.add_argument("--no-clean",     action="store_true", help="Don't delete remote files after pull")
    args = parser.parse_args()

    backup_dir = os.path.expanduser(args.backup_dir)
    out_dir    = os.path.expanduser(args.out_dir)
    os.makedirs(backup_dir, exist_ok=True)
    os.makedirs(out_dir,    exist_ok=True)
    fee_rt     = args.fee
    local_mode = args.local or args.no_pull

    print(f"PredictEngine Analyzer v{VERSION}", file=sys.stderr)
    print(f"Fee RT : {fee_rt}% round-trip", file=sys.stderr)

    # ── Step 1: pull or find session ──────────────────────────────
    session_filter = None
    since_dt       = None
    mode_label     = "local"

    if not local_mode and not args.all and not args.days and not args.since and not args.since_deploy:
        # Default: pull from server, analyze just that session
        session_name = os.path.basename(pull_from_server(clean_remote=not args.no_clean))
        session_filter = [session_name]
        mode_label = "live"
    elif local_mode:
        # Latest local session only
        sessions = sorted(glob.glob(os.path.join(backup_dir, "2*")))
        if not sessions:
            print("❌ No local sessions found.", file=sys.stderr)
            sys.exit(1)
        session_filter = [os.path.basename(sessions[-1])]
        print(f"📂 Using latest local session: {session_filter[0]}", file=sys.stderr)
        mode_label = "local"
    else:
        # Archive / filtered mode
        if args.since_deploy:
            if DEPLOY_STAMP.exists():
                since_dt, lbl = read_deploy_stamp(DEPLOY_STAMP)
                print(f"📌 Since deploy: {since_dt} ({lbl})", file=sys.stderr)
            else:
                # .deploy_stamp missing — fall back to deployments.log
                deploy_log = Path(backup_dir) / "deployments.log"
                since_dt, lbl = _find_last_deploy_from_log(deploy_log)
                if since_dt is None:
                    print(
                        f"❌ No deploy stamp at {DEPLOY_STAMP}\n"
                        f"   Also tried: {deploy_log.resolve()} — not found or empty.\n"
                        f"   Fix: create the stamp file, e.g.:\n"
                        f'   echo "$(date \'+%Y-%m-%d %H:%M:%S\') | my deploy" > {DEPLOY_STAMP}',
                        file=sys.stderr)
                    sys.exit(1)
                print(f"📌 Since deploy (from deployments.log): {since_dt}  ← {lbl}", file=sys.stderr)
                # Write stamp so next run is instant
                DEPLOY_STAMP.parent.mkdir(parents=True, exist_ok=True)
                DEPLOY_STAMP.write_text(f"{since_dt.strftime('%Y-%m-%d %H:%M:%S')} | {lbl}\n")
                print(f"   Saved stamp → {DEPLOY_STAMP}", file=sys.stderr)
            mode_label = "since-deploy"
        elif args.days:
            since_dt = datetime.now() - timedelta(days=args.days)
            print(f"📌 Since: {since_dt} (last {args.days}d)", file=sys.stderr)
            mode_label = f"last-{args.days}d"
        elif args.since:
            mode_label = f"since-{args.since}"
        else:
            mode_label = "all"

    labels = [l.upper() for l in args.labels] if args.labels else None

    # ── Step 2: load ──────────────────────────────────────────────
    sessions_found = list(iter_sessions(backup_dir, args.since, since_dt, session_filter))
    print(f"\n📂 {len(sessions_found)} session(s) to analyze", file=sys.stderr)
    if not sessions_found:
        print("❌ No sessions found.", file=sys.stderr)
        sys.exit(1)

    print("📥 Loading trades...", file=sys.stderr)
    trades = load_all_trades(backup_dir, args.since, since_dt, labels, session_filter)

    print("📥 Loading signals...", file=sys.stderr)
    signals = load_all_signals(backup_dir, args.since, since_dt, session_filter)

    # ── Step 3: analyze ───────────────────────────────────────────
    print("📊 Analyzing...", file=sys.stderr)
    trade_analysis  = analyze_trades(trades, fee_rt)
    signal_analysis = analyze_signals(signals)

    n_files = sum(
        1 for _, _, prod_dir in sessions_found
        for f in os.listdir(prod_dir)
        if f.startswith("preds_") and f.endswith(".csv")
    )

    total_trades = sum(t["trades"]    for t in trade_analysis.values())
    total_wins   = sum(t["wins"]      for t in trade_analysis.values())
    total_losses = sum(t["losses"]    for t in trade_analysis.values())
    total_net    = sum(t["net_pct"]   for t in trade_analysis.values())
    total_gross  = sum(t["gross_pct"] for t in trade_analysis.values())
    decided      = total_wins + total_losses

    summary = {
        "generated_at":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "analyzer_ver":    VERSION,
        "mode":            mode_label,
        "fee_rt":          fee_rt,
        "n_sessions":      len(sessions_found),
        "n_files":         n_files,
        "total_trades":    total_trades,
        "total_wins":      total_wins,
        "total_losses":    total_losses,
        "overall_wr":      round(total_wins / decided * 100, 1) if decided else 0,
        "total_gross_pct": round(total_gross, 4),
        "total_net_pct":   round(total_net, 4),
    }

    report = {"summary": summary, "trades": trade_analysis, "signals": signal_analysis}

    # ── Step 4: output ────────────────────────────────────────────
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.join(out_dir, args.out or f"analysis_{ts}")

    if not args.html_only:
        jp = base + ".json"
        with open(jp, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str)
        print(f"\n✅ JSON → {jp}  ({os.path.getsize(jp)//1024} KB)", file=sys.stderr)

    if not args.json_only:
        hp = base + ".html"
        with open(hp, "w", encoding="utf-8") as f:
            f.write(generate_html(report))
        print(f"✅ HTML → {hp}  ({os.path.getsize(hp)//1024} KB)", file=sys.stderr)

    # ── Console summary ───────────────────────────────────────────
    print(f"\n─── {mode_label.upper()} SUMMARY ─────────────────────────────────", file=sys.stderr)
    print(f"  Sessions : {len(sessions_found)}   Files: {n_files}", file=sys.stderr)
    print(f"  Trades   : {total_trades:,}   WR: {summary['overall_wr']}%", file=sys.stderr)
    print(f"  Gross    : {total_gross:+.3f}%  "
          f"Fees: -{total_trades*fee_rt:.2f}%  "
          f"Net: {total_net:+.3f}%", file=sys.stderr)
    print(f"\n  {'Strat':<6} {'Trades':>7} {'WR':>5} {'Gross':>9} {'Net':>9} {'Avg/T':>10}  Break-even",
          file=sys.stderr)
    print(f"  {'─'*70}", file=sys.stderr)
    for lbl in sorted(trade_analysis.keys()):
        t = trade_analysis[lbl]
        gap = fee_rt + t["avg_per_trade"]  # how much more needed (negative = needs improvement)
        be  = f"✓" if t["avg_per_trade"] >= -fee_rt else f"needs +{abs(gap):.4f}%/t"
        print(f"  {lbl:<6} {t['trades']:>7,} {t['wr']:>4.1f}% "
              f"{t['gross_pct']:>+9.3f}% {t['net_pct']:>+9.3f}% "
              f"{t['avg_per_trade']:>+10.5f}%  {be}", file=sys.stderr)


if __name__ == "__main__":
    main()