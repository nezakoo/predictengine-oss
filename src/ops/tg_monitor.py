#!/usr/bin/env python3
"""
PredictEngine — Telegram Monitor
=================================
Covers:
  • Engine process alive/dead detection
  • Crash detection via journald (catches ALL failures incl. SyntaxError, OOM)
  • Systemd service crash alerts (with last log lines)
  • Server health: CPU / RAM / Disk
  • Daily PnL summary from preds_*.csv files
  • Strategy silence detection (no trades in X hours)
  • Heartbeat ping every N hours
  • Binance WebSocket disconnect detection (log grep)

Usage (run standalone, or via cron):
  python3 tg_monitor.py [--once | --loop | --pnl | --heartbeat]

  --once      Run all checks once and exit          (good for cron every 5 min)
  --loop      Run in a loop with sleep intervals     (alternative to cron)
  --pnl       Send daily PnL summary now and exit
  --heartbeat Send heartbeat ping now and exit

Environment variables (set in /home/ubuntu/engine/.env):
  TG_BOT_TOKEN   — from @BotFather
  TG_CHAT_ID     — your personal chat ID (or group ID)
"""

import os
import sys
import glob
import time
import shutil
import subprocess
import csv
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

# ── Config ────────────────────────────────────────────────────────────────────

BOT_TOKEN  = os.getenv("TG_BOT_TOKEN", "")
CHAT_ID    = os.getenv("TG_CHAT_ID", "")
PREFIX     = os.getenv("TELEGRAM_PREFIX", "")   # e.g. "[STAGE] " — prepended to all messages

ENGINE_DIR      = Path("/home/ubuntu/engine")
LOG_DIR         = ENGINE_DIR / "logs"
DATA_DIR        = ENGINE_DIR
SERVICE_NAME    = "predict-engine"

# Health thresholds
CPU_WARN_PCT    = 80
RAM_WARN_PCT    = 85
DISK_WARN_PCT   = 90

# Strategy silence: alert if no preds file was touched in this many hours
SILENCE_HOURS   = 2

# Heartbeat: send alive ping every N hours (used in --loop mode)
HEARTBEAT_HOURS = 6

# Loop mode sleep interval (seconds)
LOOP_SLEEP_SEC  = 60    # check every 60s for fast crash detection

# State file to avoid duplicate alerts
STATE_FILE = ENGINE_DIR / ".tg_monitor_state"

# ── Telegram ──────────────────────────────────────────────────────────────────

def tg(msg: str, silent: bool = False) -> bool:
    if PREFIX:
        msg = f"{PREFIX} {msg}"
    if not BOT_TOKEN or not CHAT_ID:
        print(f"[tg] NO TOKEN/CHAT_ID — would send: {msg[:80]}")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": CHAT_ID,
                "text": msg,
                "parse_mode": "HTML",
                "disable_notification": silent,
            },
            timeout=10,
        )
        if r.status_code != 200:
            print(f"[tg] HTTP {r.status_code}: {r.text[:200]}")
            return False
        return True
    except Exception as e:
        print(f"[tg] send failed: {e}")
        return False


# ── State helpers ─────────────────────────────────────────────────────────────

def _load_state() -> dict:
    if STATE_FILE.exists():
        import json
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {}

def _save_state(state: dict):
    import json
    STATE_FILE.write_text(json.dumps(state))

def _was_alerted(state: dict, key: str, cooldown_sec: int = 300) -> bool:
    ts = state.get(key)
    if ts and (time.time() - ts) < cooldown_sec:
        return True
    return False

def _mark_alerted(state: dict, key: str):
    state[key] = time.time()


# ── Crash log detection (catches EVERYTHING) ──────────────────────────────────

def check_crash_logs(state: dict) -> list[str]:
    """
    Watch journald for crash signals in the last LOOP_SLEEP_SEC window.
    Catches: SyntaxError, AttributeError, OOM kill, any Traceback, FAILURE.
    This is the primary crash detection — works even before Python starts.
    """
    alerts = []
    try:
        # Grab journal entries from last loop cycle + small buffer
        since_sec = LOOP_SLEEP_SEC + 30
        r = subprocess.run(
            ["journalctl", "-u", SERVICE_NAME,
             f"--since={since_sec} seconds ago",
             "--no-pager", "--output=short-iso"],
            capture_output=True, text=True, timeout=10
        )
        if r.returncode != 0 or not r.stdout.strip():
            return alerts

        log = r.stdout

        # Detect crash signals
        has_failure  = "Failed with result" in log or "FAILURE" in log
        has_traceback = "Traceback (most recent call last)" in log
        # Use exact error names to avoid 'ConnectionClosedError'/'CancelledError' matching 'Error'
        has_error    = any(e in log for e in [
            "SyntaxError", "AttributeError", "ImportError", "RuntimeError",
            "KeyError", "TypeError", "MemoryError", "NameError",
            "Killed", "Out of memory",
        ])

        # Patterns that look like crashes but are benign — suppress alert
        BENIGN_PATTERNS = [
            'keepalive ping timeout',       # WS disconnect, auto-reconnects
            'ConnectionClosedError',        # WS disconnect variant
            'no close frame received',      # WS disconnect variant
            'AssertionError',               # Python 3.14 drain() bug — benign
            'assert waiter is None',        # Python 3.14 drain() assertion
            '_drain_helper',                # Python 3.14 drain() stack frame
            'keepalive_ping',               # WS keepalive task crash
            'write_frame',                  # WS write frame during disconnect
            'sys.meta_path is None',        # interpreter shutdown during restart
            'Task was destroyed but it is pending',  # asyncio cleanup on shutdown
            'CancelledError',               # asyncio task cancel on restart
        ]
        HARD_ERRORS = [
            'SyntaxError', 'AttributeError', 'ImportError', 'NameError',
            'TypeError', 'KeyError', 'RuntimeError', 'MemoryError',
            'Out of memory', 'Killed',
            # NOTE: do NOT add 'Error' here — too broad, matches ConnectionClosedError etc.
        ]
        # Benign: log contains at least one benign pattern AND no hard errors
        has_benign = any(p in log for p in BENIGN_PATTERNS)
        has_hard   = any(e in log for e in HARD_ERRORS)
        is_benign_only = (has_failure or has_traceback) and has_benign and not has_hard

        if has_failure and (has_traceback or has_error) and not is_benign_only:
            key = "crash_detected"
            # Use a short cooldown — we want every distinct crash
            if not _was_alerted(state, key, cooldown_sec=30):
                # Extract the traceback block
                lines = log.splitlines()
                tb_lines = []
                in_tb = False
                for line in lines:
                    if "Traceback (most recent call last)" in line:
                        in_tb = True
                        tb_lines = [line]
                    elif in_tb:
                        tb_lines.append(line)
                        # Stop at blank line or systemd metadata after error
                        if ("systemd" in line and "exited" in line) or \
                           ("Failed with result" in line):
                            break

                tb_text = "\n".join(tb_lines)[-3000:] if tb_lines else log[-2000:]

                # Get the last error line (most useful part)
                error_line = ""
                for line in reversed(lines):
                    stripped = line.split("] ")[-1].strip() if "] " in line else line.strip()
                    stripped = stripped.lstrip(": ")
                    if any(e in stripped for e in ["Error:", "Exception:", "SyntaxError"]):
                        error_line = stripped
                        break

                tb_safe = tb_text[-2500:].replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
                err_safe = error_line[:200].replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
                msg = f"🔴 <b>ENGINE CRASH</b>"
                if err_safe:
                    msg += f"\n<code>{err_safe}</code>"
                msg += f"\n\n<pre>{tb_safe}</pre>"

                alerts.append(msg)
                _mark_alerted(state, key)

        elif has_error and has_failure:
            # Crash without traceback (e.g. OOM kill)
            key = "crash_no_tb"
            if not _was_alerted(state, key, cooldown_sec=60):
                last_lines = "\n".join(log.splitlines()[-10:])
                alerts.append(
                    f"🔴 <b>ENGINE CRASH</b> (no traceback)\n"
                    f"<pre>{last_lines[-1500:]}</pre>"
                )
                _mark_alerted(state, key)

    except Exception as e:
        print(f"[crash_check] error: {e}")

    return alerts


# ── Engine process check ──────────────────────────────────────────────────────

def check_engine(state: dict) -> list[str]:
    alerts = []
    try:
        r = subprocess.run(
            ["systemctl", "is-active", SERVICE_NAME],
            capture_output=True, text=True, timeout=5
        )
        active = r.stdout.strip() == "active"
    except Exception:
        active = False

    if not active:
        key = "engine_down"
        if not _was_alerted(state, key, cooldown_sec=600):
            last_lines = _get_last_log_lines(10)
            msg = (
                f"🔴 <b>ENGINE DOWN</b>\n"
                f"Service <code>{SERVICE_NAME}</code> is not active.\n\n"
                f"<b>Last logs:</b>\n<pre>{last_lines}</pre>\n\n"
                f"<code>sudo systemctl restart {SERVICE_NAME}</code>"
            )
            alerts.append(msg)
            _mark_alerted(state, key)
        state.pop("engine_up_notified", None)
    else:
        # Recovery notice
        if state.get("engine_down") and not state.get("engine_up_notified"):
            alerts.append(
                f"✅ <b>ENGINE RECOVERED</b>\n"
                f"Service <code>{SERVICE_NAME}</code> is active again."
            )
            state["engine_up_notified"] = True
            state.pop("engine_down", None)
        # Clear crash cooldowns when engine is stable
        state.pop("crash_detected", None)
        state.pop("crash_no_tb", None)

    return alerts


def _get_last_log_lines(n: int = 15) -> str:
    try:
        r = subprocess.run(
            ["journalctl", "-u", SERVICE_NAME, "-n", str(n),
             "--no-pager", "--output=short"],
            capture_output=True, text=True, timeout=5
        )
        if r.returncode == 0 and r.stdout.strip():
            return r.stdout.strip()[-1500:]
    except Exception:
        pass
    logs = sorted(LOG_DIR.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True) if LOG_DIR.exists() else []
    if logs:
        try:
            lines = logs[0].read_text(errors="replace").splitlines()
            return "\n".join(lines[-n:])
        except Exception:
            pass
    return "(no logs available)"


# ── Server health ─────────────────────────────────────────────────────────────

def check_server(state: dict) -> list[str]:
    alerts = []

    try:
        load1 = os.getloadavg()[0]
        cpu_count = os.cpu_count() or 1
        cpu_pct = (load1 / cpu_count) * 100
        if cpu_pct > CPU_WARN_PCT:
            key = "cpu_high"
            if not _was_alerted(state, key, 1800):
                alerts.append(
                    f"⚠️ <b>High CPU</b>: {cpu_pct:.0f}%\n"
                    f"(1-min avg: {load1:.2f}, cores: {cpu_count})"
                )
                _mark_alerted(state, key)
        else:
            state.pop("cpu_high", None)
    except Exception:
        pass

    try:
        mem = _parse_meminfo()
        if mem:
            used_pct = 100 * (1 - mem["available"] / mem["total"])
            if used_pct > RAM_WARN_PCT:
                key = "ram_high"
                if not _was_alerted(state, key, 1800):
                    alerts.append(
                        f"⚠️ <b>High RAM</b>: {used_pct:.0f}% used\n"
                        f"({mem['available']/1024:.0f} MB free of {mem['total']/1024:.0f} MB)"
                    )
                    _mark_alerted(state, key)
            else:
                state.pop("ram_high", None)
    except Exception:
        pass

    try:
        disk = shutil.disk_usage(ENGINE_DIR)
        disk_pct = 100 * disk.used / disk.total
        if disk_pct > DISK_WARN_PCT:
            key = "disk_high"
            if not _was_alerted(state, key, 3600):
                free_gb = disk.free / 1_073_741_824
                alerts.append(
                    f"⚠️ <b>Disk almost full</b>: {disk_pct:.0f}% used\n"
                    f"({free_gb:.1f} GB free)"
                )
                _mark_alerted(state, key)
        else:
            state.pop("disk_high", None)
    except Exception:
        pass

    return alerts


def _parse_meminfo() -> dict | None:
    try:
        info = {}
        for line in Path("/proc/meminfo").read_text().splitlines():
            parts = line.split()
            if parts[0] in ("MemTotal:", "MemAvailable:"):
                info[parts[0].rstrip(":").replace("Mem","").lower()] = int(parts[1])
        return {"total": info.get("total", 0), "available": info.get("available", 0)}
    except Exception:
        return None


# ── Strategy silence check ────────────────────────────────────────────────────

def check_strategy_silence(state: dict) -> list[str]:
    alerts = []
    pattern = str(DATA_DIR / "preds_*.csv")
    files = glob.glob(pattern)
    if not files:
        return alerts

    latest_mtime = max(Path(f).stat().st_mtime for f in files)
    age_hours = (time.time() - latest_mtime) / 3600

    if age_hours > SILENCE_HOURS:
        key = "strategy_silence"
        if not _was_alerted(state, key, 3600):
            last_file = max(files, key=lambda f: Path(f).stat().st_mtime)
            alerts.append(
                f"⚠️ <b>Strategy Silence</b>\n"
                f"No preds CSV updated in {age_hours:.1f}h\n"
                f"Last: <code>{Path(last_file).name}</code>"
            )
            _mark_alerted(state, key)
    else:
        state.pop("strategy_silence", None)

    return alerts


# ── WS disconnect detection ───────────────────────────────────────────────────

def check_ws_disconnects(state: dict) -> list[str]:
    alerts = []
    try:
        r = subprocess.run(
            ["journalctl", "-u", SERVICE_NAME, "--since", "10 minutes ago",
             "--no-pager", "--output=cat"],
            capture_output=True, text=True, timeout=5
        )
        if r.returncode != 0:
            return alerts
        lines = r.stdout
        # Only count reconnects that aren't the benign Python 3.14 drain() crash
        benign_ws = lines.count("keepalive ping") + lines.count("_drain_helper") + lines.count("assert waiter")
        reconnects = lines.count("reconnect") + lines.count("ConnectionClosed") + lines.count("WS disconnect")
        reconnects = max(0, reconnects - benign_ws)
        if reconnects >= 5:
            key = "ws_reconnects"
            if not _was_alerted(state, key, 600):
                alerts.append(
                    f"⚠️ <b>WS Instability</b>\n"
                    f"{reconnects} reconnect events in last 10 min\n"
                    f"Check: <code>journalctl -u {SERVICE_NAME} -n 30</code>"
                )
                _mark_alerted(state, key)
        else:
            state.pop("ws_reconnects", None)
    except Exception:
        pass
    return alerts


# ── Daily PnL summary ─────────────────────────────────────────────────────────

def send_pnl_summary():
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    pattern = str(DATA_DIR / f"preds_{today}_*.csv")
    files = glob.glob(pattern)

    if not files:
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d")
        files = glob.glob(str(DATA_DIR / f"preds_{yesterday}_*.csv"))
        date_label = f"Yesterday ({yesterday})"
    else:
        date_label = f"Today ({today})"

    if not files:
        tg(f"📊 <b>Daily PnL</b> — no CSV files found for {today}")
        return

    lines = [f"📊 <b>Daily PnL Summary</b> — {date_label}\n"]
    total_net  = 0.0
    total_wins = 0
    total_loss = 0
    total_be   = 0

    for fpath in sorted(files):
        fname = Path(fpath).name
        parts = fname.replace(".csv","").split("_")
        strat_label = parts[4] if len(parts) > 4 else "?"
        strat_name  = "_".join(parts[5:]) if len(parts) > 5 else fname

        try:
            wins = losses = be = 0
            net = 0.0
            with open(fpath, newline="") as f:
                for row in csv.DictReader(filter(lambda r: not r.startswith("#"), f)):
                    outcome = row.get("outcome","").strip()
                    net_val = row.get("net_exit","").strip()
                    if outcome == "win":    wins += 1
                    elif outcome == "loss": losses += 1
                    elif outcome == "be":   be += 1
                    try:
                        net += float(net_val) if net_val else 0.0
                    except ValueError:
                        pass

            total = wins + losses + be
            if total == 0:
                continue

            wr = 100 * wins / total if total else 0
            emoji = "🟢" if net >= 0 else "🔴"
            lines.append(
                f"{emoji} <b>{strat_label}</b> {strat_name[:20]}\n"
                f"   {wins}W/{losses}L/{be}BE  WR:{wr:.0f}%  Net:{net:+.2f}%"
            )
            total_net  += net
            total_wins += wins
            total_loss += losses
            total_be   += be

        except Exception as e:
            lines.append(f"⚪ {strat_label}: (parse error: {e})")

    grand_total = total_wins + total_loss + total_be
    grand_wr    = 100 * total_wins / grand_total if grand_total else 0
    grand_emoji = "🟢" if total_net >= 0 else "🔴"

    lines.append(
        f"\n{grand_emoji} <b>TOTAL</b>  "
        f"{total_wins}W/{total_loss}L/{total_be}BE  "
        f"WR:{grand_wr:.0f}%  Net:{total_net:+.2f}%"
    )

    tg("\n".join(lines))


# ── Heartbeat ─────────────────────────────────────────────────────────────────

def send_heartbeat():
    try:
        load1 = os.getloadavg()[0]
        cpu_count = os.cpu_count() or 1
        cpu_pct = (load1 / cpu_count) * 100
    except Exception:
        cpu_pct = -1

    mem_str = ""
    try:
        mem = _parse_meminfo()
        if mem:
            used_pct = 100 * (1 - mem["available"] / mem["total"])
            mem_str = f"  RAM {used_pct:.0f}%"
    except Exception:
        pass

    try:
        disk = shutil.disk_usage(ENGINE_DIR)
        disk_pct = 100 * disk.used / disk.total
        disk_str = f"  Disk {disk_pct:.0f}%"
    except Exception:
        disk_str = ""

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    tg(
        f"💓 <b>Engine alive</b> — {now}\n"
        f"CPU {cpu_pct:.0f}%{mem_str}{disk_str}",
        silent=True,
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def run_all_checks():
    state = _load_state()
    all_alerts = []

    all_alerts += check_crash_logs(state)   # journal-based — catches everything
    all_alerts += check_engine(state)       # service up/down
    all_alerts += check_server(state)       # CPU/RAM/disk
    all_alerts += check_strategy_silence(state)
    all_alerts += check_ws_disconnects(state)

    for alert in all_alerts:
        tg(alert)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ALERT: {alert[:100]}")

    if not all_alerts:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] All checks OK")

    _save_state(state)


def run_loop():
    last_heartbeat = 0.0
    while True:
        run_all_checks()
        if time.time() - last_heartbeat > HEARTBEAT_HOURS * 3600:
            send_heartbeat()
            last_heartbeat = time.time()
        time.sleep(LOOP_SLEEP_SEC)


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else "--once"

    if arg == "--pnl":
        send_pnl_summary()
    elif arg == "--heartbeat":
        send_heartbeat()
    elif arg == "--loop":
        run_loop()
    else:
        run_all_checks()
