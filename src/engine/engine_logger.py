"""
engine_logger.py — Structured logging for predict-engine
=========================================================

DESTINATIONS
────────────
  Telegram      CRITICAL + ERROR only, sent inline via _tg_send() already
                in engine.py. This module wires the same handler into the
                standard logging system so any logging.error() / logging.critical()
                call anywhere in the codebase reaches Telegram automatically.

  engine.log    WARNING + above (rotating, 10 MB × 5 files).
                Trade open/close, WS disconnect/reconnect, scanner changes.
                Replaces the current journald WARNING spam.

  signals_YYYYMMDD.csv
                INFO level events: gate blocks, detected signals.
                One row per unique (strategy, symbol, reason) — deduplicated
                per 10s window so tick-level spam (K/Y every second) collapses
                to one row.

  journald      WARNING+ still flows naturally via stderr (systemd captures it).
                DEBUG is fully suppressed — no more impulse spam every second.

USAGE
─────
  # In predict_engine.py / engine.py startup (once):
  from engine_logger import setup_logging, log_signal, log_trade_open, log_trade_close, log_ws_event
  setup_logging()

  # Replace existing logging.warning() calls in strategies_signals.py:
  #   OLD: import logging; logging.warning(f"[K] impulse {sym}: ...")
  #   NEW: log_signal('K', sym, 'impulse', 'body_up short 0.70% tf=5m')
  #
  #   OLD: import logging; logging.warning(f"[K] blocked {sym}: vpin ...")
  #   NEW: log_signal('K', sym, 'blocked', f'vpin {vpin:.3f} < min {min_vpin}')

  # In strategies_engine.py fire():
  log_trade_open(p)

  # In strategies_engine.py _resolve():
  log_trade_close(p)

  # In engine.py ws_task():
  log_ws_event('disconnect', 'public', str(e))
  log_ws_event('reconnect',  'public')
"""

import csv
import logging
import logging.handlers
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from config import FEE_RT  # used in log_trade_close net calculation

# ── Paths ──────────────────────────────────────────────────────────
ENGINE_DIR  = Path(os.getenv("ENGINE_DIR", Path(__file__).parent))
LOG_DIR     = ENGINE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

ENGINE_LOG  = LOG_DIR / "engine.log"

def _signals_csv_path() -> Path:
    date = datetime.now(timezone.utc).strftime("%Y%m%d")
    return LOG_DIR / f"signals_{date}.csv"

# ── Signals CSV columns ────────────────────────────────────────────
_SIGNALS_COLS = [
    "ts", "strategy", "symbol", "event",
    "detail", "vpin", "atr", "spread", "price", "conf", "score",
]

# Dedup cache: (strategy, symbol, event, detail[:40]) → last_logged_ts
_dedup_cache: dict[tuple, float] = {}
_DEDUP_WINDOW_SEC = 10

# ── Telegram handler ───────────────────────────────────────────────

class _TelegramHandler(logging.Handler):
    """
    Sends ERROR and CRITICAL log records to Telegram inline.
    Reuses _tg_send() from engine.py — imported lazily to avoid
    circular imports (engine imports engine_logger, not the other way).
    Deduplicated: same message won't fire more than once per 5 minutes.
    """
    def __init__(self):
        super().__init__(level=logging.ERROR)
        self._cache: dict[str, float] = {}

    # Patterns that look like crashes but are benign — suppress from Telegram
    _SUPPRESS = [
        'sys.meta_path is None',          # interpreter shutdown during uvicorn log emit
        'keepalive ping timeout',          # WS disconnect, auto-reconnects
        'ConnectionClosedError',           # WS disconnect, auto-reconnects
        'no close frame received',         # WS disconnect variant
        'Task was destroyed but pending',  # asyncio cleanup on shutdown
        'CancelledError',                  # asyncio task cancel on shutdown
    ]

    def emit(self, record: logging.LogRecord):
        try:
            msg = record.getMessage()
            # Suppress known-benign patterns that spam Telegram
            if any(p in msg for p in self._SUPPRESS):
                return
            key = f"{record.levelno}:{record.name}:{msg[:80]}"
            now = time.time()
            if now - self._cache.get(key, 0) < 300:
                return
            self._cache[key] = now

            level_icon = "🔴" if record.levelno >= logging.CRITICAL else "⚠️"
            text = (
                f"{level_icon} <b>{record.levelname}</b> "
                f"[{record.name}]\n"
                f"<code>{self.format(record)[:3000]}</code>"
            )
            # Lazy import to avoid circular dependency
            try:
                import engine as E
                E._tg_send(text)
            except Exception:
                pass
        except Exception:
            pass


# ── Setup ──────────────────────────────────────────────────────────

_logging_configured = False

def setup_logging(level_file: int = logging.WARNING,
                  level_console: int = logging.WARNING) -> None:
    """
    Call once at engine startup (predict_engine.py main()).

    Sets up:
      • RotatingFileHandler  → logs/engine.log  (WARNING+, 10MB×5)
      • StreamHandler        → stderr / journald (WARNING+)
      • _TelegramHandler     → Telegram          (ERROR+)
      • Root level set to DEBUG so INFO signals can reach CSV writer
        without polluting any handler above WARNING.
    """
    global _logging_configured
    if _logging_configured:
        return
    _logging_configured = True

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)   # allow everything through; handlers filter

    fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    # Rotating file — WARNING and above
    fh = logging.handlers.RotatingFileHandler(
        ENGINE_LOG,
        maxBytes=10 * 1024 * 1024,   # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    fh.setLevel(level_file)
    fh.setFormatter(fmt)
    root.addHandler(fh)

    # Console/journald — WARNING and above (replaces the current root handler)
    # This suppresses the per-tick DEBUG spam from reaching journald.
    ch = logging.StreamHandler()
    ch.setLevel(level_console)
    ch.setFormatter(logging.Formatter("%(levelname)s [%(name)s] %(message)s"))
    root.addHandler(ch)

    # Telegram — ERROR and CRITICAL only
    th = _TelegramHandler()
    th.setFormatter(fmt)
    root.addHandler(th)

    logging.getLogger(__name__).info("engine_logger initialised → %s", ENGINE_LOG)


# ── Named loggers ──────────────────────────────────────────────────
# Use these in other modules for clean [source] prefixes in engine.log

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

# Pre-built loggers for the main modules
engine_log   = logging.getLogger("engine")
strategy_log = logging.getLogger("strategy")
ws_log       = logging.getLogger("ws")
scanner_log  = logging.getLogger("scanner")


# ── Public API ─────────────────────────────────────────────────────

def log_signal(
    strategy: str,
    symbol: str,
    event: str,          # 'detected', 'fired', 'blocked'
    detail: str = "",
    vpin: Optional[float] = None,
    atr: Optional[float] = None,
    spread: Optional[float] = None,
    price: Optional[float] = None,
    conf: Optional[float] = None,
    score: Optional[float] = None,
) -> None:
    """
    Log a gate check or signal detection to signals_YYYYMMDD.csv.

    Deduplicated: same (strategy, symbol, event, detail[:40]) suppressed
    for DEDUP_WINDOW_SEC seconds. Collapses per-tick spam to one row.

    Does NOT write to journald or engine.log — CSV only.
    """
    key = (strategy, symbol, event, detail[:40])
    now = time.time()
    if now - _dedup_cache.get(key, 0) < _DEDUP_WINDOW_SEC:
        return
    _dedup_cache[key] = now

    path = _signals_csv_path()
    write_header = not path.exists()
    try:
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if write_header:
                w.writerow(_SIGNALS_COLS)
            w.writerow([
                datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
                strategy,
                symbol,
                event,
                detail,
                f"{vpin:.3f}"   if vpin   is not None else "",
                f"{atr:.4f}"    if atr    is not None else "",
                f"{spread:.5f}" if spread is not None else "",
                f"{price:.6f}"  if price  is not None else "",
                f"{conf:.0f}"   if conf   is not None else "",
                f"{score:.1f}"  if score  is not None else "",
            ])
    except Exception:
        pass


def log_trade_open(p: dict) -> None:
    """
    Log a trade entry to engine.log at WARNING level.
    Call from strategies_engine.py fire() after self.preds.appendleft(p).

      p — the pred dict created in fire()
    """
    strategy_log.warning(
        "OPEN  [%s] %s %s  entry=%.6f  tp=%.3f%%  sl=%.3f%%  "
        "conf=%s score=%s vpin=%s",
        p.get("_strategy_label", "?"),
        p["sym"],
        p["dir"].upper(),
        p["entry"],
        p["dyn_tp"],
        p["dyn_sl"],
        p.get("conf", ""),
        round(p.get("score", 0)),
        f"{p['vpin_entry']:.3f}" if p.get("vpin_entry") is not None else "?",
    )


def log_trade_close(p: dict) -> None:
    """
    Log a trade exit to engine.log at WARNING level.
    Call from strategies_engine.py _resolve() after outcome is set.

      p — the pred dict with out3/pct3/reason/dur filled in
    """
    net = (p.get("pct3") or 0.0) - FEE_RT
    emoji = "✅" if (p.get("out3") == "win") else ("⚪" if p.get("out3") == "be" else "❌")
    strategy_log.warning(
        "CLOSE %s [%s] %s %s  net=%+.4f%%  reason=%s  dur=%.0fs",
        emoji,
        p.get("_strategy_label", "?"),
        p["sym"],
        p.get("dir", "").upper(),
        net,
        p.get("reason", "?"),
        p.get("dur") or 0,
    )


def log_ws_event(event: str, label: str, detail: str = "") -> None:
    """
    Log a WebSocket lifecycle event to engine.log.

    event  — 'connect', 'disconnect', 'reconnect', 'error'
    label  — 'public', 'market', 'bybit', 'mexc', etc.
    detail — exception message or extra context

    connect/reconnect → WARNING (visible in engine.log + journald)
    disconnect/error  → ERROR   (also sent to Telegram)

    Benign keepalive ping timeouts are silently suppressed — they
    spam logs under Python 3.13/3.14 + websockets ≥14 and are
    harmless auto-recovering disconnects.
    """
    _BENIGN = (
        'keepalive ping timeout',
        'no close frame received',
        'keepalive_ping',
        'ConnectionClosedError',
        'assert waiter is None',
    )
    if any(p in detail for p in _BENIGN):
        return  # suppress entirely — not a real error
    if event in ("disconnect", "error"):
        ws_log.error("WS %s [%s] %s", event.upper(), label, detail)
    else:
        ws_log.warning("WS %s [%s] %s", event.upper(), label, detail)


def log_scanner_change(added: list, removed: list) -> None:
    """Log coin scanner changes to engine.log at WARNING."""
    if added:
        scanner_log.warning("Scanner +%d coins: %s", len(added), ", ".join(added))
    if removed:
        scanner_log.warning("Scanner -%d coins: %s", len(removed), ", ".join(removed))


def log_engine_start(version: str, coins: list, strategies: list) -> None:
    """Log engine startup summary at WARNING."""
    engine_log.warning(
        "ENGINE START v=%s  coins=%d  strategies=%s",
        version, len(coins), ", ".join(strategies),
    )


def log_engine_stop(reason: str = "SIGINT") -> None:
    """Log engine shutdown at WARNING."""
    engine_log.warning("ENGINE STOP reason=%s", reason)