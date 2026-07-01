# PredictEngine — Telegram Monitor Setup

## 1. Create your Telegram bot (2 min)

1. Open Telegram → search `@BotFather` → `/newbot`
2. Follow prompts → you'll get a `BOT_TOKEN` like `7123456789:AAF...`
3. Start a chat with your new bot (click the link BotFather sends)
4. Get your `CHAT_ID`:
   ```bash
   curl https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates
   ```
   Look for `"id"` inside `"chat"` in the response.


## 2. Deploy to server

```bash
# Copy files
scp engine_logger.py user@host.example.com/home/ubuntu/engine/
scp tg_monitor.py user@host.example.com/home/ubuntu/engine/
scp predict-monitor.service user@host.example.com/tmp/

# SSH in
ssh user@host.example.com

# Create .env file (systemd won't read ~/.bashrc)
echo 'TG_BOT_TOKEN=YOUR_TOKEN_HERE' > /home/ubuntu/engine/.env
echo 'TG_CHAT_ID=YOUR_CHAT_ID_HERE' >> /home/ubuntu/engine/.env
chmod 600 /home/ubuntu/engine/.env

# Add EnvironmentFile to predict-engine.service
sudo sed -i '/WorkingDirectory=\/home\/ubuntu\/engine/a EnvironmentFile=/home/ubuntu/engine/.env' \
  /etc/systemd/system/predict-engine.service

# Create logs directory
mkdir -p /home/ubuntu/engine/logs

# Test manually first
cd /home/ubuntu/engine
export $(cat .env | xargs)
./venv/bin/python3 tg_monitor.py --heartbeat     # should get 💓 on Telegram
./venv/bin/python3 tg_monitor.py --pnl           # PnL summary
./venv/bin/python3 tg_monitor.py --once          # all health checks
```


## 3. Run as systemd service (recommended)

```bash
# Install service
sudo mv /tmp/predict-monitor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable predict-monitor
sudo systemctl start predict-monitor

# Verify both services running
sudo systemctl status predict-engine predict-monitor

# Watch monitor logs
journalctl -u predict-monitor -f
```

Note: `cron` is not installed by default on this server. The systemd service
runs `tg_monitor.py --loop` which checks every 5 min and sends heartbeat every 6h.


## 4. Deploy updated engine files

Deploy the updated versions of these 4 files via `deploy-hot.sh --full`:

- `engine_logger.py` — **new file**, drop into `~/engine/`
- `engine.py` — WS connect/disconnect now calls `log_ws_event()`
- `strategies_engine.py` — trade open/close logged; K/Y gate spam replaced
- `predict_engine.py` — `setup_logging()` called at startup

What each file adds:

**engine_logger.py (new):**
- `setup_logging()` — wires all handlers at startup
- `log_signal()` — K/Y gate checks → `logs/signals_YYYYMMDD.csv` (deduplicated, 10s window)
- `log_trade_open/close()` → `logs/engine.log` at WARNING
- `log_ws_event()` — connect=WARNING, disconnect=ERROR+Telegram
- `log_scanner_change()` → `logs/engine.log`
- `_TelegramHandler` — forwards ERROR/CRITICAL inline to Telegram

**engine.py:**
- `_tg_send()` helper (used by engine_logger's TelegramHandler)
- `sys.excepthook` → crash traceback to Telegram
- `_tg_async_exception_handler` → asyncio task crashes to Telegram
- WS connect/disconnect calls `log_ws_event()`
- Scanner `print()` calls replaced with `log_scanner_change()`

**strategies_engine.py:**
- All 5 `logging.warning()` calls (K×4, Y×1) → `log_signal()` into signals CSV
- `fire()` calls `log_trade_open(p)`
- `_resolve()` calls `log_trade_close(p)`

**predict_engine.py:**
- `setup_logging()` called before anything else
- `log_engine_start()` on startup
- `log_engine_stop()` on clean shutdown


## 5. Log destinations

| Source | Destination | Level | Notes |
|---|---|---|---|
| K/Y gate checks, blocked signals | `logs/signals_YYYYMMDD.csv` | INFO | Deduplicated 10s — no more per-tick spam |
| Trade open/close | `logs/engine.log` | WARNING | Rotating 10MB×5 |
| WS connect | `logs/engine.log` | WARNING | |
| WS disconnect | `logs/engine.log` + Telegram | ERROR | |
| Engine crash + traceback | Telegram | CRITICAL | |
| Asyncio task crash | Telegram | CRITICAL | |
| Scanner coin changes | `logs/engine.log` | WARNING | |
| journald (`journalctl -u predict-engine`) | WARNING+ only | — | No more impulse spam |


## What you'll get on Telegram

| Alert | Trigger |
|---|---|
| 🟢 Engine started | Every restart, with strategy list |
| 🔴 Engine crash + traceback | Any unhandled Python exception |
| 🟡 Engine loop exited | Clean shutdown |
| ⚠️ Async exception | Any asyncio task crash |
| 🔴 WS DISCONNECT | WebSocket dropped (ERROR via engine_logger) |
| 🔴 ENGINE DOWN | systemd service not active |
| ✅ ENGINE RECOVERED | Service came back up |
| ⚠️ High CPU | Load > 80% |
| ⚠️ High RAM | RAM > 85% used |
| ⚠️ Disk almost full | Disk > 90% used |
| ⚠️ Strategy Silence | No CSV updated in 2h |
| ⚠️ WS Instability | 5+ reconnects in 10 min |
| 📊 Daily PnL Summary | Midnight UTC (per strategy) |
| 💓 Engine alive | Every 6 hours |


## Log file locations

```
~/engine/
  logs/
    engine.log          ← WARNING+ events (trade open/close, WS, scanner)
    engine.log.1        ← previous rotation (up to .5)
    signals_20260530.csv ← today's gate checks and blocked signals
    signals_20260529.csv ← yesterday's (new file per day)
```

Viewing logs:
```bash
# Live trade feed
tail -f ~/engine/logs/engine.log

# Today's blocked signals (K/Y gate detail)
tail -f ~/engine/logs/signals_$(date +%Y%m%d).csv

# System-level (crashes, WARNING+ only — no more per-tick spam)
journalctl -u predict-engine -f
```


## Tune thresholds

Edit these constants at the top of `tg_monitor.py`:
```python
CPU_WARN_PCT    = 80    # % CPU load
RAM_WARN_PCT    = 85    # % RAM used
DISK_WARN_PCT   = 90    # % disk used
SILENCE_HOURS   = 2     # hours without CSV update
HEARTBEAT_HOURS = 6     # heartbeat interval
LOOP_SLEEP_SEC  = 300   # check interval in --loop mode
```

Edit these in `engine_logger.py`:
```python
_DEDUP_WINDOW_SEC = 10  # seconds between identical gate-block rows in signals CSV
```


## Useful commands

```bash
# Restart both services
sudo systemctl restart predict-engine predict-monitor

# Check status
sudo systemctl status predict-engine predict-monitor

# Live trade open/close feed
tail -f ~/engine/logs/engine.log

# Live gate signal detail
tail -f ~/engine/logs/signals_$(date +%Y%m%d).csv

# System-level events only (no spam)
journalctl -u predict-engine -f

# Live monitor logs
journalctl -u predict-monitor -f

# Send manual heartbeat
cd /home/ubuntu/engine && export $(cat .env | xargs) && ./venv/bin/python3 tg_monitor.py --heartbeat

# Send manual PnL summary
cd /home/ubuntu/engine && export $(cat .env | xargs) && ./venv/bin/python3 tg_monitor.py --pnl

# Test crash alert (sends traceback to Telegram)
cd /home/ubuntu/engine && export $(cat .env | xargs) && ./venv/bin/python3 -c "import engine; raise RuntimeError('test')"
```