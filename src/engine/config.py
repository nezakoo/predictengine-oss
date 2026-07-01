"""
PredictEngine - config.py | v13
Exchange: Binance USDT-M Futures (was Bybit)
Signals: OBI, CVD divergence, LIQ, absorption
Gates:   VPIN, Kyle's Lambda, spread, trade acceleration
Exits:   dynamic TP/SL (ATR-scaled), trailing stop, inertia kill (dp<0 only)

Fee note (Binance USDT-M Futures, Regular User, no BNB):
  Maker: 0.0200%  Taker: 0.0500%
  Round-trip (taker + taker): 0.10%
  With BNB balance (10% off):  0.09%
  FEE_RT below = 0.10 (no BNB). Change to 0.09 if you hold BNB.
  Your old Bybit value was 0.11 — Binance is slightly cheaper.
"""

from datetime import datetime
from pathlib import Path
import os

# -- Coins ---------------------------------------------------------
# NOTE: a few Bybit-only coins removed (not listed on Binance Futures):
#   BSBUSDT, BILLUSDT, SIRENUSDT, CHIPUSDT, B3USDT, MUSDT,
#   RAVEUSDT, PUMPFUNUSDT, XPLUSDT
# Binance uses 1000BONKUSDT — kept as-is (same symbol on Binance).
DEFAULT_COINS = [
    # -- BTC/ETH/SOL: needed for W decorrelation signal (btc_hist reference) --
    'BTCUSDT','ETHUSDT','SOLUSDT',
    # -- Active alts: W/C/G/K/T/X/Q/QQ targets --
    'JTOUSDT','PENDLEUSDT','ONDOUSDT','SUIUSDT',
    'NEARUSDT','TONUSDT',
    'NILUSDT','STRKUSDT','DYDXUSDT',
    'OPUSDT','GALAUSDT','JUPUSDT','FILUSDT',
    'NOTUSDT','TIAUSDT','ARBUSDT','ENAUSDT',
    'INJUSDT','DASHUSDT','APTUSDT','WLDUSDT',
    '1000BONKUSDT','DOTUSDT',
    # -- High-volatility additions --
    'HYPEUSDT','WIFUSDT','MEWUSDT','TRUMPUSDT','1000PEPEUSDT',
    # NOTE: LABUSDT, VVVUSDT, BIOUSDT removed from seed — Z blacklisted;
    # scanner will still pick them up if they rank in top 50 by volatility.
]

# -- Coin scanner --------------------------------------------------
# When USE_COIN_SCANNER=True, the engine fetches the top SCANNER_TOP_N
# coins by 24h quote volume from Binance every SCANNER_REFRESH_SEC seconds
# and updates ACTIVE_COINS automatically.
# DEFAULT_COINS is used as the initial seed list until the first scan
# completes (takes ~5s at startup). Set to False to use DEFAULT_COINS only.
#
# 50-coin config rationale:
#   Binance WS: 50 coins × 4 streams = 200 streams — well under the 1024 limit.
#   REST cycle: ~8-12s per full cycle at 50 coins (0.1s sleep between calls).
#   Strategy Z lag WS: subscribes all coins to MEXC/Bybit/Gate — fine at 50.
#
#   SCANNER_TOP_N raised 60→100: more coin universe = more signal opportunities.
#
#   SCANNER_MIN_NATR lowered 4.0→2.5: 4.0 was too strict, filtered out coins like
#   HOMEUSDT that have real intraday structure. 2.5 keeps genuine movers while
#   excluding flat stablecoins. NATR=2.5% daily range = minimum viable signal.
#
#   SCANNER_MIN_VOL_USD lowered 20M→5M: the OI gate (≥5M OI or LIQUID_WHITELIST)
#   in _standard_gate handles actual liquidity. Volume floor at 5M lets in volatile
#   small-caps that generate K/G/CGY signals. Dead coins won't pass NATR gate anyway.
#
#   SCANNER_SORT_BY: 'volatility' ranks by nATR descending (most volatile first)
#   instead of volume. This fills slots 30-50 with the most active smaller coins
#   rather than the next-biggest by dollar volume (which tend to be correlated
#   large-caps that behave identically to BTC/ETH).
USE_COIN_SCANNER     = True
SCANNER_TOP_N        = 70       # raised 45→70: ~70 top + ~18 always_keep ≈ 88 coins (more shadow-strategy data)
                                  # market streams = 88×3 = 264 → ws_task auto-splits across 2 connections (190 + 74).
                                  # public/depth streams = 88×1 = 88 → single connection (well under 190).
                                  # CEILING: keep under ~95 coins. 100 coins historically → silent WS disconnects ~every 10min.
                                  # If reconnect spam shows in logs (or REST cycle >25s), dial back toward 60.
SCANNER_MIN_NATR     = 2.5      # was 4.0 — 4.0 too strict; 2.5 captures coins like HOMEUSDT with real intraday structure
SCANNER_MIN_VOL_USD  = 5e6       # was 20M → 5M — OI gate (≥5M OI or whitelist) handles liquidity; captures volatile small-caps like HOMEUSDT
SCANNER_SORT_BY      = 'volatility'  # 'volume' or 'volatility'
SCANNER_REFRESH_SEC  = 1800     # rescan every 30 minutes

LIQUID_WHITELIST = {
    # High-OI coins that bypass the OI gate check
    'BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT','DOGEUSDT',
    'ADAUSDT','LINKUSDT','AVAXUSDT','NEARUSDT','TONUSDT',
    'LTCUSDT','DOTUSDT','ATOMUSDT','SUIUSDT','ARBUSDT',
    'INJUSDT','OPUSDT','APTUSDT','JUPUSDT','ENAUSDT',
    'PENDLEUSDT','DYDXUSDT','STRKUSDT','ONDOUSDT',
    'LABUSDT','NOTUSDT','FILUSDT','TIAUSDT','GALAUSDT',
    # Ensures OI polling for S (OI Divergence) and Z positive symbols
    'VVVUSDT','NILUSDT','BIOUSDT','WLDUSDT','JTOUSDT',
    # High-volatility additions — ensure OI gate doesn't block them
    'HYPEUSDT','WIFUSDT','1000PEPEUSDT',
    # High-NATR small-cap coins valid for K/G/C (impulse/spike strategies)
    # OI may be <50M but sufficient for 60-480s trades
    'BEATUSDT','MEWUSDT','EDENUSDT',  # high-NATR small caps for K/G/C
}

# -- API & timing --------------------------------------------------
# Binance USDT-M Futures WebSocket (combined stream endpoint)
WS_URL     = 'wss://fstream.binance.com/stream'
# Binance USDT-M Futures REST
API_URL    = 'https://fapi.binance.com'
LOOP_MS    = 100  # restored: 50ms starves WS feed with 100+ coins / 7 strategies
DISPLAY_MS = 200
GUI_MS     = 300

# -- Entry gates ---------------------------------------------------
PRED_COOLDOWN  = 120
MIN_CONF       = 65
MIN_SCORE      = 50
MIN_VOL_ATR    = 0.25
TRADE_MIN      = 1500
OBI_THR        = 0.35

# -- Strategy v16 gate kill-switches -------------------------------
# T: regime trend — longs 0%WR historically
T_LONG_DISABLED         = False   # TEST: re-enabled
T_SHORT_DISABLED        = False

K_SHORT_ENTRY_DISABLED  = False   # K shorts blocked by EMA21 filter instead

X_LONG_ENTRY_DISABLED   = False   # TEST: re-enabled
X_SHORT_ENTRY_DISABLED  = False

Q_LONG_DISABLED         = False   # inconsistent (55%→17%WR) — EMA21 filter handles
Q_MAX_OPEN_PER_SYM      = 2

E_SHORT_ONLY            = False
S_SHORT_DISABLED        = False   # signal fixed: OI logic corrected
O_SHORT_DISABLED        = False   # signal fixed: rejection candle required
O_LONG_DISABLED         = False   # signal fixed: rejection candle required
B_SHORT_DISABLED        = False   # B shorts 35%WR monitor
Z_SHORT_DISABLED        = False   # Z shorts keep
Z_LONG_DISABLED         = False   # signal fixed: now checks cause of divergence
Q_SHORT_DISABLED        = False   # Q shorts 40%WR monitor
P_LONG_DISABLED         = False   # signal fixed: close confirmation on retest

U_LONG_DISABLED         = False   # only 10T total — insufficient data for disable

R_ENTRY_DELAY_SEC       = 20      # kept — structural, not a direction kill

# -- Cross-strategy same-symbol cap --------------------------------
# Prevents O + Z + L all entering the same symbol at the same time.
# When correlated strategies pile into one coin, losses are amplified
# when it reverses (all three hit SL simultaneously).
# Cap: max N open positions on any single symbol ACROSS all strategies.
# Checked in tick_all() before fire() is called.
GLOBAL_MAX_OPEN_PER_SYM = 2   # max concurrent positions per symbol across ALL strategies

# -- Confluence / cross-strategy confirmation thresholds -----------
# O + T: block O from firing counter-trend when trend is this confirmed
CONFLUENCE_O_TREND_BLOCK_TICKS  = 6    # trend_tick_count >= this → block O against trend

# Z + OI: Z blocked when OI divergence opposes lag direction
# Set False to disable this check entirely
CONFLUENCE_Z_OI_CHECK           = True

# L + S: dynamic score multiplier based on OI agreement
CONFLUENCE_L_OI_AGREE_MULT      = 0.70  # OI agrees → require only 70% of min_score
CONFLUENCE_L_OI_DISAGREE_MULT   = 1.20  # was 1.50 → 1.20: 150% score bar too high, silent gate on L

# Master regime filter: block mean-reversion strategies counter-trend
# when trend is this strongly confirmed on a symbol
CONFLUENCE_REGIME_BLOCK_TICKS   = 10   # trend_tick_count >= this → block counter-trend mean-rev
CONFLUENCE_REGIME_ENABLED       = True  # set False to disable master filter entirely      # seconds to wait after cascade detected before entering
                                  # Test: 20s. If still >50% rev, raise to 30s.

# -- Startup warmup gate -------------------------------------------
# Block ALL strategy firing for this many seconds after engine init.
# Root cause of "first trades always win" pattern:
#   VPIN/ATR/spread/Kyle-lambda buffers are empty on restart → every
#   coin looks like a clean signal. Loss-streak counters also reset to
#   zero so previously-cooling symbols fire immediately.
# 90s allows: VPIN bucket to fill (~20-30 trades), ATR to stabilise
#   over several candles, spread history to normalise, sig_hist to
#   accumulate enough ticks for sus_ticks checks to be meaningful.
# Set to 0 to disable (e.g. during backtesting).
WARMUP_SEC = 180  # raised 90→180: buffers (VPIN/ATR/spread) need 2-3min to fully stabilise
                  # 90s blocked the very first fires but 90-180s still had elevated false signals
                  # SET TO 0 to run with no warmup (experiment: do early wins persist?)

# strategies_config_b.py
SL_CAP_LONG_HOLD        = 0.015   # max SL distance (1.5%) for long-hold strategies

# -- W strategy: BTC decorrelation gate ----------------------------
# Long requires stronger BTC impulse (14.7%WR vs 29.3%WR for shorts)
DECOR_LONG_BTC_MIN  = 0.10   # lowered back 0.35→0.10 — W silent gate fix; W has 60%WR when firing
DECOR_SHORT_BTC_MIN = 0.08   # min abs BTC 1m move % for W shorts
DECOR_LOOKBACK      = 20     # candles to compute rolling correlation

# -- Signal weights ------------------------------------------------
W_OBI   = 35
W_CVD   = 30
W_LIQ   = 10
W_ABS   = 15
W_VWD   = 10
W_BTL   =  0
W_SPOOF = 0.7

# -- VPIN gate -----------------------------------------------------
VPIN_BUCKET_VOL = 50_000
VPIN_MIN        = 0.45
VPIN_HIGH       = 0.70

# -- Kyle's Lambda gate --------------------------------------------
KYLE_LAM_GATE   = True

# -- Spread gate ---------------------------------------------------
SPREAD_MAX_PCT  = 0.05

# ── Cross-strategy position lock ──────────────────────────────────
# Prevents multiple strategies trading the same coin simultaneously.
# Required for live/demo Binance execution (one position per symbol).
#
# 'A' — first-come-first-served (prod): whichever strategy fires first
#        claims the coin; all others blocked until position closes.
# 'B' — priority-ranked (b-test): higher-quality strategies override
#        lower-quality holders. Priority: E > CGY > B > L > Y > W > K
POSITION_LOCK_MODE = 'A'

# -- Trade arrival acceleration ------------------------------------
ACCEL_MIN       = 1.2

# -- Exit parameters -----------------------------------------------
# FEE_RT = round-trip taker cost %, env-driven so prod/stage set their own in .env.
# Derive the real value from your account: `python3 tools/analysis/binance_fees.py --env prod --emit-fee-rt`
# then put FEE_RT=<value> in .env.prod AND .env.stage (stage sim must use PROD's fee to predict prod P&L).
# Fallback 0.093 = MEASURED blended round-trip (0.0465%/side x 2, 100% taker, 0 maker).
# DO NOT lower this fallback: 0.07 undercounts fees and recreates phantom sim profit in win/loss labeling.
FEE_RT         = float(os.getenv('FEE_RT', '0.093'))
WIN_THR        = 0.45
ATR_TP_MULT    = 1.10
ATR_SL_MULT    = 0.80
TRAIL_DIST     = 0.08
SIG_HOLD_SCORE = 45
MIN_HOLD_ANY   = 10
REVERSAL_SCORE = 50
REV_MIN_HOLD   = 60
MAX_WINDOW     = 300

# -- Inertia kill switch -------------------------------------------
INERTIA_SEC    = 45
INERTIA_THR    = 0.12

# -- Version -------------------------------------------------------
VERSION = {
    'v':         'v18',
    'date':      '2026-06-05',
    'exchange':  'Binance USDT-M Futures',
    'tp':        f'dyn_atr×{ATR_TP_MULT}',
    'sl':        f'dyn_atr×{ATR_SL_MULT}',
    'win_thr':   WIN_THR,
    'rev_hold':  REV_MIN_HOLD,
    'min_conf':  MIN_CONF,
    'min_score': MIN_SCORE,
    'vol_atr':   MIN_VOL_ATR,
    'weights':   f'obi{W_OBI}_cvd{W_CVD}_liq{W_LIQ}_abs{W_ABS}_vwd{W_VWD}',
    'notes':     (
        'v18: persistent state (logs/state_X.json) — charts survive hot reloads. '
        'state auto-resets on version bump. '
        'regime filter fix (K fade_dir bug). '
        'fast-path Z+exits event-driven. '
        'all direction kills disabled for full data collection. '
        'M/N disabled=True in strategies_config.'
    ),
}

_VS      = (f"{VERSION['v']}__tp_dyn_sl_dyn"
            f"_rev{REV_MIN_HOLD}s_conf{MIN_CONF}_vol{MIN_VOL_ATR}")
_preds_dir = Path(__file__).parent / 'logs' / 'preds'
_preds_dir.mkdir(parents=True, exist_ok=True)
LOG_FILE = str(_preds_dir / f"preds_{datetime.now().strftime('%Y%m%d_%H%M')}_{_VS}.csv")
