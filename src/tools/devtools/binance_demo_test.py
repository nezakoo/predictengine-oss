"""
Binance USD-M Futures — Demo Trading Test
Opens a LONG position on BTCUSDT, waits, then closes it.

Requirements:
    pip install python-binance

Setup:
    Set your API keys below (use your REAL Binance keys — demo trading
    is enabled via the base_url parameter, not separate keys).
"""

import time
from binance.um_futures import UMFutures

# ── Config ────────────────────────────────────────────────────────────────────
API_KEY = os.getenv("API_KEY", "")
API_SECRET = os.getenv("API_SECRET", "")

SYMBOL     = "BTCUSDT"
SIDE_BUY   = "BUY"
SIDE_SELL  = "SELL"
ORDER_TYPE = "MARKET"
QUANTITY   = 0.001          # BTC — adjust to what your balance allows
LEVERAGE   = 10
WAIT_SECS  = 5              # seconds to hold before closing

# Demo Trading base URL — real Binance keys, virtual balance, live market data
DEMO_BASE_URL = "https://demo-fapi.binance.com"
# ─────────────────────────────────────────────────────────────────────────────


def get_client() -> UMFutures:
    """Return a UMFutures client pointed at the demo environment."""
    return UMFutures(
        key=API_KEY,
        secret=API_SECRET,
        base_url=DEMO_BASE_URL,
    )


def set_leverage(client: UMFutures, symbol: str, leverage: int) -> None:
    resp = client.change_leverage(symbol=symbol, leverage=leverage)
    print(f"[leverage] {symbol} set to {resp['leverage']}x "
          f"(max notional: {resp.get('maxNotionalValue', 'N/A')})")


def get_price(client: UMFutures, symbol: str) -> float:
    ticker = client.ticker_price(symbol=symbol)
    return float(ticker["price"])


def open_position(client: UMFutures) -> dict:
    print(f"\n[open] Sending LONG market order — {QUANTITY} {SYMBOL} ...")
    order = client.new_order(
        symbol=SYMBOL,
        side=SIDE_BUY,
        type=ORDER_TYPE,
        quantity=QUANTITY,
    )
    print(f"[open] Order ID : {order['orderId']}")
    print(f"[open] Status   : {order['status']}")
    print(f"[open] Avg price: {order.get('avgPrice', 'pending')}")
    return order


def get_position(client: UMFutures, symbol: str) -> dict | None:
    positions = client.get_position_risk(symbol=symbol)
    for p in positions:
        if float(p["positionAmt"]) != 0:
            return p
    return None


def close_position(client: UMFutures) -> dict:
    print(f"\n[close] Sending SHORT market order to close — {QUANTITY} {SYMBOL} ...")
    order = client.new_order(
        symbol=SYMBOL,
        side=SIDE_SELL,
        type=ORDER_TYPE,
        quantity=QUANTITY,
        reduceOnly="true",      # safety: only closes, never flips
    )
    print(f"[close] Order ID : {order['orderId']}")
    print(f"[close] Status   : {order['status']}")
    print(f"[close] Avg price: {order.get('avgPrice', 'pending')}")
    return order


def print_account_balance(client: UMFutures) -> None:
    balances = client.balance()
    for b in balances:
        if b["asset"] == "USDT":
            print(f"\n[balance] USDT wallet balance : {b['balance']}")
            print(f"[balance] USDT available       : {b['availableBalance']}")
            break


def main() -> None:
    client = get_client()

    # 1. Check balance
    print("=" * 55)
    print("  Binance Demo Trading — Position Open/Close Test")
    print("=" * 55)
    print_account_balance(client)

    # 2. Set leverage
    set_leverage(client, SYMBOL, LEVERAGE)

    # 3. Show current price
    price = get_price(client, SYMBOL)
    print(f"\n[price] {SYMBOL} mark price: ${price:,.2f}")

    # 4. Open LONG
    open_order = open_position(client)

    # 5. Show open position
    time.sleep(1)
    pos = get_position(client, SYMBOL)
    if pos:
        print(f"\n[position] Amount      : {pos['positionAmt']} BTC")
        print(f"[position] Entry price : {pos['entryPrice']}")
        print(f"[position] Unrealised PnL: {pos['unRealizedProfit']}")
    else:
        print("[position] No open position found yet (may still be processing).")

    # 6. Wait a moment
    print(f"\n[wait] Holding position for {WAIT_SECS} seconds ...")
    time.sleep(WAIT_SECS)

    # 7. Check PnL before closing
    pos = get_position(client, SYMBOL)
    if pos:
        print(f"[position] Unrealised PnL before close: {pos['unRealizedProfit']}")

    # 8. Close position
    close_order = close_position(client)

    # 9. Final balance
    time.sleep(1)
    print_account_balance(client)

    print("\n[done] Test complete ✓")


if __name__ == "__main__":
    main()