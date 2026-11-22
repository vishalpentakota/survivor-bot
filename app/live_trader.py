"""
Live Trading Execution Engine — Dhan Integration
Runs every trading day during market hours (09:15–15:20 IST).
Generates signals on 15-min candles and executes via Dhan API.

TOKEN LIFECYCLE:
  Dhan free API tokens expire every 24 hours.
  The DhanTokenManager auto-renews using PIN + TOTP on each run.
  No manual intervention needed.

LEARNING MODE:
  From Aug 4-15, 2026: Max 1 trade per day to learn patterns.
"""

import sys
import os
import json
import time
import logging
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import (
    UNIVERSE, TOTAL_CAPITAL, MARKET_OPEN, MARKET_CLOSE,
    DAILY_LOSS_LIMIT, ATR_MULTIPLIER_SL, TRAILING_TRIGGER,
    MAX_TRADES_PER_DAY, LEARNING_MODE_START, LEARNING_MODE_END
)
from data_fetcher import fetch_historical_data
from signals import generate_signals
from risk_manager import calculate_position_size
from dhan_broker import DhanBroker, DhanTokenManager, DHAN_INDEX_SECURITY_IDS, load_env_file
from trade_logger import TradeLogger

# ─── Logging ────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "live_trader.log")),
    ]
)
logger = logging.getLogger("live_trader")


def check_market_hours():
    """Check if we're within NSE trading hours (IST = UTC+5:30)."""
    now = datetime.utcnow() + timedelta(hours=5, minutes=30)
    current_time = now.strftime("%H:%M")

    if current_time < MARKET_OPEN or current_time > MARKET_CLOSE:
        return False, f"Market closed. Current IST: {current_time}"

    # Weekend check
    if now.weekday() >= 5:
        return False, f"Weekend. Day: {now.strftime('%A')}"

    return True, f"Market open. IST: {current_time}"


def is_learning_mode() -> bool:
    """Check if we're in learning mode (1 trade/day limit)."""
    today = datetime.now().date()
    start = datetime.strptime(LEARNING_MODE_START, "%Y-%m-%d").date()
    end = datetime.strptime(LEARNING_MODE_END, "%Y-%m-%d").date()
    return start <= today <= end


def run_live_scan(dry_run: bool = True):
    """
    Main live trading scan.

    Args:
        dry_run: If True, only log signals (no real orders placed).
                 Set to False to execute real orders via Dhan.
    """
    logger.info("=" * 60)
    logger.info(f"LIVE SCAN — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"MODE: {'DRY RUN (signals only)' if dry_run else 'LIVE (placing real orders)'}")
    logger.info("=" * 60)

    # ── Market hours check ──
    is_open, msg = check_market_hours()
    logger.info(f"Market Status: {msg}")

    if not is_open:
        logger.info("Skipping — market not open.")
        return []

    # ── Load .env credentials ──
    load_env_file()

    # ── Initialize Dhan broker ──
    broker = None
    if not dry_run:
        try:
            token_mgr = DhanTokenManager()
            broker = DhanBroker(token_mgr)
            # Verify connection by fetching fund limits
            funds = broker.get_fund_limits()
            logger.info(f"Dhan connected. Fund limits: {funds}")
        except Exception as e:
            logger.error(f"Failed to connect to Dhan: {e}")
            logger.error("Falling back to DRY RUN mode.")
            dry_run = True

    # ── Initialize trade logger ──
    trade_logger = TradeLogger()

    # ── Check learning mode limits ──
    if is_learning_mode():
        today_trades = trade_logger.get_todays_trades()
        if len(today_trades) >= MAX_TRADES_PER_DAY:
            logger.info(f"Learning mode: Already {len(today_trades)} trade(s) today. Skipping.")
            return []

    anomalies = []
    daily_pnl = 0.0
    signals_log = []
    trade_executed = False

    for index_name, ticker in UNIVERSE.items():
        try:
            logger.info(f"Scanning {index_name} ({ticker})...")

            # Fetch latest 15-min data (last 5 days)
            df = fetch_historical_data(ticker, days=5, interval="15m")
            if df is None or df.empty or len(df) < 30:
                anomaly = f"{index_name}: Insufficient data ({len(df) if df is not None else 0} candles)"
                anomalies.append(anomaly)
                logger.warning(anomaly)
                continue

            # Generate signals
            df = generate_signals(df)
            latest = df.iloc[-1]
            prev = df.iloc[-2]

            signal_record = {
                "index": index_name,
                "timestamp": str(df.index[-1]),
                "close": round(float(latest['Close']), 2),
                "ema_fast": round(float(latest['EMA_Fast']), 2),
                "ema_slow": round(float(latest['EMA_Slow']), 2),
                "rsi": round(float(latest['RSI']), 2),
                "atr": round(float(latest['ATR']), 2),
                "signal": int(latest['Signal']),
            }
            signals_log.append(signal_record)

            if latest['Signal'] != 0:
                # Skip if already executed a trade in learning mode
                if is_learning_mode() and trade_executed:
                    logger.info(f"  [LEARNING MODE] Signal detected but daily limit reached. Skipping.")
                    continue

                direction = "LONG" if latest['Signal'] == 1 else "SHORT"
                qty = calculate_position_size(TOTAL_CAPITAL, latest['ATR'])

                if qty > 0:
                    if direction == "LONG":
                        sl = latest['Close'] - (latest['ATR'] * ATR_MULTIPLIER_SL)
                    else:
                        sl = latest['Close'] + (latest['ATR'] * ATR_MULTIPLIER_SL)

                    logger.info(
                        f"  ⚡ SIGNAL: {direction} {index_name} | "
                        f"Qty: {qty} | Entry: {latest['Close']:.2f} | SL: {sl:.2f}"
                    )

                    if not dry_run and broker:
                        # Get Dhan security ID for this index
                        sec_info = DHAN_INDEX_SECURITY_IDS.get(index_name, {})
                        security_id = sec_info.get("security_id", "")

                        if security_id:
                            order = broker.place_order(
                                symbol=index_name,
                                direction=direction,
                                qty=qty,
                                price=float(latest['Close']),
                                sl=float(sl),
                                product_type="INTRADAY",
                                order_type="MARKET",
                                exchange_segment="NSE_FNO",
                                security_id=security_id,
                            )
                            logger.info(f"  Order response: {order}")

                            # Log the trade only if order actually succeeded
                            # Dhan returns {'status': 'failure', ...} on rejection (no 'error' key)
                            order_failed = (
                                'error' in order
                                or (isinstance(order, dict) and order.get('status') == 'failure')
                            )
                            if not order_failed:
                                trade_data = {
                                    'index_name': index_name,
                                    'direction': direction,
                                    'entry_time': df.index[-1],
                                    'entry_price': float(latest['Close']),
                                    'quantity': qty,
                                    'stop_loss': float(sl),
                                    'signal_strength': {
                                        'ema_fast': float(latest['EMA_Fast']),
                                        'ema_slow': float(latest['EMA_Slow']),
                                        'rsi': float(latest['RSI']),
                                        'atr': float(latest['ATR']),
                                    },
                                }
                                trade_logger.log_trade(trade_data)
                                trade_executed = True
                                logger.info(f"  ✓ Trade logged (#{trade_logger.get_stats()['total_trades']})")
                        else:
                            anomalies.append(f"{index_name}: No Dhan security ID configured")
                    else:
                        # Dry run - still log for analysis
                        trade_data = {
                            'index_name': index_name,
                            'direction': direction,
                            'entry_time': df.index[-1],
                            'entry_price': float(latest['Close']),
                            'quantity': qty,
                            'stop_loss': float(sl),
                            'signal_strength': {
                                'ema_fast': float(latest['EMA_Fast']),
                                'ema_slow': float(latest['EMA_Slow']),
                                'rsi': float(latest['RSI']),
                                'atr': float(latest['ATR']),
                            },
                            'dry_run': True,
                        }
                        trade_logger.log_trade(trade_data)
                        trade_executed = True
                        logger.info(f"  [DRY RUN] Trade logged (#{trade_logger.get_stats()['total_trades']})")
                else:
                    anomalies.append(f"{index_name}: Position size = 0 (ATR={latest['ATR']:.2f})")
            else:
                logger.info(
                    f"  No signal. EMA9={latest['EMA_Fast']:.2f}, "
                    f"EMA26={latest['EMA_Slow']:.2f}, RSI={latest['RSI']:.1f}"
                )

        except Exception as e:
            anomalies.append(f"{index_name}: {str(e)}")
            logger.error(f"ERROR scanning {index_name}: {e}", exc_info=True)

    # ── Kill switch check ──
    if daily_pnl / TOTAL_CAPITAL <= -DAILY_LOSS_LIMIT:
        logger.critical(f"🛑 KILL SWITCH ACTIVATED — Daily loss limit hit (₹{daily_pnl:,.0f})")

    # ── Save signals log ──
    signals_path = os.path.join(os.path.dirname(__file__), "signals_log.json")
    try:
        existing = []
        if os.path.exists(signals_path):
            with open(signals_path, 'r') as f:
                existing = json.load(f)
        existing.extend(signals_log)
        # Keep last 500 entries
        existing = existing[-500:]
        with open(signals_path, 'w') as f:
            json.dump(existing, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save signals log: {e}")

    # ── Log anomalies ──
    if anomalies:
        logger.warning(f"⚠ {len(anomalies)} anomalies detected:")
        for a in anomalies:
            logger.warning(f"  - {a}")

        anomaly_log_path = os.path.join(os.path.dirname(__file__), "anomalies.log")
        with open(anomaly_log_path, 'a') as f:
            for a in anomalies:
                f.write(f"[{datetime.now().isoformat()}] {a}\n")

    logger.info("=" * 60)
    logger.info("SCAN COMPLETE")
    logger.info("=" * 60)

    return anomalies


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Nifty Algo Live Scanner")
    parser.add_argument("--live", action="store_true",
                        help="Place real orders via Dhan (default: dry run)")
    args = parser.parse_args()

    dry_run = not args.live
    anomalies = run_live_scan(dry_run=dry_run)

    if anomalies:
        print(f"\n⚠ {len(anomalies)} anomalies logged to anomalies.log")
    else:
        print("\n✓ No anomalies.")
