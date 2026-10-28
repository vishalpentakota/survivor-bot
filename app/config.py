"""
Nifty Algorithmic Trading System - Configuration
"""

# ─── Trading Universe ───────────────────────────────────────────────
# Yahoo Finance tickers for NSE indices
UNIVERSE = {
    "Nifty 50":    "^NSEI",
    "Nifty Bank":  "^NSEBANK",
    "Nifty Auto":  "^CNXAUTO",
    "Nifty Pharma":"^CNXPHARMA",
    "Nifty IT":    "^CNXIT",
    "Nifty Metal": "^CNXMETAL",
    "Nifty FMCG":  "^CNXFMCG",
}

# ─── Strategy Parameters ────────────────────────────────────────────
TIMEFRAME = "15m"
EMA_FAST = 9
EMA_SLOW = 26
RSI_PERIOD = 14
RSI_LONG_THRESHOLD = 50
RSI_SHORT_THRESHOLD = 50

# ─── Risk Management ───────────────────────────────────────────────
TOTAL_CAPITAL = 50_000          # ₹50K (starting capital)
MAX_RISK_PER_TRADE = 0.01       # 1% of capital
ATR_MULTIPLIER_SL = 1.5        # Stop-loss = 1.5x ATR
TRAILING_TRIGGER = 2000         # Trailing TP activates after 2000 pts profit
DAILY_LOSS_LIMIT = 0.03         # 3% max daily drawdown -> kill switch

# ─── Learning Mode ──────────────────────────────────────────────────
MAX_TRADES_PER_DAY = 1          # First 10 days: 1 trade/day only (learning phase)
LEARNING_MODE_START = "2026-08-03"
LEARNING_MODE_END = "2026-08-14"

# ─── Trading Hours (IST) ────────────────────────────────────────────
MARKET_OPEN = "09:15"
MARKET_CLOSE = "15:10"  # Dhan: Equity intraday square-off at 3:10 PM (from 3 Aug 2026)
FNO_CLOSE = "15:25"     # Dhan: F&O intraday square-off at 3:25 PM

# ─── Backtest Settings ──────────────────────────────────────────────
BACKTEST_DAYS = 90
LOT_SIZE = 1  # For index-level backtesting, we use notional; set 1 for simulation
