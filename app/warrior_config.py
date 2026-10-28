"""
Warrior Trading Strategy — Indian Market Parameters
Based on Ross Cameron's Small Account Challenge, adapted for NSE.

Load with: from warrior_config import WARRIOR_CFG
"""

WARRIOR_CFG = {
    # ── Stock Scanner Criteria ──────────────────────────────────
    "scanner": {
        "price_min": 50,                # ₹50 minimum
        "price_max": 500,               # ₹500 maximum
        "pct_change_min": 10,           # Up ≥10% from prev close
        "relative_volume_min": 5,       # Current vol ≥ 5x 30-day avg
        "float_max_million": 20,        # Max 20M share float
        "require_catalyst": True,       # Prefer news-driven moves
        "avoid_upper_circuit": True,    # Skip locked upper-circuit stocks
        "min_tradable_volume": 50000,   # Min shares traded today
        "allow_continuation": True,     # Allow stocks that gapped yday & held
    },

    # ── Entry Pattern Parameters ───────────────────────────────
    "entry": {
        "primary_pattern": "micro_pullback",
        "timeframes": [1, 5],            # 1-min and 5-min candles
        "macd_fast": 12,
        "macd_slow": 26,
        "macd_signal": 9,
        "pullback_volume_decline_pct": 30,   # Volume must drop ≥30% on dip
        "jackknife_wick_pct": 50,            # Wick ≥50% of candle body
        "min_confidence": 60,                # Skip signals below 60% confidence
    },

    # ── Risk & Position Sizing ──────────────────────────────────
    "risk": {
        "max_risk_per_trade_pct": 5,     # 5% of capital per trade
        "profit_target_pct": 10,         # 10% of capital target (2:1 R:R)
        "daily_max_loss_pct": 10,        # 10% max daily drawdown
        "max_consecutive_losses": 3,     # 3 losers = stop for the day
        "trailing_stop_activate_pct": 1.5,   # Trail after 1.5% profit
        "trailing_stop_distance_pct": 0.5,   # Trail 0.5% behind price
        "hard_stop_loss_pct": 2.0,       # Hard SL 2% below entry
    },

    # ── Order Execution ─────────────────────────────────────────
    "orders": {
        "order_type": "LIMIT",           # LIMIT or MARKET
        "product_type": "INTRADAY",      # MIS / INTRADAY
        "exchange_segment": "NSE_EQ",    # NSE Equity
    },

    # ── Trading Hours (IST) ─────────────────────────────────────
    "hours": {
        "pre_open": "09:00",
        "market_open": "09:15",
        "active_window_end": "12:00",    # Focus first 2-3 hours
        "market_close": "15:30",
        "square_off_time": "15:10",      # Auto square-off before close
    },

    # ── Learning Mode ───────────────────────────────────────────
    "learning": {
        "enabled": True,
        "max_trades_per_day": 1,
        "min_confidence_for_live": 80,    # Only trade ≥80% confidence in learning
    },
}