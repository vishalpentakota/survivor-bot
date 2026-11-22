"""Fail-closed broker adapter.

Live-trading activation (cutover, explicit user approval 2026-10-29):
Set SURVIVOR_LIVE_TRADING=1 (loaded from app/.env) to pass broker WRITE methods
through to the real Dhan client. Without it, every write method raises and the
engine can never place, modify, or cancel an order. Each broker instance can
also disable writes for its role. Reads are always allowed.
Rollback: restore cloud_guard.py from /opt/data/Projects/Survivor-bot/survivor-cloud-migration-rollback/.
"""
import os


def live_trading_enabled() -> bool:
    return os.environ.get("SURVIVOR_LIVE_TRADING", "") == "1"


class ReadOnlyBroker:
    ALLOWED = frozenset({"get_positions", "get_order_list", "get_order_by_id", "get_trade_book", "get_fund_limits", "intraday_minute_data", "historical_daily_data", "expiry_list", "option_chain", "quote_data", "fetch_security_list"})

    def __init__(self, broker, *, allow_writes: bool = True):
        self._broker = broker
        self._allow_writes = allow_writes

    def __getattr__(self, name):
        value = getattr(self._broker, name)
        if callable(value) and name not in self.ALLOWED:
            if self._allow_writes and live_trading_enabled():
                return value  # deliberate live-trading activation (approved cutover)
            raise RuntimeError("Migration safety lock: broker method blocked: " + name)
        return value
