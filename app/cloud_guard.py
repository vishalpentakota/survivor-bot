"""Fail-closed broker adapter. No activation switch included in this migration build."""
class ReadOnlyBroker:
    ALLOWED = frozenset({"get_positions", "get_order_list", "get_order_by_id", "get_trade_book", "get_fund_limits", "intraday_minute_data", "historical_daily_data", "expiry_list", "option_chain", "quote_data", "fetch_security_list"})
    def __init__(self, broker):
        self._broker = broker
    def __getattr__(self, name):
        value = getattr(self._broker, name)
        if callable(value) and name not in self.ALLOWED:
            raise RuntimeError("Migration safety lock: broker method blocked: " + name)
        return value
