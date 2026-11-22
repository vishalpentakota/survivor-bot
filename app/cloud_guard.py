"""Broker capabilities for a single invocation."""

from dataclasses import dataclass
import os


LIVE_SWITCH = "SURVIVOR_LIVE_TRADING"


def live_trading_enabled(file_value: str = "") -> bool:
    return os.environ.get(LIVE_SWITCH, file_value) == "1"


@dataclass(frozen=True)
class RolePolicy:
    renew_token: bool = False
    trade: bool = False
    flatten: bool = False
    repair_ip: bool = False

    @classmethod
    def resolve(cls, action: str, *, enabled: bool, live: bool, repair: bool):
        active = enabled and live
        trade = active and action == "tick"
        flatten = active and action == "flatten"
        repair_ip = active and action == "premarket" and repair
        return cls(trade or flatten or repair_ip, trade, flatten, repair_ip)


READ_ONLY = RolePolicy()


class ReadOnlyBroker:
    ALLOWED = frozenset({"get_positions", "get_order_list", "get_order_by_id", "get_trade_book", "get_fund_limits", "intraday_minute_data", "historical_daily_data", "expiry_list", "option_chain", "quote_data", "fetch_security_list"})
    ORDER_WRITES = frozenset({"place_order", "modify_order", "cancel_order"})

    def __init__(self, broker, *, policy=None, allow_writes: bool = True):
        self._broker = broker
        # Standalone callers can still opt in using the process switch.
        self._policy = (
            policy if policy is not None
            else RolePolicy(trade=allow_writes and live_trading_enabled())
        )

    def _place_exit_order(self, **kwargs):
        """Submit a market exit built from a verified broker position."""
        if self._policy.trade or self._policy.flatten:
            return self._broker.place_order(**kwargs)
        raise RuntimeError("Migration safety lock: broker method blocked: place_exit_order")

    def __getattr__(self, name):
        value = getattr(self._broker, name)
        if callable(value) and name not in self.ALLOWED:
            if self._policy.trade and name in self.ORDER_WRITES:
                return value
            if self._policy.flatten and name == "cancel_order":
                return value
            raise RuntimeError("Migration safety lock: broker method blocked: " + name)
        return value
