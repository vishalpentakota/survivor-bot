"""Market data evidence for Survivor.

exchange_calendar.json in the app directory is an operator-supplied NSE calendar
snapshot. Set "source" to the exchange publication reference and "sessions" to
a mapping of ISO dates to ["HH:MM", "HH:MM"] in IST or null for confirmed
closures. Include each date from the prior session through today, plus each
eligible contract expiry. Missing dates are unknown. Optional "min_expiry_days"
is the minimum calendar days to expiry (default 1, zero permits same-day).
"""
from datetime import date, datetime, time, timedelta, timezone
import json
import math
from pathlib import Path

from eod_ownership import IST
from verified_exit import STATUSES, WORKING, fill, instrument, orders, response_data


MAX_BAR_AGE = timedelta(minutes=3)
MAX_QUOTE_AGE = timedelta(seconds=30)
BAR_INTERVAL = timedelta(minutes=1)
BAR_FIELDS = ("open", "high", "low", "close", "volume")


def number(value, *, allow_zero=False):
    if type(value) not in (int, float, str):
        raise ValueError("Invalid market number")
    value = float(value)
    if not math.isfinite(value) or value < 0 or (value == 0 and not allow_zero):
        raise ValueError("Invalid market number")
    return value


def instant(value):
    if type(value) in (int, float):
        parsed = datetime.fromtimestamp(value, tz=timezone.utc)
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value)
    else:
        raise ValueError("Invalid market timestamp")
    if parsed.utcoffset() is None:
        raise ValueError("Market timestamp has no offset")
    return parsed.astimezone(IST)


def iso_date(value):
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("Expected ISO calendar date")
    return parsed


class ExchangeCalendar:
    def __init__(self, app_dir):
        raw = json.loads(Path(app_dir, "exchange_calendar.json").read_text(encoding="utf-8"))
        if (not isinstance(raw["source"], str) or not raw["source"].strip()
                or not isinstance(raw["sessions"], dict)):
            raise ValueError("Missing exchange calendar evidence")
        self.min_expiry_days = raw.get("min_expiry_days", 1)
        if type(self.min_expiry_days) is not int or self.min_expiry_days < 0:
            raise ValueError("Invalid minimum expiry days")
        self.sessions = {}
        for key, hours in raw["sessions"].items():
            day = iso_date(key)
            if hours is None:
                self.sessions[day] = None
                continue
            if not isinstance(hours, list) or len(hours) != 2:
                raise ValueError("Invalid exchange session")
            opening, closing = (time.fromisoformat(hour) for hour in hours)
            if any(hour.strftime("%H:%M") != given for hour, given in zip((opening, closing), hours)):
                raise ValueError("Expected IST session hours in HH:MM")
            if opening >= closing:
                raise ValueError("Invalid exchange session hours")
            self.sessions[day] = (datetime.combine(day, opening, IST), datetime.combine(day, closing, IST))

    def expected_bars(self, now, *, allow_prior=False):
        day = now.date()
        session = self.sessions[day]
        if session is None or now >= session[1]:
            raise ValueError("Calendar has no open or upcoming session today")
        if now >= session[0]:
            return session[0], now
        if not allow_prior:
            raise ValueError("Exchange session has not opened")
        while True:
            day -= timedelta(days=1)
            session = self.sessions[day]
            if session is not None:
                return session

    def expiry(self, values, today):
        if not isinstance(values, list) or not values:
            raise ValueError("Missing expiry list")
        expiries = sorted(iso_date(value) for value in values)
        for expiry in expiries:
            if (expiry - today).days >= self.min_expiry_days and self.sessions.get(expiry) is not None:
                return expiry.isoformat()
        raise ValueError("No expiry satisfies policy and exchange calendar")


def broker_expiry(broker, security_id, calendar, today):
    data = response_data(broker.expiry_list(
        under_security_id=int(security_id), under_exchange_segment="IDX_I"))
    return calendar.expiry(data["data"], today)


def checked_bars(response, now, expected=None):
    data = response_data(response)
    stamps = data["timestamp"]
    if not isinstance(stamps, list) or not stamps:
        raise ValueError("Missing bar timestamps")
    columns = {}
    for field in BAR_FIELDS:
        values = data[field]
        if not isinstance(values, list) or len(values) != len(stamps):
            raise ValueError("Missing bar values")
        columns[field] = [number(value, allow_zero=field == "volume") for value in values]
    times = [instant(stamp) for stamp in stamps if type(stamp) in (int, float)]
    if (len(times) != len(stamps) or any(stamp > now or stamp.second or stamp.microsecond for stamp in times)
            or any(first >= second for first, second in zip(times, times[1:]))):
        raise ValueError("Invalid bar timestamps")
    for opening, high, low, close in zip(*(columns[name] for name in BAR_FIELDS[:4])):
        if not low <= min(opening, close) <= max(opening, close) <= high:
            raise ValueError("Invalid OHLC")
    if expected is not None:
        opening, reference = expected
        session_times = [stamp for stamp in times if stamp >= opening]
        prior = reference.date() != now.date()
        if (not session_times or (prior and session_times[-1] != reference - BAR_INTERVAL)
                or (not prior and not timedelta() <= reference - session_times[-1] <= MAX_BAR_AGE)
                or any(second - first != BAR_INTERVAL for first, second in zip(session_times, session_times[1:]))):
            raise ValueError("Stale or missing session bars")
    return times, columns


def latest_quote(broker, security_id, exchange_segment, calendar, clock):
    payload = response_data(broker.quote_data(securities={exchange_segment: [int(security_id)]}))
    if "status" in payload and payload["status"] != "success":
        raise ValueError("Quote payload rejected")
    quote = payload["data"][exchange_segment][str(security_id)]
    if "status" in quote and quote["status"] != "success":
        raise ValueError("Quote rejected")
    now = clock()
    opening, _ = calendar.expected_bars(now)
    stamp = instant(quote["last_trade_time"])
    if stamp < opening or not timedelta() <= now - stamp <= MAX_QUOTE_AGE:
        raise ValueError("Stale or future quote timestamp")
    return number(quote["last_price"])


def pending_orders(broker, positions):
    pending = {"protective": 0, "entry-capable": 0}
    for row in orders(broker).values():
        status = row["orderStatus"]
        if status not in STATUSES:
            raise ValueError("Unknown order status")
        key = instrument(row)
        filled = fill(row)
        side = row["transactionType"]
        if side not in ("BUY", "SELL"):
            raise ValueError("Invalid order side")
        if status not in WORKING:
            continue
        order_type = row["orderType"]
        remaining = int(row["quantity"]) - filled
        if order_type not in ("MARKET", "LIMIT", "STOP_LOSS", "STOP_LOSS_MARKET") or remaining <= 0:
            raise ValueError("Invalid pending order")
        net = positions.get(key, 0)
        stop = order_type in ("STOP_LOSS", "STOP_LOSS_MARKET")
        if stop:
            number(row["triggerPrice"])
        reduces = (side == "SELL" and net > 0) or (side == "BUY" and net < 0)
        pending["protective" if stop and reduces and remaining <= abs(net) else "entry-capable"] += 1
    return pending
