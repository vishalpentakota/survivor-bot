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
import logging
import math
from pathlib import Path

from eod_ownership import IST
from verified_exit import STATUSES, WORKING, fill, instrument, orders, response_data


logger = logging.getLogger("survivor.market_preflight")


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
        if not value.strip():
            raise ValueError("Missing market timestamp (no trade data)")
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            try:
                parsed = datetime.strptime(value, "%d/%m/%Y %H:%M:%S")
            except ValueError as exc:
                raise ValueError(f"Unrecognized market timestamp: {value!r}") from exc
            if parsed.strftime("%d/%m/%Y %H:%M:%S") != value:
                raise ValueError(f"Unrecognized market timestamp: {value!r}")
            parsed = parsed.replace(tzinfo=IST)
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
    tag = f"{exchange_segment}:{security_id}"
    try:
        raw = broker.quote_data(securities={exchange_segment: [int(security_id)]})
    except Exception as exc:
        logger.warning("Quote failed for %s [cause=broker-unavailable]: %s: %s", tag, type(exc).__name__, exc)
        raise
    if not isinstance(raw, dict) or raw.get("status") != "success":
        status = raw.get("status") if isinstance(raw, dict) else type(raw).__name__
        remarks = raw.get("remarks") if isinstance(raw, dict) else None
        cause = "rejected" if isinstance(raw, dict) and status not in (None, "") else "broker-unavailable"
        logger.warning("Quote failed for %s [cause=%s]: status=%r remarks=%r", tag, cause, status, remarks)
        raise ValueError(f"Quote {cause} (status={status!r}, remarks={remarks!r})")
    payload = response_data(raw)
    if not isinstance(payload, dict):
        logger.warning("Quote failed for %s [cause=rejected]: non-dict payload %s", tag, type(payload).__name__)
        raise ValueError("Quote payload rejected")
    if "status" in payload and payload["status"] != "success":
        logger.warning("Quote failed for %s [cause=rejected]: payload status=%r remarks=%r",
                       tag, payload.get("status"), payload.get("remarks"))
        raise ValueError("Quote payload rejected")
    try:
        quote = payload["data"][exchange_segment][str(security_id)]
    except (KeyError, TypeError) as exc:
        logger.warning("Quote failed for %s [cause=no-trade-data]: instrument absent from quote payload", tag)
        raise ValueError("Quote has no trade data (instrument absent)") from exc
    if not isinstance(quote, dict) or ("status" in quote and quote["status"] != "success"):
        status = quote.get("status") if isinstance(quote, dict) else type(quote).__name__
        logger.warning("Quote failed for %s [cause=rejected]: quote status=%r", tag, status)
        raise ValueError("Quote rejected")
    raw_stamp = quote.get("last_trade_time")
    if raw_stamp is None or (isinstance(raw_stamp, str) and not raw_stamp.strip()):
        logger.warning("Quote failed for %s [cause=no-trade-data]: empty last_trade_time", tag)
        raise ValueError("Quote has no trade data (empty last_trade_time)")
    now = clock()
    opening, _ = calendar.expected_bars(now)
    try:
        stamp = instant(raw_stamp)
    except ValueError as exc:
        logger.warning("Quote failed for %s [cause=parse-failure]: last_trade_time=%r (%s)", tag, raw_stamp, exc)
        raise
    age = now - stamp
    if stamp < opening or age > MAX_QUOTE_AGE:
        logger.warning("Quote failed for %s [cause=stale-timestamp]: stamp=%s now=%s age=%.1fs",
                       tag, stamp.isoformat(), now.isoformat(), age.total_seconds())
        raise ValueError("Stale or future quote timestamp")
    if age < timedelta():
        logger.warning("Quote failed for %s [cause=future-timestamp]: stamp=%s now=%s age=%.1fs",
                       tag, stamp.isoformat(), now.isoformat(), age.total_seconds())
        raise ValueError("Stale or future quote timestamp")
    try:
        return number(quote["last_price"])
    except (KeyError, ValueError) as exc:
        logger.warning("Quote failed for %s [cause=invalid-price]: last_price=%r (%s)",
                       tag, quote.get("last_price"), exc)
        raise


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
