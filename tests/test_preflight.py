"""Premarket readiness and market-data gates with a frozen IST clock."""
import contextlib
from datetime import datetime, timedelta
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import test_write_safety as fixtures


class PreflightTests(unittest.TestCase):
    def setUp(self):
        fixtures.WriteSafetyTests.setUp(self)
        self.now = datetime.fromisoformat("2026-11-23T09:00:00+05:30")
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory(dir=fixtures.APP.parent)))
        self.enterContext(patch.object(self.engine, "BASE_DIR", str(self.directory)))
        self.enterContext(patch.object(self.engine, "datetime", SimpleNamespace(
            now=lambda tz=None: self.now.astimezone(tz) if tz else self.now.replace(tzinfo=None),
            fromisoformat=datetime.fromisoformat)))
        self.state = {"status": "ALIVE"}
        self.enterContext(patch.object(self.engine, "load_state", return_value=self.state))
        self.save = self.enterContext(patch.object(self.engine, "save_state"))
        self.sdk = self.sdk.dhanhq.return_value
        self.sdk.get_positions.return_value = self.ok([])
        self.sdk.get_order_list.return_value = self.ok([])
        self.sdk.get_fund_limits.return_value = self.ok({"availabelBalance": 30000, "utilizedAmount": 0})
        self.sdk.expiry_list.return_value = self.ok({"data": ["2026-11-24"]})
        self.http.get.return_value.json.return_value = {"ordersAllowed": True}
        self.client = self.engine.SurvivorDhanClient(policy=self.engine.RolePolicy(trade=True))
        self.sessions = {
            "2026-11-19": ["09:15", "15:30"],
            "2026-11-20": None,
            "2026-11-21": None,
            "2026-11-22": None,
            "2026-11-23": ["09:15", "15:30"],
            "2026-11-24": ["09:15", "15:30"],
        }
        self.calendar()
        self.bars("2026-11-19T15:29:00+05:30")

    @staticmethod
    def ok(data):
        return {"status": "success", "data": data}

    def calendar(self, **policy):
        (self.directory / "exchange_calendar.json").write_text(json.dumps({
            "source": "Mock exchange calendar fixture",
            "sessions": self.sessions,
            **policy,
        }))

    def bars(self, stamp):
        payload = {"timestamp": [datetime.fromisoformat(stamp).timestamp()],
                   "open": [100], "high": [101], "low": [99], "close": [100], "volume": [1]}
        self.sdk.intraday_minute_data.return_value = self.ok(payload)
        return payload

    def quote(self, price=100, stamp=None, status="success"):
        self.sdk.quote_data.return_value = {
            "status": status, "data": {"data": {"NSE_EQ": {"23": {
                "last_price": price, "last_trade_time": self.now.isoformat() if stamp is None else stamp,
            }}}}
        }

    def check(self, expected, message):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(self.engine.cmd_premarket(), expected, output.getvalue())
        self.assertIn(message, output.getvalue())
        self.save.assert_not_called()
        self.renew.assert_not_called()
        self.http.put.assert_not_called()
        for method in ("place_order", "modify_order", "cancel_order"):
            getattr(self.sdk, method).assert_not_called()

    def test_prior_close_before_open_across_explicit_holiday_and_weekend(self):
        self.check(0, "SYSTEM CHECK PASSED")

    def test_prior_close_is_stale_in_session(self):
        self.now = self.now.replace(hour=10)
        self.check(1, "bars")

    def test_current_bar_with_utc_offset(self):
        self.now = self.now.replace(hour=10)
        self.bars("2026-11-23T04:29:00+00:00")
        self.check(0, "SYSTEM CHECK PASSED")

    def test_unknown_calendar_gap_and_closed_session(self):
        del self.sessions["2026-11-20"]
        self.calendar()
        self.check(1, "calendar")
        self.sessions["2026-11-23"] = None
        self.calendar()
        self.check(1, "calendar")

    def test_unknown_positions_and_orders(self):
        for read, item, message in (
            (self.sdk.get_positions, {"data": []}, "positions"),
            (self.sdk.get_positions, self.ok([{"netQty": 0}]), "positions"),
            (self.sdk.get_order_list, {"status": "failure", "data": []}, "orders"),
            (self.sdk.get_order_list, self.ok([{"orderId": "1"}]), "orders"),
            (self.sdk.get_order_list, self.ok([{"orderId": "1", "orderStatus": "UNKNOWN"}]), "orders"),
        ):
            with self.subTest(item=item):
                read.return_value = item
                self.check(1, message)
                read.return_value = self.ok([])
        self.sdk.get_positions.side_effect = TimeoutError
        self.check(1, "positions")

    def test_pending_entries_and_protective_orders_both_deny(self):
        order = dict(orderId="1", orderStatus="PART_TRADED", securityId="23",
                     exchangeSegment="NSE_EQ", productType="INTRADAY", transactionType="BUY",
                     orderType="LIMIT", quantity=2, filledQty=1)
        self.sdk.get_order_list.return_value = self.ok([order])
        self.check(1, "entry-capable")
        self.sdk.get_positions.return_value = self.ok([dict(
            securityId="23", exchangeSegment="NSE_EQ", productType="INTRADAY", netQty=1)])
        order.update(orderStatus="TRIGGER_PENDING", transactionType="SELL",
                     orderType="STOP_LOSS", quantity=1, filledQty=0, triggerPrice=95)
        self.check(1, "protective")

    def test_expiry_policy_and_explicit_expiry_session(self):
        self.sdk.expiry_list.return_value = self.ok({"data": ["2026-11-23"]})
        self.check(1, "expiry")
        self.calendar(min_expiry_days=0)
        self.check(0, "SYSTEM CHECK PASSED")
        self.sdk.expiry_list.return_value = self.ok({"data": ["2026-11-22", "2026-11-25"]})
        self.check(1, "expiry")
        self.sdk.expiry_list.return_value = self.ok({"data": ["2026-11-23-invalid"]})
        self.check(1, "expiry")
        self.sdk.expiry_list.return_value = {"data": {"data": ["2026-11-24"]}}
        self.check(1, "expiry")

    def test_malformed_future_missing_and_stale_bars(self):
        self.now = self.now.replace(hour=10)
        for stamp, invalid in (
            ("2026-11-23T10:01:00+05:30", {}),
            ("2026-11-23T09:56:00+05:30", {}),
            ("2026-11-23T09:59:00+05:30", {"close": [float("nan")]}),
            ("2026-11-23T09:59:00+05:30", {"high": []}),
            ("2026-11-23T09:59:00+05:30", {"timestamp": [None]}),
        ):
            with self.subTest(stamp=stamp, invalid=invalid):
                self.bars(stamp).update(invalid)
                self.check(1, "bars")
        self.sdk.intraday_minute_data.return_value.pop("status")
        self.check(1, "bars")

    def test_latest_quote_price_timestamp_status_and_scope(self):
        self.now = self.now.replace(hour=10)
        read = lambda: self.engine._get_latest_tick_ltp(self.client, "23", "NSE_EQ", "EQUITY")
        self.quote(stamp="2026-11-23T04:29:45+00:00")
        self.assertEqual(read(), 100)
        for price, stamp, status in (
            (float("inf"), None, "success"), (True, None, "success"), (0, None, "success"),
            (100, "", "success"), (100, "2026-11-23T10:00:00", "success"),
            (100, (self.now - timedelta(seconds=31)).timestamp(), "success"),
            (100, (self.now + timedelta(seconds=1)).timestamp(), "success"),
            (100, None, "failure"), (100, None, None),
        ):
            with self.subTest(price=price, stamp=stamp, status=status):
                self.quote(price, stamp, status)
                self.assertIsNone(read())
        self.quote()
        self.sdk.quote_data.return_value["data"]["data"]["NSE_EQ"]["24"] = (
            self.sdk.quote_data.return_value["data"]["data"]["NSE_EQ"].pop("23"))
        self.assertIsNone(read())

    def test_unverified_price_blocks_entry_and_chase(self):
        self.now = self.now.replace(hour=10)
        self.quote(status="failure")
        trade = dict(security_id="23", exchange_segment="NSE_EQ", instrument_type="EQUITY",
                     tick_size=0.05, lot_size=1, quantity=1, stop_loss=95)
        with patch.object(self.engine, "_exit_coordinator") as coordinator:
            self.assertEqual(self.engine._start_entry(self.client, self.state, trade, "BUY", "LIMIT", 99), "denied")
            coordinator.assert_not_called()
        coordinator = Mock()
        coordinator.snapshot.return_value = ({}, {})
        self.assertFalse(self.engine._entry_chase(self.client, coordinator, {"trade": trade, "limit": 99},
                                                  {"requested_type": "LIMIT", "side": "BUY"}, "1", {}, {}))
        self.sdk.modify_order.assert_not_called()

    def test_monitor_holds_verified_stop_during_quote_outage_and_dispatches_eod(self):
        self.now = self.now.replace(hour=10)
        self.quote(price=110, stamp=(self.now - timedelta(seconds=31)).isoformat())
        self.sdk.get_positions.return_value = self.ok([dict(
            securityId="23", exchangeSegment="NSE_EQ", productType="INTRADAY", netQty=1)])
        self.state["active_trade"] = dict(
            security_id="23", exchange_segment="NSE_EQ", instrument_type="EQUITY",
            quantity=1, entry_price=100, direction="LONG", tick_size=.05, sl_order_id="2",
            stop_loss=95, target_price=120, weapon="EQUITY_5X",
        )
        elapsed = iter((0, 0, 2, 0, 0))
        with (patch.object(self.engine, "time", SimpleNamespace(time=lambda: next(elapsed), sleep=Mock())),
              patch.object(self.engine, "_recovery_pending", return_value=False),
              patch.object(self.engine, "_inspect_active_stop", return_value="RESTING") as stop,
              patch.object(self.engine, "_flatten_trade", return_value=0) as flatten):
            self.assertIsNone(self.engine.monitor_active_trade_loop(
                self.client, self.state, max_seconds=1, quiet_scan=True))
            stop.assert_called_once_with(self.client, self.state, self.state["active_trade"], .05)
            self.assertEqual(self.state["active_trade"]["stop_loss"], 95)
            self.sdk.modify_order.assert_not_called()
            flatten.assert_not_called()
            self.now = self.now.replace(hour=15, minute=10)
            self.assertEqual(self.engine.monitor_active_trade_loop(
                self.client, self.state, max_seconds=1, quiet_scan=True), 0)
            flatten.assert_called_once_with(self.client, self.state, self.state["active_trade"], "EOD_SQUARE_OFF")

    def test_denied_repair_cannot_reset_state(self):
        self.sdk.get_order_list.side_effect = TimeoutError
        with patch.object(self.engine, "_state_transaction", return_value=contextlib.nullcontext()):
            self.assertEqual(self.engine.cmd_premarket(
                policy=self.engine.RolePolicy(repair_ip=True)), 1)
        self.save.assert_not_called()
        self.assertEqual(self.state, {"status": "ALIVE"})

    def test_missing_middle_minute_is_denied(self):
        self.now = self.now.replace(hour=10)
        bars = self.bars("2026-11-23T09:59:00+05:30")
        bars["timestamp"].insert(0, (self.now - timedelta(minutes=3)).timestamp())
        for field in ("open", "high", "low", "close", "volume"):
            bars[field].insert(0, bars[field][0])
        self.check(1, "bars")


class QuoteOutageRecovery(unittest.TestCase):
    def test_verified_emergency_liquidation_without_quote_or_calendar(self):
        from test_verified_exits import VerifiedExits

        fixture = VerifiedExits()
        self.addCleanup(fixture.doCleanups)
        fixture.setUp()
        now = datetime.fromisoformat("2026-11-23T10:00:00+05:30")
        self.enterContext(patch.object(fixture.engine, "datetime", SimpleNamespace(now=lambda tz: now)))
        self.enterContext(patch.object(fixture.engine, "BASE_DIR", fixture.engine.EXIT_LEDGER.parent))
        fixture.sdk.quote_data.side_effect = TimeoutError
        rows = fixture.opening_orders()
        fixture.book(rows)
        fixture.exit_submit(rows, iter([("TRADED", 5, 0)]))

        self.assertTrue(fixture.client().emergency_flatten_all().flat)
        self.assertEqual(fixture.sdk.place_order.call_args.kwargs["transaction_type"], "SELL")
        fixture.sdk.quote_data.assert_not_called()
        fixture.sdk.modify_order.assert_not_called()
