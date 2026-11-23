"""Review regressions using independently seeded mock executions."""
from datetime import datetime
import json
import os
from pathlib import Path
import runpy
import sys
import unittest
from unittest.mock import patch

import test_verified_exits as fixtures
from test_write_safety import FLAG


class ExitRework(unittest.TestCase):
    setUp = fixtures.VerifiedExits.setUp
    response = staticmethod(fixtures.VerifiedExits.response)
    position = staticmethod(fixtures.VerifiedExits.position)
    client = fixtures.VerifiedExits.client
    row = fixtures.VerifiedExits.row
    book = fixtures.VerifiedExits.book
    opening_orders = fixtures.VerifiedExits.opening_orders
    exit_submit = fixtures.VerifiedExits.exit_submit
    active = fixtures.VerifiedExits.active

    def test_repeated_stale_position_with_empty_order_book_never_authorizes_exit(self):
        self.book({})
        for _ in range(3):
            self.assertFalse(self.client().emergency_flatten_all().flat)
        self.sdk.place_order.assert_not_called()

    def test_verified_rejection_retries_once_and_settles(self):
        rows = self.opening_orders() | {"stop": self.row("stop", "SELL", "PENDING", 5, 0)}
        self.book(rows)
        self.exit_submit(rows, iter([("TRADED", 5, 0)]))

        def cancel(oid):
            if self.sdk.cancel_order.call_count == 1:
                return self.response({"orderStatus": "REJECTED", "orderId": oid})
            rows[oid]["orderStatus"] = "CANCELLED"
            return self.response({"orderId": oid})

        self.sdk.cancel_order.side_effect = cancel
        self.assertFalse(self.client().emergency_flatten_all().flat)
        self.sdk.place_order.assert_not_called()
        self.assertTrue(self.client().emergency_flatten_all().flat)
        self.assertEqual(self.sdk.cancel_order.call_count, 2)

    def test_cancel_retry_is_bounded_and_checks_current_identity(self):
        for matching in (True, False):
            with self.subTest(matching=matching):
                self.engine.EXIT_LEDGER.unlink(missing_ok=True)
                self.sdk.reset_mock()
                rows = self.opening_orders() | {"stop": self.row("stop", "SELL", "PENDING", 5, 0)}
                self.book(rows)
                self.sdk.cancel_order.return_value = self.response({"orderStatus": "REJECTED", "orderId": "stop"})
                self.assertFalse(self.client().emergency_flatten_all().flat)
                if not matching:
                    rows["stop"]["quantity"] = 6
                for _ in range(3):
                    self.assertFalse(self.client().emergency_flatten_all().flat)
                self.assertEqual(self.sdk.cancel_order.call_count, 2 if matching else 1)
                self.sdk.place_order.assert_not_called()

    def test_ambiguous_cancel_ack_is_not_retried(self):
        rows = self.opening_orders() | {"stop": self.row("stop", "SELL", "PENDING", 5, 0)}
        self.book(rows)
        self.sdk.cancel_order.return_value = {"status": "failure", "remarks": {"error_message": "timeout"}}
        for _ in range(2):
            self.assertFalse(self.client().emergency_flatten_all().flat)
        self.sdk.cancel_order.assert_called_once_with("stop")
        self.sdk.place_order.assert_not_called()

    def test_unknown_exit_is_local_but_unidentified_book_row_blocks_account(self):
        rows = self.opening_orders()
        rows["opening-b"] = dict(self.row("opening-b", "SELL", "TRADED", 4, 4), securityId="24")
        rows["unknown"] = {"orderId": "unknown", "orderStatus": "PENDING"}
        self.book(rows)
        nets = {"23": 5, "24": -4}
        self.sdk.get_positions.side_effect = lambda: self.response([
            dict(self.position(net), securityId=key) for key, net in nets.items()])

        def submit(**params):
            key = params["security_id"]
            if key == "23":
                raise TimeoutError
            rows["exit-b"] = dict(self.row("exit-b", "BUY", "TRADED", 4, 4), securityId=key)
            nets[key] = 0
            return self.response({"orderId": "exit-b"})

        self.sdk.place_order.side_effect = submit
        self.assertFalse(self.client().emergency_flatten_all().flat)
        self.sdk.place_order.assert_not_called()
        del rows["unknown"]
        self.assertFalse(self.client().emergency_flatten_all().flat)
        self.assertFalse(self.client().emergency_flatten_all().flat)
        self.assertEqual([c.kwargs["security_id"] for c in self.sdk.place_order.call_args_list], ["23", "24"])
        self.assertEqual(nets, {"23": 5, "24": 0})
        self.sdk.cancel_order.assert_not_called()

    def test_alert_records_confirmed_partial_execution_without_verified_position(self):
        rows = self.opening_orders()
        self.book(rows)
        self.exit_submit(rows, iter([("CANCELLED", 2, 5)]))
        with patch.object(self.engine, "EXIT_ESCALATION_SECONDS", 0):
            self.assertFalse(self.client().emergency_flatten_all().flat)
        ledger = json.loads(self.engine.EXIT_LEDGER.read_text())
        alert, = ledger["alerts"].values()
        self.assertIn("instrument ('23', 'NSE_EQ', 'INTRADAY')", alert)
        self.assertIn("intent/order 2", alert)
        self.assertIn("'2': 2", alert)
        self.assertIn("remaining verified qty UNKNOWN", alert)

    def test_mixed_execution_records_stop_and_market_separately_from_trigger(self):
        rows = {
            "entry": self.row("entry", "BUY", "TRADED", 5, 5, 100, createTime="2026-11-23 10:00:00"),
            "stop": self.row("stop", "SELL", "CANCELLED", 5, 2, 95),
        }
        self.book(rows)
        self.sdk.get_positions.return_value = self.response([self.position(3)])
        self.exit_submit(rows, iter([("TRADED", 3, 0)]))
        self.assertTrue(self.client().emergency_flatten_all().flat)
        active = self.active(sl_order_id="stop")
        state = {"active_trade": active}
        with patch.object(self.engine, "save_state"):
            self.assertTrue(self.engine._finalize_closed_trade(self.client(), state, active, "EOD_SQUARE_OFF"))
        trade, = json.loads(Path(self.engine.JOURNAL_FILE).read_text())
        self.assertEqual((trade["execution_source"], trade["exit_trigger"], trade["exit_price"]),
                         ("MIXED", "EOD_SQUARE_OFF", 98.6))

    def test_journal_replay_without_broker_applies_risk_once(self):
        rows = {"entry": self.row("entry", "BUY", "TRADED", 5, 5, 100, createTime="2026-11-23 10:00:00")}
        self.book(rows)
        self.exit_submit(rows, iter([("TRADED", 5, 0)]))
        self.assertTrue(self.client().emergency_flatten_all().flat)
        active = self.active(flatten_pending=True)
        state = {"active_trade": active, "daily_date": "2026-11-23", "daily_consecutive_losses": 1}
        engine = self.engine
        with patch.object(engine, "datetime", wraps=datetime) as clock:
            clock.now.return_value = datetime(2026, 11, 23, 10, tzinfo=engine.IST)
            with patch.object(engine, "save_state", side_effect=OSError):
                self.assertFalse(engine._finalize_closed_trade(self.client(), state, active, "STOP_LOSS_HIT"))
            self.assertEqual(state["daily_consecutive_losses"], 1)
            self.sdk.get_order_list.side_effect = AssertionError("replay should not read broker")
            with patch.object(engine, "save_state"):
                self.assertEqual(engine.monitor_active_trade_loop(self.client(), state), 0)
                self.assertTrue(engine._finalize_closed_trade(self.client(), state, active, "STOP_LOSS_HIT"))
        self.assertEqual((state["losses"], state["daily_consecutive_losses"], state["daily_halted"]), (1, 2, True))
        self.assertEqual(len(json.loads(Path(engine.JOURNAL_FILE).read_text())), 1)

    def test_cli_pending_tick_is_nonzero_and_idle_tick_succeeds(self):
        engine = self.engine
        staged = engine.EXIT_LEDGER.parent / "survivor_engine.py"
        staged.write_bytes(Path(engine.__file__).read_bytes())
        now = datetime(2026, 11, 23, 16, tzinfo=engine.IST)
        (staged.parent / "survivor_state.json").write_text(json.dumps({
            "status": "ALIVE", "mortality_armed": True, "peak_equity": 30000,
            "daily_start_equity": 30000, "daily_date": "2026-11-23",
        }))
        self.sdk.get_fund_limits.return_value = self.response({"availabelBalance": 30000})
        self.sdk.get_positions.return_value = self.response([])
        row = self.row("working", "BUY", "PENDING", 5, 0)
        self.book({"working": row})
        self.sdk.cancel_order.return_value = self.response({"orderId": "working"})
        with patch.object(sys, "argv", [str(staged), "tick", "--live"]), patch.dict(
                os.environ, {FLAG: "1"}), patch("datetime.datetime", wraps=datetime) as clock:
            clock.now.return_value = now
            with self.assertRaises(SystemExit) as failure:
                runpy.run_path(str(staged), run_name="__main__")
            self.assertEqual(failure.exception.code, 1)
            row["orderStatus"] = "CANCELLED"
            runpy.run_path(str(staged), run_name="__main__")
            now = datetime(2026, 11, 23, 8, tzinfo=engine.IST)
            clock.now.return_value = now
            runpy.run_path(str(staged), run_name="__main__")
        self.sdk.cancel_order.assert_called_once_with("working")
        self.sdk.place_order.assert_not_called()
