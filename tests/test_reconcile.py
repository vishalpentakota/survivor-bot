"""Reconciliation acceptance using the staged mock broker and real state/outbox."""
from contextlib import redirect_stdout
from io import StringIO
import json
from unittest import TestCase
from unittest.mock import patch

import test_exit_restart_matrix as fixtures


class Reconcile(TestCase):
    setUp = fixtures.RestartMatrix.setUp
    response = staticmethod(fixtures.RestartMatrix.response)
    position = staticmethod(fixtures.RestartMatrix.position)
    client = fixtures.RestartMatrix.client
    row = fixtures.RestartMatrix.row
    book = fixtures.RestartMatrix.book
    opening_orders = fixtures.RestartMatrix.opening_orders
    exit_submit = fixtures.RestartMatrix.exit_submit
    active = fixtures.RestartMatrix.active
    configure_persistence = fixtures.RestartMatrix.configure_persistence
    initialize = fixtures.RestartMatrix.initialize
    set_position = fixtures.RestartMatrix.set_position
    state = fixtures.RestartMatrix.state
    journal = fixtures.RestartMatrix.journal
    write_state = fixtures.RestartMatrix.write_state

    def stage(self):
        self.configure_persistence()
        self.initialize("absent")
        self.write_state(dict(self.state(), active_trade=None))
        self.enterContext(patch.object(self.engine, "BASE_DIR", self.engine.EXIT_LEDGER.parent))
        self.sdk.get_fund_limits.return_value = self.response(
            {"availabelBalance": 23500.18, "utilizedAmount": 0})
        self.book({})
        self.set_position(0)

    def report(self):
        output = StringIO()
        with redirect_stdout(output):
            status = self.engine.cmd_reconcile()
        return status, output.getvalue()

    def test_api_failure_or_order_contradiction_is_durably_unknown(self):
        self.stage()
        for failure in ("positions", "orders", "funds", "contradiction"):
            with self.subTest(failure=failure):
                with patch.object(self.sdk, "get_positions", wraps=self.sdk.get_positions) as pos, \
                        patch.object(self.sdk, "get_order_list", wraps=self.sdk.get_order_list) as orders, \
                        patch.object(self.sdk, "get_fund_limits", wraps=self.sdk.get_fund_limits) as funds:
                    if failure == "positions":
                        pos.return_value = {"status": "failure"}
                    elif failure == "orders":
                        orders.side_effect = TimeoutError
                    elif failure == "funds":
                        funds.return_value = {"status": "failure"}
                    else:
                        orders.return_value = self.response(list(self.rows.values()))
                    status, output = self.report()
                self.assertEqual(status, 1)
                self.assertIn("UNKNOWN", output)
                self.assertIn("INCOMPLETE", output)
                self.assertNotIn("Cash Overnight: True", output)
        with self.engine._eod_store().transaction() as db:
            self.assertEqual(db.execute("SELECT acknowledged FROM outbox").fetchall(), [(0,)])

    def test_verified_zero_position_is_flat_and_drift_is_separate(self):
        self.stage()
        self.sdk.get_positions.return_value = self.response([self.position(0)])
        status, output = self.report()
        self.assertEqual(status, 0)
        self.assertIn("Cash Overnight: True", output)
        self.assertIn("Equity drift: ₹-6,500.00", output)
        self.assertNotIn("Actual broker charges: ₹-6,500.00", output)

    def test_unmatched_halt_trade_preserves_journal_and_records_unknown(self):
        self.stage()
        active = self.active()
        old = [dict(active, net_pnl=-6500, reconciled_with_broker=False)]
        self.journal_path.write_text(json.dumps(old))
        state = dict(self.state(), active_trade=active)
        self.write_state(state)
        self.assertEqual(self.engine._flatten_trade(self.client(), state, active, "HALT_FLATTENED"), 1)
        self.assertEqual(self.journal(), old)
        self.assertIsNotNone(self.state()["active_trade"])
        self.assertEqual(self.report()[0], 1)
        with self.engine._eod_store().transaction() as db:
            messages = db.execute("SELECT message FROM incidents").fetchall()
        self.assertTrue(any("HALT_FLATTENED" in msg and "UNKNOWN" in msg for msg, in messages))

    def test_actual_short_partial_fills_and_replay(self):
        self.stage()
        active = self.active(weapon="EQUITY", direction="SHORT", sl_order_id="stop",
                             stop_execution_id="partial")
        self.book({
            "entry": self.row("entry", "SELL", "TRADED", 5, 5, 100,
                              brokerageAndStatutoryCharges=2),
            "stop": self.row("stop", "BUY", "CANCELLED", 5, 2, 95,
                             brokerageAndStatutoryCharges=1),
            "partial": self.row("partial", "BUY", "TRADED", 3, 3, 90,
                                brokerageAndStatutoryCharges=2),
        })
        before = dict(self.state(), active_trade=active)
        self.write_state(before)
        self.assertEqual(self.engine._flatten_trade(self.client(), before.copy(), active, "HALT_FLATTENED"), 0)
        row, = self.journal()
        self.assertEqual((row["gross_pnl"], row["net_pnl"], row["actual_broker_charges"],
                          row["exit_trigger"]), (40, 35, 5, "HALT_FLATTENED"))
        first = self.state()
        self.assertTrue(row["reconciled_with_broker"])
        self.assertEqual((first["actual_realized_pnl_total"], first["wins"], first["daily_consecutive_losses"]),
                         (35, 5, 0))
        self.write_state(before)
        self.sdk.reset_mock()
        self.assertTrue(self.engine._finalize_closed_trade(
            None, self.engine.load_state(), active, "HALT_FLATTENED", replay_only=True))
        self.sdk.get_positions.assert_not_called()
        self.assertEqual(self.state()["actual_realized_pnl_total"], 35)
        self.assertEqual(self.report()[0], 0)
        self.assertEqual(self.state()["actual_realized_pnl_total"], 35)

    def test_same_security_trades_use_separate_broker_order_ids(self):
        self.stage()
        earlier = self.active(order_id="entry-a", sl_order_id="stop-a",
                              entry_time="2026-11-23T09:30:00+05:30")
        later = self.active(order_id="entry-b", sl_order_id="stop-b", direction="SHORT")
        self.journal_path.write_text(json.dumps([earlier, later]))
        self.book({
            "entry-a": self.row("entry-a", "BUY", "TRADED", 5, 5, 100,
                                createTime="2026-11-23 09:30:00", brokerageAndStatutoryCharges=3),
            "stop-a": self.row("stop-a", "SELL", "TRADED", 5, 5, 110,
                               createTime="2026-11-23 09:35:00", brokerageAndStatutoryCharges=2),
            "entry-b": self.row("entry-b", "SELL", "TRADED", 3, 3, 120,
                                createTime="2026-11-23 10:00:00", brokerageAndStatutoryCharges=4),
            "stop-b": self.row("stop-b", "BUY", "TRADED", 3, 3, 110,
                               createTime="2026-11-23 10:05:00", brokerageAndStatutoryCharges=1),
        })
        status, output = self.report()
        self.assertEqual(status, 0, output)
        self.assertIn("Actual broker charges: ₹10.00", output)
        self.assertIn("Verified net P&L: ₹+70.00", output)
        rows = self.journal()
        self.assertEqual([row["net_pnl"] for row in rows], [45, 25])
        self.assertEqual([[item["id"] for item in row["exit_proof"]["exits"]] for row in rows],
                         [["stop-a"], ["stop-b"]])
        self.assertEqual(self.report()[0], 0)
        self.assertEqual(self.journal(), rows)
        self.assertEqual(self.state()["actual_broker_charges_total"], 10)

    def test_estimated_net_does_not_apply_strategy_risk_or_actual_pnl(self):
        self.stage()
        active = self.active(sl_order_id="stop", entry_cycle="confirmed-workflow")
        self.book({
            "entry": self.row("entry", "BUY", "TRADED", 5, 5, 100),
            "stop": self.row("stop", "SELL", "TRADED", 5, 5, 101,
                             brokerageAndStatutoryCharges=-6500),
        })
        before = dict(self.state(), active_trade=active)
        self.assertEqual(self.engine._flatten_trade(self.client(), before.copy(), active, "HALT_FLATTENED"), 0)
        row, = self.journal()
        self.assertEqual((row["pnl_provenance"], row["gross_pnl"], row["actual_broker_charges"]),
                         ("estimated_roundtrip", 5, None))
        self.assertTrue(row["fills_verified"])
        self.assertFalse(row["reconciled_with_broker"])
        after = self.state()
        for key in ("losses", "daily_consecutive_losses", "realized_pnl_total", "daily_halted"):
            self.assertEqual(before.get(key), after.get(key))
        status, output = self.report()
        self.assertEqual(status, 1)
        self.assertIn("Estimated roundtrip charges per trade: ₹55.00", output)
        self.assertNotIn("Actual broker charges:", output)
        self.assertEqual(self.state()["actual_realized_pnl_total"], 0)

    def test_actual_fee_contradiction_fences_replay(self):
        self.stage()
        active = self.active(sl_order_id="stop")
        orders = {
            "entry": self.row("entry", "BUY", "TRADED", 5, 5, 100, brokerageAndStatutoryCharges=5),
            "stop": self.row("stop", "SELL", "TRADED", 5, 5, 110, brokerageAndStatutoryCharges=5),
        }
        self.book(orders)
        pending = dict(self.state(), active_trade=active)
        self.assertEqual(self.engine._flatten_trade(self.client(), pending.copy(), active, "HALT_FLATTENED"), 0)
        before = self.journal()
        orders["stop"]["brokerageAndStatutoryCharges"] = 11
        self.assertEqual(self.report()[0], 1)
        self.assertEqual(self.journal(), before)
        self.write_state(pending)
        self.assertFalse(self.engine._finalize_closed_trade(
            None, self.engine.load_state(), active, "HALT_FLATTENED", replay_only=True))
        self.assertTrue(self.state()["active_trade"])

    def test_ownerless_halt_exit_records_order_id_and_unknown_attribution(self):
        self.stage()
        rows = self.opening_orders()
        self.book(rows)
        self.set_position(5)
        self.exit_submit(rows, iter([("TRADED", 5, 0)]))
        self.assertEqual(self.engine._flatten_trade(self.client(), self.state(), None, "HALT_FLATTENED"), 1)
        self.assertEqual(self.journal(), [])
        with self.engine._eod_store().transaction() as db:
            message, = db.execute("SELECT message FROM incidents").fetchone()
        self.assertIn("UNKNOWN for order(s) 2", message)

    def test_unattributed_balanced_orders_are_incomplete(self):
        self.stage()
        self.book({
            "unmatched-entry": self.row("unmatched-entry", "BUY", "TRADED", 5, 5, 100),
            "unmatched-exit": self.row("unmatched-exit", "SELL", "TRADED", 5, 5, 90),
        })
        status, output = self.report()
        self.assertEqual(status, 1, output)
        self.assertNotIn("Cash Overnight: True", output)
        self.assertEqual(self.journal(), [])
        self.assertEqual(self.state()["actual_realized_pnl_total"], 0)

    def test_late_position_failure_keeps_exact_replay_fenced(self):
        self.stage()
        active = self.active(sl_order_id="stop")
        self.book({
            "entry": self.row("entry", "BUY", "TRADED", 5, 5, 100, brokerageAndStatutoryCharges=2),
            "stop": self.row("stop", "SELL", "TRADED", 5, 5, 110, brokerageAndStatutoryCharges=3),
        })
        pending = dict(self.state(), active_trade=active)
        self.assertEqual(self.engine._flatten_trade(self.client(), pending.copy(), active, "HALT_FLATTENED"), 0)
        before = self.journal()
        self.sdk.get_positions.side_effect = [self.response([])] * 3 + [TimeoutError]
        status, output = self.report()
        self.assertEqual(status, 1, output)
        self.assertNotIn("Cash Overnight: True", output)
        self.assertEqual(self.journal(), before)
        self.write_state(pending)
        self.assertFalse(self.engine._finalize_closed_trade(
            None, self.engine.load_state(), active, "HALT_FLATTENED", replay_only=True))
