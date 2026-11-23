"""Reconciliation acceptance using the staged mock broker and real state/outbox."""
from contextlib import redirect_stdout
from io import StringIO
import json
import os
import sys
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
    restart = fixtures.RestartMatrix.restart

    def stage(self):
        self.configure_persistence()
        self.initialize("absent")
        self.write_state(dict(self.state(), active_trade=None, daily_halted=False))
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
        after = self.state()
        for key in ("losses", "daily_consecutive_losses", "total_trades", "realized_pnl_total",
                    "daily_halted", "applied_exit_proofs"):
            self.assertEqual(after.get(key), state.get(key))
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

    def exit_route(self, route, state):
        if route == "MANUAL_EMERGENCY_EXIT":
            with patch.object(sys, "argv", ["survivor_engine.py", "flatten", "--live"]), \
                    patch.dict(os.environ, {"SURVIVOR_LIVE_TRADING": "1"}):
                try:
                    return self.engine.main() or 0
                except SystemExit as exc:
                    return exc.code
        return self.engine._flatten_trade(self.client(), state, state["active_trade"], route)

    def test_net_after_fees_and_late_actual_charges_by_exit_route(self):
        self.configure_persistence()
        self.enterContext(patch.object(self.engine, "BASE_DIR", self.engine.EXIT_LEDGER.parent))
        for route in ("EOD_SQUARE_OFF", "STOP_LOSS_HIT", "HALT_FLATTENED", "MANUAL_EMERGENCY_EXIT"):
            for exit_price, gross, net in ((101, 5, -50), (120, 100, 45)):
                with self.subTest(route=route, exit_price=exit_price):
                    self.initialize("absent")
                    self.sdk.get_fund_limits.return_value = self.response(
                        {"availabelBalance": 23500.18, "utilizedAmount": 0})
                    losing = net < 0
                    stop = route == "STOP_LOSS_HIT"
                    active = self.active(entry_cycle="confirmed-workflow",
                                         sl_order_id="stop" if stop else None)
                    before = dict(self.state(), active_trade=active, daily_halted=False)
                    self.write_state(before)

                    # Even a flat position cannot establish the closing fill.
                    self.set_position(0)
                    self.assertNotEqual(self.exit_route(route, self.engine.load_state()), 0)
                    self.assertEqual(self.journal(), [])
                    for key in ("losses", "wins", "total_trades", "daily_consecutive_losses",
                                "daily_halted", "realized_pnl_total"):
                        self.assertEqual(self.state()[key], before[key])
                    self.sdk.place_order.assert_not_called()

                    exit_id = "stop" if stop else "2"
                    if stop:
                        self.rows[exit_id] = self.row(exit_id, "SELL", "TRADED", 5, 5, exit_price)
                    else:
                        self.set_position(5)
                        self.exit_submit(self.rows, iter([("TRADED", 5, 0)]))
                        submit = self.sdk.place_order.side_effect

                        def fill_exit(**kwargs):
                            response = submit(**kwargs)
                            self.rows[exit_id]["averageTradedPrice"] = exit_price
                            return response

                        self.sdk.place_order.side_effect = fill_exit

                    self.assertEqual(self.exit_route(route, self.engine.load_state()), 0)
                    row, = self.journal()
                    self.assertEqual(row["exit_trigger"], route)
                    self.assertEqual(row["exit_reason"], "EXCHANGE_SL_FILLED" if stop else route)
                    self.assertEqual([item["id"] for item in row["exit_proof"]["exits"]], [exit_id])
                    self.assertEqual((row["gross_pnl"], row["net_pnl"],
                                      row["estimated_roundtrip_charges"]), (gross, net, 55))
                    self.assertEqual((row["pnl_provenance"], row["actual_broker_charges"]),
                                     ("estimated_roundtrip", None))
                    self.assertTrue(row["fills_verified"])
                    self.assertFalse(row["reconciled_with_broker"])
                    scored = self.state()
                    self.assertIsNone(scored["active_trade"])
                    self.assertEqual((scored["losses"], scored["wins"], scored["total_trades"],
                                      scored["daily_consecutive_losses"], scored["daily_halted"],
                                      scored["realized_pnl_total"]),
                                     (7 + losing, 4 + (not losing), 12, 2 if losing else 0,
                                      losing, 400 + net))
                    self.assertEqual(len(scored["applied_exit_proofs"]), 1)
                    self.assertEqual(scored["realized_pnl_total_provenance"],
                                     "includes_estimated_roundtrip")
                    if losing:
                        self.assertIn("2 consecutive losses", scored["daily_halt_reason"])
                    else:
                        self.assertEqual(scored["daily_halt_reason"], "")

                    # Each reconciliation reloads the persisted state and constructs a client.
                    self.sdk.reset_mock()
                    status, output = self.report()
                    self.assertEqual(status, 1, output)
                    self.assertNotIn("Actual broker charges:", output)
                    self.assertEqual(self.state()["actual_broker_charges_total"], 0)
                    self.assertEqual(self.state()["actual_realized_pnl_total"], 0)
                    self.assertEqual(self.journal(), [row])

                    # Actual charges yield +2 on the estimated-loss trade.
                    self.rows["entry"]["brokerageAndStatutoryCharges"] = 1
                    self.rows[exit_id]["brokerageAndStatutoryCharges"] = 2
                    for _ in range(2):
                        status, output = self.report()
                        self.assertEqual(status, 0, output)
                        self.assertIn("Actual broker charges: ₹3.00", output)
                        actual, = self.journal()
                        self.assertTrue(actual["reconciled_with_broker"])
                        self.assertEqual(actual["pnl_provenance"], "broker_actual")
                        self.assertEqual(actual["actual_broker_charges"], 3)
                        self.assertEqual(actual["net_pnl"], gross - 3)
                        for key in ("trade_no", "exit_time", "exit_trigger", "gross_pnl"):
                            self.assertEqual(actual[key], row[key])
                        expected_proof = dict(row["exit_proof"],
                            entry=dict(row["exit_proof"]["entry"], broker_charge=1),
                            exits=[dict(row["exit_proof"]["exits"][0], broker_charge=2)])
                        self.assertEqual(actual["exit_proof"], expected_proof)
                        after = self.state()
                        self.assertEqual(after["actual_broker_charges_total"], 3)
                        self.assertEqual(after["actual_realized_pnl_total"], gross - 3)
                        for key in ("losses", "wins", "total_trades", "daily_consecutive_losses",
                                    "daily_halted", "daily_halt_reason", "applied_exit_proofs",
                                    "realized_pnl_total", "realized_pnl_total_provenance",
                                    "active_trade"):
                            self.assertEqual(after[key], scored[key])
                    self.sdk.place_order.assert_not_called()
                    self.sdk.modify_order.assert_not_called()
                    self.sdk.cancel_order.assert_not_called()

    def test_estimated_net_applies_strategy_risk_without_actual_pnl(self):
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
        self.assertEqual(row["net_pnl"], -50)
        self.assertEqual(after["losses"], before["losses"] + 1)
        self.assertEqual(after["daily_consecutive_losses"], before["daily_consecutive_losses"] + 1)
        self.assertEqual(after["total_trades"], before["total_trades"] + 1)
        self.assertEqual(after["realized_pnl_total"], before["realized_pnl_total"] - 50)
        self.assertEqual(after["realized_pnl_total_provenance"], "includes_estimated_roundtrip")
        self.assertTrue(after["daily_halted"])
        status, output = self.report()
        self.assertEqual(status, 1)
        self.assertIn("Estimated roundtrip charges per trade: ₹55.00", output)
        self.assertNotIn("Actual broker charges:", output)
        self.assertNotIn("Verified net P&L:", output)
        self.assertEqual(self.state()["actual_realized_pnl_total"], 0)

    def close_without_itemized_charges(self, exit_price, *, order_id="entry",
                                       stop_id="stop", entry_time="2026-11-23T10:00:00+05:30"):
        active = self.active(order_id=order_id, sl_order_id=stop_id, entry_time=entry_time,
                             entry_cycle="confirmed-workflow")
        self.book({
            order_id: self.row(order_id, "BUY", "TRADED", 5, 5, 100),
            stop_id: self.row(stop_id, "SELL", "TRADED", 5, 5, exit_price),
        })
        state = dict(self.state(), active_trade=active)
        self.write_state(state)
        self.assertEqual(self.engine._flatten_trade(self.client(), state, active, "STOP_LOSS_HIT"), 0)

    def test_confirmed_stop_without_itemized_charges_halts_after_two_losses(self):
        self.stage()
        before = dict(self.state(), mortality_armed=True)
        self.assertEqual(before["daily_consecutive_losses"], 1)
        self.write_state(before)
        self.close_without_itemized_charges(90)
        row, = self.journal()
        self.assertEqual((row["exit_reason"], row["gross_pnl"], row["net_pnl"]),
                         ("EXCHANGE_SL_FILLED", -50, -105))
        self.assertEqual((row["pnl_provenance"], row["actual_broker_charges"],
                          row["reconciled_with_broker"], row["fills_verified"]),
                         ("estimated_roundtrip", None, False, True))
        after = self.state()
        self.assertEqual((after["losses"], after["daily_consecutive_losses"],
                          after["total_trades"], after["realized_pnl_total"]),
                         (8, 2, 12, 295))
        self.assertIsNone(after["active_trade"])
        self.assertEqual(len(after["applied_exit_proofs"]), 1)
        self.assertTrue(after["daily_halted"])
        self.assertIn("2 consecutive losses", after["daily_halt_reason"])
        self.assertEqual(after["last_sl_direction"], "LONG")
        self.assertEqual(after["last_sl_time"], row["exit_time"])
        self.assertIsNone(after.get("actual_broker_charges_total"))
        self.assertIsNone(after.get("actual_realized_pnl_total"))
        output = StringIO()
        with redirect_stdout(output):
            self.assertIsNone(self.restart())
        self.assertIn("Daily halt active: 2 consecutive losses", output.getvalue())
        restarted = self.state()
        for key in ("losses", "daily_consecutive_losses", "total_trades", "realized_pnl_total",
                    "daily_halted", "daily_halt_reason", "applied_exit_proofs"):
            self.assertEqual(restarted[key], after[key])
        self.assertEqual(self.journal(), [row])
        self.sdk.place_order.assert_not_called()
        self.sdk.modify_order.assert_not_called()
        self.sdk.cancel_order.assert_not_called()

    def test_estimated_gross_loss_triggers_green_floor_after_banked_gain(self):
        self.stage()
        self.close_without_itemized_charges(
            220, order_id="earlier", stop_id="earlier-stop",
            entry_time="2026-11-23T09:30:00+05:30")
        self.assertEqual(self.journal()[0]["net_pnl"], 545)
        self.assertEqual(self.state()["daily_consecutive_losses"], 0)
        self.assertFalse(self.state()["daily_halted"])

        self.close_without_itemized_charges(90)
        after = self.state()
        self.assertEqual([row["net_pnl"] for row in self.journal()], [545, -105])
        self.assertEqual(after["daily_consecutive_losses"], 1)
        self.assertTrue(after["daily_halted"])
        self.assertIn("Green floor", after["daily_halt_reason"])

    def test_estimated_net_triggers_green_target(self):
        self.stage()
        self.close_without_itemized_charges(412)
        row, = self.journal()
        self.assertEqual((row["gross_pnl"], row["net_pnl"], row["pnl_provenance"]),
                         (1560, 1505, "estimated_roundtrip"))
        after = self.state()
        self.assertEqual((after["wins"], after["total_trades"], after["daily_consecutive_losses"]),
                         (5, 12, 0))
        self.assertTrue(after["daily_halted"])
        self.assertIn("Green Target Banked", after["daily_halt_reason"])

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
