"""Persistent restart cases with a mock SDK. Run through qa_offline.py exit_restart_matrix."""
from datetime import datetime
import json
from pathlib import Path
import unittest
from unittest.mock import patch

import test_exit_rework as fixtures


class RestartMatrix(unittest.TestCase):
    setUp = fixtures.ExitRework.setUp
    response = staticmethod(fixtures.ExitRework.response)
    position = staticmethod(fixtures.ExitRework.position)
    client = fixtures.ExitRework.client
    row = fixtures.ExitRework.row
    book = fixtures.ExitRework.book
    opening_orders = fixtures.ExitRework.opening_orders
    exit_submit = fixtures.ExitRework.exit_submit
    active = fixtures.ExitRework.active

    def initialize(self, journal):
        eng = self.engine
        stage = eng.EXIT_LEDGER.parent
        self.state_path = stage / "state.json"
        self.journal_path = Path(eng.JOURNAL_FILE)
        self.state_path.unlink(missing_ok=True)
        self.journal_path.unlink(missing_ok=True)
        eng.EXIT_LEDGER.unlink(missing_ok=True)
        self.sdk.reset_mock(side_effect=True, return_value=True)
        self.sdk.MARKET = "MARKET"
        self.sdk.SL = "SL"
        self.active_trade = self.active(flatten_pending=True, flatten_reason="STOP_LOSS_HIT")
        if journal == "earlier_verified":
            earlier = self.active(order_id="previous", sl_order_id="previous-stop",
                                  entry_time="2026-11-23T09:30:00+05:30")
            self.book({
                "previous": self.row("previous", "BUY", "TRADED", 5, 5, 100),
                "previous-stop": self.row("previous-stop", "SELL", "TRADED", 5, 5, 90),
            })
            self.set_position(0)
            state = {"active_trade": earlier}
            self.assertTrue(eng._finalize_closed_trade(self.client(), state, earlier, "STOP_LOSS_HIT"))
            self.assertIn("exit_proof", self.journal()[0])
        elif journal == "current_legacy":
            self.journal_path.write_text(json.dumps([dict(
                self.active_trade, trade_no=1, exit_price=1, gross_pnl=-495,
                net_pnl=-550, reconciled_with_broker=True,
            )]))
        elif journal == "same_entry_id_legacy":
            self.journal_path.write_text(json.dumps([dict(
                self.active_trade, entry_time="2026-11-23T09:30:00+05:30", trade_no=1,
                net_pnl=-550, reconciled_with_broker=True,
            )]))
        self.write_state({"status": "ALIVE", "active_trade": self.active_trade,
                          "daily_date": "2026-11-23", "daily_consecutive_losses": 1,
                          "losses": 7, "wins": 4, "total_trades": 11,
                          "realized_pnl_total": 400})
        self.rows = {"entry": self.row("entry", "BUY", "TRADED", 5, 5, 100,
                                      createTime="2026-11-23 10:00:00")}
        self.book(self.rows)
        self.set_position(5)
        self.sdk.reset_mock()

    def configure_persistence(self):
        eng = self.engine
        stage = eng.EXIT_LEDGER.parent
        self.enterContext(patch.object(eng, "STATE_FILE", str(stage / "state.json")))
        self.enterContext(patch.object(eng, "_STATE_LOCK_FILE", str(stage / "state.lock")))
        clock = self.enterContext(patch.object(eng, "datetime", wraps=datetime))
        clock.now.return_value = datetime(2026, 11, 23, 10, 10, tzinfo=eng.IST)

    def write_state(self, state):
        self.state_path.write_text(json.dumps(state))

    def state(self):
        return json.loads(self.state_path.read_text())

    def journal(self):
        return json.loads(self.journal_path.read_text()) if self.journal_path.exists() else []

    def set_position(self, net):
        self.sdk.get_positions.return_value = (
            {"status": "failure"} if net is None else self.response([self.position(net)] if net else []))

    def restart(self):
        # Reload the actual state file and construct a new client on every tick.
        return self.engine._cmd_tick_inner(policy=self.engine.RolePolicy(trade=True))

    def assert_pending(self, before):
        state = self.state()
        self.assertTrue(state["active_trade"]["flatten_pending"])
        self.assertEqual(state["active_trade"]["order_id"], "entry")
        self.assertEqual((state["status"], state["losses"], state["daily_consecutive_losses"],
                          state["total_trades"], state["realized_pnl_total"], state["daily_halted"]),
                         ("ALIVE", 7, 1, 11, 400, False))
        self.assertEqual(self.journal(), before)

    def assert_writes(self, quantities):
        self.assertEqual([call.kwargs["quantity"] for call in self.sdk.place_order.call_args_list], quantities)
        self.sdk.cancel_order.assert_not_called()
        self.sdk.modify_order.assert_not_called()

    def assert_closed(self, old, net):
        state = self.state()
        self.assertIsNone(state["active_trade"])
        self.assertEqual((state["status"], state["losses"], state["wins"], state["total_trades"],
                          state["daily_consecutive_losses"], state["daily_halted"],
                          state["realized_pnl_total"]),
                         ("ALIVE", 8, 4, 12, 2, True, 400 + net))
        current = self.journal()[-1]
        self.assertEqual(current["order_id"], "entry")
        self.assertEqual(current["net_pnl"], net)
        self.assertEqual(current["exit_proof"]["trade"][0:4],
                         ["entry", "23", "NSE_EQ", self.active_trade["entry_time"]])
        if old and old[0]["entry_time"] == self.active_trade["entry_time"]:
            self.assertEqual(len(self.journal()), 1)
            self.assertEqual(current["trade_no"], old[0]["trade_no"])
        else:
            self.assertEqual(self.journal(), [*old, current])
        return current

    def test_unresolved_intents_across_restart(self):
        self.configure_persistence()
        for journal in ("absent", "current_legacy", "earlier_verified", "same_entry_id_legacy"):
            for intent in ("working", "missing_id_in_book", "lost_ack"):
                for net in (5, 3, 0, None):
                    with self.subTest(journal=journal, intent=intent, net=net):
                        self.initialize(journal)
                        old = self.journal()
                        self.exit_submit(self.rows, iter([("PENDING", 0, 5)]))
                        if intent == "lost_ack":
                            self.sdk.place_order.side_effect = TimeoutError
                        self.assertEqual(self.restart(), 1)
                        self.assert_pending(old)
                        if intent == "missing_id_in_book":
                            del self.rows["2"]
                        elif intent == "working" and net == 3:
                            self.rows["2"].update(orderStatus="PART_TRADED", filledQty=2)
                        self.set_position(net)
                        for _ in range(2):
                            reads = self.sdk.get_positions.call_count
                            self.assertEqual(self.restart(), 1)
                            self.assertGreater(self.sdk.get_positions.call_count, reads)
                            self.assert_pending(old)
                            self.assert_writes([5])
                            ledger = json.loads(self.engine.EXIT_LEDGER.read_text())
                            pending, = ledger["cycle"]["pending_by_key"].values()
                            self.assertEqual(pending["oid"], None if intent == "lost_ack" else "2")
                            self.assertEqual(pending["quantity"], 5)

    def test_exact_proof_with_newer_unresolved_intent(self):
        self.configure_persistence()
        eng = self.engine
        for intent in ("working", "missing_id_in_book", "lost_ack"):
            for net in (5, 3, 0, None):
                with self.subTest(intent=intent, net=net):
                    self.initialize("absent")
                    pending = self.state()
                    self.exit_submit(self.rows, iter([("TRADED", 5, 0)]))
                    self.assertEqual(self.restart(), 0)
                    verified = self.journal()
                    self.write_state(pending)
                    # Seed an already-issued newer intent, then exercise only restart recovery.
                    reply = self.response({"orderId": "later"}) if intent != "lost_ack" else {"status": "failure"}
                    self.sdk.place_order.side_effect = None
                    self.sdk.place_order.return_value = reply
                    with eng.exit_lock(eng.EXIT_LEDGER) as ledger:
                        coordinator = eng._exit_coordinator(self.client())
                        cycle = coordinator.start(ledger, flatten=True)
                        key = ("23", "NSE_EQ", "INTRADAY")
                        key_id = coordinator.key_id(key)
                        cycle["pending_by_key"] = {}
                        coordinator.submit(ledger, cycle, key, 5, 5, key_id=key_id)
                    if intent == "working":
                        self.rows["later"] = self.row("later", "SELL", "PART_TRADED" if net == 3 else "PENDING",
                                                      5, 2 if net == 3 else 0)
                    self.set_position(net)
                    self.sdk.reset_mock()
                    for _ in range(2):
                        reads = self.sdk.get_positions.call_count
                        self.assertEqual(self.restart(), 1)
                        self.assertGreater(self.sdk.get_positions.call_count, reads)
                        self.assert_pending(verified)
                        self.assert_writes([])
                        ledger = json.loads(eng.EXIT_LEDGER.read_text())
                        self.assertIn(key_id, ledger["cycle"]["pending_by_key"])

    def test_lost_ack_requires_matching_terminal_order_even_when_flat(self):
        self.configure_persistence()
        for journal in ("absent", "current_legacy", "earlier_verified"):
            with self.subTest(journal=journal):
                self.initialize(journal)
                old = self.journal()
                self.sdk.place_order.side_effect = TimeoutError
                self.assertEqual(self.restart(), 1)
                tag = self.sdk.place_order.call_args.kwargs["tag"]
                self.rows["late"] = self.row("late", "SELL", "TRADED", 5, 5,
                                             createTime="2026-11-23 10:02:00", correlationId="different")
                self.set_position(0)
                self.assertEqual(self.restart(), 1)
                self.assert_pending(old)
                self.assert_writes([5])
                self.rows["late"]["correlationId"] = tag
                self.assertEqual(self.restart(), 0)
                closed = self.assert_closed(old, -50)
                self.assertEqual([item["id"] for item in closed["exit_proof"]["exits"]], ["late"])
                self.assert_writes([5])

    def test_fresh_residual_after_terminal_exit_only(self):
        self.configure_persistence()
        for journal in ("absent", "current_legacy", "earlier_verified"):
            for terminal in ("CANCELLED", "REJECTED"):
                for broker in ("stale_long", "flat", "unavailable", "detail_conflict", "residual"):
                    with self.subTest(journal=journal, terminal=terminal, broker=broker):
                        self.initialize(journal)
                        old = self.journal()
                        if terminal == "REJECTED":
                            # The partial was executed by the preceding stop. A rejected order itself
                            # cannot contain a fill.
                            self.active_trade["sl_order_id"] = "stop"
                            state = self.state()
                            state["active_trade"] = self.active_trade
                            self.write_state(state)
                            self.rows["stop"] = self.row("stop", "SELL", "CANCELLED", 5, 2, 95)
                            self.set_position(3)
                        self.exit_submit(self.rows, iter([("PENDING", 0, 5 if terminal == "CANCELLED" else 3),
                                                         ("TRADED", 3, 0)]))
                        self.assertEqual(self.restart(), 1)
                        exit_id = "2" if terminal == "CANCELLED" else "3"
                        exit_row = self.rows[exit_id]
                        exit_row.update(orderStatus=terminal, filledQty=2 if terminal == "CANCELLED" else 0,
                                        averageTradedPrice=95)
                        self.set_position({"stale_long": 5, "flat": 0, "unavailable": None,
                                           "detail_conflict": 3, "residual": 3}[broker])
                        initial = 5 if terminal == "CANCELLED" else 3
                        if broker != "residual":
                            detail = self.sdk.get_order_by_id.side_effect
                            if broker == "detail_conflict":
                                self.sdk.get_order_by_id.side_effect = lambda oid: (
                                    self.response(dict(exit_row, quantity=10)) if oid == exit_id else detail(oid))
                            for _ in range(2):
                                reads = self.sdk.get_order_list.call_count
                                self.assertEqual(self.restart(), 1)
                                self.assertGreater(self.sdk.get_order_list.call_count, reads)
                                self.assert_pending(old)
                                self.assert_writes([initial])
                            self.sdk.get_order_by_id.side_effect = detail
                            self.set_position(3)
                        self.assertEqual(self.restart(), 0)
                        closed = self.assert_closed(old, -62)
                        self.assertEqual(len(closed["exit_proof"]["exits"]), 2)
                        self.assert_writes([initial, 3])

    def test_persistence_and_zero_read_replay(self):
        self.configure_persistence()
        eng = self.engine
        for journal in ("absent", "current_legacy", "earlier_verified", "same_entry_id_legacy"):
            for cut in ("none", "journal", "state"):
                with self.subTest(journal=journal, cut=cut):
                    self.initialize(journal)
                    old = self.journal()
                    self.exit_submit(self.rows, iter([("TRADED", 5, 0)]))
                    persist = eng.persist_exit_data

                    def interrupted(path, data):
                        if (cut == "journal" and path == eng.JOURNAL_FILE or
                                cut == "state" and path == eng.STATE_FILE and data["active_trade"] is None):
                            raise OSError("interrupted persistence")
                        persist(path, data)

                    with patch.object(eng, "persist_exit_data", side_effect=interrupted):
                        rc = self.restart()
                    self.assertEqual(rc, int(cut != "none"))
                    self.assert_writes([5])
                    if cut == "journal":
                        self.assert_pending(old)
                        self.assertEqual(self.restart(), 0)
                    elif cut == "state":
                        self.assert_pending(self.journal())
                        self.sdk.reset_mock()
                        with patch.object(eng, "save_state", side_effect=OSError):
                            self.assertEqual(self.restart(), 1)
                        self.assertEqual(self.sdk.mock_calls, [])
                        with patch.object(eng, "SurvivorDhanClient", side_effect=eng.CachedTokenUnavailable):
                            self.assertEqual(self.restart(), 0)
                        self.assertEqual(self.sdk.mock_calls, [])
                    closed = self.assert_closed(old, -50)
                    self.assertEqual([item["id"] for item in closed["exit_proof"]["exits"]], ["2"])
                    applied = self.state()
                    self.sdk.reset_mock()
                    # Replayed active payload with already-applied risk must only clear the marker.
                    self.write_state(dict(applied, active_trade=self.active_trade))
                    self.assertEqual(self.restart(), 0)
                    stable = self.state()
                    for _ in range(2):
                        state = eng.load_state(read_only=True)
                        self.assertTrue(eng._finalize_closed_trade(
                            self.client(), state, self.active_trade, "STOP_LOSS_HIT"))
                    self.assertEqual(self.sdk.mock_calls, [])
                    self.assertEqual(self.state(), stable)
                    self.assert_closed(old, -50)
                    self.assertEqual(self.journal()[-1], closed)

    def test_newer_contradiction_disqualifies_durable_replay(self):
        self.configure_persistence()
        for observation in ("long", "working", "changed_execution"):
            with self.subTest(observation=observation):
                self.initialize("current_legacy")
                eng = self.engine
                self.exit_submit(self.rows, iter([("TRADED", 5, 0)]))
                pending = self.state()
                self.assertEqual(self.restart(), 0)
                verified = self.journal()
                self.write_state(pending)
                # A fresh broker read after journal commit but before risk recovery
                # must prevent subsequent zero-read replay, including after process restart.
                if observation == "long":
                    self.set_position(5)
                elif observation == "working":
                    self.rows["2"].update(orderStatus="PART_TRADED", filledQty=2)
                else:
                    self.rows["2"]["averageTradedPrice"] = 85
                with eng.exit_lock(eng.EXIT_LEDGER) as ledger:
                    self.assertIsNone(eng._exit_coordinator(self.client()).closure(ledger))
                for _ in range(2):
                    reads = self.sdk.get_order_list.call_count
                    self.assertEqual(self.restart(), 1)
                    self.assertGreater(self.sdk.get_order_list.call_count, reads)
                    self.assert_pending(verified)
                    self.assert_writes([5])
