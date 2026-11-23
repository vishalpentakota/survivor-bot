"""Round 5 replay fences, exercised through the offline QA runner."""
import json
import unittest
from unittest.mock import patch

import test_exit_restart_matrix as fixtures


class ExitInvalidation(fixtures.RestartMatrix):
    def prepare(self):
        self.initialize("current_legacy")
        pending = self.engine.load_state(read_only=True)
        self.exit_submit(self.rows, iter([("TRADED", 5, 0)]))
        self.assertEqual(self.restart(), 0)
        self.write_state(pending)
        return self.journal()

    def ledger(self):
        return json.loads(self.engine.EXIT_LEDGER.read_text())

    def closure(self, *, settle=True, write_failure=None):
        eng = self.engine
        coordinator = eng._exit_coordinator(self.client())
        save = coordinator.save

        def persist(ledger):
            if ledger.get("replay_check") == write_failure:
                raise OSError("simulated ledger write failure")
            save(ledger)

        with eng.exit_lock(eng.EXIT_LEDGER) as ledger:
            if write_failure is None:
                return coordinator.closure(ledger, settle=settle)
            with patch.object(coordinator, "save", side_effect=persist):
                return coordinator.closure(ledger, settle=settle)

    def add_buy(self):
        self.rows["later"] = self.row("later", "BUY", "TRADED", 5, 5, 100,
                                      createTime="2026-11-23 10:12:00")

    def configure_refusal(self, case):
        if case in ("flatten_cycle", "settle_disabled", "no_pending", "settle_refused"):
            # Keep the older proof alongside a newer unresolved cycle to test
            # both replay gates independently.
            eng = self.engine
            with eng.exit_lock(eng.EXIT_LEDGER) as ledger:
                coordinator = eng._exit_coordinator(self.client())
                saved = ledger["replay_executions"]
                cycle = coordinator.start(ledger, flatten=case == "flatten_cycle")
                if case == "settle_refused":
                    cycle["pending"] = {"oid": "missing"}
                ledger["replay_executions"] = saved
                coordinator.save(ledger)
        elif case == "nonzero_position":
            self.set_position(5)
        elif case == "nonzero_final_position":
            self.sdk.get_positions.side_effect = [
                self.response([]), self.response([self.position(5)]),
            ]
        elif case == "working_order":
            self.rows["later"] = self.row("later", "BUY", "PENDING", 5, 0)
        elif case == "changed_saved_fill":
            self.rows["2"]["averageTradedPrice"] = 85
        elif case == "changed_baseline_fill":
            eng = self.engine
            self.rows["earlier"] = self.row("earlier", "BUY", "TRADED", 5, 5)
            with eng.exit_lock(eng.EXIT_LEDGER) as ledger:
                ledger["flat_book"]["earlier"] = 4
                eng._exit_coordinator(self.client()).save(ledger)
        elif case in ("nonzero_execution_net", "unavailable_net_detail"):
            self.add_buy()
            if case == "unavailable_net_detail":
                detail = self.sdk.get_order_by_id.side_effect
                self.sdk.get_order_by_id.side_effect = lambda oid: (
                    {"status": "failure"} if oid == "later" else detail(oid))
        elif case == "missing_saved_order":
            del self.rows["2"]
        elif case == "disagreeing_saved_detail":
            detail = self.sdk.get_order_by_id.side_effect
            self.sdk.get_order_by_id.side_effect = lambda oid: (
                self.response(dict(self.rows["2"], averageTradedPrice=85))
                if oid == "2" else detail(oid))
        elif case == "disagreeing_baseline_detail":
            self.configure_refusal("changed_baseline_fill")
            detail = self.sdk.get_order_by_id.side_effect
            self.sdk.get_order_by_id.side_effect = lambda oid: (
                self.response(dict(self.rows["earlier"], averageTradedPrice=85))
                if oid == "earlier" else detail(oid))
        elif case == "unavailable_positions":
            self.set_position(None)
        elif case == "unavailable_orders":
            self.sdk.get_order_list.side_effect = None
            self.sdk.get_order_list.return_value = {"status": "failure"}
        elif case in ("moving_snapshot", "moving_final_snapshot", "working_final_order"):
            if case == "working_final_order":
                self.rows["later"] = self.row("later", "BUY", "PENDING", 5, 0)
            else:
                self.add_buy()
            book = self.response([row for oid, row in self.rows.items() if oid != "later"])
            later_book = self.response(list(self.rows.values()))
            self.sdk.get_order_list.side_effect = (
                [book, later_book] if case == "moving_snapshot" else [book, book, later_book, later_book])

        elif case == "unknown_status":
            self.rows["2"]["orderStatus"] = "UNKNOWN"
        elif case == "malformed_order":
            del self.rows["2"]["filledQty"]
        else:
            self.fail(f"unknown refusal: {case}")

    def assert_unresolved_restart(self, journal, pending):
        self.assertNotEqual(self.restart(), 0)
        self.assert_pending(journal)
        current = self.state()
        current.pop("last_heartbeat")
        expected = pending.copy()
        expected.pop("last_heartbeat")
        self.assertEqual(current, expected)
        self.assert_writes([5])

    def test_closure_refusal_restart_matrix(self):
        self.configure_persistence()
        cases = {
            "contradicted": (
                "nonzero_position", "nonzero_final_position", "working_order", "changed_saved_fill",
                "changed_baseline_fill", "nonzero_execution_net", "working_final_order",
            ),
            "verification_required": (
                "flatten_cycle", "settle_disabled", "no_pending", "settle_refused",
                "missing_saved_order", "disagreeing_saved_detail",
                "disagreeing_baseline_detail", "unavailable_positions", "unavailable_orders",
                "moving_snapshot", "moving_final_snapshot", "unknown_status",
                "malformed_order", "unavailable_net_detail",
            ),
        }
        exceptions = {
            "unavailable_positions": ValueError, "unavailable_orders": ValueError,
            "moving_snapshot": ValueError,
            "unknown_status": ValueError, "malformed_order": KeyError,
            "unavailable_net_detail": ValueError,
        }
        for expected, refusals in cases.items():
            for case in refusals:
                with self.subTest(case=case):
                    journal = self.prepare()
                    proof = self.ledger()["replay_executions"]
                    self.configure_refusal(case)
                    pending = self.state()
                    eng = self.engine
                    with patch.object(eng, "save_state", side_effect=AssertionError) as save:
                        if case in exceptions:
                            with self.assertRaises(exceptions[case]):
                                self.closure()
                        else:
                            self.assertIsNone(self.closure(settle=case != "settle_disabled"))
                        save.assert_not_called()
                    self.assertEqual(self.ledger()["replay_check"], expected)
                    self.assertEqual(self.ledger()["replay_executions"], proof)
                    if case.startswith("moving_") or case == "working_final_order":
                        self.book(self.rows)
                    if case == "nonzero_final_position":
                        self.sdk.get_positions.side_effect = None
                        self.set_position(5)
                    self.assert_unresolved_restart(journal, pending)
                    if expected == "contradicted":
                        self.set_position(None)
                        self.assert_unresolved_restart(journal, pending)
                        self.assertEqual(self.ledger()["replay_check"], expected)

    def test_fence_write_failure_before_read_or_after_contradiction(self):
        self.configure_persistence()
        for failure in ("verification_required", "contradicted"):
            with self.subTest(failure=failure):
                journal = self.prepare()
                pending = self.state()
                self.add_buy()
                reads = self.sdk.get_order_list.call_count
                with self.assertRaises(OSError):
                    self.closure(write_failure=failure)
                if failure == "verification_required":
                    self.assertEqual(self.sdk.get_order_list.call_count, reads)
                    self.assertNotIn("replay_check", self.ledger())
                else:
                    self.assertEqual(self.ledger()["replay_check"], "verification_required")
                    self.assert_unresolved_restart(journal, pending)

    def test_interrupted_read_leaves_fence(self):
        self.configure_persistence()
        journal = self.prepare()
        pending = self.state()
        self.add_buy()
        with patch.object(self.sdk, "get_positions", side_effect=SystemExit):
            with self.assertRaises(SystemExit):
                self.closure()
        self.assertEqual(self.ledger()["replay_check"], "verification_required")
        with patch.object(self.engine, "save_state", side_effect=OSError):
            self.assert_unresolved_restart(journal, pending)
        self.assert_unresolved_restart(journal, pending)

    def test_fresh_agreement_clears_fence_and_applies_once(self):
        self.configure_persistence()
        for first in ("contradiction", "unavailable"):
            with self.subTest(first=first):
                journal = self.prepare()
                pending = self.state()
                if first == "contradiction":
                    self.add_buy()
                    self.assertIsNone(self.closure())
                    del self.rows["later"]
                else:
                    self.set_position(None)
                    with self.assertRaises(ValueError):
                        self.closure()
                    self.set_position(0)
                self.assertIsNotNone(self.closure())
                self.assertIn("replay_check", self.ledger())
                self.sdk.reset_mock()
                self.assertEqual(self.restart(), 0)
                self.assertGreater(self.sdk.get_positions.call_count, 0)
                self.assertNotIn("replay_check", self.ledger())
                self.assertEqual(self.journal(), journal)
                self.assert_closed(journal, -50)
                applied = self.state()
                self.write_state(dict(applied, active_trade=pending["active_trade"]))
                self.sdk.reset_mock()
                self.assertEqual(self.restart(), 0)
                self.assertEqual(self.sdk.mock_calls, [])
                self.assertEqual(self.state(), applied)

    def test_exact_match_preserves_zero_read_once_only_replay(self):
        self.configure_persistence()
        journal = self.prepare()
        self.assertIsNotNone(self.closure())
        self.assertNotIn("replay_check", self.ledger())
        for attempt in range(2):
            if attempt:
                self.write_state(dict(self.state(), active_trade=self.active_trade))
            self.sdk.reset_mock()
            self.assertEqual(self.restart(), 0)
            self.assertEqual(self.sdk.mock_calls, [])
            self.assert_closed(journal, -50)


# Reuse setup helpers without running the inherited matrix in this mode.
def load_tests(loader, tests, pattern):
    return unittest.TestSuite(ExitInvalidation(name) for name in (
        "test_closure_refusal_restart_matrix",
        "test_fence_write_failure_before_read_or_after_contradiction",
        "test_interrupted_read_leaves_fence",
        "test_fresh_agreement_clears_fence_and_applies_once",
        "test_exact_match_preserves_zero_read_once_only_replay",
    ))
