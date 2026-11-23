"""Run with the frozen socket-denied qa_offline.py emergency_recovery mode."""
from datetime import datetime
import json
import multiprocessing
import os
from pathlib import Path
import runpy
import sys
import unittest
from unittest.mock import patch

import test_verified_exits as fixtures
from test_write_safety import FLAG


class EmergencyRecovery(unittest.TestCase):
    response = staticmethod(fixtures.VerifiedExits.response)
    position = staticmethod(fixtures.VerifiedExits.position)
    row = fixtures.VerifiedExits.row
    book = fixtures.VerifiedExits.book
    opening_orders = fixtures.VerifiedExits.opening_orders
    active = fixtures.VerifiedExits.active

    def setUp(self):
        fixtures.VerifiedExits.setUp(self)
        self.stage = self.engine.EXIT_LEDGER.parent
        self.script = self.stage / "survivor_engine.py"
        self.script.write_bytes(Path(self.engine.__file__).read_bytes())
        self.state = self.stage / "survivor_state.json"
        self.journal = self.stage / "survivor_trades.json"
        for name, path in {
            "BASE_DIR": self.stage, "STATE_FILE": self.state, "JOURNAL_FILE": self.journal,
            "EXIT_LEDGER": self.stage / "survivor_exits.json",
            "_STATE_LOCK_FILE": self.stage / ".survivor_state.flock",
        }.items():
            self.enterContext(patch.object(self.engine, name, path))
        self.state.write_text(json.dumps(dict(status="ALIVE", mortality_armed=True,
                                             active_trade=self.active(quantity=65))))
        self.rows = {"entry": self.row("entry", "BUY", "TRADED", 65, 65, 100,
                                       createTime="2026-11-23 10:00:00")}
        self.book(self.rows)
        self.sdk.get_positions.return_value = self.response([self.position(65)])
        self.sdk.get_fund_limits.return_value = self.response({"availabelBalance": 1000})
        self.exit_status = "TRADED"
        self.sdk.place_order.side_effect = self.submit

    def submit(self, **order):
        try:
            state = self.read_state()
        except (UnicodeError, ValueError):
            incident = json.loads(self.state.with_suffix(".json.recovery").read_text())
        else:
            incident = state["emergency_recovery"]
        self.assertEqual(incident["status"], "pending")
        ledger = json.loads(Path(self.engine.EXIT_LEDGER).read_text())
        intent, = ledger["cycle"]["pending_by_key"].values()
        self.assertEqual((intent["tag"], intent["quantity"]), (order["tag"], 65))
        self.assertEqual((order["transaction_type"], order["quantity"]), ("SELL", 65))
        oid = str(len(self.rows) + 1)
        filled = 65 if self.exit_status == "TRADED" else 0
        self.rows[oid] = self.row(oid, "SELL", self.exit_status, 65, filled, 90,
                                  correlationId=order["tag"], createTime="2026-11-23 10:02:00")
        self.sdk.get_positions.return_value = self.response([self.position(65)] if not filled else [])
        return self.response({"orderId": oid})

    def dispatch(self, action):
        now = datetime(2026, 11, 23, 10, 10, tzinfo=self.engine.IST)
        with (patch.object(sys, "argv", [str(self.script), action, "--live"]),
              patch.dict(os.environ, {FLAG: "1"}),
              patch("datetime.datetime", wraps=datetime) as clock):
            clock.now.return_value = now
            try:
                runpy.run_path(str(self.script), run_name="__main__")
            except SystemExit as exc:
                return exc.code
        return 0

    def read_state(self):
        return json.loads(self.state.read_text())

    def test_death_floor_rejection_keeps_long_and_pending_incident(self):
        self.exit_status = "REJECTED"
        self.assertEqual(self.dispatch("tick"), 1)
        state = self.read_state()
        self.assertEqual(state["status"], "DEAD_COMA")
        self.assertEqual(state["active_trade"]["quantity"], 65)
        self.assertEqual(state["emergency_recovery"]["status"], "pending")
        self.assertFalse(self.journal.exists())
        self.assertGreater(self.sdk.place_order.call_count, 0)

    def test_manual_flatten_closes_full_65_and_accounts_once(self):
        self.assertEqual(self.dispatch("flatten"), 0)
        self.assertEqual(self.dispatch("flatten"), 0)
        self.assertIsNone(self.read_state()["active_trade"])
        state = self.read_state()
        self.assertEqual((state["total_trades"], state["losses"], state["realized_pnl_total"]),
                         (1, 1, -705.0))
        close, = json.loads(self.journal.read_text())
        self.assertEqual(close["quantity"], 65)
        self.sdk.place_order.assert_called_once()

    def test_lock_none_cannot_save_or_write_broker(self):
        before = self.state.read_bytes()
        with patch.object(self.engine, "_acquire_state_lock", return_value=None):
            with self.assertRaises(OSError):
                self.engine.save_state({"status": "DEAD_COMA"})
            self.assertEqual(self.engine.cmd_tick(policy=self.engine.RolePolicy(trade=True)), 1)
            self.assertEqual(self.engine.cmd_flatten(self.engine.RolePolicy(flatten=True)), 1)
        self.assertEqual(self.state.read_bytes(), before)
        self.sdk.place_order.assert_not_called()
        self.sdk.cancel_order.assert_not_called()

    def test_corrupt_bytes_are_preserved_and_manual_restart_uses_broker(self):
        original = b'{"active_trade":\xff'
        self.state.write_bytes(original)
        with self.assertRaises(OSError):
            self.engine.load_state()
        evidence, = self.stage.glob("survivor_state.json.corrupt.*")
        self.assertEqual(evidence.read_bytes(), original)
        self.assertEqual(self.dispatch("tick"), 1)
        self.sdk.place_order.assert_not_called()
        self.assertEqual(self.dispatch("flatten"), 1)
        self.assertEqual(self.dispatch("flatten"), 1)
        self.sdk.place_order.assert_called_once()
        self.assertEqual(self.state.read_bytes(), original)
        self.assertEqual(evidence.read_bytes(), original)
        self.assertFalse(self.journal.exists())

    def test_stale_load_mutate_save_cannot_replace_newer_state(self):
        first, stale = self.engine.load_state(), self.engine.load_state()
        first["daily_halted"] = True
        self.engine.save_state(first)
        stale["available_cash"] = 65
        with self.assertRaises(OSError):
            self.engine.save_state(stale)
        self.assertTrue(self.read_state()["daily_halted"])

    def test_old_live_lock_survives_and_dead_owner_is_reclaimed(self):
        context = multiprocessing.get_context("fork")
        ready, release = context.Event(), context.Event()

        def own():
            fd = self.engine._acquire_state_lock()
            if fd is None:
                return
            ready.set()
            release.wait(5)
            os._exit(0)

        owner = context.Process(target=own)
        owner.start()
        try:
            self.assertTrue(ready.wait(5))
            path = self.engine._STATE_LOCK_FILE
            os.utime(path, (1, 1))
            inode = path.stat().st_ino
            self.assertIsNone(self.engine._acquire_state_lock(timeout=0.01))
            self.assertEqual(path.stat().st_ino, inode)
        finally:
            release.set()
            owner.join(5)
            if owner.is_alive():
                owner.kill()
                owner.join()
        self.assertEqual(owner.exitcode, 0)
        fd = self.engine._acquire_state_lock()
        self.assertIsNotNone(fd)
        self.engine._release_state_lock(fd)
        self.assertEqual(path.stat().st_ino, inode)

    def test_crash_after_broker_acceptance_recovers_original_correlation(self):
        class ProcessInterrupted(BaseException):
            pass

        def accept_then_crash(**order):
            self.submit(**order)
            raise ProcessInterrupted

        self.sdk.place_order.side_effect = accept_then_crash
        with self.assertRaises(ProcessInterrupted):
            self.dispatch("flatten")
        self.assertEqual(self.read_state()["emergency_recovery"]["status"], "pending")
        self.assertFalse(self.journal.exists())
        self.assertEqual(self.dispatch("tick"), 0)
        self.assertEqual(self.read_state()["total_trades"], 1)
        self.sdk.place_order.assert_called_once()

    def test_crash_after_journal_rechecks_broker_before_applying_risk(self):
        class ProcessInterrupted(BaseException):
            pass

        replace = os.replace

        def cut_state_publish(source, target):
            if Path(target) == self.state and self.journal.exists():
                raise ProcessInterrupted
            return replace(source, target)

        with patch("os.replace", side_effect=cut_state_publish):
            with self.assertRaises(ProcessInterrupted):
                self.dispatch("tick")
        pending = self.read_state()
        self.assertEqual(pending["total_trades"], 0)
        self.assertEqual(pending["active_trade"]["quantity"], 65)
        journal_inode = self.journal.stat().st_ino
        incident_id = pending["emergency_recovery"]["id"]
        self.sdk.get_positions.return_value = self.response([self.position(65)])
        self.assertEqual(self.dispatch("tick"), 1)
        self.assertEqual(self.read_state(), pending)
        self.sdk.get_positions.return_value = self.response([])
        self.assertEqual(self.dispatch("flatten"), 0)
        self.assertEqual(self.dispatch("tick"), 0)
        self.assertEqual(self.read_state()["total_trades"], 1)
        self.assertEqual(self.read_state()["emergency_recovery"]["id"], incident_id)
        self.assertEqual(self.journal.stat().st_ino, journal_inode)
        self.sdk.place_order.assert_called_once()

    def test_process_owner_excludes_cli_tick_and_flatten(self):
        context = multiprocessing.get_context("fork")
        ready, release = context.Event(), context.Event()

        def own():
            def pause_at_broker_read():
                ready.set()
                release.wait(5)
                raise SystemExit(0)

            self.sdk.get_positions.side_effect = pause_at_broker_read
            if self.dispatch("tick"):
                raise SystemExit(1)

        owner = context.Process(target=own)
        owner.start()
        try:
            self.assertTrue(ready.wait(5))
            before = self.state.read_bytes()
            self.assertEqual(self.dispatch("tick"), 1)
            self.assertEqual(self.dispatch("flatten"), 1)
            self.assertEqual(self.state.read_bytes(), before)
            self.sdk.place_order.assert_not_called()
            self.sdk.cancel_order.assert_not_called()
        finally:
            release.set()
            owner.join(5)
            if owner.is_alive():
                owner.kill()
                owner.join()
        self.assertEqual(owner.exitcode, 0)
        self.assertEqual(self.dispatch("flatten"), 0)
        self.sdk.place_order.assert_called_once()

    def test_opaque_legacy_lock_is_never_evicted_by_age(self):
        legacy = self.stage / ".survivor_state.lock"
        legacy.touch()
        os.utime(legacy, (1, 1))
        self.assertEqual(self.dispatch("flatten"), 1)
        self.assertTrue(legacy.exists())
        self.sdk.place_order.assert_not_called()


    def test_incident_publication_failure_prevents_broker_mutation(self):
        before = self.state.read_bytes()
        replace = os.replace

        def fail_incident_publish(source, target):
            if Path(target) == self.state:
                raise OSError("mock filesystem failure")
            return replace(source, target)

        with patch("os.replace", side_effect=fail_incident_publish):
            self.assertEqual(self.dispatch("flatten"), 1)
            self.assertEqual(self.dispatch("tick"), 1)
        self.assertEqual(self.state.read_bytes(), before)
        self.assertFalse(Path(self.engine.EXIT_LEDGER).exists())
        self.sdk.place_order.assert_not_called()
        self.sdk.cancel_order.assert_not_called()

    def test_missing_primary_after_quarantine_stays_fenced(self):
        self.state.write_bytes(b"[]")
        with self.assertRaises(OSError):
            self.engine.load_state()
        self.state.unlink()
        with self.assertRaises(OSError):
            self.engine.load_state(read_only=True)
        with self.assertRaises(OSError):
            self.engine.save_state({"status": "AWAITING_FUNDING"})
        self.assertFalse(self.state.exists())
