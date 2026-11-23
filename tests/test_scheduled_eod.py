"""Socket-denied acceptance tests run by the staging EOD runner."""

from datetime import datetime
import ast
from contextlib import redirect_stdout
import io
import json
import multiprocessing
import os
import sqlite3
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from test_verified_exits import VerifiedExits
from test_write_safety import FLAG, WriteSafetyTests


class ScheduledEod(unittest.TestCase):
    position = staticmethod(VerifiedExits.position)

    def setUp(self):
        WriteSafetyTests.setUp(self)
        script = Path(self.engine.__file__).resolve()
        self.assertEqual(script, Path(__file__).resolve().parents[1] / "app" / "survivor_engine.py")
        self.script = script
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory(dir=script.parents[1])))
        self.clock = multiprocessing.get_context("fork").Value("d", 0)
        self.on_sleep = None
        self.notify = None
        self.configure_cli = None
        self.instant = datetime.fromisoformat("2026-11-23T15:10:00+05:30")
        self.broker = self.sdk.dhanhq.return_value
        self.broker.MARKET = "MARKET"
        self.broker.BUY = "BUY"
        self.broker.SELL = "SELL"
        self.broker.INTRA = "INTRADAY"
        self.broker.get_positions.side_effect = self.positions
        self.broker.get_order_list.side_effect = lambda: VerifiedExits.response(list(self.account()["orders"].values()))
        self.broker.get_order_by_id.side_effect = lambda oid: VerifiedExits.response(self.account()["orders"][oid])
        self.broker.place_order.side_effect = self.sell
        self.broker.cancel_order.side_effect = lambda *a, **k: self.unexpected_write("cancel")
        self.broker.modify_order.side_effect = lambda *a, **k: self.unexpected_write("modify")
        self.timeline = self.directory / "observations"
        self.save_account({"net": 5, "writes": [], "orders": {"buy": self.order("buy", "BUY")}})
        self.broker.get_fund_limits.return_value = VerifiedExits.response({"availabelBalance": 30000})

    def order(self, oid, side, **fields):
        return VerifiedExits.row(self, oid, side, "TRADED", 5, 5, **fields)

    def account(self):
        return json.loads((self.directory / "account").read_text())

    def save_account(self, state):
        temp = self.directory / "account-next"
        temp.write_text(json.dumps(state))
        temp.replace(self.directory / "account")

    def positions(self):
        net = self.account()["net"]
        with self.timeline.open("a") as observations:
            observations.write(f"{net}\n")
        return VerifiedExits.response([VerifiedExits.position(net)] if net else [])

    def unexpected_write(self, method):
        account = self.account()
        account["writes"].append([method])
        self.save_account(account)
        raise AssertionError(f"Unexpected {method}")

    def sell(self, **params):
        account = self.account()
        self.assertEqual((account["net"], params["transaction_type"], params["quantity"]), (5, "SELL", 5))
        account["net"] = 0
        account["writes"].append(["SELL", 5])
        account["orders"]["sell"] = self.order("sell", "SELL", correlationId=params["tag"])
        self.save_account(account)
        return VerifiedExits.response({"orderId": "sell"})

    def dispatch(self, *args):
        from datetime import timedelta
        from types import SimpleNamespace
        tree = ast.parse(self.script.read_bytes())
        main = next(i for i, node in enumerate(tree.body) if isinstance(node, ast.If)
                    and "__main__" in ast.unparse(node.test))
        tree.body.insert(main, ast.parse("configure(globals())").body[0])

        def now(tz=None):
            instant = self.instant + timedelta(seconds=self.clock.value)
            return instant.astimezone(tz) if tz is not None else instant.replace(tzinfo=None)

        def sleep(seconds):
            self.clock.value += seconds
            if self.on_sleep:
                self.on_sleep()

        def configure(scope):
            scope.update(BASE_DIR=str(self.directory), STATE_FILE=self.directory / "state",
                         JOURNAL_FILE=self.directory / "journal", EXIT_LEDGER=self.directory / "exit",
                         _STATE_LOCK_FILE=self.directory / "state.lock",
                         datetime=SimpleNamespace(now=now), FLATTEN_POLL_S=0,
                         time=SimpleNamespace(time=lambda: now(self.instant.tzinfo).timestamp(),
                                              monotonic=lambda: self.clock.value, sleep=sleep),
                         load_env_file=lambda **_: {})
            if self.notify is not None:
                scope["send_telegram"] = self.notify
            if self.configure_cli is not None:
                self.configure_cli(scope)

        with (patch.object(sys, "argv", [str(self.script), *args]),
              patch.dict(os.environ, {FLAG: "1"}), redirect_stdout(io.StringIO()) as output):
            try:
                exec(compile(ast.fix_missing_locations(tree), str(self.script), "exec"),
                     dict(__name__="__main__", __file__=str(self.script), configure=configure))
            except SystemExit as exc:
                return exc.code, output.getvalue()
        return 0, output.getvalue()

    def start(self, args, entered, release, *, pause_at=1):
        context = multiprocessing.get_context("fork")
        result = self.directory / "owner-result"

        def run():
            reads = 0

            def pause():
                nonlocal reads
                reads += 1
                response = self.positions()
                if reads == pause_at:
                    entered.set()
                    if not release.wait(5):
                        raise TimeoutError("test barrier")
                return response

            self.broker.get_positions.side_effect = pause
            result.write_text(json.dumps(self.dispatch(*args)))

        process = context.Process(target=run)
        process.start()

        def stop():
            if process.is_alive():
                process.kill()
            process.join(5)
            process.close()

        self.addCleanup(stop)
        self.assertTrue(entered.wait(5))
        return process, result

    def assert_account(self, net, writes):
        state = self.account()
        self.assertEqual((state["net"], state["writes"]), (net, writes))

    def race(self, owner, loser):
        ctx = multiprocessing.get_context("fork")
        entered, release = ctx.Event(), ctx.Event()
        process, result = self.start(owner, entered, release)
        self.assert_account(5, [])
        self.assertEqual(self.timeline.read_text().splitlines(), ["5"])
        status, output = self.dispatch(*loser)
        self.assertNotEqual(status, 0, output)
        self.sdk.dhanhq.assert_not_called()
        self.assert_account(5, [])
        self.assertEqual(self.timeline.read_text().splitlines(), ["5"])
        release.set()
        process.join(5)
        self.assertEqual(process.exitcode, 0)
        status, output = json.loads(result.read_text())
        self.assertEqual(status, 0, output)
        self.assert_account(0, [["SELL", 5]])
        self.assertEqual(self.timeline.read_text().splitlines()[-1], "0")
        self.clock.value = 300
        status, output = self.dispatch("flatten", "--live", "--scheduled")
        self.assertEqual(status, 0, output)
        self.assert_account(0, [["SELL", 5]])
        self.assertEqual(self.timeline.read_text().splitlines()[-1], "0")

    def test_cutoff_tick_first(self):
        self.race(("tick", "--live"), ("flatten", "--live", "--scheduled"))

    def test_cutoff_scheduler_first(self):
        self.race(("flatten", "--live", "--scheduled"), ("tick", "--live"))

    def incident(self, kind):
        with sqlite3.connect(self.directory / "survivor_eod.sqlite3") as db:
            return db.execute("SELECT resolved, message, acknowledged FROM incidents "
                              "JOIN outbox USING(id) WHERE kind=?", (kind,)).fetchone()

    def test_pre_cutoff_tick_closes_orphan_and_waiter_confirms_flat(self):
        context = multiprocessing.get_context("fork")
        entered, release = context.Event(), context.Event()
        self.clock.value = -0.125
        owner, result = self.start(("tick", "--live"), entered, release, pause_at=2)
        self.assert_account(5, [])
        self.clock.value = 0.0

        def advance():
            if self.clock.value >= 0.125:
                release.set()
                owner.join(5)
                self.assertEqual(owner.exitcode, 0)
                self.assertEqual(json.loads(result.read_text())[0], 0)
                self.assert_account(0, [["SELL", 5]])
                self.on_sleep = None

        self.on_sleep = advance
        status, output = self.dispatch("flatten", "--live", "--scheduled")
        self.assertEqual(status, 0, output)
        self.assertGreaterEqual(self.clock.value, 0.125)
        self.assertLess(self.clock.value, 0.150)
        self.assert_account(0, [["SELL", 5]])
        self.assertEqual(self.timeline.read_text().splitlines()[-1], "0")

    def test_hung_pre_cutoff_owner_deadline_and_restart(self):
        context = multiprocessing.get_context("fork")
        entered, release = context.Event(), context.Event()
        self.clock.value = -0.125
        owner, _ = self.start(("tick", "--live"), entered, release, pause_at=2)
        lock = self.directory / ".survivor_engine.lock"
        inode = lock.stat().st_ino
        os.utime(lock, (1, 1))
        self.assert_account(5, [])
        self.clock.value = 29.975
        status, output = self.dispatch("flatten", "--live", "--scheduled", "--quiet")
        self.assertNotEqual(status, 0, output)
        self.assertIn("deadline breached", output)
        self.assertAlmostEqual(self.clock.value, 30.0, places=4)
        self.assertTrue(owner.is_alive())
        self.assertEqual(lock.stat().st_ino, inode)
        self.sdk.dhanhq.assert_not_called()
        self.assert_account(5, [])
        resolved, message, acknowledged = self.incident("deadline_breach")
        self.assertEqual((resolved, acknowledged), (0, 0))
        self.assertIn('["23", "NSE_EQ", "INTRADAY", 5]', message)
        owner.kill()
        owner.join(5)
        self.assert_account(5, [])
        self.assertNotEqual(self.dispatch("flatten", "--live", "--scheduled")[0], 0)
        self.assert_account(5, [])
        self.clock.value = 300
        status, output = self.dispatch("flatten", "--live", "--scheduled")
        self.assertEqual(status, 0, output)
        self.assert_account(0, [["SELL", 5]])
        self.assertEqual(self.timeline.read_text().splitlines()[-1], "0")
        self.assertEqual(lock.stat().st_ino, inode)
        self.assertEqual(self.incident("deadline_breach"), (1, message, 0))

    def test_unknown_and_lost_acknowledgement_recover_independently_of_delivery(self):
        self.broker.get_positions.side_effect = None
        self.broker.get_positions.return_value = {"status": "failure"}
        status, output = self.dispatch("flatten", "--live", "--scheduled", "--quiet")
        self.assertNotEqual(status, 0, output)
        self.assertIn("UNKNOWN", output)
        self.assertEqual(self.incident("unknown")[::2], (0, 0))
        self.assert_account(5, [])
        self.clock.value = 300
        self.broker.get_positions.side_effect = self.positions

        def lost_ack(**params):
            self.sell(**params)
            raise TimeoutError("simulated reply lost after execution")

        def lagging():
            observed = self.positions()
            if self.account()["net"] == 0:
                return VerifiedExits.response([self.position(5)])
            return observed

        self.broker.get_positions.side_effect = lagging
        self.broker.place_order.side_effect = lost_ack
        status, output = self.dispatch("flatten", "--live", "--scheduled")
        self.assertNotEqual(status, 0, output)
        self.assert_account(0, [["SELL", 5]])
        self.assertEqual(self.incident("incomplete")[::2], (0, 0))
        self.clock.value = 1200
        self.broker.get_positions.side_effect = self.positions
        status, output = self.dispatch("flatten", "--live", "--scheduled")
        self.assertEqual(status, 0, output)
        self.assert_account(0, [["SELL", 5]])
        self.assertEqual(self.timeline.read_text().splitlines()[-1], "0")
        self.assertEqual(self.incident("incomplete")[::2], (1, 0))

        self.sdk.reset_mock()
        deliveries = self.directory / "deliveries"

        def acknowledge(message):
            with deliveries.open("a") as stream:
                stream.write(message + "\n")
            return True

        self.notify = acknowledge
        self.assertEqual(self.dispatch("eod-alerts")[0], 0)
        self.assertEqual(len(deliveries.read_text().splitlines()), 2)
        self.assertEqual(self.dispatch("eod-alerts")[0], 0)
        self.assertEqual(len(deliveries.read_text().splitlines()), 2)
        self.assertEqual(self.incident("incomplete")[::2], (1, 1))
        self.assertEqual(self.incident("unknown")[::2], (1, 1))
        self.assert_account(0, [["SELL", 5]])
        self.sdk.dhanhq.assert_not_called()

    def test_scheduler_normalizes_offset_and_preserves_manual_flatten(self):
        from eod_ownership import scheduled_dispatch

        utc = datetime.fromisoformat("2026-11-23T09:40:00+00:00")
        wrong = datetime.fromisoformat("2026-11-23T15:10:00+00:00")
        calls = []

        def dispatch(instant, args):
            calls.append(instant)
            return self.dispatch(*args)

        self.assertIsNone(scheduled_dispatch(wrong, dispatch))
        self.assertEqual(calls, [])
        self.assert_account(5, [])
        status, output = scheduled_dispatch(utc, dispatch)
        self.assertEqual(status, 0, output)
        self.assertEqual(calls, [self.instant])
        self.assert_account(0, [["SELL", 5]])
        self.assertEqual(self.timeline.read_text().splitlines()[-1], "0")
        self.clock.value = 6000
        status, output = self.dispatch("flatten", "--live")
        self.assertEqual(status, 0, output)
        self.assert_account(0, [["SELL", 5]])

    def test_entry_intent_crosses_cutoff_before_broker_mutation(self):
        opening = self.account()
        self.save_account({"net": 0, "writes": [], "orders": {}})
        self.clock.value = -3240.125
        (self.directory / "exchange_calendar.json").write_text(json.dumps({
            "source": "Mock exchange calendar fixture",
            "sessions": {"2026-11-23": ["09:15", "15:30"]},
        }))
        self.broker.quote_data.return_value = VerifiedExits.response({"data": {"NSE_EQ": {"99": {
            "last_price": 100, "last_trade_time": "2026-11-23T14:15:59+05:30",
        }}}})
        coordinator = self.engine.ExitCoordinator
        save = coordinator.save
        crossed = []

        def configure(scope):
            scope["INDEX_UNIVERSE"] = {}
            scope["scan_equity_candidates"] = lambda _: dict(
                security_id="99", symbol="ENTRY", direction="LONG", ltp=100, atr=1, reason="fixture")

        def cross_after_save(instance, ledger):
            save(instance, ledger)
            pending = (ledger.get("cycle") or {}).get("pending")
            if pending and pending.get("entry"):
                crossed.append(pending["tag"])
                self.assert_account(0, [])
                self.clock.value = 0.001
                self.save_account(opening)

        self.configure_cli = configure
        with patch.object(coordinator, "save", cross_after_save):
            status, output = self.dispatch("tick", "--live")
        self.assertEqual(status, 0, output)
        self.assertEqual(len(crossed), 1)
        self.assertEqual(self.timeline.read_text().splitlines()[0], "0")
        self.assertIn("5", self.timeline.read_text().splitlines())
        self.assertEqual(self.timeline.read_text().splitlines()[-1], "0")
        self.assert_account(0, [["SELL", 5]])

    def test_scheduled_flat_read_after_deadline_is_nonzero(self):
        self.save_account({"net": 0, "writes": [], "orders": {}})
        self.clock.value = 29.990

        def delayed():
            self.clock.value = 30.001
            return self.positions()

        self.broker.get_positions.side_effect = delayed
        status, output = self.dispatch("flatten", "--live", "--scheduled", "--quiet")
        self.assertNotEqual(status, 0, output)
        self.assertIn("deadline breached", output)
        self.assert_account(0, [])
        self.assertEqual(self.timeline.read_text().splitlines(), ["0"])
        self.assertEqual(self.incident("deadline_breach")[::2], (0, 0))
