"""Role-specific permissions exercised with the offline broker fixtures."""
from copy import deepcopy
from datetime import datetime, timedelta
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import test_write_safety as fixtures


class RolePermissionsTests(unittest.TestCase):
    setUp = fixtures.WriteSafetyTests.setUp

    def run_floor_role(self, role, *, live=False, flag="1"):
        os.environ[fixtures.FLAG] = flag
        engine = self.engine
        state = {"status": "ALIVE", "mortality_armed": True, "peak_equity": 30000,
                 "active_trade": {"order_id": "mock-order"}}
        authorized = live and flag == "1"
        sdk = self.sdk.dhanhq.return_value
        if authorized:
            # The account has a working orphan entry, with no active trade to finalize.
            state["active_trade"] = None
            row = fixtures.confirmed_cancel(sdk)
        before = deepcopy(state)
        with tempfile.TemporaryDirectory(dir=fixtures.APP.parent) as directory:
            obituary = Path(directory) / "obituary"
            with (
                patch.object(engine, "load_state", return_value=state),
                patch.object(engine, "save_state") as save,
                patch.object(engine, "load_journal", return_value=[]),
                patch.object(engine, "OBITUARY_FILE", str(obituary)),
                patch.object(engine, "EXIT_LEDGER", Path(directory) / "exit.json"),
                patch.object(engine.time, "sleep"),
                patch.object(engine, "acquire_engine_lock", return_value=9) as acquire,
                patch.object(engine, "release_engine_lock"),
                patch.object(engine.SurvivorDhanClient, "get_live_equity_and_positions", autospec=True,
                             return_value=(1000, 1000, 0, [])) as read_equity,
                patch.object(engine, "_finalize_closed_trade") as finalize,
            ):
                if role == "status":
                    engine.cmd_status()
                else:
                    result = engine.cmd_tick(live=live)
            if authorized:
                self.assertEqual(result, 0)
                sdk.cancel_order.assert_called_once_with("mock-order")
                self.assertEqual(row["orderStatus"], "CANCELLED")
                self.assertEqual(save.call_args.args[0]["status"], "DEAD_COMA")
                self.assertTrue(obituary.exists())
            else:
                acquire.assert_not_called()
                sdk.cancel_order.assert_not_called()
                client = read_equity.call_args.args[0]
                for method in ("cancel_order", "place_order", "modify_order"):
                    with self.subTest(method=method), self.assertRaises(RuntimeError):
                        getattr(client.dhan, method)()
                save.assert_not_called()
                self.assertEqual(state, before)
                self.assertFalse(obituary.exists())
            sdk.place_order.assert_not_called()
            sdk.modify_order.assert_not_called()
            finalize.assert_not_called()

    def test_status_at_floor(self):
        self.run_floor_role("status")

    def test_dry_tick_at_floor(self):
        self.run_floor_role("tick")

    def test_live_tick_at_floor(self):
        self.run_floor_role("tick", live=True)

    def test_live_switch_alone_at_floor(self):
        self.run_floor_role("tick", live=True, flag="0")

    def test_readonly_client_guard_survives_cache_reconnect(self):
        os.environ[fixtures.FLAG] = "1"
        client = self.engine.SurvivorDhanClient(allow_token_renewal=False)
        with (
            patch.object(self.engine, "os") as filesystem,
            patch.object(self.engine.time, "sleep"),
        ):
            filesystem.open.side_effect = FileExistsError
            client.reconnect_with_fresh_token()
        filesystem.open.assert_not_called()
        filesystem.remove.assert_not_called()
        self.renew.assert_not_called()
        sdk = self.sdk.dhanhq.return_value
        client.dhan.get_positions()
        sdk.get_positions.assert_called_once()
        for method in ("cancel_order", "place_order", "modify_order"):
            with self.subTest(method=method), self.assertRaises(RuntimeError):
                getattr(client.dhan, method)()
        client.ensure_ip_whitelisted()
        self.http.get.assert_called_once()
        self.http.put.assert_not_called()

    def test_dry_tick_does_not_finalize_daily_halt_or_eod(self):
        os.environ[fixtures.FLAG] = "1"
        engine = self.engine
        for hour, opening_equity in ((11, 30000), (16, 20000)):
            with self.subTest(hour=hour):
                now = datetime(2026, 1, 5, hour, tzinfo=engine.IST)
                state = {
                    "status": "ALIVE", "mortality_armed": True, "peak_equity": 30000,
                    "daily_date": "2026-01-05", "daily_start_equity": opening_equity,
                    "active_trade": {"entry_price": 100},
                }
                before = deepcopy(state)
                with (
                    patch.object(engine, "datetime") as engine_time,
                    patch.object(engine, "load_state", return_value=state),
                    patch.object(engine, "save_state") as save,
                    patch.object(engine, "acquire_engine_lock") as acquire,
                    patch.object(engine, "_finalize_closed_trade") as finalize,
                    patch.object(engine.SurvivorDhanClient, "get_live_equity_and_positions",
                                 return_value=(20000, 20000, 0, [])),
                ):
                    engine_time.now.return_value = now
                    engine.cmd_tick()
                acquire.assert_not_called()
                finalize.assert_not_called()
                save.assert_not_called()
                self.assertEqual(state, before)

    def test_status_missing_or_expired_cache(self):
        for expiry in (None, datetime.now() - timedelta(hours=1)):
            for flag in ("0", "1"):
                with self.subTest(expiry=expiry, flag=flag):
                    os.environ[fixtures.FLAG] = flag

                    def load_cache(manager):
                        manager.access_token = "mock-expired" if expiry else None
                        manager.token_expiry = expiry

                    with (
                        patch.object(self.engine, "load_state", return_value={"status": "ALIVE"}),
                        patch.object(self.token_module.DhanTokenManager, "_load_state", load_cache),
                        patch.object(self.engine.SurvivorDhanClient, "get_live_equity_and_positions",
                                     side_effect=LookupError),
                    ):
                        try:
                            self.engine.cmd_status()
                        except self.token_module.CachedTokenUnavailable:
                            pass
                        except LookupError:
                            self.fail("status reached the broker without a valid cached token")
                    self.renew.assert_not_called()
                    self.sdk.dhanhq.assert_not_called()
                    self.http.put.assert_not_called()
