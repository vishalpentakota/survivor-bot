"""Isolated regressions for Survivor's broker and premarket write gates."""
import contextlib
import importlib
import io
import logging
import os
from pathlib import Path
import runpy
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

APP = Path(__file__).resolve().parents[1] / "app"
FLAG = "SURVIVOR_LIVE_TRADING"


def confirmed_cancel(sdk):
    row = dict(orderId="mock-order", orderStatus="PENDING", securityId="23",
               exchangeSegment="NSE_EQ", productType="INTRADAY", transactionType="BUY",
               quantity=5, filledQty=0)
    sdk.get_order_list.side_effect = lambda: {"status": "success", "data": [row.copy()]}
    sdk.get_order_by_id.side_effect = lambda oid: {"status": "success", "data": row.copy()}

    def cancel(oid):
        assert oid == row["orderId"]
        row["orderStatus"] = "CANCELLED"
        return {"status": "success", "data": {"orderId": oid}}

    sdk.cancel_order.side_effect = cancel
    return row


class WriteSafetyTests(unittest.TestCase):
    def setUp(self):
        stack = contextlib.ExitStack()
        self.addCleanup(stack.close)
        stack.enter_context(patch.dict(os.environ, {}, clear=True))
        stack.enter_context(patch.object(sys, "path", [str(APP), *sys.path]))
        self.sdk = Mock()
        self.sdk.dhanhq.return_value.get_positions.return_value = {"status": "success", "data": []}
        self.sdk.dhanhq.return_value.get_order_list.return_value = {
            "status": "success", "data": [{"orderStatus": "PENDING", "orderId": "mock-order"}]
        }
        self.http = Mock()
        mismatch = {"ordersAllowed": False, "detectedIP": "192.0.2.1",
                    "modifyDateSecondary": "2000-01-01"}
        self.http.get.return_value = Mock(status_code=200, text="192.0.2.1")
        self.http.get.return_value.json.return_value = mismatch
        connection = types.ModuleType("urllib3.util.connection")
        urllib = types.ModuleType("urllib3")
        urllib.util = types.ModuleType("urllib3.util")
        urllib.util.connection = connection
        stack.enter_context(patch.dict(sys.modules, {
            "pandas": types.SimpleNamespace(DataFrame=object, Series=object),
            "numpy": types.ModuleType("numpy"),
            "urllib3": urllib, "urllib3.util": urllib.util,
            "urllib3.util.connection": connection,
            "dhanhq": self.sdk, "requests": self.http,
        }))
        stack.enter_context(patch.object(logging, "FileHandler"))
        stack.enter_context(patch.object(logging, "basicConfig"))
        stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
        stack.enter_context(contextlib.redirect_stderr(io.StringIO()))
        self.engine = importlib.import_module("survivor_engine")
        self.token_module = importlib.import_module("dhan_broker")
        stack.enter_context(patch.object(self.token_module, "load_env_file"))
        stack.enter_context(patch.object(self.engine, "load_env_file"))

        def seed_token(manager):
            manager.access_token = "mock-token"
            manager.token_expiry = datetime.now() + timedelta(hours=2)

        stack.enter_context(patch.object(self.token_module.DhanTokenManager, "_load_state",
                                         autospec=True, side_effect=seed_token))
        self.renew = stack.enter_context(patch.object(self.token_module.DhanTokenManager,
                                                       "_renew_token", return_value="mock-renewal"))

    def check_reconnect(self, flag, contended, cached=True, *, live=False):
        if flag is not None:
            os.environ[FLAG] = flag
        policy = self.engine._resolve_role("tick", live=live)
        client = self.engine.SurvivorDhanClient(policy=policy)
        sdk = self.sdk.dhanhq.return_value
        fs = Mock(O_CREAT=os.O_CREAT, O_EXCL=os.O_EXCL, O_WRONLY=os.O_WRONLY,
                  environ=os.environ.copy())
        fs.open.side_effect = FileExistsError if contended else None
        fs.open.return_value = 99
        fs.path = Mock()
        fs.path.exists.return_value = True
        with (patch.object(self.engine, "os", fs),
              patch.object(self.token_module, "os", fs),
              patch.object(self.engine.time, "sleep") as sleep):
            if not cached:
                self.assert_reconnect_denied(client, fs, sleep)
                return
            client.reconnect_with_fresh_token()
        self.sdk.dhanhq.assert_called()
        client.dhan.get_positions()
        sdk.get_positions.assert_called_once()
        methods = ("place_order", "modify_order", "cancel_order")
        allowed = []
        for method in methods:
            try:
                getattr(client.dhan, method)()
            except RuntimeError:
                continue
            allowed.append(method)
        expected = list(methods) if live and flag == "1" else []
        self.assertEqual(allowed, expected)
        self.assertEqual([method for method in methods if getattr(sdk, method).called], expected)
        for method in expected:
            getattr(sdk, method).assert_called_once()

    def assert_reconnect_denied(self, client, fs, sleep):
        manager, broker = client.tm, client.dhan
        token_state = manager.access_token, manager.token_expiry
        self.sdk.reset_mock()
        manager_class = self.token_module.DhanTokenManager
        with (patch.object(manager_class, "_load_state"),
              patch.object(manager_class, "_save_state") as save,
              patch.object(self.token_module, "open", create=True) as cache_open,
              patch.object(self.token_module, "tempfile") as cache_temp):
            with self.assertRaises(self.token_module.CachedTokenUnavailable):
                client.reconnect_with_fresh_token()
        self.assertIs(client.tm, manager)
        self.assertIs(client.dhan, broker)
        self.assertEqual((manager.access_token, manager.token_expiry), token_state)
        self.renew.assert_not_called()
        save.assert_not_called()
        cache_open.assert_not_called()
        sleep.assert_not_called()
        for mock in (fs, cache_temp, self.sdk, self.sdk.dhanhq.return_value, self.http):
            self.assertEqual(mock.mock_calls, [])

    def test_fresh_absent(self):
        self.check_reconnect(None, contended=False, cached=False)

    def test_fresh_disabled(self):
        self.check_reconnect("0", contended=False, cached=False)

    def test_cached_absent(self):
        self.check_reconnect(None, contended=True)

    def test_cached_disabled(self):
        self.check_reconnect("0", contended=True)

    def test_contended_expired_absent(self):
        self.check_reconnect(None, contended=True, cached=False)

    def test_contended_expired_disabled(self):
        self.check_reconnect("0", contended=True, cached=False)

    def test_authorized_reconnect(self):
        self.check_reconnect("1", contended=True, live=True)

    def run_flatten(self, flag=None, live=False):
        if flag is not None:
            os.environ[FLAG] = flag
        args = ["survivor_engine.py", "flatten"] + (["--live"] if live else [])
        with patch.object(sys, "argv", args), patch.object(sys, "path", sys.path.copy()):
            runpy.run_path(str(APP / "survivor_engine.py"), run_name="__main__")

    def assert_flatten_denied(self, **kwargs):
        with self.assertRaises(SystemExit) as error:
            self.run_flatten(**kwargs)
        self.assertNotEqual(error.exception.code, 0)
        self.sdk.dhanhq.assert_not_called()

    def test_flatten_requires_live(self):
        self.assert_flatten_denied(flag="1")

    def test_flatten_requires_enabled_environment(self):
        self.assert_flatten_denied(live=True)

    def test_flatten_rejects_disabled_environment(self):
        self.assert_flatten_denied(flag="0", live=True)

    def test_authorized_flatten(self):
        sdk = self.sdk.dhanhq.return_value
        row = confirmed_cancel(sdk)
        with tempfile.TemporaryDirectory() as directory:
            staged = Path(directory) / "survivor_engine.py"
            staged.write_bytes((APP / staged.name).read_bytes())
            with patch.dict(os.environ, {FLAG: "1"}), patch.object(
                    sys, "argv", [str(staged), "flatten", "--live"]):
                runpy.run_path(str(staged), run_name="__main__")
        sdk.cancel_order.assert_called_once_with("mock-order")
        self.assertEqual(row["orderStatus"], "CANCELLED")

    def run_premarket(self, *, policy=None):
        sdk = self.sdk.dhanhq.return_value
        sdk.get_fund_limits.return_value = {"status": "success", "data": {}}
        sdk.expiry_list.return_value = {"status": "success", "data": {"data": ["2099-01-01"]}}
        class EmptyFeed(list):
            empty = True

        with (tempfile.TemporaryDirectory(dir=APP.parent) as directory,
              patch.object(self.engine, "BASE_DIR", directory),
              patch.object(self.engine, "_STATE_LOCK_FILE", Path(directory) / "state.lock"),
              patch.object(self.engine, "load_state", return_value={"status": "ALIVE"}),
              patch.object(self.engine, "save_state"),
              patch.object(self.engine.SurvivorDhanClient, "fetch_candles", return_value=EmptyFeed())):
            self.engine.cmd_premarket(policy=policy)

    def test_read_only_premarket_checks_without_repair(self):
        with patch.object(self.engine.SurvivorDhanClient, "reconnect_with_fresh_token") as reconnect:
            self.run_premarket()
        self.http.get.assert_called_once()
        self.http.put.assert_not_called()
        reconnect.assert_not_called()
        self.renew.assert_not_called()

    def test_read_only_premarket_without_cached_token(self):
        os.environ[FLAG] = "0"
        with (patch.object(self.token_module.DhanTokenManager, "_load_state"),
              patch.object(self.engine.SurvivorDhanClient, "reconnect_with_fresh_token")):
            self.run_premarket()
        self.renew.assert_not_called()
        self.sdk.dhanhq.assert_not_called()
        self.http.get.assert_not_called()
        self.http.put.assert_not_called()

    def test_authorized_premarket_repairs_with_mocks(self):
        os.environ[FLAG] = "1"
        policy = self.engine._resolve_role("premarket", live=True, repair=True)
        with patch.object(self.engine.SurvivorDhanClient, "reconnect_with_fresh_token") as reconnect:
            self.run_premarket(policy=policy)
        self.http.put.assert_called_once()
        self.assertEqual(reconnect.call_count, 2)
