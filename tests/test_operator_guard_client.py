"""Offline operator regressions for broker capabilities and token state."""
from datetime import datetime, timedelta
import json
import os
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, call, mock_open, patch

import test_emergency_recovery as recovery
import test_write_safety as fixtures


class FlattenProof(unittest.TestCase):
    setUp = recovery.EmergencyRecovery.setUp
    response = staticmethod(recovery.EmergencyRecovery.response)
    position = staticmethod(recovery.EmergencyRecovery.position)
    row = recovery.EmergencyRecovery.row
    book = recovery.EmergencyRecovery.book
    opening_orders = recovery.EmergencyRecovery.opening_orders
    active = recovery.EmergencyRecovery.active
    submit = recovery.EmergencyRecovery.submit
    read_state = recovery.EmergencyRecovery.read_state

    def test_authorized_flatten_waits_for_orders_and_position_to_agree(self):
        policy = self.engine.RolePolicy(flatten=True)
        self.sdk.get_positions.return_value = self.response([self.position(66)])
        self.assertEqual(self.engine.cmd_flatten(policy), 1)
        self.sdk.place_order.assert_not_called()
        self.sdk.cancel_order.assert_not_called()
        self.assertEqual(self.read_state()["emergency_recovery"]["status"], "pending")
        self.assertFalse(self.journal.exists())

        self.sdk.get_positions.return_value = self.response([self.position(65)])
        self.assertEqual(self.engine.cmd_flatten(policy), 0)
        self.assertEqual(self.engine.cmd_flatten(policy), 0)
        self.sdk.place_order.assert_called_once()
        order = self.sdk.place_order.call_args.kwargs
        self.assertEqual((order["transaction_type"], order["quantity"], order["price"]),
                         ("SELL", 65, 0))
        self.assertIsNone(self.read_state()["active_trade"])


class GuardAndTokenState(unittest.TestCase):
    def setUp(self):
        from pathlib import Path
        import importlib
        with patch.object(sys, "path", [str(Path(__file__).resolve().parents[1] / "app"), *sys.path]):
            dhan_broker = importlib.import_module("dhan_broker")
        loader = dhan_broker.DhanTokenManager._load_state
        self.renew_impl = dhan_broker.DhanTokenManager._renew_token
        self.env_loader = dhan_broker.load_env_file
        fixtures.WriteSafetyTests.setUp(self)
        self.enterContext(patch.object(dhan_broker.DhanTokenManager, "_load_state", loader))

    def test_read_only_exit_method_is_denied_even_with_live_switch(self):
        from cloud_guard import READ_ONLY, ReadOnlyBroker

        os.environ[fixtures.FLAG] = "1"
        sdk = self.sdk.dhanhq.return_value
        broker = ReadOnlyBroker(sdk, policy=READ_ONLY)
        with self.assertRaisesRegex(RuntimeError, "blocked: place_exit_order"):
            broker._place_exit_order(security_id="23", transaction_type="SELL", quantity=5)
        sdk.place_order.assert_not_called()

    def token_files(self, expiry):
        module = self.token_module
        fs = Mock(wraps=os, O_CREAT=os.O_CREAT, O_EXCL=os.O_EXCL, O_WRONLY=os.O_WRONLY)
        fs.environ = {}
        fs.path = Mock(wraps=os.path)
        fs.path.exists.return_value = True
        fs.open = Mock(return_value=77)
        for name in ("close", "unlink", "replace"):
            setattr(fs, name, Mock())
        self.enterContext(patch.object(module, "os", fs))
        self.enterContext(patch.object(module, "TOKEN_STATE_FILE", "/mock/token.json"))
        self.enterContext(patch.object(module, "open", mock_open(read_data=json.dumps(
            {"accessToken": "fixture-cache", "expiry": expiry.isoformat()}
        )), create=True))
        self.enterContext(patch.object(module, "datetime", wraps=datetime)).now.return_value = self.now
        return fs

    def test_read_only_uses_near_expiry_cache_and_denies_at_expiry(self):
        module = self.token_module
        self.now = datetime(2026, 11, 23, 10)
        expiry = self.now + timedelta(minutes=5)
        fs = self.token_files(expiry)
        manager = module.DhanTokenManager()
        self.assertEqual(manager.get_token(), "fixture-cache")
        module.datetime.now.return_value = expiry
        with self.assertRaises(module.CachedTokenUnavailable):
            manager.get_token()
        with self.assertRaises(module.CachedTokenUnavailable):
            manager.renew_token()
        fs.open.assert_not_called()
        fs.replace.assert_not_called()
        self.renew.assert_not_called()

    def test_refresh_failure_cleans_lock_and_can_retry(self):
        module = self.token_module
        self.now = datetime(2026, 11, 23, 10)
        fs = self.token_files(self.now + timedelta(minutes=5))
        manager = module.DhanTokenManager(policy=self.engine.RolePolicy(renew_token=True))
        self.assertIsNone(manager.access_token)
        self.renew.side_effect = [TimeoutError("synthetic refresh failure"), "fixture-refreshed"]
        with self.assertRaisesRegex(TimeoutError, "synthetic refresh failure"):
            manager.get_token()
        self.assertEqual(fs.unlink.call_args_list, [call("/mock/token.json.lock")])
        self.assertEqual(manager.get_token(), "fixture-refreshed")
        self.assertEqual(fs.close.call_args_list, [call(77), call(77)])
        self.assertEqual(fs.unlink.call_args_list, [call("/mock/token.json.lock")] * 2)
        fs.replace.assert_not_called()

    def test_failed_atomic_replacement_preserves_current_token_and_retries(self):
        module = self.token_module
        self.now = datetime(2026, 11, 23, 10)
        expiry = self.now + timedelta(hours=2)
        fs = self.token_files(expiry)
        manager = module.DhanTokenManager(policy=self.engine.RolePolicy(renew_token=True),
                                          pin="fixture-pin", totp="fixture-seed")
        auth = Mock()
        auth.post.return_value = Mock(status_code=200)
        auth.post.return_value.json.return_value = {"accessToken": "fixture-replacement"}
        temporary = Mock(name="temporary_file")
        temporary.name = "/mock/temp-token"
        create_temp = MagicMock()
        create_temp.return_value.__enter__.return_value = temporary
        fs.replace.side_effect = [OSError("synthetic replace failure"), None]
        with (patch.object(module.DhanTokenManager, "_renew_token", self.renew_impl),
              patch.dict(sys.modules, {"requests": auth, "pyotp": SimpleNamespace(
                  TOTP=lambda seed: SimpleNamespace(now=lambda: "fixture-code"))}),
              patch.object(module, "time", SimpleNamespace(time=lambda: 15)),
              patch.object(module.tempfile, "NamedTemporaryFile", create_temp)):
            with self.assertRaisesRegex(OSError, "synthetic replace failure"):
                manager.renew_token()
            self.assertEqual(fs.unlink.call_args_list,
                             [call("/mock/temp-token"), call("/mock/token.json.lock")])
            self.assertEqual((manager.get_token(), manager.token_expiry), ("fixture-cache", expiry))
            temporary.reset_mock()
            self.assertEqual(manager.renew_token(), "fixture-replacement")
        self.assertEqual(auth.post.call_count, 2)
        self.assertEqual(fs.replace.call_args_list, [call("/mock/temp-token", "/mock/token.json")] * 2)
        self.assertEqual(create_temp.call_args_list, [call(mode="w", dir="/mock", delete=False)] * 2)
        self.assertEqual(json.loads("".join(write.args[0] for write in temporary.write.call_args_list)), {
                "access_token": "fixture-replacement",
                "expiry": (self.now + timedelta(hours=23)).isoformat(),
            })

    def test_env_loader_precedence_and_return_value(self):
        import dhan_broker

        fs = Mock(wraps=os)
        fs.environ = {fixtures.FLAG: "0", "FIXTURE_SETTING": "process"}
        fs.path = Mock(wraps=os.path)
        fs.path.exists.return_value = True
        content = '# fixture\n\nFIXTURE_SETTING = "file=setting"\nSURVIVOR_LIVE_TRADING=1\n'
        with (patch.object(dhan_broker, "os", fs),
              patch.object(dhan_broker, "open", mock_open(read_data=content), create=True) as read,
              patch.object(self.engine, "load_env_file", self.env_loader)):
            values = self.env_loader("/mock/.env", exclude=(fixtures.FLAG,))
            read.assert_called_once_with("/mock/.env", "r")
            self.assertEqual(values, {"FIXTURE_SETTING": "file=setting", fixtures.FLAG: "1"})
            self.assertEqual(fs.environ, {"FIXTURE_SETTING": "file=setting", fixtures.FLAG: "0"})
            from cloud_guard import live_trading_enabled
            with patch.dict(os.environ, fs.environ, clear=True):
                self.assertFalse(self.engine._resolve_role("tick", live=True).trade)
                del os.environ[fixtures.FLAG]
                self.assertTrue(self.engine._resolve_role("tick", live=True).trade)
                self.assertNotIn(fixtures.FLAG, os.environ)
                self.assertFalse(live_trading_enabled())
            fs.path.exists.return_value = False
            read.reset_mock()
            self.assertEqual(self.env_loader("/mock/.env"), {})
            read.assert_not_called()
