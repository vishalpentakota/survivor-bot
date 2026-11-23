"""Standalone probe coverage on the source checkout, outside release archives."""
import os
import runpy
import sys
import unittest
from unittest.mock import Mock, call, patch

import test_write_safety as fixtures


class CloudProbeCLI(unittest.TestCase):
    setUp = fixtures.WriteSafetyTests.setUp

    def invoke(self, *args):
        probe = fixtures.APP / "cloud_probe.py"
        with patch.object(sys, "argv", [str(probe), *args]):
            runpy.run_path(str(probe), run_name="__main__")

    def test_default_cli_only_reads_public_ip(self):
        with patch.object(self.engine, "SurvivorDhanClient") as client:
            self.invoke()
        client.assert_not_called()
        self.assertEqual(self.http.mock_calls, [
            call.get("https://api.ipify.org", timeout=15), call.get().raise_for_status(),
        ])
        self.sdk.dhanhq.assert_not_called()
        self.renew.assert_not_called()

    def test_broker_cli_reads_using_cache_only_under_live_switch(self):
        os.environ[fixtures.FLAG] = "1"
        broker = self.sdk.dhanhq.return_value
        for name in ("get_fund_limits", "get_positions", "get_order_list"):
            getattr(broker, name).return_value = {"status": "success", "data": []}
        filesystem = Mock(environ={})
        with (patch.object(self.token_module, "os", filesystem),
              patch.object(self.engine.SurvivorDhanClient, "fetch_candles",
                           return_value=()) as candles,
              patch.object(self.engine, "SurvivorDhanClient", wraps=self.engine.SurvivorDhanClient) as client,
              patch.object(self.engine, "INDEX_UNIVERSE", {"fixture": {"security_id": "13"}})):
            self.invoke("--broker")
        client.assert_called_once_with(allow_token_renewal=False, allow_order_writes=False)
        candles.assert_called_once_with("13", exchange_segment="IDX_I", instrument_type="INDEX",
                                        days_back=5, resample_tf="3min")
        self.assertEqual(broker.mock_calls, [call.get_fund_limits(), call.get_positions(), call.get_order_list()])
        self.assertEqual(self.http.mock_calls, [
            call.get("https://api.ipify.org", timeout=15), call.get().raise_for_status(),
            call.get("https://api.dhan.co/v2/ip/getIP",
                     headers={"access-token": "mock-token", "client-id": ""}, timeout=15),
            call.get().raise_for_status(), call.get().json(),
        ])
        self.renew.assert_not_called()
        filesystem.open.assert_not_called()
        filesystem.replace.assert_not_called()
        filesystem.unlink.assert_not_called()

    def test_broker_cli_without_cache_stops_before_sdk_and_dhan_http(self):
        os.environ[fixtures.FLAG] = "1"
        with patch.object(self.token_module.DhanTokenManager, "_load_state", return_value=None):
            with self.assertRaises(self.token_module.CachedTokenUnavailable):
                self.invoke("--broker")
        self.sdk.dhanhq.assert_not_called()
        self.renew.assert_not_called()
        self.assertEqual(self.http.mock_calls, [
            call.get("https://api.ipify.org", timeout=15), call.get().raise_for_status(),
        ])
