"""Option pricing and settlement through live tick and real entry recovery."""
import json
import unittest
from unittest.mock import patch

import test_entry_protection as fixtures


class OptionPricing(unittest.TestCase):
    setUp = fixtures.EntryProtection.setUp
    engine_paths = fixtures.EntryProtection.engine_paths
    sleep = fixtures.EntryProtection.sleep
    candles = fixtures.EntryProtection.candles
    response = staticmethod(fixtures.EntryProtection.response)
    position = fixtures.EntryProtection.position
    place = fixtures.EntryProtection.place
    terminal_entry = fixtures.EntryProtection.terminal_entry
    resume = fixtures.EntryProtection.resume

    def quote(self, securities):
        payload = fixtures.EntryProtection.quote(self, securities)
        segment, (security,) = next(iter(securities.items()))
        row = payload["data"]["data"][segment][str(security)]
        row.update(bid_price=80, ask_price=130, depth={
            "buy": [{"price": 80}], "sell": [{"price": 130}],
        })
        if self.book and not self.net:
            row["last_price"] = self.working_ltp
        return payload

    def option_tick(self):
        self.segment = "NSE_FNO"
        self.monitor_price = 100
        engine = self.engine
        self.sdk.get_fund_limits.return_value = self.response({"availabelBalance": 30000})
        (self.directory / ".tick_size_cache.json").write_text(
            json.dumps({"NSE_FNO:23": {"lot_size": 5, "tick_size": 0.125}}))
        option = {"security_id": "23", "symbol": "FIX CE", "lot_size": 5,
                  "bid": 80, "ask": 130, "ltp": 90, "expiry": "2026-11-24"}
        with (patch.object(engine, "INDEX_UNIVERSE", {"fixture": {
                    "security_id": "22", "lot_size": 5, "symbol_prefix": "FIX"}}),
              patch.object(engine, "evaluate_index_signal", return_value={"signal": "LONG", "reason": "fixture"}),
              patch.object(engine.SurvivorDhanClient, "get_nearest_nifty_option", return_value=option),
              patch.object(engine, "verify_option_chart_momentum", return_value=(True, "momentum")),
              patch.object(engine, "monitor_active_trade_loop", return_value=0)):
            return engine.cmd_tick(policy=engine.RolePolicy(trade=True), quiet_scan=True)

    def assert_entry(self):
        entry = self.sdk.place_order.call_args_list[0].kwargs
        self.assertEqual((entry["order_type"], entry["transaction_type"], entry["quantity"],
                          entry["price"]), ("LIMIT", "BUY", 5, 100))

    def assert_stop(self):
        self.assertEqual(self.sdk.place_order.call_count, 2)
        self.assert_entry()
        stop = self.sdk.place_order.call_args_list[1].kwargs
        self.assertEqual((stop["order_type"], stop["transaction_type"], stop["quantity"],
                          stop["trigger_price"]), ("STOP_LOSS", "SELL", 5, 90))
        self.assertEqual(self.net, 5)
        active = self.engine.load_state()["active_trade"]
        self.assertEqual((active["quantity"], active["entry_price"], active["target_price"]),
                         (5, 100.125, 117.5))

    def test_option_chase_uses_verified_ltp_and_caps_one_amendment(self):
        self.working_ltp = 101.375

        def amend(**order):
            self.terminal_entry("TRADED", 5, 5)
            return self.response({"orderId": "1"})

        self.sdk.modify_order.side_effect = amend
        self.assertEqual(self.option_tick(), 0)
        self.assert_stop()
        self.sdk.modify_order.assert_called_once()
        order = self.sdk.modify_order.call_args.kwargs
        self.assertEqual((order["order_id"], order["price"], order["quantity"]), ("1", 101.5, 5))
        self.sdk.cancel_order.assert_not_called()
        self.resume()
        self.assert_stop()
        self.sdk.modify_order.assert_called_once()

    def test_option_ltp_beyond_cap_cancels_original_without_replacement(self):
        self.working_ltp = 101.625

        def cancel(oid):
            self.terminal_entry("CANCELLED", 0, 0)
            return self.response({"orderId": oid})

        self.sdk.cancel_order.side_effect = cancel
        self.assertEqual(self.option_tick(), 0)
        self.assert_entry()
        self.sdk.cancel_order.assert_called_once_with("1")
        self.sdk.modify_order.assert_not_called()
        self.sdk.place_order.assert_called_once()
        self.assertEqual(self.net, 0)
        self.assertIsNone(self.engine.load_state()["active_trade"])

    def test_option_partial_during_chase_quote_cancels_then_closes_only_residual(self):
        self.working_ltp = 101.375

        def quote(securities):
            response = self.quote(securities)
            if self.book and not self.net:
                self.terminal_entry("PART_TRADED", 2, 2)
            return response

        def cancel(oid):
            self.terminal_entry("CANCELLED", 2, 2)
            return self.response({"orderId": oid})

        self.sdk.quote_data.side_effect = quote
        self.sdk.cancel_order.side_effect = cancel
        self.assertEqual(self.option_tick(), 0)
        self.assert_entry()
        self.sdk.modify_order.assert_not_called()
        self.sdk.cancel_order.assert_called_once_with("1")
        self.assertEqual(self.sdk.place_order.call_count, 2)
        exit_order = self.sdk.place_order.call_args.kwargs
        self.assertEqual((exit_order["order_type"], exit_order["transaction_type"],
                          exit_order["quantity"]), ("MARKET", "SELL", 2))
        self.assertEqual(self.net, 0)
        self.resume()
        self.assertEqual(self.sdk.place_order.call_count, 2)
        row, = json.loads(self.engine.JOURNAL_FILE.read_text())
        self.assertEqual(row["quantity"], 2)
