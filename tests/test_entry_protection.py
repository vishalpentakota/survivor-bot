"""Entry and stop acceptance through the frozen socket-denied runner."""
from datetime import datetime
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import test_write_safety as fixtures


class EntryProtection(unittest.TestCase):
    def setUp(self):
        fixtures.WriteSafetyTests.setUp(self)
        root = Path(self.engine.__file__).resolve().parents[1]
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory(dir=root)))
        self.assertEqual(root, Path(__file__).resolve().parents[1])
        self.engine_paths()
        self.sdk = self.sdk.dhanhq.return_value
        self.sdk.BUY, self.sdk.SELL, self.sdk.INTRA = "BUY", "SELL", "INTRADAY"
        self.sdk.LIMIT, self.sdk.MARKET, self.sdk.SL, self.sdk.DAY = "LIMIT", "MARKET", "STOP_LOSS", "DAY"
        self.client = self.engine.SurvivorDhanClient(policy=self.engine.RolePolicy(trade=True))
        self.book = {}
        self.details = {}
        self.net = 0
        self.sdk.get_order_list.side_effect = lambda: self.response([row.copy() for row in self.book.values()])
        self.sdk.get_order_by_id.side_effect = lambda oid: self.response(self.details[oid].copy())
        self.sdk.get_positions.side_effect = lambda: self.response([self.position()] if self.net else [])
        self.sdk.place_order.side_effect = self.place
        self.sdk.quote_data.side_effect = self.quote
        self.sdk.cancel_order.side_effect = lambda oid: self.response({"orderId": oid})
        self.entry_status = "PENDING"
        self.entry_fill = 0
        self.stop_status = "TRIGGER_PENDING"
        self.stop_fill = 0
        self.lose_entry = self.lose_stop = False
        self.stop_read_overrides = {}
        self.fills_at_send = False
        self.lot_size = 1
        self.monitor_price = None
        self.entry_quote = 100
        self.time_elapsed = 0
        self.segment, self.side, self.symbol = "NSE_EQ", "BUY", "STAGED"
        self.state = self.engine.load_state()

    def engine_paths(self):
        now = datetime.fromisoformat("2026-11-23T10:00:00+05:30")
        for name, value in {
            "BASE_DIR": str(self.directory), "STATE_FILE": self.directory / "state",
            "_STATE_LOCK_FILE": self.directory / "state.lock", "EXIT_LEDGER": self.directory / "exit",
            "JOURNAL_FILE": self.directory / "journal",
            "datetime": SimpleNamespace(now=lambda tz: now.astimezone(tz), fromisoformat=datetime.fromisoformat),
        }.items():
            self.enterContext(patch.object(self.engine, name, value))
        import entry_protection
        import verified_exit

        self.enterContext(patch.object(entry_protection.time, "sleep", self.sleep))
        self.assertIs(verified_exit.time.sleep, entry_protection.time.sleep)
        self.enterContext(patch.object(self.engine, "time", SimpleNamespace(
            sleep=self.sleep, time=lambda: self.time_elapsed)))
        self.enterContext(patch.object(self.engine, "send_telegram", return_value=False))
        (self.directory / "exchange_calendar.json").write_text(json.dumps({
            "source": "Mock exchange calendar fixture",
            "sessions": {"2026-11-23": ["09:15", "15:30"], "2026-11-24": ["09:15", "15:30"]},
        }))
        self.enterContext(patch.object(self.engine.SurvivorDhanClient, "fetch_candles",
                                      side_effect=self.candles))

    def sleep(self, seconds):
        if seconds == self.engine.MONITOR_POLL_S:
            self.time_elapsed += seconds

    def candles(self, *args, **kwargs):
        return SimpleNamespace(empty=True)

    def quote(self, securities):
        segment, (security,) = next(iter(securities.items()))
        value = self.monitor_price if self.net else self.entry_quote
        return self.response({"data": {segment: {str(security): {
            "last_price": value, "last_trade_time": "2026-11-23T10:00:00+05:30",
        }}}})

    @staticmethod
    def response(data):
        return {"status": "success", "data": data}

    def position(self):
        return dict(securityId="23", exchangeSegment=self.segment, productType="INTRADAY", netQty=self.net)

    def place(self, **params):
        oid = str(len(self.book) + 1)
        stop = params["order_type"] == self.sdk.SL
        status, filled = (self.stop_status, self.stop_fill) if stop else (self.entry_status, self.entry_fill)
        if params["order_type"] == self.sdk.MARKET and oid != "1":
            self.assertEqual(params["quantity"], abs(self.net))
            self.assertEqual(params["transaction_type"], "SELL" if self.net > 0 else "BUY")
            self.assertTrue(all(row["orderStatus"] in self.engine.TERMINAL for row in self.book.values()))
            status, filled = "TRADED", params["quantity"]
            self.net = 0
        if oid == "1" and self.fills_at_send:
            status, filled = "TRADED", params["quantity"]
            self.net = filled if params["transaction_type"] == "BUY" else -filled
        if stop:
            self.assertEqual(params["quantity"], abs(self.net))
            self.assertEqual(params["transaction_type"], "SELL" if self.net > 0 else "BUY")
            self.net += filled if params["transaction_type"] == "BUY" else -filled
        with self.engine.EXIT_LEDGER.open() as storage:
            cycle = json.load(storage)["cycle"]
        intent = cycle["pending"] or next(iter(cycle["pending_by_key"].values()))
        self.assertEqual((intent["oid"], intent["tag"]), (None, params["tag"]))
        row = dict(self.position(), orderId=oid, transactionType=params["transaction_type"],
                   quantity=params["quantity"], filledQty=filled, orderStatus=status,
                   averageTradedPrice=100.125, orderType=params["order_type"], price=params["price"],
                   triggerPrice=params.get("trigger_price", 0), correlationId=params["tag"],
                   createTime=f"2026-11-23 10:00:0{oid}")
        self.book[oid] = row
        self.details[oid] = dict(row, **(self.stop_read_overrides if stop else {}))
        if (oid == "1" and self.lose_entry) or (stop and self.lose_stop):
            raise TimeoutError
        return self.response({"orderId": oid})

    def terminal_entry(self, status, filled, net):
        self.book["1"].update(orderStatus=status, filledQty=filled)
        self.details["1"] = self.book["1"].copy()
        self.net = net

    def start(self):
        return self.engine._start_entry(self.client, self.state, {
            "symbol": self.symbol, "weapon": "OPTIONS" if self.segment == "NSE_FNO" else "EQUITY_5X",
            "security_id": "23", "exchange_segment": self.segment, "expiry": "2026-11-24",
            "instrument_type": "OPTIDX" if self.segment == "NSE_FNO" else "EQUITY",
            "direction": "LONG" if self.side == "BUY" else "SHORT", "entry_price": 100,
            "quantity": 5, "stop_loss": 94.96 if self.side == "BUY" else 105.04,
            "target_price": 130 if self.side == "BUY" else 70, "entry_time": "2026-11-23T10:00:00+05:30",
            "tick_size": 0.125, "lot_size": self.lot_size, "reason": "fixture", "live": True,
        }, self.side, "LIMIT", 100.18)

    def resume(self):
        self.client = self.engine.SurvivorDhanClient(policy=self.engine.RolePolicy(trade=True))
        self.state = self.engine.load_state()
        return self.engine._resume_entry(self.client, self.state)

    def test_cancel_request_requires_terminal_and_flat(self):
        self.start()
        self.sdk.cancel_order.assert_called_once_with("1")
        self.assertIsNone(self.state["active_trade"])
        self.start()
        self.sdk.place_order.assert_called_once()
        self.terminal_entry("CANCELLED", 0, 0)
        self.resume()
        self.assertEqual(self.state["daily_trades_count"], 0)
        self.assertIsNone(json.loads(self.engine.EXIT_LEDGER.read_text())["cycle"])
        self.sdk.place_order.assert_called_once()

    def test_late_partial_with_lag_and_lost_stop_ack(self):
        self.lose_entry = self.lose_stop = True
        self.sdk.cancel_order.side_effect = TimeoutError
        self.start()
        self.terminal_entry("CANCELLED", 2, 0)
        self.resume()
        self.sdk.place_order.assert_called_once()
        self.net = 2
        self.resume()
        active = self.state["active_trade"]
        self.assertEqual((active["order_id"], active["sl_order_id"], active["quantity"],
                          active["entry_price"], active["stop_loss"]), ("1", "2", 2, 100.125, 95.0))
        self.assertEqual(self.sdk.place_order.call_args.kwargs["quantity"], 2)
        self.assertEqual(self.state["daily_trades_count"], 1)
        self.resume()
        self.assertEqual(self.sdk.place_order.call_count, 2)
        self.assertEqual(self.state["daily_trades_count"], 1)

    def test_failed_cancel_with_full_fill_polls_through_position_lag(self):
        def late_full(oid):
            self.assertEqual(oid, "1")
            self.terminal_entry("TRADED", 5, 0)
            reads = iter([0, 0, 5])

            def lagged():
                self.net = next(reads, 5)
                return self.response([self.position()] if self.net else [])

            self.sdk.get_positions.side_effect = lagged
            return {"status": "failure", "data": {"orderId": oid, "orderStatus": "REJECTED"}}

        self.sdk.cancel_order.side_effect = late_full
        self.assertEqual(self.start(), "ready")
        self.assertEqual(self.sdk.cancel_order.call_count, 1)
        self.assertEqual(self.state["active_trade"]["quantity"], 5)
        self.assertEqual(self.state["daily_trades_count"], 1)

    def test_unknown_entry_ack_and_absent_order_block_real_tick(self):
        self.sdk.place_order.side_effect = TimeoutError
        self.assertEqual(self.start(), "pending")
        self.assertEqual(self.engine.cmd_tick(policy=self.engine.RolePolicy(trade=True)), 1)
        self.sdk.place_order.assert_called_once()
        self.sdk.cancel_order.assert_not_called()
        ledger = json.loads(self.engine.EXIT_LEDGER.read_text())
        self.assertTrue(ledger["alerts"])
        self.assertIsNone(ledger["cycle"]["pending"]["oid"])
        self.assertEqual(self.state["daily_trades_count"], 0)

    def test_position_ahead_of_entry_never_places_stop(self):
        self.start()
        self.terminal_entry("CANCELLED", 0, 5)
        self.assertEqual(self.resume(), "pending")
        self.sdk.place_order.assert_called_once()
        self.assertEqual(self.state["daily_trades_count"], 0)

    def test_regressing_working_fill_does_not_clear_entry(self):
        self.start()
        self.terminal_entry("PART_TRADED", 3, 0)
        self.assertEqual(self.resume(), "pending")
        self.terminal_entry("CANCELLED", 0, 0)
        self.assertEqual(self.resume(), "pending")
        self.sdk.place_order.assert_called_once()

    def test_inconsistent_stop_is_recovered_only_after_independent_truth(self):
        self.fills_at_send = True
        self.stop_read_overrides = {"orderId": "different"}
        self.assertEqual(self.start(), "pending")
        self.assertNotIn("stop_loss", self.state["active_trade"])
        for field, value in (
            ("securityId", "999"), ("exchangeSegment", "NSE_FNO"), ("productType", "CNC"),
            ("transactionType", "BUY"), ("quantity", 6), ("price", 1), ("triggerPrice", 1),
            ("orderType", "LIMIT"), ("orderStatus", "TRANSIT"),
        ):
            with self.subTest(field=field):
                self.details["2"] = dict(self.book["2"], **{field: value})
                self.assertEqual(self.resume(), "pending")
                self.assertNotIn("sl_order_id", self.state["active_trade"])
                self.assertEqual(self.sdk.place_order.call_count, 2)
        self.details["2"] = self.book["2"].copy()
        self.assertEqual(self.resume(), "ready")
        self.assertEqual(self.state["active_trade"]["sl_order_id"], "2")
        self.assertEqual(self.state["daily_trades_count"], 1)

    def test_confirmed_rejection_closes_with_verified_market_execution(self):
        self.fills_at_send = True
        self.stop_status = "REJECTED"
        self.assertEqual(self.start(), "close")
        self.assertNotIn("stop_loss", self.state["active_trade"])
        self.assertEqual(self.engine._entry_tick_result(self.client, self.state, "close", True), 0)
        self.assertIsNone(self.state["active_trade"])
        journal, = json.loads(self.engine.JOURNAL_FILE.read_text())
        self.assertNotIn("stop_loss", journal)
        self.assertEqual((journal["quantity"], journal["execution_source"]), (5, "MARKET"))
        self.assertEqual([call.kwargs["order_type"] for call in self.sdk.place_order.call_args_list],
                         ["LIMIT", "STOP_LOSS", "MARKET"])

    def test_cancelled_stop_is_unprotected_even_with_a_success_ack(self):
        self.fills_at_send = True
        self.stop_status = "CANCELLED"
        self.assertEqual(self.start(), "close")
        self.assertNotIn("stop_loss", self.state["active_trade"])
        self.assertTrue(self.state["active_trade"]["flatten_pending"])

    def test_partial_stop_must_be_terminal_before_market_residual(self):
        self.fills_at_send = True
        self.stop_status, self.stop_fill = "PART_TRADED", 2
        self.assertEqual(self.start(), "close")
        self.assertEqual(self.engine._entry_tick_result(self.client, self.state, "close", True), 1)
        self.sdk.cancel_order.assert_called_once_with("2")
        self.assertEqual(self.sdk.place_order.call_count, 2)
        self.book["2"]["orderStatus"] = "CANCELLED"
        self.details["2"] = self.book["2"].copy()
        self.assertEqual(self.engine.cmd_tick(policy=self.engine.RolePolicy(trade=True)), 0)
        self.assertEqual(self.sdk.place_order.call_args.kwargs["quantity"], 3)
        journal, = json.loads(self.engine.JOURNAL_FILE.read_text())
        self.assertEqual((journal["quantity"], journal["execution_source"]), (5, "MIXED"))

    def test_outage_leaves_intent_and_alert_without_stop_or_exit(self):
        self.fills_at_send = self.lose_stop = True

        def send(**params):
            if params["order_type"] == self.sdk.SL:
                self.sdk.get_order_by_id.side_effect = TimeoutError
                raise TimeoutError
            return self.place(**params)

        self.sdk.place_order.side_effect = send
        self.assertEqual(self.start(), "pending")
        self.assertEqual(self.engine.cmd_tick(policy=self.engine.RolePolicy(trade=True)), 1)
        self.assertNotIn("stop_loss", self.engine.load_state()["active_trade"])
        self.assertEqual(self.sdk.place_order.call_count, 2)
        self.sdk.cancel_order.assert_not_called()
        self.assertTrue(json.loads(self.engine.EXIT_LEDGER.read_text())["alerts"])

    def test_monitor_rejection_retains_price_then_retries_confirmed_amendment_once(self):
        self.fills_at_send = True
        self.assertEqual(self.start(), "ready")
        self.monitor_price = 108.125
        self.sdk.modify_order.side_effect = [
            {"status": "failure", "data": {"orderId": "2", "orderStatus": "REJECTED"}},
            self.response({"orderId": "2"}),
        ]
        self.monitor()
        self.assertEqual(self.state["active_trade"]["stop_loss"], 95.0)
        self.assertNotIn("stop_amendment", self.state["active_trade"])
        self.monitor()
        self.assertEqual(self.state["active_trade"]["stop_loss"], 95.0)
        self.monitor()
        self.assertEqual(self.sdk.modify_order.call_count, 2)
        self.apply_modify()
        self.monitor()
        self.monitor()
        self.assertEqual(self.state["active_trade"]["stop_loss"], 104.125)
        self.assertNotIn("stop_amendment", self.state["active_trade"])
        self.assertEqual(self.sdk.modify_order.call_count, 2)

    def test_monitor_lost_amend_ack_survives_restart(self):
        self.fills_at_send = True
        self.side = "SELL"
        self.assertEqual(self.start(), "ready")
        self.monitor_price = 82
        self.sdk.modify_order.side_effect = TimeoutError
        self.monitor()
        self.assertEqual(self.state["active_trade"]["stop_loss"], 105.0)
        self.resume()
        self.monitor()
        self.sdk.modify_order.assert_called_once()
        write = self.sdk.modify_order.call_args.kwargs
        self.assertEqual((write["trigger_price"], write["price"], write["quantity"]), (99.75, 101.75, 5))
        self.apply_modify()
        self.monitor()
        self.assertEqual(self.state["active_trade"]["stop_loss"], 99.75)
        self.assertNotIn("stop_amendment", self.state["active_trade"])

    def apply_modify(self):
        write = self.sdk.modify_order.call_args.kwargs
        self.book["2"].update(price=write["price"], triggerPrice=write["trigger_price"])
        self.details["2"] = self.book["2"].copy()

    def monitor(self):
        return self.engine.monitor_active_trade_loop(self.client, self.state, max_seconds=1, quiet_scan=True)

    def test_direct_stop_lost_ack_is_recovered_by_same_intent(self):
        self.net, self.lose_stop = 5, True
        self.book["opening"] = dict(self.position(), orderId="opening", orderStatus="TRADED",
                                    transactionType="BUY", filledQty=5, quantity=5, averageTradedPrice=100)
        self.details["opening"] = self.book["opening"].copy()
        for _ in range(2):
            client = self.engine.SurvivorDhanClient(policy=self.engine.RolePolicy(trade=True))
            result = client.place_protective_sl_order("23", "NSE_EQ", "SELL", 5, 94.96)
            self.assertEqual((result["placed"], result["status"], result["filled_qty"]), ("2", "RESTING", 5))
        self.sdk.place_order.assert_called_once()

    def option_tick(self, signal, symbol):
        self.segment, self.fills_at_send = "NSE_FNO", True
        self.monitor_price = 106.125
        engine = self.engine
        self.sdk.get_fund_limits.return_value = self.response({"availabelBalance": 30000})
        (self.directory / ".tick_size_cache.json").write_text(
            json.dumps({"NSE_FNO:23": {"lot_size": 3, "tick_size": 0.125}}))
        self.entry_quote = 100.125
        option = {"security_id": "23", "symbol": symbol, "lot_size": 5, "ltp": 100,
                  "ask": 100.18, "expiry": "2026-11-24"}
        with (patch.object(engine, "INDEX_UNIVERSE", {"fixture": {
                    "security_id": "22", "lot_size": 5, "symbol_prefix": "FIX"}}),
              patch.object(engine, "evaluate_index_signal", return_value={"signal": signal, "reason": "fixture"}),
              patch.object(engine.SurvivorDhanClient, "get_nearest_nifty_option", return_value=option) as pick,
              patch.object(engine, "verify_option_chart_momentum", return_value=(True, "momentum"))):
            def modify(**kwargs):
                self.apply_modify()
                return self.response({"orderId": kwargs["order_id"]})

            self.sdk.modify_order.side_effect = modify
            engine.cmd_tick(policy=engine.RolePolicy(trade=True), quiet_scan=True)
        self.assertEqual(pick.call_args.args[0], signal)
        entry, stop = [call.kwargs for call in self.sdk.place_order.call_args_list]
        self.assertEqual((entry["price"], entry["quantity"], entry["transaction_type"]), (100.125, 3, "BUY"))
        self.assertEqual((stop["quantity"], stop["transaction_type"]), (3, "SELL"))
        self.sdk.modify_order.assert_called_once()
        active = engine.load_state()["active_trade"]
        self.assertEqual((active["symbol"], active["stop_loss"], active["quantity"]), (symbol, 102.125, 3))

    def test_call_tick_entry_stop_and_trail_use_contract_metadata(self):
        self.option_tick("LONG", "STAGED CE")

    def test_put_tick_entry_stop_and_trail_use_contract_metadata(self):
        self.option_tick("SHORT", "STAGED PE")

    def test_restart_after_stop_ack_persistence_failure(self):
        import verified_exit

        self.fills_at_send = True
        persist = verified_exit.persist

        def crash(path, state):
            pending = (state.get("cycle") or {}).get("pending")
            if pending and pending.get("protection") and pending.get("oid"):
                raise OSError("simulated crash after stop send")
            persist(path, state)

        with patch.object(verified_exit, "persist", side_effect=crash):
            self.assertEqual(self.start(), "pending")
        self.assertNotIn("sl_order_id", self.engine.load_state()["active_trade"])
        self.assertEqual(self.resume(), "ready")
        self.assertEqual(self.sdk.place_order.call_count, 2)
        self.assertEqual(self.state["daily_trades_count"], 1)

    def test_restart_after_atomic_active_trade_publication(self):
        import verified_exit

        self.fills_at_send = True
        persist = verified_exit.persist

        def crash(path, state):
            if (state.get("cycle") or {}).get("protected"):
                raise OSError("simulated crash after state handoff")
            persist(path, state)

        with patch.object(verified_exit, "persist", side_effect=crash):
            self.assertEqual(self.start(), "pending")
        self.assertEqual(self.engine.load_state()["active_trade"]["sl_order_id"], "2")
        self.assertEqual(self.resume(), "ready")
        self.assertEqual(self.sdk.place_order.call_count, 2)
        self.assertEqual(self.state["daily_trades_count"], 1)

    def test_verified_working_entry_can_chase_one_tick_aligned_price(self):
        def amend(**kwargs):
            self.assertEqual(kwargs["order_id"], "1")
            self.assertEqual(kwargs["price"], 101.5)
            self.assertEqual(kwargs["quantity"], 5)
            self.terminal_entry("TRADED", 5, 5)
            return self.response({"orderId": "1"})

        with patch.object(self.engine, "_get_latest_tick_ltp", return_value=101):
            self.sdk.modify_order.side_effect = amend
            self.assertEqual(self.start(), "ready")
        self.sdk.modify_order.assert_called_once()
        self.sdk.cancel_order.assert_not_called()

    def test_lot_rounding_for_entry_and_bad_residual_closes_at_confirmed_net(self):
        self.lot_size = 2
        self.start()
        self.assertEqual(self.sdk.place_order.call_args.kwargs["quantity"], 4)
        self.terminal_entry("CANCELLED", 3, 3)
        self.assertEqual(self.resume(), "close")
        self.assertEqual(self.engine._entry_tick_result(self.client, self.state, "close", True), 0)
        self.assertEqual(self.sdk.place_order.call_args.kwargs["quantity"], 3)
        self.assertEqual(self.sdk.place_order.call_args.kwargs["order_type"], "MARKET")

    def test_monitor_stop_outage_retains_verified_history_and_escalates(self):
        self.fills_at_send = True
        self.assertEqual(self.start(), "ready")
        self.monitor_price = 108.125
        self.sdk.get_order_by_id.side_effect = TimeoutError
        self.assertEqual(self.monitor(), 1)
        self.assertEqual(self.engine.load_state()["active_trade"]["stop_loss"], 95.0)
        self.sdk.modify_order.assert_not_called()
        self.sdk.cancel_order.assert_not_called()
        self.assertEqual(self.sdk.place_order.call_count, 2)
        self.assertTrue(json.loads(self.engine.EXIT_LEDGER.read_text())["alerts"])

    def test_monitor_cancelled_stop_uses_verified_close(self):
        self.fills_at_send = True
        self.assertEqual(self.start(), "ready")
        self.book["2"]["orderStatus"] = "CANCELLED"
        self.details["2"] = self.book["2"].copy()
        self.assertEqual(self.monitor(), 0)
        self.assertIsNone(self.engine.load_state()["active_trade"])
        self.sdk.modify_order.assert_not_called()
        self.assertEqual(self.sdk.place_order.call_args.kwargs["order_type"], "MARKET")
