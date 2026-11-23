#!/usr/bin/env python3
"""
Conventional regression suite for the survivor-engine safety patches (P1-P8).

These tests assert the REQUIRED SAFE BEHAVIOR — they pass only after the patches
are implemented. They are NOT bug-reproduction tests (the baseline audit in
../test_known_issues.py is preserved untouched for that, and its verdicts must
flip from CONFIRMED to REFUTED on the patched engine).

Covered: partial fills, timeouts, duplicate ticks, rejected exits, restart recovery.
Mocked broker only — no network, no real orders, no credentials.
"""
import sys, os, json, time, types, tempfile, subprocess, threading, unittest
import logging
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

APP = Path(__file__).resolve().parents[1] / "app"
sys.path.insert(0, str(APP))

import pandas as pd
with patch("logging.FileHandler", return_value=logging.NullHandler()):
    import survivor_engine as se
from cloud_guard import LIVE_SWITCH, READ_ONLY, ReadOnlyBroker, RolePolicy

if Path(se.__file__).resolve() != APP / "survivor_engine.py":
    raise ImportError(f"Unexpected survivor_engine import: {se.__file__}")

TICK_POLICY = RolePolicy(renew_token=True, trade=True)

REAL_DT = se.datetime
NOW = {"dt": REAL_DT(2026, 10, 28, 10, 30, 0, tzinfo=se.IST)}   # mutable per-test
TODAY = "2026-10-28"

class FixedDatetime(REAL_DT):
    @classmethod
    def now(cls, tz=None):
        base = NOW["dt"]
        if tz is None:
            return base.replace(tzinfo=None)
        return base if tz is se.IST else base.astimezone(tz)

RealClient = se.SurvivorDhanClient


def redirect_paths(tmp):
    se.BASE_DIR = tmp
    se.EXIT_LEDGER = os.path.join(tmp, "survivor_exits.json")
    se.STATE_FILE = os.path.join(tmp, "survivor_state.json")
    se.JOURNAL_FILE = os.path.join(tmp, "survivor_trades.json")
    se.OBITUARY_FILE = os.path.join(tmp, "survivor_obituary.md")
    se._STATE_LOCK_FILE = os.path.join(tmp, ".survivor_state.flock")

def bars(n, close, volume):
    t0 = NOW["dt"].timestamp() - n * 60
    return {"status": "success", "data": {
        "open": [close]*n, "high": [close]*n, "low": [close]*n,
        "close": [close]*n, "volume": [volume]*n,
        "timestamp": [t0 + i*60 for i in range(n)]}}

def active_trade(**over):
    t = {
        "weapon": "OPTIONS", "symbol": "NIFTY 2026-10-29 25000 CE",
        "security_id": "99991", "exchange_segment": "NSE_FNO",
        "instrument_type": "OPTIDX", "direction": "LONG",
        "underlying_direction": "LONG", "underlying_index": "Nifty 50",
        "quantity": 65, "entry_price": 100.0, "stop_loss": 90.0,
        "target_price": 120.0, "entry_time": f"{TODAY}T10:00:00+05:30",
        "order_id": "ORD-BUY-1", "sl_order_id": "ORD-SL-1",
        "reason": "regression", "live": True,
    }
    t.update(over); return t

def seed_state(extra=None):
    st = {
        "status": "ALIVE", "mortality_armed": True,
        "seed_capital": 30000.0, "death_floor": 3300.0,
        "peak_equity": 30000.0, "current_equity": 30000.0,
        "available_cash": 30000.0, "unrealized_pnl": 0.0,
        "realized_pnl_total": 0.0,
        "daily_start_equity": 30000.0, "daily_date": TODAY,
        "daily_trades_count": 0, "daily_consecutive_losses": 0,
        "daily_halted": False, "daily_halt_reason": "",
        "active_trade": None, "total_trades": 0, "wins": 0, "losses": 0,
    }
    if extra: st.update(extra)
    with open(se.STATE_FILE, "w") as f: json.dump(st, f)

def journal():
    if not os.path.exists(se.JOURNAL_FILE): return []
    with open(se.JOURNAL_FILE) as f: return json.load(f)

class MockDhan:
    """Broker mock with a fill simulator: TRADED market orders close positions (truth)."""
    BUY, SELL, SL, LIMIT, MARKET, INTRA, DAY = (
        "BUY", "SELL", "STOP_LOSS", "LIMIT", "MARKET", "INTRADAY", "DAY")
    def __init__(self):
        self.calls = []
        self.funds = {"status": "success", "data": {"availabelBalance": 30000.0, "utilizedAmount": 0.0}}
        self.real_positions = []       # broker truth
        self.positions_resp = None     # verbatim override (dict or Exception)
        self.orders = []               # get_order_list book (for finalize reconciliation)
        self.status_queue = []         # FIFO statuses for new protective and market orders
        self.fills_update_positions = True
        self.quote_after_stop = False
        self.funds_gate = None         # threading.Event: block inside get_fund_limits
        self.candles, self.expiry_resp, self.option_chain_resp, self.security_master = {}, None, None, None
        self.place_order_result = None
        self.buy_fill_qty = {}      # security_id -> netQty that appears when a BUY entry is placed
        self.fill_price = 100.0
        self._oid = 0
        self._by_oid = {}
    # ── reads
    def get_fund_limits(self):
        self.calls.append(("get_fund_limits",))
        if self.funds_gate is not None: self.funds_gate.wait(timeout=30)
        return self.funds
    def get_positions(self):
        self.calls.append(("get_positions",))
        if isinstance(self.positions_resp, Exception): raise self.positions_resp
        if self.positions_resp is not None: return self.positions_resp
        return {"status": "success", "data": list(self.real_positions)}
    def get_order_list(self):
        self.calls.append(("get_order_list",))
        return {"status": "success", "data": [row.copy() for row in self.orders + list(self._by_oid.values())]}
    def get_order_by_id(self, oid):
        self.calls.append(("get_order_by_id", oid))
        row = self._by_oid.get(oid)
        if row is None:
            row = next((order for order in self.orders if order["orderId"] == oid), None)
        return {"status": "success", "data": row.copy() if row else {"orderStatus": "PENDING"}}
    def quote_data(self, securities):
        segment, (sid,) = next(iter(securities.items()))
        candles = self.candles.get(str(sid))
        has_exposure = any(str(row["securityId"]) == str(sid) and row["netQty"]
                           for row in self.real_positions)
        use_candle = candles and has_exposure and (not self.quote_after_stop or self.placed(self.SL))
        price = candles["data"]["close"][-1] if use_candle else self.fill_price
        return {"status": "success", "data": {"data": {segment: {str(sid): {
            "last_price": price, "last_trade_time": NOW["dt"].isoformat(),
        }}}}}
    def intraday_minute_data(self, security_id=None, **kw):
        return self.candles.get(str(security_id), {"status": "failure", "data": None})
    def expiry_list(self, **kw): return self.expiry_resp or {"status": "failure", "data": None}
    def option_chain(self, **kw):
        self.calls.append(("option_chain", kw))
        return self.option_chain_resp or {"status": "failure", "data": None}
    def fetch_security_list(self, mode="compact"): return self.security_master
    # ── writes (blocked by ReadOnlyBroker in staging; recorded here)
    def place_order(self, **kw):
        self.calls.append(("place_order", kw))
        if self.place_order_result is not None: return self.place_order_result
        self._oid += 1
        oid = f"ORD-{self._oid}"
        sid = str(kw["security_id"])
        kind = kw["order_type"]
        quantity = kw["quantity"]
        filled = 0
        status = "PENDING"
        price = self.fill_price
        if kind == self.LIMIT:
            filled = self.buy_fill_qty.get(sid, 0)
            if filled:
                status = "TRADED" if filled == quantity else "CANCELLED"
                self.real_positions = [{"securityId": sid, "netQty": filled,
                                        "exchangeSegment": kw["exchange_segment"],
                                        "productType": kw["product_type"]}]
        elif self.status_queue:
            status = self.status_queue.pop(0)
            if status == "TRADED":
                filled = quantity
                for position in self.real_positions:
                    if str(position["securityId"]) == sid:
                        candles = self.candles.get(sid)
                        if candles:
                            price = candles["data"]["close"][-1]
                        if self.fills_update_positions and kind == self.MARKET:
                            position["netQty"] = 0
        self._by_oid[oid] = {
            "orderId": oid, "orderStatus": status, "securityId": sid,
            "exchangeSegment": kw["exchange_segment"], "productType": kw["product_type"],
            "transactionType": kw["transaction_type"], "quantity": quantity, "orderType": kind,
            "averageTradedPrice": price if filled else 0.0, "filledQty": filled,
            "createTime": (NOW["dt"] + timedelta(seconds=self._oid)).isoformat(),
            "correlationId": kw["tag"], "price": kw["price"],
            "triggerPrice": kw.get("trigger_price", 0),
        }
        return {"status": "success", "data": {"orderId": oid}}
    def cancel_order(self, oid):
        self.calls.append(("cancel_order", oid))
        for row in [*self.orders, *self._by_oid.values()]:
            if row["orderId"] == oid:
                row["orderStatus"] = "CANCELLED"
        return {"status": "success"}
    def modify_order(self, **kw): self.calls.append(("modify_order", kw)); return {"status": "success"}
    # ── helpers
    def writes(self): return [c for c in self.calls if c[0] in ("place_order", "cancel_order", "modify_order")]
    def placed(self, order_type=None):
        return [w for w in self.writes() if w[0] == "place_order"
                and (order_type is None or w[1].get("order_type") == order_type)]

def make_client(mock, *, policy=TICK_POLICY):
    c = object.__new__(RealClient)
    c._policy = policy
    c.dhan = ReadOnlyBroker(mock, policy=policy)
    c.tm = types.SimpleNamespace(get_token=lambda: "T", client_id="C",
                                 get_dhan_context=lambda: {}, access_token="T", token_expiry=None)
    return c

def real_option_chain(expiry="2026-10-29", last=25000.0, ltp=100.0):
    return {"status": "success", "data": {"data": {
        "last_price": last,
        "oc": {"25000": {"ce": {
            "last_price": ltp, "top_bid_price": ltp - 1.0, "top_ask_price": ltp + 1.0,
            "security_id": "99991", "lot_size": 65, "oi": 5000, "volume": 20000,
            "greeks": {"delta": 0.50, "theta": -5.0}, "implied_volatility": 15.0}}}}}}

class SafeBehaviorRegression(unittest.TestCase):
    def setUp(self):
        self.tmp = self.enterContext(tempfile.TemporaryDirectory(prefix="survivor_regr_"))
        paths = ("BASE_DIR", "EXIT_LEDGER", "STATE_FILE", "JOURNAL_FILE",
                 "OBITUARY_FILE", "_STATE_LOCK_FILE")
        for name in paths:
            self.addCleanup(setattr, se, name, getattr(se, name))
        redirect_paths(self.tmp)
        (Path(self.tmp) / "exchange_calendar.json").write_text(json.dumps({
            "source": "Regression exchange calendar fixture",
            "sessions": {day: ["09:15", "15:30"] for day in (TODAY, "2026-10-29")},
        }))
        NOW["dt"] = REAL_DT(2026, 10, 28, 10, 30, 0, tzinfo=se.IST)
        se.datetime = FixedDatetime
        self.enterContext(patch.object(se, "send_telegram", return_value=True))
        # speed: collapse the engine's wall-clock waits (patched engine reads these)
        for k, v in [("ENTRY_SETTLE_POLL_S", 0.001), ("ENTRY_SETTLE_TRIES", 3),
                     ("FLATTEN_POLL_S", 0.001), ("FLATTEN_POLL_N", 3),
                     ("MONITOR_POLL_S", 0.01)]:
            self.enterContext(patch.object(se, k, v))
        # snapshot module globals we may patch per-test
        self._saved = {k: getattr(se, k) for k in
                       ("SurvivorDhanClient", "evaluate_index_signal",
                        "verify_option_chart_momentum", "scan_equity_candidates",
                        "compute_indicators")}
        self.mock = MockDhan()
        self.constructor_policy = threading.local()
        se.SurvivorDhanClient = self.make_tick_client
        seed_state()
    def tearDown(self):
        for k, v in self._saved.items(): setattr(se, k, v)
        se.datetime = REAL_DT
    # ── helpers
    def enable_long_signal(self):
        self.mock.candles["25"] = bars(60, 25000.0, 1000)
        se.evaluate_index_signal = lambda df: {"signal": "LONG", "confidence": 99.0,
                                               "reason": "regression setup", "setup": "FAST_STRIKE"}
        se.verify_option_chart_momentum = lambda client, sid: (True, "mock momentum")
        self.mock.expiry_resp = {"status": "success", "data": {"data": ["2026-10-29"]}}
        self.mock.option_chain_resp = real_option_chain()
    def seed_broker_trade(self, trade, *, stop_status=None, exit_price=None):
        sid = trade["security_id"]
        segment = trade["exchange_segment"]
        qty = trade["quantity"]
        side = "SELL" if trade["weapon"] == "EQUITY" and trade["direction"] == "SHORT" else "BUY"
        opposite = "BUY" if side == "SELL" else "SELL"
        entry_time = REAL_DT.fromisoformat(trade["entry_time"])
        instrument = {"securityId": sid, "exchangeSegment": segment, "productType": "INTRADAY"}
        self.mock.real_positions = [
            dict(instrument, netQty=0 if exit_price is not None else qty if side == "BUY" else -qty)
        ]
        self.mock.orders = [dict(
            instrument, orderId=trade["order_id"], orderStatus="TRADED",
            transactionType=side, orderType="LIMIT", quantity=qty,
            filledQty=qty, averageTradedPrice=trade["entry_price"],
            createTime=entry_time.isoformat(),
        )]
        if stop_status is not None:
            prices = se.stop_prices(trade["stop_loss"], 0.05, opposite)
            self.mock.orders.append(dict(
                instrument, orderId=trade["sl_order_id"], orderStatus=stop_status,
                transactionType=opposite, orderType="STOP_LOSS", quantity=qty,
                filledQty=qty if exit_price is not None else 0,
                averageTradedPrice=exit_price or 0.0,
                createTime=(entry_time + timedelta(seconds=1)).isoformat(),
                price=prices["price"], triggerPrice=prices["trigger_price"],
            ))

    def make_tick_client(self, *, policy):
        self.constructor_policy.received.append(policy)
        self.assertIs(policy, TICK_POLICY, "cmd_tick must forward its trading policy")
        return make_client(self.mock, policy=policy)

    def run_cmd_tick(self, *, expects_client=True):
        self.constructor_policy.received = []
        se.cmd_tick(policy=TICK_POLICY, quiet_scan=False)
        self.assertEqual(self.constructor_policy.received, [TICK_POLICY] if expects_client else [])


    # ═══ P1: fail-closed position-status handling ═══
    def test_p1_position_api_failure_does_not_close_trade(self):
        seed_state({"active_trade": active_trade()})
        self.seed_broker_trade(active_trade())
        self.mock.positions_resp = {"status": "failure", "remarks": {"error_code": "DH-902"}, "data": []}
        c = make_client(self.mock)
        st = se.load_state()
        se.monitor_active_trade_loop(c, st, live=True, max_seconds=0.5, quiet_scan=True)
        self.assertIsNotNone(st["active_trade"], "failure response must NOT finalize the trade")
        self.assertEqual(journal(), [], "no journal entry may be written on unverifiable positions")
        self.assertEqual(self.mock.writes(), [], "no exit order may be placed on unverifiable positions")
        self.assertGreaterEqual(st.get("pos_check_failures", 0), 1)

    def test_p1_position_api_exception_does_not_close_trade(self):
        seed_state({"active_trade": active_trade()})
        self.seed_broker_trade(active_trade())
        self.mock.positions_resp = ConnectionError("broker unreachable")
        c = make_client(self.mock)
        st = se.load_state()
        se.monitor_active_trade_loop(c, st, live=True, max_seconds=0.5, quiet_scan=True)
        self.assertIsNotNone(st["active_trade"])
        self.assertEqual(journal(), [])

    def test_p1_genuine_exchange_sl_fill_still_detected(self):
        seed_state({"active_trade": active_trade()})
        self.seed_broker_trade(active_trade(), stop_status="TRADED", exit_price=90.0)
        self.mock.positions_resp = {"status": "success", "data": []}
        c = make_client(self.mock)
        st = se.load_state()
        se.monitor_active_trade_loop(c, st, live=True, max_seconds=0.5, quiet_scan=True)
        self.assertIsNone(st["active_trade"], "verified-flat position must finalize the trade")
        self.assertEqual(journal()[-1]["exit_reason"], "EXCHANGE_SL_FILLED")

    def test_p1_consecutive_failures_block_new_entries(self):
        self.enable_long_signal()
        trade = active_trade()
        seed_state({"active_trade": trade})
        self.seed_broker_trade(trade, stop_status="PENDING")
        self.mock.positions_resp = {"status": "failure", "data": []}
        c = make_client(self.mock)
        se.monitor_active_trade_loop(c, se.load_state(), live=True, max_seconds=0.4, quiet_scan=True)

        for restart in (False, True):
            with self.subTest(restart=restart):
                if restart:
                    self.run_cmd_tick()
                persisted = se.load_state()["active_trade"]
                self.assertEqual(persisted["order_id"], trade["order_id"])
                self.assertTrue(persisted.get("flatten_pending"), "flatten_pending must persist")
                recovery = json.loads(Path(se.EXIT_LEDGER).read_text())["cycle"]
                self.assertTrue(recovery["flatten"])
                self.assertTrue(recovery["unresolved"])
                self.assertEqual(self.mock.real_positions[0]["netQty"], trade["quantity"])
                self.assertEqual(journal(), [])
                self.assertEqual(self.mock.placed("LIMIT"), [])
                self.assertEqual(self.mock.writes(), [])

    # ═══ P2: fill-confirmed entries, timeouts, partial fills ═══
    def test_p2_entry_timeout_cancels_pending_and_aborts(self):
        self.enable_long_signal()
        self.mock.real_positions = []            # entry never fills
        self.run_cmd_tick()
        self.assertEqual(self.mock.placed("STOP_LOSS"), [], "no SL may be placed without a confirmed fill")
        self.assertEqual(len(self.mock.placed("LIMIT")), 1, "entry was submitted")
        cancels = [w for w in self.mock.writes() if w[0] == "cancel_order"]
        self.assertEqual(len(cancels), 1, "pending entry must be cancelled on timeout")
        st = json.load(open(se.STATE_FILE))
        self.assertIsNone(st["active_trade"], "unfilled entry must not become an active trade")
        self.assertEqual(st["daily_trades_count"], 0, "aborted entry must not consume the daily count")
        self.assertEqual(journal(), [])

    def test_p2_partial_fill_sizes_sl_and_journal_to_filled_qty(self):
        # Use the normal entry workflow with two 25-unit lots. The option
        # scanner submits one lot, so its partial fills cannot be whole lots.
        trade = active_trade(quantity=50, lot_size=25, tick_size=0.05, expiry="2026-10-29")
        del trade["order_id"], trade["sl_order_id"]
        self.mock.buy_fill_qty = {"99991": 25}
        self.mock.quote_after_stop = True
        self.mock.status_queue = ["PENDING", "TRADED"]
        self.mock.candles["99991"] = bars(30, 80.0, 100)

        client = make_client(self.mock)
        state = se.load_state()
        outcome = se._start_entry(client, state, trade, "BUY", "LIMIT", 100.0)
        self.assertEqual(outcome, "ready")
        entry, = self.mock.placed("LIMIT")
        self.assertEqual(entry[1]["quantity"], 50)
        se._entry_tick_result(client, state, outcome, quiet_scan=True)
        sl_orders = self.mock.placed("STOP_LOSS")
        self.assertEqual(len(sl_orders), 1)
        self.assertEqual(sl_orders[0][1]["quantity"], 25, "SL quantity must match the FILLED quantity")
        st = json.load(open(se.STATE_FILE))
        self.assertIsNone(st["active_trade"], "trade closes within the tick (SL hit) — quantity evidence is in the journal")
        self.assertEqual(st["daily_trades_count"], 1)
        j = journal()
        self.assertEqual(j[-1]["quantity"], 25, "journal must record the filled quantity")
        self.assertAlmostEqual(j[-1]["gross_pnl"], (80.0 - 100.0) * 25, places=2,
                               msg="journal P&L must scale with FILLED lots, not requested lots")

    # ═══ P3: rejected SL / rejected exits ═══
    def test_p3_sl_triple_rejected_flattens_position(self):
        self.enable_long_signal()
        self.mock.buy_fill_qty = {"99991": 65}     # entry fills fully
        self.mock.status_queue = ["REJECTED", "TRADED"]
        tick_size = 0.05
        tick = Decimal(str(tick_size))
        (Path(self.tmp) / ".tick_size_cache.json").write_text(json.dumps({
            "NSE_FNO:99991": {"tick_size": tick_size},
        }))
        start_entry = se._start_entry

        def start_with_unaligned_stop(client, state, trade, side, order_type, price):
            self.assertEqual(trade["tick_size"], tick_size)
            # Exercise protection rounding after the signal path's own rounding.
            trade = dict(trade, stop_loss=90.073)
            return start_entry(client, state, trade, side, order_type, price)

        with patch.object(se, "_start_entry", start_with_unaligned_stop):
            self.run_cmd_tick()
        (_, submitted_stop), = self.mock.placed("STOP_LOSS")
        self.assertEqual(Decimal(str(submitted_stop["trigger_price"])) % tick, 0,
                         "submitted STOP_LOSS trigger must align to the trade tick")
        self.assertEqual(Decimal(str(submitted_stop["price"])) % tick, 0,
                         "submitted STOP_LOSS limit must align to the trade tick")
        stop, = (row for row in self.mock._by_oid.values() if row["orderType"] == "STOP_LOSS")
        self.assertEqual(stop["orderStatus"], "REJECTED")
        self.assertEqual(stop["filledQty"], 0)
        self.assertEqual(len(self.mock.placed("MARKET")), 1, "unprotected position must be market-flattened")
        self.assertEqual(self.mock.real_positions[0]["netQty"], 0, "position must be closed (broker truth)")
        st = json.load(open(se.STATE_FILE))
        self.assertIsNone(st["active_trade"])
        closed, = journal()
        self.assertEqual(closed["exit_trigger"], "UNPROTECTED_POSITION_EXIT")
        self.assertTrue(closed["fills_verified"])
        self.assertEqual(closed["quantity"], 65)
        exit_fill, = closed["exit_proof"]["exits"]
        self.assertEqual(exit_fill["side"], "SELL")
        self.assertEqual(exit_fill["quantity"], 65)

    def test_p3_flatten_rejected_then_filled_on_retry(self):
        self.seed_broker_trade(active_trade())
        self.mock.status_queue = ["REJECTED", "TRADED"]
        c = make_client(self.mock)
        actions = c.emergency_flatten_all()
        self.assertEqual(self.mock.real_positions[0]["netQty"], 0, "retry must close the position")
        self.assertEqual(len(self.mock.placed("MARKET")), 2)
        rejected, filled = self.mock._by_oid.values()
        self.assertEqual(rejected["orderStatus"], "REJECTED")
        self.assertEqual(filled["filledQty"], 65)
        self.assertTrue(actions.flat)
        self.assertEqual(actions.exit_ids, (filled["orderId"],))

    def test_p3_flatten_persistently_rejected_reports_failure(self):
        self.seed_broker_trade(active_trade())
        self.mock.status_queue = ["REJECTED"] * 9
        c = make_client(self.mock)
        actions = c.emergency_flatten_all()
        self.assertNotEqual(self.mock.real_positions[0]["netQty"], 0, "position must remain open")
        self.assertFalse(actions.flat)
        self.assertTrue(self.mock.placed("MARKET"), "flatten must attempt a broker exit")
        self.assertTrue(all(row["orderStatus"] == "REJECTED" for row in self.mock._by_oid.values()))
        recovery = json.loads(Path(se.EXIT_LEDGER).read_text())["cycle"]
        self.assertTrue(recovery["unresolved"])

    # ═══ P4: halt + verified flatten ═══
    def test_p4_halt_with_verified_flat_clears_trade(self):
        self.mock.funds = {"status": "success", "data": {"availabelBalance": 24000.0, "utilizedAmount": 0.0}}
        self.seed_broker_trade(active_trade())
        self.mock.status_queue = ["TRADED"]      # flatten fills
        seed_state({"daily_start_equity": 30000.0, "active_trade": active_trade()})
        self.run_cmd_tick()
        st = json.load(open(se.STATE_FILE))
        self.assertTrue(st["daily_halted"])
        self.assertIsNone(st["active_trade"], "trade may only be cleared after verified flat")
        self.assertEqual(self.mock.real_positions[0]["netQty"], 0)

    def test_p4_halt_flatten_failure_retains_trade_as_pending(self):
        self.mock.funds = {"status": "success", "data": {"availabelBalance": 24000.0, "utilizedAmount": 0.0}}
        self.seed_broker_trade(active_trade())
        self.mock.status_queue = ["REJECTED"] * 9   # flatten persistently rejected
        seed_state({"daily_start_equity": 30000.0, "active_trade": active_trade()})
        self.run_cmd_tick()
        st = json.load(open(se.STATE_FILE))
        self.assertTrue(st["daily_halted"], "halt must engage regardless of flatten success")
        self.assertIsNotNone(st["active_trade"], "unverified flatten must NOT clear the trade")
        self.assertTrue(st["active_trade"].get("flatten_pending"),
                        "failed flatten must mark the trade flatten_pending for retry")
        self.assertNotEqual(self.mock.real_positions[0]["netQty"], 0)

    def test_p4_flatten_pending_retried_and_completed_next_tick(self):
        self.mock.funds = {"status": "success", "data": {"availabelBalance": 24000.0, "utilizedAmount": 0.0}}
        self.seed_broker_trade(active_trade())
        seed_state({"daily_start_equity": 30000.0,
                    "active_trade": active_trade(flatten_pending=True, flatten_reason="HALT_FLATTENED"),
                    "daily_halted": True,
                    "daily_halt_reason": "Daily loss limit hit"})
        self.mock.status_queue = ["TRADED"]      # retry succeeds
        self.run_cmd_tick()
        st = json.load(open(se.STATE_FILE))
        self.assertEqual(self.mock.real_positions[0]["netQty"], 0, "pending flatten must be retried")
        self.assertIsNone(st["active_trade"], "retried flatten completes the trade")
        self.assertEqual(journal()[-1]["exit_reason"], "HALT_FLATTENED")

    # ═══ P5: single-owner engine lock / duplicate ticks ═══
    def test_p5_duplicate_tick_skipped_while_owner_runs(self):
        self.mock.funds_gate = threading.Event()
        results = {}
        def tick_a():
            try: self.run_cmd_tick()
            except Exception as e: results["a"] = e
        def tick_b():
            try: self.run_cmd_tick(expects_client=False)
            except Exception as e: results["b"] = e
        ta = threading.Thread(target=tick_a); ta.start()
        deadline = time.time() + 5
        while time.time() < deadline and not any(c[0] == "get_fund_limits" for c in self.mock.calls):
            time.sleep(0.01)
        self.assertTrue(any(c[0] == "get_fund_limits" for c in self.mock.calls), "tick A never started")
        tb = threading.Thread(target=tick_b); tb.start(); tb.join(timeout=10)
        self.assertFalse(tb.is_alive(), "duplicate tick must return promptly")
        self.assertNotIn("b", results, f"duplicate tick errored: {results.get('b')}")
        self.assertEqual(len([c for c in self.mock.calls if c[0] == "get_fund_limits"]), 1,
                         "duplicate tick must not reach the trading logic")
        self.assertEqual(self.mock.writes(), [])
        self.mock.funds_gate.set(); ta.join(timeout=15)
        self.assertFalse(ta.is_alive())

    def test_p5_stale_engine_lock_recovered(self):
        p = subprocess.Popen(["/bin/sleep", "0.05"]); p.wait()   # now-dead pid
        lock = os.path.join(self.tmp, ".survivor_engine.lock")
        with open(lock, "w") as f: f.write(str(p.pid))
        self.run_cmd_tick()   # must recover the stale lock and proceed
        self.assertTrue(any(c[0] == "get_fund_limits" for c in self.mock.calls),
                        "stale lock (dead owner) must not block the engine")

        for args, expected_policy in (
            (("status", "--live"), READ_ONLY),
            (("tick",), READ_ONLY),
            (("flatten", "--live"), RolePolicy(renew_token=True, flatten=True)),
        ):
            with self.subTest(command=" ".join(args)):
                received = []

                def construct_client(*, policy):
                    received.append(policy)
                    self.assertEqual(policy, expected_policy,
                                     "command must construct the client with its designated policy")
                    return make_client(self.mock, policy=policy)

                with (
                    patch.object(sys, "argv", ["survivor_engine.py", *args]),
                    patch.dict(os.environ, {LIVE_SWITCH: "1"}),
                    patch.object(se, "load_env_file", autospec=True, return_value={}),
                    patch.object(se, "SurvivorDhanClient", construct_client),
                ):
                    se.main()
                self.assertEqual(received, [expected_policy])

    # ═══ P6: hard 0DTE ban ═══
    def test_p6_no_sameday_expiry_on_primary_path(self):
        self.mock.expiry_resp = {"status": "success", "data": {"data": [TODAY]}}  # only today listed
        c = make_client(self.mock)
        res = c.get_nearest_nifty_option("LONG", 100000.0)
        self.assertIsNone(res, "same-day expiry must never be selected")
        self.assertEqual([c0 for c0 in self.mock.calls if c0[0] == "option_chain"], [],
                         "option_chain must not be queried for a 0DTE expiry")

    def test_p6_no_sameday_expiry_in_fallback(self):
        self.mock.candles["13"] = bars(60, 25000.0, 1000)
        self.mock.candles["99991"] = bars(60, 100.0, 12345)
        self.mock.security_master = pd.DataFrame([{
            "SEM_INSTRUMENT_NAME": "OPTIDX", "SEM_TRADING_SYMBOL": "NIFTY-28-OCT-2026-25000-CE",
            "SEM_OPTION_TYPE": "CE", "SEM_EXPIRY_DATE": TODAY,
            "SEM_SMST_SECURITY_ID": "99991", "SEM_STRIKE_PRICE": 25000.0}])
        c = make_client(self.mock)
        res = c._fallback_option_from_security_master("LONG", 100000.0, 13, 65, "NIFTY", TODAY)
        self.assertIsNone(res, "security-master fallback must refuse same-day expiry")

    # ═══ P7: synthetic-quote entry gate ═══
    def test_p7_synthetic_fallback_quote_never_traded(self):
        se.evaluate_index_signal = lambda df: {"signal": "LONG", "confidence": 99.0,
                                               "reason": "regression", "setup": "FAST_STRIKE"}
        se.verify_option_chart_momentum = lambda client, sid: (True, "mock")
        # optionchain unavailable -> engine falls back to security-master (synthetic quotes)
        self.mock.expiry_resp = {"status": "success", "data": {"data": ["2026-10-29"]}}
        self.mock.candles["25"] = bars(60, 25000.0, 1000)
        self.mock.candles["99991"] = bars(60, 100.0, 12345)
        self.mock.security_master = pd.DataFrame([{
            "SEM_INSTRUMENT_NAME": "OPTIDX", "SEM_TRADING_SYMBOL": "BANKNIFTY-29-OCT-2026-25000-CE",
            "SEM_OPTION_TYPE": "CE", "SEM_EXPIRY_DATE": "2026-10-29",
            "SEM_SMST_SECURITY_ID": "99991", "SEM_STRIKE_PRICE": 25000.0}])
        self.run_cmd_tick()
        self.assertEqual(self.mock.writes(), [],
                         "an entry must never be placed from fabricated fallback quotes")
        st = json.load(open(se.STATE_FILE))
        self.assertIsNone(st["active_trade"])

    def test_p7_fallback_quotes_tagged_synthetic(self):
        self.mock.candles["13"] = bars(60, 25000.0, 1000)
        self.mock.candles["99991"] = bars(60, 100.0, 12345)
        self.mock.security_master = pd.DataFrame([{
            "SEM_INSTRUMENT_NAME": "OPTIDX", "SEM_TRADING_SYMBOL": "NIFTY-29-OCT-2026-25000-CE",
            "SEM_OPTION_TYPE": "CE", "SEM_EXPIRY_DATE": "2026-10-29",
            "SEM_SMST_SECURITY_ID": "99991", "SEM_STRIKE_PRICE": 25000.0}])
        c = make_client(self.mock)
        res = c._fallback_option_from_security_master("LONG", 100000.0, 13, 65, "NIFTY", TODAY)
        self.assertIsNotNone(res)
        self.assertTrue(res.get("synthetic"), "fallback candidates must be tagged synthetic")

    # ═══ P8: remaining-risk sizing + restart recovery ═══
    def test_p8_entry_refused_when_risk_exceeds_remaining_daily_budget(self):
        self.enable_long_signal()
        self.mock.funds = {"status": "success", "data": {"availabelBalance": 29000.0, "utilizedAmount": 0.0}}
        seed_state({"daily_start_equity": 30000.0})   # -1,000 on the day; budget left = 305 < risk 725
        self.run_cmd_tick()
        self.assertEqual(self.mock.writes(), [],
                         "no entry may be taken when per-trade risk exceeds the remaining daily budget")
        st = json.load(open(se.STATE_FILE))
        self.assertIsNone(st["active_trade"])
        self.assertEqual(st["daily_trades_count"], 0)

    def test_p8_restart_reconciles_trade_closed_while_down(self):
        seed_state({"active_trade": active_trade()})
        self.seed_broker_trade(active_trade(), stop_status="TRADED", exit_price=95.0)
        self.run_cmd_tick()
        st = json.load(open(se.STATE_FILE))
        self.assertIsNone(st["active_trade"], "closed-while-down trade must be reconciled")
        j = journal()
        self.assertEqual(j[-1]["exit_trigger"], "RESTART_RECONCILED_CLOSED")
        self.assertEqual(j[-1]["exit_reason"], "EXCHANGE_SL_FILLED")
        self.assertEqual(j[-1]["execution_source"], "EXCHANGE_STOP")
        exit_fill, = j[-1]["exit_proof"]["exits"]
        self.assertEqual(exit_fill["id"], "ORD-SL-1")
        self.assertTrue(j[-1]["fills_verified"])
        self.assertEqual(self.mock.writes(), [])
        self.assertAlmostEqual(j[-1]["exit_price"], 95.0, msg="exit price must come from broker fills")

    def test_p8_restart_resumes_still_open_position(self):
        seed_state({"active_trade": active_trade()})
        self.seed_broker_trade(active_trade(), stop_status="PENDING")
        self.mock.candles["99991"] = bars(30, 80.0, 100)   # SL hit -> managed exit
        self.mock.status_queue = ["TRADED"]
        self.run_cmd_tick()
        j = journal()
        self.assertEqual(j[-1]["exit_reason"], "STOP_LOSS_HIT",
                         "still-open trade must resume being managed, not falsely finalized")
        self.assertEqual(self.mock.real_positions[0]["netQty"], 0)

    def test_p8_orphan_position_flattened_on_restart(self):
        seed_state({"active_trade": None})      # engine forgot the trade; broker still has it
        self.seed_broker_trade(active_trade())
        self.mock.status_queue = ["TRADED"]
        self.run_cmd_tick()
        self.assertEqual(self.mock.real_positions[0]["netQty"], 0, "orphan position must be flattened")
        self.assertEqual(len(self.mock.placed("MARKET")), 1)


    # ═══ P9: recovery-mode protections (green floor, setup budget, re-entry cap, RSI ceiling, stale equity) ═══

    def _journal_seed(self, entries):
        with open(se.JOURNAL_FILE, "w") as f: json.dump(entries, f)

    def _entry(self, **over):
        e = {"setup": "ORB_IGNITE", "direction": "LONG", "underlying_direction": "LONG",
             "security_id": "99991", "entry_time": f"{TODAY}T09:50:00+05:30",
             "entry_price": 100.0, "exit_time": f"{TODAY}T09:55:00+05:30",
             "net_pnl": 0.0, "gross_pnl": 0.0}
        e.update(over); return e

    def record_verified_exit(self, exit_price, reason, *, order_number, entry_minute):
        state = se.load_state()
        trade = active_trade(
            order_id=f"HIST-BUY-{order_number}", sl_order_id=f"HIST-EXIT-{order_number}",
            entry_time=f"{TODAY}T10:{entry_minute:02}:00+05:30",
        )
        state["active_trade"] = trade
        se.save_state(state)
        prior_orders = self.mock.orders
        self.seed_broker_trade(trade, stop_status="TRADED", exit_price=exit_price)
        self.mock.orders = prior_orders + self.mock.orders
        self.assertTrue(se._finalize_closed_trade(make_client(self.mock), state, trade, reason))

    def test_p9_green_day_keeps_trading_no_halt_at_500(self):
        self.record_verified_exit(110.0, "TARGET_HIT", order_number=1, entry_minute=0)
        self.record_verified_exit(102.0, "TARGET_HIT", order_number=2, entry_minute=5)
        self.record_verified_exit(105.0, "TARGET_HIT", order_number=3, entry_minute=10)
        st = se.load_state()
        self.assertFalse(st["daily_halted"], "a win on a green day must NOT halt — maximize recovery")

    def test_p9_green_floor_halts_when_banked_profit_breached(self):
        self.record_verified_exit(110.0, "TARGET_HIT", order_number=1, entry_minute=0)
        self.record_verified_exit(102.0, "TARGET_HIT", order_number=2, entry_minute=5)
        self.record_verified_exit(85.0, "STOP_LOSS_HIT", order_number=3, entry_minute=10)
        st = se.load_state()
        self.assertTrue(st["daily_halted"], "breach of the ₹500 banked-profit floor must halt the day")
        self.assertIn("Green floor", st["daily_halt_reason"])

    def test_p9_small_green_day_not_floored_keeps_trading(self):
        self.record_verified_exit(105.0, "TARGET_HIT", order_number=1, entry_minute=0)
        self.record_verified_exit(90.0, "STOP_LOSS_HIT", order_number=2, entry_minute=5)
        st = se.load_state()
        self.assertFalse(st["daily_halted"], "no halt unless the ₹500 floor was actually banked")

    def test_p9_setup_budget_blocks_third_same_setup_entry(self):
        # 2 ORB_IGNITE LONG entries done today -> a 3rd same-setup entry must be refused
        self.enable_long_signal()
        se.evaluate_index_signal = lambda df: {"signal": "LONG", "confidence": 99.0,
                                               "reason": "regression", "setup": "ORB_IGNITE"}
        self._journal_seed([self._entry(), self._entry(entry_time=f"{TODAY}T09:55:00+05:30")])
        self.run_cmd_tick()
        self.assertEqual(self.mock.writes(), [], "3rd entry on the same setup must be blocked")
        st = json.load(open(se.STATE_FILE))
        self.assertEqual(st["daily_trades_count"], 0)

    def test_p9_reentry_cap_blocks_paying_above_first_fill(self):
        # day's first fill was cheap; current LTP above it -> refuse to pay up
        self.enable_long_signal()
        self._journal_seed([self._entry(entry_price=0.05)])
        self.run_cmd_tick()
        self.assertEqual(self.mock.writes(), [], "re-entry above the day's first fill must be blocked")
        st = json.load(open(se.STATE_FILE))
        self.assertEqual(st["daily_trades_count"], 0)

    def test_p9_reentry_allows_same_or_better_price(self):
        # control: prior fill at a huge price -> cap does NOT block; entry proceeds
        self.enable_long_signal()
        self._journal_seed([self._entry(entry_price=10**9)])
        self.run_cmd_tick()
        self.assertNotEqual(self.mock.writes(), [], "entry at/below the day's first fill must proceed")

    def _signal_df(self, rsi, *, short=False):
        _t0 = REAL_DT(2026, 10, 28, 9, 21, 0, tzinfo=se.IST)
        _td = __import__('datetime').timedelta
        idx = [_t0 + _td(minutes=3*i) for i in range(25)]
        close = [90.0]*23 + ([100.0, 90.0] if short else [100.0, 110.0])
        d = pd.DataFrame({"open": [100.0]*25, "high": [100.0]*25, "low": [100.0]*25,
                          "close": close, "volume": [100.0]*25}, index=idx)
        st_dir = -1 if short else 1
        for col, v in [("ema9", 105.0), ("ema21", 104.0), ("vwap", 100.0), ("atr", 2.0),
                       ("supertrend", 99.0), ("macd_hist", 1.0), ("adx", 30.0)]:
            d[col] = v
        d["st_dir"] = st_dir
        d["rsi"] = rsi
        d["regime"] = "TRENDING"
        return d

    def test_p9_rsi_ceiling_blocks_exhaustion_long(self):
        # 09:21-10:30 ORB window, all conditions green but RSI 80 (exhaustion) -> NO entry
        orig = se.compute_indicators; se.compute_indicators = lambda df: df
        try:
            sig = se.evaluate_index_signal(self._signal_df(80.0))
            self.assertEqual(sig["signal"], "NONE",
                             "RSI 80 LONG must be blocked by the exhaustion ceiling")
            sig60 = se.evaluate_index_signal(self._signal_df(60.0))
            self.assertEqual(sig60["signal"], "LONG", "RSI 60 LONG in-range must still fire")
        finally:
            se.compute_indicators = orig

    def test_p9_rsi_floor_blocks_exhaustion_short(self):
        orig = se.compute_indicators; se.compute_indicators = lambda df: df
        try:
            sig = se.evaluate_index_signal(self._signal_df(20.0, short=True))
            self.assertEqual(sig["signal"], "NONE",
                              "RSI 20 SHORT must be blocked by the exhaustion floor")
        finally:
            se.compute_indicators = orig

    def _equity_active(self, entry_time):
        return active_trade(weapon="EQUITY", symbol="IRFC", security_id="46681",
                            exchange_segment="NSE_EQ", instrument_type="EQUITY",
                            direction="SHORT", quantity=575,
                            entry_price=100.0, stop_loss=101.0, target_price=95.0,
                            entry_time=f"{TODAY}T{entry_time}+05:30")

    def test_p9_stale_equity_short_exits_after_20_min(self):
        # equity position flat for >20 min exits as STALE instead of riding to the SL
        seed_state({"active_trade": self._equity_active("10:05:00")})   # held 25 min by NOW=10:30
        self.seed_broker_trade(se.load_state()["active_trade"], stop_status="PENDING")
        self.mock.candles["46681"] = bars(30, 100.0, 100)   # flat LTP == entry
        self.mock.status_queue = ["TRADED"]
        self.run_cmd_tick()
        j = journal()
        self.assertEqual(j[-1]["exit_reason"], "STALE_THETA_EXIT",
                         "flat equity position older than EQUITY_STALE_MINS must exit")
        self.assertEqual(self.mock.real_positions[0]["netQty"], 0)

    def test_p9_fresh_equity_short_not_stale_killed(self):
        # control: held only 15 min -> no stale exit, trade stays open
        seed_state({"active_trade": self._equity_active("10:15:00")})
        self.seed_broker_trade(se.load_state()["active_trade"], stop_status="PENDING")
        self.mock.candles["46681"] = bars(30, 100.0, 100)
        monitor = se.monitor_active_trade_loop
        with patch.object(se, "monitor_active_trade_loop",
                          side_effect=lambda *args, **kwargs: monitor(
                              *args, **dict(kwargs, max_seconds=0.03))):
            self.run_cmd_tick()
        self.assertEqual(journal(), [],
                         "equity position younger than EQUITY_STALE_MINS must not be stale-killed")

    def test_p9_equity_setup_budget_blocks_third_same_security_entry(self):
        se.evaluate_index_signal = lambda df: {"signal": "NONE", "reason": "none"}
        se.scan_equity_candidates = lambda client: {
            "symbol": "IRFC", "security_id": "46681", "direction": "SHORT",
            "ltp": 100.0, "atr": 1.0, "reason": "regression equity"}
        self._journal_seed([self._entry(setup="EQ_SCALP", direction="SHORT", security_id="46681"),
                            self._entry(setup="EQ_SCALP", direction="SHORT", security_id="46681")])
        self.run_cmd_tick()
        self.assertEqual(self.mock.writes(), [],
                         "3rd equity entry on the same security/direction must be blocked")

    # ═══ Live-entry pruning (replay 2026-11-22): disabled setups / indices ═══

    INDEX_SIDS = {"Nifty 50": "13", "Nifty Bank": "25", "FinNifty": "27",
                  "Nifty Next 50": "38", "Nifty Midcap Select": "442"}

    def enable_index_signals(self, per_index):
        """Give exactly the named indices a signal; every other index gets no
        candles (scan error -> skipped). evaluate_index_signal is patched to
        decode which index a df belongs to from its constant close price."""
        self.mock.expiry_resp = {"status": "success", "data": {"data": ["2026-10-29"]}}
        self.mock.option_chain_resp = real_option_chain()
        se.verify_option_chart_momentum = lambda client, sid: (True, "mock momentum")
        token = {name: 1000.0 + i for i, name in enumerate(self.INDEX_SIDS)}
        by_token = {}
        for name, sig in per_index.items():
            t = token[name]
            self.mock.candles[self.INDEX_SIDS[name]] = bars(60, t, 1000)
            by_token[t] = sig
        def fake_eval(df):
            if df is None or len(df) == 0:
                return {"signal": "NONE", "setup": "NONE", "confidence": 0.0, "reason": "no data"}
            c = round(float(df["close"].iloc[-1]), 2)
            return by_token.get(c, {"signal": "NONE", "setup": "NONE",
                                    "confidence": 0.0, "reason": "no setup"})
        se.evaluate_index_signal = fake_eval

    def spy_option_index(self):
        """Record which index the entry scan picked, via the option-lookup kwargs."""
        recorded = {}
        orig = RealClient.get_nearest_nifty_option
        def spy(client_self, *a, **kw):
            recorded["index_name"] = kw.get("index_name")
            recorded["under_security_id"] = kw.get("under_security_id")
            return orig(client_self, *a, **kw)
        RealClient.get_nearest_nifty_option = spy
        self.addCleanup(setattr, RealClient, "get_nearest_nifty_option", orig)
        return recorded

    def test_prune_flags_exclude_exactly_the_replay_losers(self):
        self.assertEqual(se.DISABLED_SETUPS, {"SUPERTREND_FLIP"})
        self.assertEqual(se.DISABLED_INDICES, {"Nifty 50"})

    def test_prune_supertrend_flip_signal_skipped(self):
        sig = {"signal": "LONG", "setup": "SUPERTREND_FLIP", "confidence": 99.0, "reason": "flip"}
        self.enable_index_signals({n: dict(sig) for n in self.INDEX_SIDS})
        self.run_cmd_tick()
        self.assertEqual(self.mock.writes(), [], "SUPERTREND_FLIP must never reach the broker")
        self.assertEqual(journal(), [])
        st = json.load(open(se.STATE_FILE))
        self.assertIsNone(st["active_trade"])
        self.assertEqual(st["daily_trades_count"], 0)

    def test_prune_nifty50_signal_skipped(self):
        sig = {"signal": "LONG", "setup": "FAST_STRIKE_TREND", "confidence": 99.0, "reason": "trend"}
        self.enable_index_signals({"Nifty 50": sig})
        self.run_cmd_tick()
        self.assertEqual(self.mock.writes(), [], "Nifty 50 live entries must be pruned")
        self.assertEqual(journal(), [])

    def test_prune_banknifty_midcap_still_enter(self):
        bank = {"signal": "LONG", "setup": "FAST_STRIKE_TREND", "confidence": 90.0, "reason": "trend"}
        mid = {"signal": "LONG", "setup": "ORB_IGNITE", "confidence": 70.0, "reason": "orb"}
        seen = self.spy_option_index()
        self.enable_index_signals({"Nifty Bank": bank, "Nifty Midcap Select": mid})
        self.run_cmd_tick()
        self.assertEqual(len(self.mock.placed("LIMIT")), 1, "BankNifty/Midcap entries must still fire")
        self.assertEqual(seen.get("index_name"), "BANKNIFTY",
                         "highest-confidence ENABLED index must win the entry")
        self.assertEqual(seen.get("under_security_id"), 25)

    def test_prune_disabled_signal_does_not_shadow_enabled_one(self):
        n50 = {"signal": "LONG", "setup": "SUPERTREND_FLIP", "confidence": 99.0, "reason": "flip"}
        mid = {"signal": "LONG", "setup": "SUPERTREND_FLIP", "confidence": 85.0, "reason": "flip"}
        bank = {"signal": "LONG", "setup": "FAST_STRIKE_TREND", "confidence": 70.0, "reason": "trend"}
        seen = self.spy_option_index()
        self.enable_index_signals({"Nifty 50": n50, "Nifty Midcap Select": mid, "Nifty Bank": bank})
        self.run_cmd_tick()
        self.assertEqual(len(self.mock.placed("LIMIT")), 1,
                         "a disabled signal must not block a lower-confidence enabled entry")
        self.assertEqual(seen.get("index_name"), "BANKNIFTY")

    def test_prune_niftynxt50_not_caught_by_nifty_rule(self):
        nxt = {"signal": "LONG", "setup": "FAST_STRIKE_TREND", "confidence": 80.0, "reason": "trend"}
        seen = self.spy_option_index()
        self.enable_index_signals({"Nifty Next 50": nxt})
        self.run_cmd_tick()
        self.assertEqual(len(self.mock.placed("LIMIT")), 1,
                         "Nifty Next 50 (NIFTYNXT50) must not match the 'Nifty 50' prune rule")
        self.assertEqual(seen.get("index_name"), "NIFTYNXT50")
        self.assertEqual(seen.get("under_security_id"), 38)

    # ═══ Guard integrity (unchanged staging safeguard) ═══
    def test_z_guard_blocks_all_broker_writes(self):
        m = MockDhan()
        g = ReadOnlyBroker(m)
        self.assertEqual(g.get_positions()["status"], "success")
        for name, call in [("place_order", lambda: g.place_order(security_id="1")),
                           ("cancel_order", lambda: g.cancel_order("X")),
                           ("modify_order", lambda: g.modify_order(order_id="X"))]:
            with self.assertRaises(RuntimeError, msg=f"{name} must be blocked"):
                call()
        self.assertEqual(m.writes(), [], "no write may reach the broker mock through the guard")

if __name__ == "__main__":
    unittest.main(verbosity=2)
