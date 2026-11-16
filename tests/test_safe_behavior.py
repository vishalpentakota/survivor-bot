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

APP = "/opt/data/survivor-cloud-migration-vnext/app"
sys.path.insert(0, APP)

# Pre-configure root logging so survivor_engine's import-time basicConfig is a no-op
logging.basicConfig(level=logging.CRITICAL, format="%(message)s")

import pandas as pd
import survivor_engine as se
from cloud_guard import ReadOnlyBroker

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
TMP = tempfile.mkdtemp(prefix="survivor_regr_")

def redirect_paths():
    se.BASE_DIR = TMP
    se.STATE_FILE = os.path.join(TMP, "survivor_state.json")
    se.JOURNAL_FILE = os.path.join(TMP, "survivor_trades.json")
    se.OBITUARY_FILE = os.path.join(TMP, "survivor_obituary.md")
    se._STATE_LOCK_FILE = os.path.join(TMP, ".survivor_state.lock")

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
        self.status_queue = []         # FIFO statuses served by get_order_by_id
        self.fills_update_positions = True
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
        return {"status": "success", "data": list(self.orders)}
    def get_order_by_id(self, oid):
        self.calls.append(("get_order_by_id", oid))
        rec = self._by_oid.get(oid)
        if rec is None:
            return {"status": "success", "data": {"orderStatus": "PENDING"}}
        if self.status_queue:
            rec["orderStatus"] = self.status_queue.pop(0)
        if rec["orderStatus"] == "TRADED":
            if rec["filledQty"] == 0:
                rec["filledQty"] = rec["quantity"]
                rec["averageTradedPrice"] = self.fill_price
            if self.fills_update_positions and rec["orderType"] == "MARKET":
                for p in self.real_positions:
                    if str(p.get("securityId")) == str(rec["securityId"]):
                        p["netQty"] = 0
        return {"status": "success", "data": dict(rec)}
    def intraday_minute_data(self, security_id=None, **kw):
        return self.candles.get(str(security_id), {"status": "failure", "data": None})
    def expiry_list(self, **kw): return self.expiry_resp or {"status": "failure", "data": None}
    def option_chain(self, **kw): return self.option_chain_resp or {"status": "failure", "data": None}
    def fetch_security_list(self, mode="compact"): return self.security_master
    # ── writes (blocked by ReadOnlyBroker in staging; recorded here)
    def place_order(self, **kw):
        self.calls.append(("place_order", kw))
        if self.place_order_result is not None: return self.place_order_result
        self._oid += 1
        oid = f"ORD-{self._oid}"
        self._by_oid[oid] = {"orderId": oid, "orderStatus": "PENDING",
                             "securityId": str(kw.get("security_id")),
                             "transactionType": kw.get("transaction_type"),
                             "quantity": kw.get("quantity"), "orderType": kw.get("order_type"),
                             "averageTradedPrice": 0.0, "filledQty": 0}
        # simulate the entry fill appearing in the position book when the BUY is placed
        sid = str(kw.get("security_id"))
        if kw.get("transaction_type") == "BUY" and sid in self.buy_fill_qty:
            self.real_positions = [{"securityId": sid, "netQty": int(self.buy_fill_qty[sid]),
                                    "exchangeSegment": "NSE_FNO", "productType": "INTRADAY",
                                    "tradingSymbol": "NIFTY-29-OCT-2026-25000-CE"}]
        return {"status": "success", "data": {"orderId": oid}}
    def cancel_order(self, oid):
        self.calls.append(("cancel_order", oid))
        if oid in self._by_oid: self._by_oid[oid]["orderStatus"] = "CANCELLED"
        return {"status": "success"}
    def modify_order(self, **kw): self.calls.append(("modify_order", kw)); return {"status": "success"}
    # ── helpers
    def writes(self): return [c for c in self.calls if c[0] in ("place_order", "cancel_order", "modify_order")]
    def placed(self, order_type=None):
        return [w for w in self.writes() if w[0] == "place_order"
                and (order_type is None or w[1].get("order_type") == order_type)]

def make_client(mock):
    c = object.__new__(RealClient)
    c.dhan = mock
    c.tm = types.SimpleNamespace(get_token=lambda: "T", client_id="C",
                                 get_dhan_context=lambda: {}, access_token="T", token_expiry=None)
    return c

def real_option_chain(expiry="2026-10-29", last=25000.0, ltp=100.0):
    return {"status": "success", "data": {"data": {
        "last_price": last,
        "oc": {"25000": {"ce": {
            "last_price": ltp, "top_bid_price": ltp - 1.0, "top_ask_price": ltp + 1.0,
            "security_id": "99991", "oi": 5000, "volume": 20000,
            "greeks": {"delta": 0.50, "theta": -5.0}, "implied_volatility": 15.0}}}}}}

class SafeBehaviorRegression(unittest.TestCase):
    def setUp(self):
        redirect_paths()
        NOW["dt"] = REAL_DT(2026, 10, 28, 10, 30, 0, tzinfo=se.IST)
        se.datetime = FixedDatetime
        for f in (se.STATE_FILE, se.JOURNAL_FILE):
            if os.path.exists(f): os.unlink(f)
        if os.path.exists(os.path.join(TMP, ".survivor_engine.lock")):
            os.unlink(os.path.join(TMP, ".survivor_engine.lock"))
        # speed: collapse the engine's wall-clock waits (patched engine reads these)
        for k, v in [("ENTRY_SETTLE_POLL_S", 0.001), ("ENTRY_SETTLE_TRIES", 3),
                     ("SL_STATUS_SLEEP", 0.001), ("SL_RETRY_SLEEP", 0.001),
                     ("FLATTEN_POLL_S", 0.001), ("FLATTEN_POLL_N", 3),
                     ("FINALIZE_SLEEP", 0.001), ("MONITOR_POLL_S", 0.01)]:
            setattr(se, k, v)
        # snapshot module globals we may patch per-test
        self._saved = {k: getattr(se, k) for k in
                       ("SurvivorDhanClient", "evaluate_index_signal",
                        "verify_option_chart_momentum", "scan_equity_candidates",
                        "compute_indicators")}
        self.mock = MockDhan()
        se.SurvivorDhanClient = lambda: make_client(self.mock)
        seed_state()
    def tearDown(self):
        for k, v in self._saved.items(): setattr(se, k, v)
        se.datetime = REAL_DT
    # ── helpers
    def enable_long_signal(self):
        se.evaluate_index_signal = lambda df: {"signal": "LONG", "confidence": 99.0,
                                               "reason": "regression setup", "setup": "FAST_STRIKE"}
        se.verify_option_chart_momentum = lambda client, sid: (True, "mock momentum")
        self.mock.expiry_resp = {"status": "success", "data": {"data": ["2026-10-29"]}}
        self.mock.option_chain_resp = real_option_chain()
    def run_cmd_tick(self, live=True):
        se.cmd_tick(live=live, quiet_scan=False)

    # ═══ P1: fail-closed position-status handling ═══
    def test_p1_position_api_failure_does_not_close_trade(self):
        seed_state({"active_trade": active_trade()})
        self.mock.real_positions = [{"securityId": "99991", "netQty": 65}]
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
        self.mock.real_positions = [{"securityId": "99991", "netQty": 65}]
        self.mock.positions_resp = ConnectionError("broker unreachable")
        c = make_client(self.mock)
        st = se.load_state()
        se.monitor_active_trade_loop(c, st, live=True, max_seconds=0.5, quiet_scan=True)
        self.assertIsNotNone(st["active_trade"])
        self.assertEqual(journal(), [])

    def test_p1_genuine_exchange_sl_fill_still_detected(self):
        seed_state({"active_trade": active_trade()})
        self.mock.real_positions = []   # position genuinely closed by exchange SL
        self.mock.positions_resp = {"status": "success", "data": []}
        c = make_client(self.mock)
        st = se.load_state()
        se.monitor_active_trade_loop(c, st, live=True, max_seconds=0.5, quiet_scan=True)
        self.assertIsNone(st["active_trade"], "verified-flat position must finalize the trade")
        self.assertEqual(journal()[-1]["exit_reason"], "EXCHANGE_SL_FILLED")

    def test_p1_consecutive_failures_block_new_entries(self):
        seed_state({"active_trade": active_trade()})
        self.mock.positions_resp = {"status": "failure", "data": []}
        c = make_client(self.mock)
        st = se.load_state()
        for _ in range(3):
            se.monitor_active_trade_loop(c, st, live=True, max_seconds=0.4, quiet_scan=True)
        self.assertTrue(st.get("entries_blocked_reason"),
                        "3 consecutive position-API failures must block new entries")
        self.assertGreaterEqual(st.get("pos_check_failures", 0), 3)

    # ═══ P2: fill-confirmed entries, timeouts, partial fills ═══
    def test_p2_entry_timeout_cancels_pending_and_aborts(self):
        self.enable_long_signal()
        self.mock.real_positions = []            # entry never fills
        self.mock.status_queue = ["PENDING"]     # entry order still pending at abort check
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
        self.enable_long_signal()
        self.mock.buy_fill_qty = {"99991": 25}      # entry order fills only 25 of 65
        self.mock.status_queue = ["PENDING"]     # SL order accepted, pending (not rejected)
        self.mock.candles["99991"] = bars(30, 80.0, 100)   # 1-min LTP 80 <= SL -> immediate exit
        self.mock.status_queue += ["TRADED"]      # flatten order fills
        self.run_cmd_tick()
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
        self.mock.status_queue = ["REJECTED", "REJECTED", "REJECTED", "TRADED"]
        self.run_cmd_tick()
        self.assertEqual(len(self.mock.placed("STOP_LOSS")), 3, "SL retried 3x")
        self.assertEqual(len(self.mock.placed("MARKET")), 1, "unprotected position must be market-flattened")
        self.assertEqual(self.mock.real_positions[0]["netQty"], 0, "position must be closed (broker truth)")
        st = json.load(open(se.STATE_FILE))
        self.assertIsNone(st["active_trade"])
        self.assertEqual(journal()[-1]["exit_reason"], "UNPROTECTED_POSITION_FLATTENED")

    def test_p3_flatten_rejected_then_filled_on_retry(self):
        self.mock.real_positions = [{"securityId": "99991", "netQty": 65}]
        self.mock.status_queue = ["REJECTED", "TRADED"]
        c = make_client(self.mock)
        actions = c.emergency_flatten_all()
        self.assertEqual(self.mock.real_positions[0]["netQty"], 0, "retry must close the position")
        self.assertEqual(len(self.mock.placed("MARKET")), 2)
        self.assertTrue(any("TRADED" in a for a in actions), actions)

    def test_p3_flatten_persistently_rejected_reports_failure(self):
        self.mock.real_positions = [{"securityId": "99991", "netQty": 65}]
        self.mock.status_queue = ["REJECTED"] * 9
        c = make_client(self.mock)
        actions = c.emergency_flatten_all()
        self.assertNotEqual(self.mock.real_positions[0]["netQty"], 0, "position must remain open")
        self.assertTrue(any("STILL OPEN" in a or "FAILED" in a for a in actions), actions)

    # ═══ P4: halt + verified flatten ═══
    def test_p4_halt_with_verified_flat_clears_trade(self):
        self.mock.funds = {"status": "success", "data": {"availabelBalance": 24000.0, "utilizedAmount": 0.0}}
        self.mock.real_positions = [{"securityId": "99991", "netQty": 65}]
        self.mock.status_queue = ["TRADED"]      # flatten fills
        seed_state({"daily_start_equity": 30000.0, "active_trade": active_trade()})
        self.run_cmd_tick()
        st = json.load(open(se.STATE_FILE))
        self.assertTrue(st["daily_halted"])
        self.assertIsNone(st["active_trade"], "trade may only be cleared after verified flat")
        self.assertEqual(self.mock.real_positions[0]["netQty"], 0)

    def test_p4_halt_flatten_failure_retains_trade_as_pending(self):
        self.mock.funds = {"status": "success", "data": {"availabelBalance": 24000.0, "utilizedAmount": 0.0}}
        self.mock.real_positions = [{"securityId": "99991", "netQty": 65}]
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
        self.mock.real_positions = [{"securityId": "99991", "netQty": 65}]
        seed_state({"daily_start_equity": 30000.0,
                    "active_trade": active_trade(flatten_pending=True),
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
            try: self.run_cmd_tick()
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
        lock = os.path.join(TMP, ".survivor_engine.lock")
        with open(lock, "w") as f: f.write(str(p.pid))
        self.run_cmd_tick()   # must recover the stale lock and proceed
        self.assertTrue(any(c[0] == "get_fund_limits" for c in self.mock.calls),
                        "stale lock (dead owner) must not block the engine")

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
        self.mock.expiry_resp = {"status": "failure", "data": None}
        self.mock.candles["13"] = bars(60, 25000.0, 1000)
        self.mock.candles["99991"] = bars(60, 100.0, 12345)
        self.mock.security_master = pd.DataFrame([{
            "SEM_INSTRUMENT_NAME": "OPTIDX", "SEM_TRADING_SYMBOL": "NIFTY-29-OCT-2026-25000-CE",
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
        self.mock.real_positions = []          # closed externally while engine was down
        self.mock.orders = [                    # broker records show the real exit
            {"orderId": "ORD-SL-1", "orderStatus": "TRADED", "averageTradedPrice": 95.0,
             "securityId": "99991", "transactionType": "SELL"},
            {"orderId": "ORD-BUY-1", "orderStatus": "TRADED", "averageTradedPrice": 100.0},
        ]
        self.run_cmd_tick()
        st = json.load(open(se.STATE_FILE))
        self.assertIsNone(st["active_trade"], "closed-while-down trade must be reconciled")
        j = journal()
        self.assertEqual(j[-1]["exit_reason"], "RESTART_RECONCILED_CLOSED")
        self.assertAlmostEqual(j[-1]["exit_price"], 95.0, msg="exit price must come from broker fills")

    def test_p8_restart_resumes_still_open_position(self):
        seed_state({"active_trade": active_trade()})
        self.mock.real_positions = [{"securityId": "99991", "netQty": 65}]
        self.mock.candles["99991"] = bars(30, 80.0, 100)   # SL hit -> managed exit
        self.mock.status_queue = ["TRADED"]
        self.run_cmd_tick()
        j = journal()
        self.assertEqual(j[-1]["exit_reason"], "STOP_LOSS_HIT",
                         "still-open trade must resume being managed, not falsely finalized")
        self.assertEqual(self.mock.real_positions[0]["netQty"], 0)

    def test_p8_orphan_position_flattened_on_restart(self):
        seed_state({"active_trade": None})      # engine forgot the trade; broker still has it
        self.mock.real_positions = [{"securityId": "99991", "netQty": 65}]
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

    def _seed_state_from_file(self):
        st = json.load(open(se.STATE_FILE))
        st["daily_date"] = TODAY
        json.dump(st, open(se.STATE_FILE, "w"))
        return st

    def test_p9_green_day_keeps_trading_no_halt_at_500(self):
        # +₹569 banked and still green -> NO halt: recovery mode maximizes a green day
        self._journal_seed([self._entry(net_pnl=517.0), self._entry(net_pnl=52.25)])
        st = self._seed_state_from_file()
        c = make_client(self.mock)
        se._finalize_closed_trade(c, st, active_trade(), "TARGET_HIT", 105.0)
        st2 = json.load(open(se.STATE_FILE))
        self.assertFalse(st2["daily_halted"], "a win on a green day must NOT halt — maximize recovery")

    def test_p9_green_floor_halts_when_banked_profit_breached(self):
        # day peak +₹569 (>= floor) then a big loss drops today below the floor -> halt & protect
        self._journal_seed([self._entry(net_pnl=517.0), self._entry(net_pnl=52.25)])
        st = self._seed_state_from_file()
        c = make_client(self.mock)
        se._finalize_closed_trade(c, st, active_trade(), "STOP_LOSS_HIT", 85.0)   # big loss
        st2 = json.load(open(se.STATE_FILE))
        self.assertTrue(st2["daily_halted"], "breach of the ₹500 banked-profit floor must halt the day")
        self.assertIn("Green floor", st2["daily_halt_reason"])

    def test_p9_small_green_day_not_floored_keeps_trading(self):
        # day peak only +₹300 (< floor): a single pullback loss must NOT halt (floor never banked)
        self._journal_seed([self._entry(net_pnl=300.0)])
        st = self._seed_state_from_file()
        c = make_client(self.mock)
        se._finalize_closed_trade(c, st, active_trade(), "STOP_LOSS_HIT", 90.0)
        st2 = json.load(open(se.STATE_FILE))
        self.assertFalse(st2["daily_halted"], "no halt unless the ₹500 floor was actually banked")

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
        self.mock.real_positions = [{"securityId": "46681", "netQty": -575}]
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
        self.mock.real_positions = [{"securityId": "46681", "netQty": -575}]
        self.mock.candles["46681"] = bars(30, 100.0, 100)
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
