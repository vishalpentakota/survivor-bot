"""Regression coverage for the deployed strategy entry protections."""

from datetime import datetime
import sys
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
import survivor_engine as engine


class EntryReleaseParity(unittest.TestCase):
    def submit(self, trade, hhmm):
        coordinator = Mock()

        def enter(*args, **kwargs):
            if coordinator.entry_allowed():
                send(*args, **kwargs)

        coordinator.enter.side_effect = enter
        send = Mock()
        client = SimpleNamespace(_policy=engine.RolePolicy(trade=True),
                                 dhan=SimpleNamespace(INTRA="INTRADAY"))
        with (patch.object(engine, "datetime") as clock,
              patch.object(engine, "ExchangeCalendar"),
              patch.object(engine, "_get_latest_tick_ltp", return_value=100),
              patch.object(engine, "_exit_coordinator", return_value=coordinator),
              patch.object(engine, "_resume_entry", return_value="filled")):
            clock.now.return_value = datetime.fromisoformat(f"2026-11-23T{hhmm}:00+05:30")
            result = engine._start_entry(client, {}, trade, "BUY", "LIMIT", 100)
        return result, send.call_count

    def test_order_submission_respects_both_windows(self):
        for segment in ("NSE_EQ", "NSE_FNO"):
            trade = dict(security_id="1", exchange_segment=segment, instrument_type="TEST",
                         tick_size=0.05, lot_size=1, quantity=1, stop_loss=95)
            for time in ("09:29", "09:30", "11:30", "11:31", "13:29", "13:30", "14:15", "14:16"):
                with self.subTest(segment=segment, time=time):
                    allowed = time in {"09:30", "11:30", "13:30", "14:15"}
                    self.assertEqual(self.submit(trade, time),
                                     ("filled", 1) if allowed else ("denied", 0))

    def test_equity_long_and_short_require_nonchoppy_signal(self):
        for direction, trend in (
            ("LONG", dict(close=110, ema9=105, ema21=100, st_dir=1, rsi=65)),
            ("SHORT", dict(close=90, ema9=95, ema21=100, st_dir=-1, rsi=35)),
        ):
            for regime in ("CHOPPY", "TRENDING"):
                with self.subTest(direction=direction, regime=regime):
                    bar = dict(trend, regime=regime, vwap=100, atr=2, rel_vol=2, high=109, low=91)
                    candles = engine.pd.DataFrame([bar] * 25)
                    client = Mock()
                    client.fetch_candles.return_value = candles
                    with (patch.object(engine, "LIQUID_EQUITY_UNIVERSE", {"TEST": "1"}),
                          patch.object(engine, "compute_indicators", return_value=candles)):
                        candidate = engine.scan_equity_candidates(client)
                    if regime == "CHOPPY":
                        self.assertIsNone(candidate)
                    else:
                        self.assertEqual(candidate["direction"], direction)

    def test_submission_preflight_rejects_explicit_equity_choppy(self):
        trade = dict(security_id="1", exchange_segment="NSE_EQ", instrument_type="EQUITY",
                     tick_size=0.05, lot_size=1, quantity=1, stop_loss=95)
        self.assertEqual(self.submit(dict(trade, regime="CHOPPY"), "10:00"), ("denied", 0))
        self.assertEqual(self.submit(dict(trade, regime="TRENDING"), "10:00"), ("filled", 1))

    def test_live_tick_routes_only_in_window_trending_equity_to_entry(self):
        broker = SimpleNamespace(BUY="BUY", SELL="SELL", MARKET="MARKET")
        client = Mock(dhan=broker)
        client.get_live_equity_and_positions.return_value = (30000, 30000, 0, [])
        state = dict(status="ALIVE", mortality_armed=True, daily_halted=False,
                     daily_date="2026-11-23", daily_start_equity=30000, peak_equity=30000)
        for time, regime, expected in (
            ("12:00", "TRENDING", False),
            ("10:00", "CHOPPY", False),
            ("13:30", "TRENDING", True),
        ):
            with self.subTest(time=time, regime=regime):
                bar = dict(close=110, ema9=105, ema21=100, st_dir=1, rsi=65, regime=regime,
                           vwap=100, atr=2, rel_vol=2, high=109, low=91)
                candles = engine.pd.DataFrame([bar] * 25)
                client.fetch_candles.return_value = candles
                client.fetch_candles.reset_mock()
                with (patch.object(engine, "datetime", wraps=datetime) as clock,
                      patch.object(engine, "SurvivorDhanClient", return_value=client),
                      patch.object(engine, "load_state", return_value=state.copy()),
                      patch.object(engine, "save_state"),
                      patch.object(engine, "load_journal", return_value=[]),
                      patch.object(engine, "_resume_entry", return_value="idle"),
                      patch.object(engine, "_recovery_pending", return_value=False),
                      patch.object(engine, "_safe_get_positions", return_value=[]),
                      patch.object(engine, "_get_latest_tick_ltp", return_value=110),
                      patch.object(engine, "_instrument_metadata", return_value={}),
                      patch.object(engine, "INDEX_UNIVERSE", {}),
                      patch.object(engine, "LIQUID_EQUITY_UNIVERSE", {"TEST": "1"}),
                      patch.object(engine, "compute_indicators", return_value=candles),
                      patch.object(engine, "ExchangeCalendar"),
                      patch.object(engine, "_start_entry", return_value="idle") as start):
                    clock.now.return_value = datetime.fromisoformat(f"2026-11-23T{time}:00+05:30")
                    engine._tick_with_state(policy=engine.RolePolicy(trade=True), quiet_scan=True, owner_fd=None)
                self.assertEqual(start.call_count, int(expected))
                if time == "12:00":
                    client.fetch_candles.assert_not_called()
