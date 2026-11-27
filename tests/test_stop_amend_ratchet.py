"""Permanent regression coverage for Dhan quote timestamp parsing and stop-amend ratchet."""
from datetime import datetime, timedelta
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import test_write_safety as fixtures


IST_OFFSET = timedelta(hours=5, minutes=30)

# Real Dhan quote_data last_trade_time samples captured live on 2026-11-27 (IST) for t_7a74d6b3.
DHAN_QUOTE_SAMPLES = (
    ("NSE_EQ", "TCS", "11536", 2592.0, "27/11/2026 12:34:30", "2026-11-27T12:34:31+05:30"),
    ("IDX_I", "NIFTY 50", "13", 25725.0, "27/11/2026 12:51:07", "2026-11-27T12:51:08+05:30"),
    ("NSE_FNO", "NIFTY-Dec2026-25700-CE", "47835", 160.4, "27/11/2026 12:52:11", "2026-11-27T12:52:11+05:30"),
)


class StopAmendRatchetTests(unittest.TestCase):
    def setUp(self):
        fixtures.WriteSafetyTests.setUp(self)
        self.now = datetime.fromisoformat("2026-11-27T12:52:11+05:30")
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory(dir=fixtures.APP.parent)))
        self.enterContext(patch.object(self.engine, "BASE_DIR", str(self.directory)))
        self.enterContext(patch.object(self.engine, "datetime", SimpleNamespace(
            now=lambda tz=None: self.now.astimezone(tz) if tz else self.now.replace(tzinfo=None),
            fromisoformat=datetime.fromisoformat,
        )))
        (self.directory / "exchange_calendar.json").write_text(json.dumps({
            "source": "Synthetic test calendar",
            "sessions": {
                "2026-11-26": ["09:15", "15:30"],
                "2026-11-27": ["09:15", "15:30"],
            },
        }), encoding="utf-8")
        self.sdk = self.sdk.dhanhq.return_value
        self.client = self.engine.SurvivorDhanClient(policy=self.engine.RolePolicy(trade=True))

    def _set_quote(self, seg, sec_id, price, stamp, status="success"):
        self.sdk.quote_data.return_value = {
            "status": status,
            "data": {
                "status": "success",
                "data": {seg: {str(sec_id): {"last_price": price, "last_trade_time": stamp}}},
            },
        }

    def test_real_dhan_quote_timestamps_parse_as_ist(self):
        import market_preflight as mp

        for seg, name, sec_id, price, raw_stamp, observed_iso in DHAN_QUOTE_SAMPLES:
            with self.subTest(segment=seg, instrument=name):
                parsed = mp.instant(raw_stamp)
                self.assertEqual(parsed.utcoffset(), IST_OFFSET)
                self.now = datetime.fromisoformat(observed_iso)
                self._set_quote(seg, sec_id, price, raw_stamp)
                ltp = self.engine._get_latest_tick_ltp(self.client, sec_id, seg, "OPTIDX")
                self.assertEqual(ltp, price)

    def test_iso_with_offset_no_regression_and_naive_rejected(self):
        import market_preflight as mp

        iso_bar = mp.instant("2026-11-26T15:29:00+05:30")
        self.assertEqual(iso_bar.utcoffset(), IST_OFFSET)
        self.assertEqual(iso_bar.isoformat(), "2026-11-26T15:29:00+05:30")

        for bad in ("", "   ", "2026-11-27T12:52:11", "2026/11/27 12:52:11", None, True):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    mp.instant(bad)

    def test_stop_amend_allowed_permits_favourable_sell_and_denies_buy(self):
        """Port of exit-gate CASE6: _inspect_active_stop -> amend_allowed(side)."""
        seg, _, sec_id, ltp, raw_stamp, observed_iso = DHAN_QUOTE_SAMPLES[2]
        self.now = datetime.fromisoformat(observed_iso)
        self._set_quote(seg, sec_id, ltp, raw_stamp)

        captured = {}

        def fake_monitor_stop(coordinator, active, saver, *, tick, trigger, amend_allowed):
            captured["amend_allowed"] = amend_allowed
            return "CAPTURED"

        active = {"security_id": sec_id, "exchange_segment": seg, "instrument_type": "OPTIDX"}
        with (patch.object(self.engine, "monitor_stop", fake_monitor_stop),
              patch.object(self.engine, "_exit_coordinator", return_value=None)):
            status = self.engine._inspect_active_stop(
                self.client, {"status": "ALIVE", "active_trade": None}, active, 0.05, trigger=150.0
            )

        self.assertEqual(status, "CAPTURED")
        amend_allowed = captured["amend_allowed"]
        self.assertIs(amend_allowed("SELL"), True)
        self.assertIs(amend_allowed("BUY"), False)

        # Fail-closed when the quote becomes stale or empty.
        self._set_quote(seg, sec_id, ltp, "")
        self.assertIs(amend_allowed("SELL"), False)

    def test_quote_failure_causes_are_distinguished_in_logs(self):
        seg, _, sec_id, ltp, _, observed_iso = DHAN_QUOTE_SAMPLES[2]
        self.now = datetime.fromisoformat(observed_iso)
        cases = (
            ("no-trade-data", ltp, "", "success"),
            ("parse-failure", ltp, "2026-11-27T12:52:11", "success"),
            ("stale-timestamp", ltp, (self.now - timedelta(seconds=31)).strftime("%d/%m/%Y %H:%M:%S"), "success"),
            ("future-timestamp", ltp, (self.now + timedelta(seconds=10)).strftime("%d/%m/%Y %H:%M:%S"), "success"),
            ("rejected", ltp, self.now.strftime("%d/%m/%Y %H:%M:%S"), "failure"),
        )
        for expected_cause, price, stamp, status in cases:
            with self.subTest(cause=expected_cause):
                self._set_quote(seg, sec_id, price, stamp, status=status)
                with self.assertLogs("survivor", level="WARNING") as logs:
                    result = self.engine._get_latest_tick_ltp(self.client, sec_id, seg, "OPTIDX")
                self.assertIsNone(result)
                combined = "\n".join(logs.output)
                self.assertIn(f"cause={expected_cause}", combined)


if __name__ == "__main__":
    unittest.main()
