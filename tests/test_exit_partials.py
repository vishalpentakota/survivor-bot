"""Adverse interleavings for durable partial-fill recovery."""
from concurrent.futures import ThreadPoolExecutor
import json
from threading import Event
import unittest
from unittest.mock import patch

import test_exit_rework as fixtures


class ExitPartials(unittest.TestCase):
    setUp = fixtures.ExitRework.setUp
    response = staticmethod(fixtures.ExitRework.response)
    position = staticmethod(fixtures.ExitRework.position)
    client = fixtures.ExitRework.client
    row = fixtures.ExitRework.row
    book = fixtures.ExitRework.book
    opening_orders = fixtures.ExitRework.opening_orders
    exit_submit = fixtures.ExitRework.exit_submit

    def flatten(self):
        return self.client().emergency_flatten_all().flat

    def position_read(self, net):
        self.sdk.get_positions.return_value = self.response([self.position(net)])

    def start_partial_market(self, outcomes):
        rows = self.opening_orders()
        self.book(rows)
        self.exit_submit(rows, iter(outcomes))
        with patch.object(self.engine, 'EXIT_ESCALATION_SECONDS', 0):
            self.assertFalse(self.flatten())
        self.sdk.cancel_order.assert_not_called()
        return rows

    def test_rejected_cancel_rejects_regression_and_mismatched_identity(self):
        stop = self.row('stop', 'SELL', 'PENDING', 5, 0, orderType='STOP_LOSS')
        rows = self.opening_orders() | {'stop': stop}
        self.book(rows)
        self.sdk.cancel_order.side_effect = [
            self.response({'orderId': 'stop', 'orderStatus': 'REJECTED'}),
            TimeoutError(),
        ]
        self.assertFalse(self.flatten())
        stop.update(orderStatus='PART_TRADED', filledQty=2)
        self.assertFalse(self.flatten())  # Both order endpoints agree, but position is stale.
        stop.update(filledQty=1)
        self.position_read(4)
        self.assertFalse(self.flatten())  # All endpoints regress together across restart.
        stop.update(filledQty=2, orderType='MARKET')
        self.position_read(3)
        self.assertFalse(self.flatten())
        stop.update(orderType='STOP_LOSS')
        self.sdk.get_order_by_id.side_effect = lambda oid: self.response(
            dict(stop, filledQty=1) if oid == 'stop' else rows[oid])
        self.assertFalse(self.flatten())
        self.sdk.cancel_order.assert_called_once_with('stop')
        self.book(rows)
        self.assertFalse(self.flatten())  # Retry outcome is ambiguous.
        stop.update(filledQty=3)
        self.position_read(2)
        self.assertFalse(self.flatten())
        self.assertEqual(self.sdk.cancel_order.call_count, 2)
        self.sdk.place_order.assert_not_called()

    def test_market_waits_for_deadline_and_rechecks_detail_after_snapshot(self):
        with patch.object(self.engine.time, 'time', return_value=100):
            rows = self.start_partial_market([('PART_TRADED', 2, 5), ('TRADED', 3, 0)])
        self.position_read(3)
        with patch.object(self.engine.time, 'time', return_value=99):
            self.assertFalse(self.flatten())
        self.sdk.cancel_order.assert_not_called()

        def stale_book_detail(oid):
            if oid == '2' and self.sdk.get_positions.call_count >= 2:
                return self.response(dict(rows[oid], orderStatus='TRADED', filledQty=5))
            return self.response(rows[oid])

        self.sdk.get_positions.reset_mock()
        self.sdk.get_order_by_id.side_effect = stale_book_detail
        with patch.object(self.engine.time, 'time', return_value=101):
            self.assertFalse(self.flatten())
        self.sdk.cancel_order.assert_not_called()
        self.sdk.place_order.assert_called_once()

    def test_full_late_market_fill_during_cancel_with_stale_position(self):
        rows = self.start_partial_market([('PART_TRADED', 2, 5)])
        self.position_read(3)

        def cancel(oid):
            rows[oid].update(orderStatus='TRADED', filledQty=5)
            return self.response({'orderId': oid, 'orderStatus': 'REJECTED'})

        self.sdk.cancel_order.side_effect = cancel
        self.assertFalse(self.flatten())
        rows['2'].update(orderStatus='PART_TRADED', filledQty=2)
        self.assertFalse(self.flatten())
        rows['2'].update(orderStatus='TRADED', filledQty=5)
        self.position_read(0)
        self.assertTrue(self.flatten())
        self.sdk.cancel_order.assert_called_once_with('2')
        self.sdk.place_order.assert_called_once()
        self.assertEqual(json.loads(self.engine.EXIT_LEDGER.read_text())['fills']['2']['source'], 'MARKET')

    def test_concurrent_recovery_places_only_fresh_residual_after_late_partial(self):
        rows = self.start_partial_market([('PART_TRADED', 2, 5), ('TRADED', 1, 0)])
        self.position_read(3)

        def cancel(oid):
            rows[oid].update(orderStatus='CANCELLED', filledQty=4)
            return self.response({'orderId': oid})

        self.sdk.cancel_order.side_effect = cancel
        self.assertFalse(self.flatten())
        self.sdk.place_order.assert_called_once()
        self.position_read(1)
        entered_submit = Event()
        second_entered = Event()
        release_submit = Event()
        submit = self.sdk.place_order.side_effect

        def blocked_submit(**params):
            entered_submit.set()
            if not release_submit.wait(5):
                raise TimeoutError()
            return submit(**params)

        def recover_again():
            second_entered.set()
            return self.flatten()

        self.sdk.place_order.side_effect = blocked_submit
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.flatten)
            try:
                self.assertTrue(entered_submit.wait(5))
                second = pool.submit(recover_again)
                self.assertTrue(second_entered.wait(5))
            finally:
                release_submit.set()
            self.assertTrue(first.result(5))
            self.assertTrue(second.result(5))
        self.sdk.cancel_order.assert_called_once_with('2')
        self.assertEqual([call.kwargs['quantity'] for call in self.sdk.place_order.call_args_list], [5, 1])
        fills = json.loads(self.engine.EXIT_LEDGER.read_text())['fills']
        self.assertEqual({oid: entry['source'] for oid, entry in fills.items()}, {'2': 'MARKET', '3': 'MARKET'})
