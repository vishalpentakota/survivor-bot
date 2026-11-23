"""Exit regressions using the offline harness and a staged ledger."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import runpy
import sys
from threading import Event
import tempfile
import unittest
from unittest.mock import patch

import test_write_safety as fixtures


class VerifiedExits(unittest.TestCase):
    def setUp(self):
        fixtures.WriteSafetyTests.setUp(self)
        root = Path(self.engine.__file__).resolve().parents[1]
        directory = self.enterContext(tempfile.TemporaryDirectory(dir=root))
        self.enterContext(patch.object(self.engine, 'EXIT_LEDGER', Path(directory) / 'exits.json'))
        self.enterContext(patch.object(self.engine, 'JOURNAL_FILE', str(Path(directory) / 'journal.json')))
        self.enterContext(patch.object(self.engine.time, 'sleep'))
        self.enterContext(patch.object(self.engine, 'send_telegram', return_value=False))
        self.sdk = self.sdk.dhanhq.return_value
        self.sdk.MARKET = "MARKET"
        self.sdk.SL = "SL"
        self.sdk.get_order_list.return_value = self.response([])
        self.sdk.get_positions.return_value = self.response([self.position(5)])

    @staticmethod
    def response(data):
        return {'status': 'success', 'data': data}

    @staticmethod
    def position(qty):
        return dict(securityId='23', exchangeSegment='NSE_EQ', productType='INTRADAY', netQty=qty)

    def client(self):
        return self.engine.SurvivorDhanClient(policy=self.engine.RolePolicy(flatten=True))

    def test_lost_ack_blocks_resubmit_after_restart(self):
        self.sdk.place_order.side_effect = TimeoutError
        self.assertFalse(self.client().emergency_flatten_all().flat)
        self.assertFalse(self.client().emergency_flatten_all().flat)
        self.sdk.place_order.assert_called_once()

    def test_flat_requires_readable_orders(self):
        self.sdk.get_positions.return_value = self.response([])
        self.sdk.get_order_list.return_value = self.response([{}])
        self.assertFalse(self.client().emergency_flatten_all().flat)
        self.sdk.place_order.assert_not_called()

    def row(self, oid, side, status, quantity, filled, price=101, **extra):
        return dict(self.position(0), orderId=oid, transactionType=side, orderStatus=status,
                    quantity=quantity, filledQty=filled, averageTradedPrice=price, **extra)

    def book(self, rows):
        self.sdk.get_order_list.side_effect = lambda: self.response(list(rows.values()))
        self.sdk.get_order_by_id.side_effect = lambda oid: self.response(rows[oid])

    def exit_submit(self, rows, outcomes):
        def submit(**kwargs):
            oid = str(len(rows) + 1)
            side, qty = kwargs['transaction_type'], kwargs['quantity']
            status, filled, remaining = next(outcomes)
            with open(self.engine.EXIT_LEDGER, encoding='utf-8') as ledger:
                intent = json.load(ledger)['cycle']['pending']
            self.assertEqual((intent['oid'], intent['tag'], intent['quantity']), (None, kwargs['tag'], qty))
            rows[oid] = self.row(oid, side, status, qty, filled, correlationId=kwargs['tag'],
                                 createTime='2026-11-23 10:02:00')
            self.sdk.get_positions.return_value = self.response([self.position(remaining)])
            return self.response({'orderId': oid})
        self.sdk.place_order.side_effect = submit

    def test_long_short_and_already_flat(self):
        for net in (5, -5, 0):
            with self.subTest(net=net):
                self.engine.EXIT_LEDGER.unlink(missing_ok=True)
                self.sdk.reset_mock()
                rows = {}
                self.book(rows)
                self.sdk.get_positions.return_value = self.response([self.position(net)])
                self.exit_submit(rows, iter([('TRADED', 5, 0)]))
                self.assertTrue(self.client().emergency_flatten_all().flat)
                if net:
                    self.assertEqual(self.sdk.place_order.call_args.kwargs['transaction_type'],
                                     'SELL' if net > 0 else 'BUY')
                else:
                    self.sdk.place_order.assert_not_called()

    def test_terminal_reject_and_partial_cancel_reduce_retry_size(self):
        rows = {}
        self.book(rows)
        self.exit_submit(rows, iter([('REJECTED', 0, 5), ('CANCELLED', 2, 3), ('TRADED', 3, 0)]))
        self.assertTrue(self.client().emergency_flatten_all().flat)
        self.assertEqual([call.kwargs['quantity'] for call in self.sdk.place_order.call_args_list], [5, 5, 3])

    def test_partial_cancel_with_stale_position_blocks_retry(self):
        rows = {}
        self.book(rows)
        self.exit_submit(rows, iter([('CANCELLED', 2, 5), ('TRADED', 3, 0)]))
        self.assertFalse(self.client().emergency_flatten_all().flat)
        self.sdk.place_order.assert_called_once()
        self.sdk.get_positions.return_value = self.response([self.position(3)])
        self.assertTrue(self.client().emergency_flatten_all().flat)
        self.assertEqual(self.sdk.place_order.call_args.kwargs['quantity'], 3)

    def test_unavailable_or_malformed_position_preserves_protection(self):
        rows = {'sl': self.row('sl', 'SELL', 'PENDING', 5, 0)}
        self.book(rows)
        for reply in ({'status': 'failure'}, self.response([{'netQty': 0}]),
                      self.response([self.position(3.5)]), self.response(None)):
            with self.subTest(reply=reply):
                self.sdk.get_positions.return_value = reply
                self.assertFalse(self.client().emergency_flatten_all().flat)
        self.sdk.cancel_order.assert_not_called()
        self.sdk.place_order.assert_not_called()

    def test_unknown_order_or_bad_terminal_count_keeps_intent(self):
        rows = {}
        self.book(rows)
        self.exit_submit(rows, iter([('TRADED', 2, 3)]))
        self.assertFalse(self.client().emergency_flatten_all().flat)
        rows['1']['orderStatus'] = 'UNRECOGNIZED'
        self.assertFalse(self.client().emergency_flatten_all().flat)
        rows.clear()
        self.assertFalse(self.client().emergency_flatten_all().flat)
        self.sdk.place_order.assert_called_once()

    def test_cancel_failure_then_late_fill_before_confirmed_cancel(self):
        rows = {'sl': self.row('sl', 'SELL', 'PENDING', 5, 0)}
        self.book(rows)
        self.sdk.cancel_order.return_value = {'status': 'failure'}
        self.assertFalse(self.client().emergency_flatten_all().flat)
        self.sdk.place_order.assert_not_called()
        rows['sl'] = self.row('sl', 'SELL', 'CANCELLED', 5, 2)
        self.assertFalse(self.client().emergency_flatten_all().flat)
        self.sdk.place_order.assert_not_called()
        self.sdk.get_positions.return_value = self.response([self.position(3)])
        self.exit_submit(rows, iter([('TRADED', 3, 0)]))
        self.assertTrue(self.client().emergency_flatten_all().flat)
        self.assertEqual(self.sdk.place_order.call_args.kwargs['quantity'], 3)

    def test_lost_ack_resolves_only_by_correlated_terminal_order(self):
        rows = {}
        self.book(rows)

        def accepted(**params):
            rows['lost'] = self.row('lost', 'SELL', 'PENDING', 5, 0, correlationId=params['tag'])
            raise TimeoutError

        self.sdk.place_order.side_effect = accepted
        self.assertFalse(self.client().emergency_flatten_all().flat)
        self.assertFalse(self.client().emergency_flatten_all().flat)
        rows['lost'].update(orderStatus='TRADED', filledQty=5)
        self.sdk.get_positions.return_value = self.response([])
        self.assertTrue(self.client().emergency_flatten_all().flat)
        self.sdk.place_order.assert_called_once()

    def test_concurrent_invocations_share_durable_intent(self):
        first_submit = Event()
        release_submit = Event()

        def timeout(**kwargs):
            first_submit.set()
            self.assertTrue(release_submit.wait(2))
            raise TimeoutError

        self.sdk.place_order.side_effect = timeout
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.client().emergency_flatten_all)
            self.assertTrue(first_submit.wait(2))
            second = pool.submit(self.client().emergency_flatten_all)
            release_submit.set()
            self.assertFalse(first.result(2).flat)
            self.assertFalse(second.result(2).flat)
        self.sdk.place_order.assert_called_once()

    def test_durable_alert_survives_order_resolution_and_restart(self):
        rows = {}
        self.book(rows)
        self.exit_submit(rows, iter([('PENDING', 0, 5)]))
        with patch.object(self.engine, 'EXIT_ESCALATION_SECONDS', 0),              patch.object(self.engine, 'send_telegram', side_effect=OSError) as notify:
            self.assertFalse(self.client().emergency_flatten_all().flat)
            notify.assert_called_once()
        rows['1'].update(orderStatus='TRADED', filledQty=5)
        self.sdk.get_positions.return_value = self.response([])
        with patch.object(self.engine, 'send_telegram', return_value=False) as notify:
            self.assertTrue(self.client().emergency_flatten_all().flat)
            notify.assert_called_once()
        state = json.loads(self.engine.EXIT_LEDGER.read_text())
        self.assertIsNone(state['cycle'])
        self.assertEqual(len(state['alerts']), 1)
        with patch.object(self.engine, 'send_telegram', return_value=True) as notify:
            self.assertTrue(self.client().emergency_flatten_all().flat)
            notify.assert_called_once()
        self.assertEqual(json.loads(self.engine.EXIT_LEDGER.read_text())['alerts'], {})

    def test_persistence_failure_prevents_submission(self):
        with patch('verified_exit.persist', side_effect=OSError):
            self.assertFalse(self.client().emergency_flatten_all().flat)
        self.sdk.place_order.assert_not_called()

    def active(self, **changes):
        return dict(dict(security_id='23', exchange_segment='NSE_EQ', instrument_type='EQUITY',
                         symbol='STAGED', weapon='EQUITY_5X', entry_price=100, stop_loss=95,
                         target_price=105, direction='LONG', quantity=5, order_id='entry',
                         entry_time='2026-11-23T10:00:00+05:30'), **changes)

    def test_monitor_retries_pending_exit_without_journaling_rejection(self):
        rows = {'entry': self.row('entry', 'BUY', 'TRADED', 5, 5, 100, createTime='2026-11-23 10:00:00')}
        self.book(rows)
        self.exit_submit(rows, iter([('REJECTED', 0, 5)] * 3 + [('TRADED', 5, 0)]))
        state = {'active_trade': self.active(flatten_pending=True)}
        with patch.object(self.engine, 'save_state') as save:
            self.engine.monitor_active_trade_loop(self.client(), state)
            self.assertIsNotNone(state['active_trade'])
            self.assertFalse(Path(self.engine.JOURNAL_FILE).exists())
            self.engine.monitor_active_trade_loop(self.client(), state)
        self.assertIsNone(state['active_trade'])
        journal = json.loads(Path(self.engine.JOURNAL_FILE).read_text())
        self.assertEqual((len(journal), journal[0]['entry_price'], journal[0]['exit_price']), (1, 100, 101))
        save.assert_called()

    def test_lost_protection_ack_blocks_competing_market_exit(self):
        self.sdk.place_order.side_effect = TimeoutError
        client = self.engine.SurvivorDhanClient(policy=self.engine.RolePolicy(trade=True))
        result = client.place_protective_sl_order('23', 'NSE_EQ', 'SELL', 5, 95)
        self.assertIsNone(result['placed'])
        self.assertFalse(client.emergency_flatten_all().flat)
        self.sdk.place_order.assert_called_once()

    def test_authorized_cli_unresolved_is_nonzero_then_verified_flat_succeeds(self):
        with patch.dict(os.environ, {fixtures.FLAG: '1'}), patch.object(
                sys, 'argv', ['survivor_engine.py', 'flatten', '--live']):
            # Execute genuine CLI dispatch, with only its ledger path redirected.
            source = Path(self.engine.__file__)
            staged = self.engine.EXIT_LEDGER.parent / source.name
            staged.write_bytes(source.read_bytes())
            self.sdk.get_positions.return_value = {'status': 'failure'}
            with self.assertRaises(SystemExit) as error:
                runpy.run_path(str(staged), run_name='__main__')
            self.assertEqual(error.exception.code, 1)
            self.sdk.get_positions.return_value = self.response([])
            runpy.run_path(str(staged), run_name='__main__')
        self.sdk.place_order.assert_not_called()
        self.http.put.assert_not_called()

    def test_stale_position_after_completed_cycle_cannot_reverse_account(self):
        rows = {}
        self.book(rows)
        self.exit_submit(rows, iter([('TRADED', 5, 0)]))
        self.assertTrue(self.client().emergency_flatten_all().flat)
        self.sdk.get_positions.return_value = self.response([self.position(5)])
        self.assertFalse(self.client().emergency_flatten_all().flat)
        self.sdk.place_order.assert_called_once()

    def test_ack_save_failure_recovers_original_intent(self):
        import verified_exit

        rows = {}
        self.book(rows)
        self.exit_submit(rows, iter([('TRADED', 5, 0)]))
        original = verified_exit.persist

        def fail_after_ack(path, ledger):
            pending = (ledger['cycle'] or {}).get('pending')
            if pending and pending['oid']:
                raise OSError('simulated storage failure after broker ack')
            original(path, ledger)

        with patch.object(verified_exit, 'persist', side_effect=fail_after_ack):
            self.assertFalse(self.client().emergency_flatten_all().flat)
        ledger = json.loads(self.engine.EXIT_LEDGER.read_text())
        self.assertIsNone(ledger['cycle']['pending']['oid'])
        self.assertTrue(self.client().emergency_flatten_all().flat)
        self.sdk.place_order.assert_called_once()

    def test_live_tick_unprotected_trade_stays_active_on_unknown_exit(self):
        from datetime import datetime

        engine = self.engine
        self.sdk.BUY, self.sdk.SELL, self.sdk.INTRA = 'BUY', 'SELL', 'INTRADAY'
        self.sdk.get_fund_limits.return_value = self.response({'availabelBalance': 30000})
        self.sdk.get_positions.return_value = self.response([])
        rows = {}
        self.book(rows)
        calls = iter(['TRADED', 'REJECTED', 'TIMEOUT'])

        def submit(**kwargs):
            status = next(calls)
            if status == 'TIMEOUT':
                raise TimeoutError
            qty = kwargs['quantity']
            oid = 'entry' if status == 'TRADED' else 'sl'
            rows[oid] = self.row(oid, kwargs['transaction_type'], status, qty,
                                 qty if status == 'TRADED' else 0, 100,
                                 correlationId=kwargs['tag'], createTime='2026-11-23 10:00:00')
            self.sdk.get_positions.return_value = self.response([self.position(qty)])
            return self.response({'orderId': oid})

        self.sdk.place_order.side_effect = submit
        state = {'status': 'ALIVE', 'mortality_armed': True, 'peak_equity': 30000}
        now = datetime(2026, 11, 23, 10, tzinfo=engine.IST)
        candidate = dict(security_id='23', symbol='STAGED', direction='LONG', ltp=100, atr=1, reason='fixture')
        with patch.object(engine, 'datetime', wraps=datetime) as clock,              patch.object(engine, 'load_state', return_value=state), patch.object(engine, 'save_state'),              patch.object(engine, 'acquire_engine_lock', return_value=9), patch.object(engine, 'release_engine_lock'),              patch.object(engine, 'INDEX_UNIVERSE', {}), patch.object(engine, 'scan_equity_candidates', return_value=candidate):
            clock.now.return_value = now
            engine.cmd_tick(policy=engine.RolePolicy(trade=True))
        self.assertTrue(state['active_trade']['flatten_pending'])
        self.assertEqual(state['active_trade']['order_id'], 'entry')
        self.assertFalse(Path(engine.JOURNAL_FILE).exists())
        self.assertEqual([c.kwargs['order_type'] for c in self.sdk.place_order.call_args_list],
                         ['MARKET', 'SL', 'MARKET'])
