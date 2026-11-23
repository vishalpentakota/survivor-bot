"""Exercise durable replay fencing and independent exits through restart ticks."""
import json
import unittest

import test_exit_restart_matrix as fixtures


class ExitFencePermutations(fixtures.RestartMatrix):
    def probe(self, disputed, recoverable, reverse, evidence):
        self.initialize("current_legacy")
        self.rows["entry"]["securityId"] = disputed
        pending = self.engine.load_state(read_only=True)
        pending["active_trade"]["security_id"] = disputed
        pending.update(daily_halted=True, daily_halt_reason="Existing halt")
        self.write_state(pending)
        self.journal_path.write_text(json.dumps([dict(self.journal()[0], security_id=disputed)]))

        nets = {disputed: 5}
        self.sdk.get_positions.side_effect = lambda: self.response([
            dict(self.position(net), securityId=key) for key, net in nets.items() if net
        ])

        def submit(**params):
            key = params["security_id"]
            side = params["transaction_type"]
            quantity = params["quantity"]
            oid = "initial-exit" if key == disputed else "independent-exit"
            self.rows[oid] = dict(
                self.row(oid, side, "TRADED", quantity, quantity, 90,
                         createTime="2026-11-23 10:02:00", correlationId=params["tag"]),
                securityId=key,
            )
            nets[key] += quantity if side == "BUY" else -quantity
            return self.response({"orderId": oid})

        self.sdk.place_order.side_effect = submit
        self.assertEqual(self.restart(), 0)
        self.assertIsNone(self.state()["active_trade"])
        journal = self.journal()
        self.assertIn("exit_proof", journal[0])
        self.write_state(pending)

        self.rows["later"] = dict(self.row("later", "BUY", "TRADED", 5, 5,
                                           createTime="2026-11-23 10:12:00"),
                                   securityId=disputed)
        engine = self.engine
        with engine.exit_lock(engine.EXIT_LEDGER) as ledger:
            self.assertIsNone(engine._exit_coordinator(self.client()).closure(ledger))
        original_proof = json.loads(engine.EXIT_LEDGER.read_text())["replay_executions"]

        # Even after the disputed position agrees with the later fill, its
        # pending trade must stay fenced while the other exit is recovered.
        net = 4 if recoverable == "24" else -4
        nets.update({disputed: 5, recoverable: net})
        if evidence != "uncorroborated":
            self.rows["independent-entry"] = dict(self.opening_orders(net, recoverable)["opening"],
                                                   orderId="independent-entry")
        if evidence == "unknown":
            self.rows["unattributed"] = {"orderId": "unattributed", "orderStatus": "PENDING"}
        if reverse:
            self.rows = dict(reversed(list(self.rows.items())))
            nets = dict(reversed(list(nets.items())))
        self.book(self.rows)
        self.sdk.reset_mock()

        exit_request = (recoverable, "SELL" if net > 0 else "BUY", abs(net))
        expected = [exit_request] if evidence == "verified" else []
        for _ in range(3):
            reads = self.sdk.get_positions.call_count
            self.assertEqual(self.restart(), 1)
            self.assertGreater(self.sdk.get_positions.call_count, reads)
            persisted = self.state()
            persisted.pop("last_heartbeat", None)
            unchanged = pending.copy()
            unchanged.pop("last_heartbeat", None)
            self.assertEqual(persisted, unchanged)
            self.assertEqual(self.journal(), journal)
            ledger = json.loads(engine.EXIT_LEDGER.read_text())
            self.assertEqual(ledger["replay_executions"], original_proof)
            self.assertEqual(ledger["replay_check"], "contradicted")
            orders = [(call.kwargs["security_id"], call.kwargs["transaction_type"], call.kwargs["quantity"])
                      for call in self.sdk.place_order.call_args_list]
            self.assertEqual(orders, expected)
            self.assertEqual(nets, {disputed: 5, recoverable: 0 if expected else net})
            self.sdk.cancel_order.assert_not_called()
            self.sdk.modify_order.assert_not_called()

    def permutations(self, evidence):
        self.configure_persistence()
        for disputed, recoverable in (("23", "24"), ("24", "23")):
            for reverse in (False, True):
                with self.subTest(disputed=disputed, recoverable=recoverable, reverse=reverse):
                    self.probe(disputed, recoverable, reverse, evidence)

    def test_verified_recovery_in_both_directions_and_orders(self):
        self.permutations("verified")

    def test_uncorroborated_exposure_cannot_authorize_recovery(self):
        self.permutations("uncorroborated")

    def test_unattributed_account_evidence_blocks_all_writes(self):
        self.permutations("unknown")


def load_tests(loader, tests, pattern):
    return unittest.TestSuite(ExitFencePermutations(name) for name in (
        "test_verified_recovery_in_both_directions_and_orders",
        "test_uncorroborated_exposure_cannot_authorize_recovery",
        "test_unattributed_account_evidence_blocks_all_writes",
    ))
