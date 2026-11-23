"""Validate scratch rollback evidence without loading either trading engine."""

from datetime import date, datetime
from hashlib import sha256
import json
import math
import os
import stat


STATE = "survivor_state.json"
JOURNAL = "survivor_trades.json"
LEDGER = "survivor_exits.json"
POSITIONS = "rollback_positions.json"


def check(condition, reason):
    if not condition:
        raise ValueError(reason)


def unique_object(pairs):
    result = dict(pairs)
    check(len(result) == len(pairs), "duplicate JSON field")
    return result


def number(value):
    return type(value) in (float, int) and math.isfinite(value)


def text(value):
    return isinstance(value, str) and bool(value.strip())


def timestamp(value):
    parsed = datetime.fromisoformat(value)
    check(parsed.utcoffset() is not None, "timestamp requires a timezone")
    return parsed


def read_evidence(path):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            check(stat.S_ISREG(os.fstat(fd).st_mode), "expected regular file")
            with os.fdopen(fd, "rb", closefd=False) as stream:
                raw = stream.read()
        finally:
            os.close(fd)
        return json.loads(raw.decode("utf-8"), object_pairs_hook=unique_object,
                          parse_constant=lambda value: check(False, "non-finite JSON value")), sha256(raw).hexdigest()
    except (OSError, ValueError, RecursionError) as exc:
        raise ValueError(f"{path.name}: {exc}") from exc


def validate_state(state):
    check(isinstance(state, dict) and "active_trade" in state and state["active_trade"] is None
          and state.get("status") in ("ALIVE", "DEAD_COMA", "AWAITING_FUNDING"),
          "state is invalid or has an active trade")
    balances = {"seed_capital", "death_floor", "peak_equity", "current_equity", "available_cash",
                "unrealized_pnl", "realized_pnl_total", "daily_start_equity"}
    timestamps = {"created_at", "last_heartbeat", "last_sl_time", "death_timestamp"}
    optional = {"emergency_recovery", "applied_exit_proofs", "pos_check_failures", "entries_blocked_reason",
                "actual_realized_pnl_total", "actual_broker_charges_total", "realized_pnl_total_provenance"}
    required = balances | timestamps | {
        "status", "active_trade", "mortality_armed", "daily_date", "daily_halted", "daily_halt_reason",
        "daily_trades_count", "daily_consecutive_losses", "last_sl_direction", "total_trades",
        "wins", "losses", "death_reason",
    }
    check(required <= state.keys() <= required | optional, "unknown or missing state fields")
    check(type(state.get("daily_halted")) is bool and type(state.get("mortality_armed")) is bool
          and all(type(state.get(key)) is int and state[key] >= 0 for key in
                  ("total_trades", "wins", "losses", "daily_trades_count", "daily_consecutive_losses"))
          and all(number(state[key]) for key in balances)
          and isinstance(state["daily_halt_reason"], str)
          and state["last_sl_direction"] in (None, "LONG", "SHORT")
          and state["unrealized_pnl"] == 0
          and type(state.get("pos_check_failures", 0)) is int and state.get("pos_check_failures", 0) == 0
          and state.get("entries_blocked_reason") in (None, ""),
          "state has invalid counters, balances, or unresolved entry/position evidence")
    timestamp(state["created_at"])
    for key in timestamps - {"created_at"}:
        if state[key] is not None:
            timestamp(state[key])
    if state["daily_date"] != "":
        date.fromisoformat(state["daily_date"])
    check(state["death_reason"] is None or text(state["death_reason"]), "invalid state death reason")
    if "emergency_recovery" in state:
        recovery = state["emergency_recovery"]
        check(isinstance(recovery, dict) and set(recovery) == {"status", "id", "reason", "started_at"}
              and recovery["status"] == "verified" and all(text(recovery.get(key)) for key in ("id", "reason", "started_at")),
              "state recovery is unresolved or invalid")
        timestamp(recovery["started_at"])


def closure(row):
    check(row["fills_verified"] is True and row["reconciled_with_broker"] is True
          and row["pnl_provenance"] == "broker_actual" and text(row["exit_reason"]),
          "journal row is not fully reconciled")
    started, ended = timestamp(row["entry_time"]), timestamp(row["exit_time"])
    identity = [str(row[key]) for key in
                ("order_id", "security_id", "exchange_segment", "entry_time", "weapon", "direction")]
    order, security, segment, _, weapon, direction = identity
    proof = row["exit_proof"]
    instrument = [security, segment, "INTRADAY"]
    check(started <= ended and text(row["order_id"]) and security.isdecimal()
          and (segment, weapon) in {("NSE_FNO", "OPTIONS"), ("NSE_EQ", "EQUITY_5X")}
          and direction in ("LONG", "SHORT") and proof["trade"] == identity
          and proof["instrument"] == instrument and text(proof["epoch"]),
          "journal closure identity is inconsistent")
    entry, exits = proof["entry"], proof["exits"]
    check(isinstance(exits, list) and bool(exits), "missing exit executions")
    side = "BUY" if weapon == "OPTIONS" or direction == "LONG" else "SELL"
    opposite = "SELL" if side == "BUY" else "BUY"
    executions = {}
    for execution, expected_side in [(entry, side), *((item, opposite) for item in exits)]:
        oid = execution["id"]
        check(text(oid) and oid not in executions and execution["side"] == expected_side
              and execution["instrument"] == instrument
              and execution["status"] in ("TRADED", "CANCELLED", "EXPIRED")
              and type(execution["quantity"]) is int and execution["quantity"] > 0
              and number(execution["price"]) and execution["price"] > 0
              and number(execution["broker_charge"]) and execution["broker_charge"] >= 0,
              "invalid journal execution")
        executions[oid] = execution
    quantity = entry["quantity"]
    check(entry["id"] == order and quantity == sum(item["quantity"] for item in exits),
          "journal executions do not balance")
    exit_price = sum(item["price"] * item["quantity"] for item in exits) / quantity
    gross = (exit_price - entry["price"]) * quantity * (1 if side == "BUY" else -1)
    charges = round(sum(item["broker_charge"] for item in executions.values()), 2)
    expected = dict(quantity=quantity, entry_price=entry["price"], exit_price=round(exit_price, 2),
                    gross_pnl=round(gross, 2), net_pnl=round(gross - charges, 2),
                    actual_broker_charges=charges)
    check(all(number(row[key]) and row[key] == value for key, value in expected.items()),
          "journal amounts contradict execution proof")
    return identity, proof["epoch"], executions


def validate_journal(state, journal):
    check(isinstance(journal, list), "invalid journal")
    identities, proofs, claimed = [], [], set()
    for row in journal:
        identity, epoch, executions = closure(row)
        check(identity not in identities and claimed.isdisjoint(executions), "duplicate journal executions")
        identities.append(identity)
        proofs.append((epoch, executions))
        claimed.update(executions)
    applied = state.get("applied_exit_proofs", [])
    wins = sum(row["net_pnl"] > 0 for row in journal)
    check(isinstance(applied, list) and sorted(applied) == sorted(identities)
          and state["total_trades"] == len(journal) and state["wins"] == wins
          and state["losses"] == len(journal) - wins
          and state["realized_pnl_total"] == round(sum(row["net_pnl"] for row in journal), 2),
          "state and journal reconciliation disagree")
    for key, field in (("actual_realized_pnl_total", "net_pnl"),
                       ("actual_broker_charges_total", "actual_broker_charges")):
        if key in state:
            check(number(state[key]) and state[key] == round(sum(row[field] for row in journal), 2),
                  "state actual totals disagree with reconciled journal")
    return proofs


def validate_ledger(ledger, proofs):
    check(isinstance(ledger, dict) and set(ledger) - {"replay_epoch", "replay_executions"} ==
          {"cycle", "alerts", "fills", "flat_book"} and ledger["cycle"] is None
          and ledger["alerts"] == {} and ledger["fills"] == {},
          "exit ledger has pending or unknown work")
    book = ledger["flat_book"]
    check(isinstance(book, dict) and all(text(key) and type(qty) is int and qty >= 0
                                        for key, qty in book.items()),
          "exit ledger has no verified flat book")
    verified = {oid: execution["quantity"] for _, executions in proofs
                for oid, execution in executions.items()}
    check({oid: qty for oid, qty in book.items() if qty} == verified,
          "exit ledger and journal execution quantities disagree")
    if proofs or "replay_epoch" in ledger or "replay_executions" in ledger:
        check((ledger.get("replay_epoch"), ledger.get("replay_executions")) in proofs,
              "exit ledger replay evidence disagrees with journal")


def preflight(app, *, now):
    """Return the validated state and journal or raise before engine import."""
    try:
        # This marker means a corrupt-state flatten could not settle local accounting.
        recovery = app / (STATE + ".recovery")
        try:
            recovery.lstat()
        except FileNotFoundError:
            pass
        else:
            raise ValueError("out-of-band state recovery requires accounting review")
        state, state_hash = read_evidence(app / STATE)
        journal, journal_hash = read_evidence(app / JOURNAL)
        ledger, ledger_hash = read_evidence(app / LEDGER)
        snapshot, _ = read_evidence(app / POSITIONS)
        check(isinstance(snapshot, dict) and set(snapshot) ==
              {"source", "status", "scope", "observed_at", "positions", "pending_orders", "evidence_sha256"}
              and snapshot["source"] == "synthetic-broker-fixture"
              and snapshot["scope"] == "all-account-exposure" and snapshot["status"] == "verified"
              and snapshot["positions"] == [] and snapshot["pending_orders"] == [],
              "position evidence does not verify flat synthetic account exposure")
        age = (now - timestamp(snapshot["observed_at"])).total_seconds()
        check(0 <= age <= 60, "position evidence is stale or future-dated")
        check(snapshot["evidence_sha256"] == {STATE: state_hash, JOURNAL: journal_hash, LEDGER: ledger_hash},
              "position snapshot is bound to different evidence")
        validate_state(state)
        validate_ledger(ledger, validate_journal(state, journal))
    except (OSError, ValueError, TypeError, KeyError, OverflowError, RecursionError) as exc:
        raise RuntimeError(f"ROLLBACK REFUSED: {exc}") from exc
    return state, journal
