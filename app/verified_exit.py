"""Persistent coordination for Survivor's position-reducing orders."""
from collections import defaultdict
from contextlib import contextmanager
from copy import deepcopy
import fcntl
import json
import math
import os
from pathlib import Path
import re
import tempfile
import time
from uuid import uuid4


TERMINAL = {"TRADED", "REJECTED", "CANCELLED", "EXPIRED"}
WORKING = {"PENDING", "TRANSIT", "PART_TRADED", "TRIGGER_PENDING", "CONFIRM"}
STATUSES = TERMINAL | WORKING


def integer(value):
    if type(value) is int or isinstance(value, str) and re.fullmatch(r"-?[0-9]+", value):
        return int(value)
    raise ValueError("Invalid integer in broker data")


def identifier(value):
    if type(value) not in (str, int) or not str(value).strip():
        raise ValueError("Missing broker identifier")
    return str(value).strip()


def instrument(row):
    return tuple(identifier(row[field]) for field in ("securityId", "exchangeSegment", "productType"))


def response_data(response):
    if not isinstance(response, dict) or response.get("status") != "success":
        raise ValueError("Broker read unavailable")
    return response["data"]


def positions(broker):
    rows = response_data(broker.get_positions())
    if not isinstance(rows, list):
        raise ValueError("Invalid position book")
    result = {}
    for row in rows:
        key = instrument(row)
        if key in result:
            raise ValueError("Duplicate position")
        result[key] = integer(row["netQty"])
    return result


def orders(broker):
    rows = response_data(broker.get_order_list())
    if not isinstance(rows, list):
        raise ValueError("Invalid order book")
    result = {}
    for row in rows:
        oid = identifier(row["orderId"])
        if oid in result:
            raise ValueError("Invalid order book entry")
        result[oid] = row.copy()
    return result


def order_detail(broker, oid):
    row = response_data(broker.get_order_by_id(oid))
    if isinstance(row, list) and len(row) == 1:
        row = row[0]
    if identifier(row["orderId"]) != oid or row["orderStatus"] not in STATUSES:
        raise ValueError("Invalid order detail")
    return row


def fill(row):
    quantity = integer(row["quantity"])
    filled = integer(row["filledQty"] if "filledQty" in row else row["tradedQuantity"])
    status = row["orderStatus"]
    if (status not in STATUSES or quantity <= 0 or not 0 <= filled <= quantity
            or status == "TRADED" and filled != quantity or status == "REJECTED" and filled):
        raise ValueError("Inconsistent order quantities")
    return filled


def same_execution(first, second):
    return (
        instrument(first) == instrument(second)
        and first["transactionType"] == second["transactionType"]
        and first["orderStatus"] == second["orderStatus"]
        and integer(first["quantity"]) == integer(second["quantity"])
        and fill(first) == fill(second)
        and (not fill(first) or float(first["averageTradedPrice"]) == float(second["averageTradedPrice"]))
    )


def execution_evidence(oid, row):
    return {"id": oid, "status": row["orderStatus"], "quantity": fill(row),
            "price": float(row["averageTradedPrice"]), "side": row["transactionType"],
            "instrument": list(instrument(row)), "created": row.get("createTime")}


@contextmanager
def locked(path):
    # Keep a stable inode for flock. Kernel releases the lock on process exit.
    with open(str(path) + ".lock", "a", encoding="utf-8") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            with open(path, encoding="utf-8") as storage:
                state = json.load(storage)
        except FileNotFoundError:
            state = {"cycle": None, "alerts": {}, "fills": {}, "flat_book": None}
        if (set(state) - {"replay_epoch", "replay_executions", "replay_check"} != {"cycle", "alerts", "fills", "flat_book"}
                or not isinstance(state["alerts"], dict) or not isinstance(state["fills"], dict)
                or state["cycle"] is not None and not isinstance(state["cycle"], dict)):
            raise ValueError("Invalid exit ledger")
        yield state


def persist(path, state):
    path = Path(path)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                     prefix=".exit-", delete=False) as storage:
        temporary = storage.name
        try:
            json.dump(state, storage, allow_nan=False)
            storage.flush()
            os.fsync(storage.fileno())
            os.replace(temporary, path)
            directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


class FlattenResult(list):
    def __init__(self, message, *, flat=False, unknown=False):
        super().__init__([message])
        self.flat = flat
        self.unknown = unknown


class MutationBlocked(RuntimeError):
    """A local policy refused a broker mutation before submission."""


class ExitCoordinator:
    def __init__(self, broker, path, notify, *, polls, interval, deadline, entry_allowed=None, budget=None):
        self.broker = broker
        self.path = path
        self.notify = notify
        self.polls = polls
        self.interval = interval
        self.deadline = deadline
        self.entry_allowed = entry_allowed
        self.budget = budget

    def check_deadline(self):
        if self.budget is not None and self.budget() <= 0:
            raise MutationBlocked("Scheduled liquidation deadline expired")

    def check_entry_allowed(self):
        if self.entry_allowed is not None and not self.entry_allowed():
            raise MutationBlocked("Entry window closed")

    def save(self, state):
        persist(self.path, state)

    def deliver(self, state):
        for alert_id, message in list(state["alerts"].items()):
            try:
                delivered = self.notify(f"{alert_id}: {message}")
            except Exception:
                continue
            if delivered is True:
                del state["alerts"][alert_id]
                self.save(state)

    def start(self, state, *, flatten, replay_fenced=False):
        state["cycle"] = {
            "id": uuid4().hex, "deadline": time.time() + self.deadline,
            "pending": None, "flatten": flatten, "escalated": False, "confirmed": {}, "exposure": {},
        }
        # Persist newer intent before any broker write, even if its acknowledgement is lost.
        if replay_fenced:
            state["cycle"]["replay_epoch"] = state["replay_epoch"]
        else:
            state["replay_epoch"] = state["cycle"]["id"]
            state.pop("replay_executions", None)
        self.save(state)
        return state["cycle"]

    def escalate(self, state, message, verified):
        cycle = state["cycle"]
        if cycle and not cycle["escalated"] and time.time() >= cycle["deadline"]:
            cycle["escalated"] = True
            keys = set(verified) | {tuple(p["key"]) for p in cycle["pending_by_key"].values()}
            unknown = cycle.get("account_unknown", {})
            details = []
            if unknown:
                details.append(f"instrument UNKNOWN, intent/order {unknown['oid']}, "
                               "confirmed fills UNKNOWN, remaining verified qty UNKNOWN")
            for key in sorted(keys):
                pending = cycle["pending_by_key"].get(self.key_id(key), {})
                identity = pending.get("oid") or pending.get("tag") or "UNKNOWN"
                confirmed = {oid: qty for oid, (instrument_key, qty, _) in cycle["confirmed"].items()
                             if tuple(instrument_key) == key}
                confirmed.update({oid: qty for oid, (instrument_key, qty) in cycle.get("observed", {}).items()
                                  if tuple(instrument_key) == key})
                if identity == "UNKNOWN" and confirmed:
                    identity = ", ".join(confirmed)
                remaining = verified.get(key, "UNKNOWN")
                details.append(f"instrument {key}, intent/order {identity}, confirmed fills {confirmed}, "
                               f"remaining verified qty {remaining}")
            state["alerts"][cycle["id"]] = f"{message}. " + (
                "; ".join(details) or "instrument UNKNOWN, intent/order UNKNOWN, "
                "confirmed fills UNKNOWN, remaining verified qty UNKNOWN")
            self.save(state)
            self.deliver(state)

    @staticmethod
    def key_id(key):
        return json.dumps(key, separators=(",", ":"))

    def snapshot(self):
        self.check_deadline()
        before = orders(self.broker)
        self.check_deadline()
        exposure = positions(self.broker)
        self.check_deadline()
        after = orders(self.broker)
        self.check_deadline()
        if after != before:
            raise ValueError("Order book changed during position read")
        return before, exposure

    def fenced_instruments(self, state, book):
        executions = state["replay_executions"]
        if not executions or not executions.keys() <= book.keys():
            raise ValueError("Replay executions cannot be attributed")
        fenced = set()
        for oid, evidence in executions.items():
            fenced.add(tuple(evidence["instrument"]))
            fenced.add(instrument(book[oid]))
            fenced.add(instrument(self.detail(oid)))
        return fenced

    def flatten(self, max_retries):
        with locked(self.path) as state:
            replay_fenced = "replay_check" in state
            cycle = state["cycle"]
            if replay_fenced and (state["replay_check"] != "contradicted"
                                  or not state.get("replay_executions")
                                  or not state.get("replay_epoch")
                                  or cycle and cycle.get("replay_epoch") != state["replay_epoch"]):
                return FlattenResult("Flatten INCOMPLETE: replay evidence requires verification")
            self.deliver(state)
            cycle = cycle or self.start(state, flatten=True, replay_fenced=replay_fenced)
            if not cycle["flatten"] and cycle.get("protected"):
                cycle["deadline"] = time.time() + self.deadline
            cycle["flatten"] = True
            pending = cycle.setdefault("pending_by_key", {})
            if cycle["pending"]:
                legacy = cycle["pending"]
                if "key" not in legacy:
                    # Preserve uncertainty about the previously attempted cancellation.
                    cycle["account_unknown"] = legacy
                else:
                    pending[self.key_id(legacy["key"])] = legacy
                cycle["pending"] = None
            self.save(state)
            message = "Exit has no verified terminal order and consistent broker position"
            verified = {}
            attempts = defaultdict(int)
            deferred = set()
            unknown_exposure = False
            try:
                for _ in range(self.polls + max_retries):
                    verified = {}
                    book, exposure = self.snapshot()
                    grouped = defaultdict(dict)
                    for oid, row in book.items():
                        grouped[instrument(row)][oid] = row
                    fenced = self.fenced_instruments(state, book) if replay_fenced else set()
                    unknown = cycle.get("account_unknown")
                    if unknown:
                        oid = unknown["oid"]
                        row = book.get(oid)
                        if not row or row["orderStatus"] not in TERMINAL or not same_execution(
                                row, order_detail(self.broker, oid)):
                            break
                        del cycle["account_unknown"]
                        self.save(state)

                    keys = dict.fromkeys([*exposure, *grouped, *(tuple(p["key"]) for p in pending.values()),
                                          *(tuple(key) for key, _ in cycle["exposure"].values())])
                    reserved = set()
                    for intent in pending.values():
                        for oid, row in book.items():
                            if (oid == intent["oid"] or intent.get("tag") and row.get("correlationId") == intent["tag"]):
                                if instrument(row) != tuple(intent["key"]):
                                    reserved.add(instrument(row))
                    for key in keys:
                        verified[key] = "UNKNOWN"
                    changed = False
                    for key in keys:
                        key_id = self.key_id(key)
                        if key in fenced or key in reserved or (key_id in deferred and key_id not in pending):
                            continue
                        try:
                            changed = self.advance(state, cycle, key, grouped[key], book, exposure,
                                                   attempts, max_retries, deferred, verified)
                        except (KeyError, ValueError, TypeError):
                            # Identified but unverified data blocks only its instrument.
                            continue
                        if changed:
                            break
                    if changed:
                        continue
                    if (not pending and not any(net for key, net in exposure.items() if key not in fenced) and
                            not any(row["orderStatus"] in WORKING for key, rows in grouped.items()
                                    if key not in fenced for row in rows.values()) and
                            all(value == 0 for key, value in verified.items() if key not in fenced) and
                            self.snapshot() == (book, exposure)):
                        if cycle.get("unresolved"):
                            self.escalate(state, message, verified)
                        state["flat_book"] = (state["flat_book"] or {}) | {
                            oid: fill(row) for key, rows in grouped.items() if key not in fenced
                            for oid, row in rows.items()
                        }
                        state["cycle"] = None
                        self.save(state)
                        if replay_fenced:
                            return FlattenResult("Flatten INCOMPLETE: disputed trade remains fenced")
                        return FlattenResult("Broker confirmed flat with no outstanding orders", flat=True)
                    delay = self.interval if self.budget is None else min(self.interval, max(0, self.budget()))
                    time.sleep(delay)
            except Exception as exc:
                verified = {key: "UNKNOWN" for key in verified}
                unknown_exposure = True
                message = f"Exit verification interrupted ({type(exc).__name__})"
            if not cycle.get("unresolved"):
                cycle["unresolved"] = True
                self.save(state)
            self.escalate(state, message, verified)
            return FlattenResult(f"Flatten INCOMPLETE: {message}", unknown=unknown_exposure)

    def detail(self, oid):
        self.check_deadline()
        try:
            return order_detail(self.broker, oid)
        except Exception as exc:
            raise ValueError("Order detail unavailable") from exc

    def advance(self, state, cycle, key, rows, book, exposure, attempts, limit, deferred, verified):
        key_id = self.key_id(key)
        pending = cycle["pending_by_key"].get(key_id)
        net = exposure.get(key, 0)
        for oid, (stored_key, filled, status) in cycle["confirmed"].items():
            if tuple(stored_key) == key and (
                    oid not in rows or rows[oid]["orderStatus"] != status or fill(rows[oid]) != filled):
                return False
        for stored_key, expected in cycle["exposure"].values():
            if tuple(stored_key) == key and net != expected:
                return False

        if pending:
            oid = pending["oid"]
            if oid is None:
                matches = [oid for oid, row in book.items() if row.get("correlationId") == pending["tag"]]
                if len(matches) != 1:
                    return False
                oid = matches[0]
            row = rows.get(oid)
            if not row:
                return False
            detail = self.detail(oid)
            if not self.matches_pending(pending, row, detail):
                return False
            filled = fill(row)
            status = row["orderStatus"]
            if (filled < pending.get("verified_fill", pending["filled"])
                    or pending.get("verified_terminal", status) != status):
                return False
            evidence = {"oid": oid, "verified_fill": filled}
            if "market" not in pending and self.is_market(pending):
                evidence["market"] = True
            for field in ("orderType", "correlationId"):
                if field in row or field in detail:
                    evidence[field] = row[field] if field in row else detail[field]
            if status in TERMINAL:
                evidence["verified_terminal"] = status
            if any(pending.get(field) != value for field, value in evidence.items()):
                pending.update(evidence)
                self.save(state)
            cycle.setdefault("observed", {})[oid] = [list(key), filled]
            if status in TERMINAL:
                return self.settle(state, cycle, book, exposure, key_id=key_id)
            return self.advance_cancellation(state, cycle, key_id, pending, row, rows,
                                             book, exposure, deferred)

        signed, working = self.execution_net(state, cycle, key, rows)
        if working:
            oid, row = working[0]
            detail = self.detail(oid)
            # Cancellation is safe for an identified order with no established
            # fill discrepancy. Reconcile its terminal execution before exiting.
            if (signed == net
                    and same_execution(row, detail)
                    and self.snapshot() == (book, exposure)):
                self.cancel(state, cycle, oid, row, detail, exposure, key_id=key_id)
                if cycle["pending_by_key"][key_id].get("cancel_rejected"):
                    deferred.add(key_id)
                return True
            return False
        if signed != net or self.snapshot() != (book, exposure):
            return False
        verified[key] = net
        if net and attempts[key_id] < limit:
            attempts[key_id] += 1
            self.submit(state, cycle, key, net, abs(net), key_id=key_id)
            return True
        return False

    def matches_pending(self, pending, row, detail):
        if (not same_execution(row, detail) or instrument(row) != tuple(pending["key"])
                or row["transactionType"] != pending["side"]
                or integer(row["quantity"]) != pending["quantity"]):
            return False
        for field, expected in (("correlationId", pending.get("tag")),
                                ("orderType", self.broker.MARKET if self.is_market(pending)
                                 else pending.get("requested_type"))):
            values = [candidate[field] for candidate in (row, detail) if field in candidate]
            known = pending.get(field, expected)
            if field in pending and not values:
                return False
            if values and any(value != (known if known is not None else values[0]) for value in values):
                return False
        return True

    @staticmethod
    def is_market(pending):
        # Earlier submissions recorded a tag and the entry/protection flags.
        return pending.get("market", bool(pending.get("tag")) and not (
            pending.get("entry") or pending.get("protection") or pending.get("cancel_requested")))

    def advance_cancellation(self, state, cycle, key_id, pending, row, rows, book, exposure, deferred):
        key = tuple(pending["key"])
        net = exposure.get(key, 0)
        filled = fill(row)
        remaining = pending["quantity"] - filled
        delta = filled - pending["filled"]
        expected = pending["net"] + (delta if pending["side"] == "BUY" else -delta)
        if key_id in deferred or not remaining or net != expected:
            return False
        market = pending.get("market")
        if market and (remaining != abs(net) or pending["side"] != ("SELL" if net > 0 else "BUY")):
            return False
        if pending.get("cancel_requested"):
            if not pending.get("cancel_rejected") or pending.get("cancel_attempts", 1) >= 2:
                return False
        elif market:
            if row["orderStatus"] != "PART_TRADED" or not filled or time.time() < cycle["deadline"]:
                return False
        elif not (pending.get("protection") or pending.get("entry")):
            return False
        if pending.get("cancel_requested") or market:
            signed, _ = self.execution_net(state, cycle, key, rows)
            if signed != net:
                return False
        if self.snapshot() != (book, exposure):
            return False
        oid = pending["oid"]
        self.cancel(state, cycle, oid, row, self.detail(oid), exposure, key_id=key_id, prior=pending)
        if cycle["pending_by_key"][key_id].get("cancel_rejected"):
            deferred.add(key_id)
        return True

    def execution_net(self, state, cycle, key, rows):
        # Dhan's order executions independently establish signed exposure. A
        # successful but empty book cannot corroborate a nonzero position.
        signed = 0
        working = []
        baseline = state["flat_book"] or {}
        for oid, row in rows.items():
            filled = fill(row)
            side = row["transactionType"]
            if side not in {"BUY", "SELL"}:
                raise ValueError("Unverified execution")
            if oid in baseline:
                if filled != baseline[oid] or row["orderStatus"] not in TERMINAL:
                    raise ValueError("Unverified execution")
                continue
            if filled:
                if not same_execution(row, self.detail(oid)):
                    raise ValueError("Unverified execution")
                cycle.setdefault("observed", {})[oid] = [list(key), filled]
            signed += filled if side == "BUY" else -filled
            if row["orderStatus"] in WORKING:
                working.append((oid, row))

        return signed, working

    def settle(self, state, cycle, book, exposure, *, key_id=None):
        pending = cycle["pending"] if key_id is None else cycle["pending_by_key"][key_id]
        oid = pending["oid"]
        if oid is None:
            matches = [key for key, row in book.items() if row.get("correlationId") == pending["tag"]]
            if len(matches) != 1:
                return False
            oid = matches[0]
        if oid not in book or book[oid]["orderStatus"] not in TERMINAL:
            return False
        row = self.detail(oid)
        listed = book[oid]
        key = tuple(pending["key"])
        filled = fill(row)
        if (not self.matches_pending(pending, listed, row)
                or filled < pending.get("verified_fill", pending["filled"])
                or pending.get("verified_terminal", row["orderStatus"]) != row["orderStatus"]):
            return False
        cycle["observed"] = cycle.get("observed", {}) | {oid: [list(key), filled]}
        delta = filled - pending["filled"]
        expected = pending["net"] + (delta if pending["side"] == "BUY" else -delta)
        if pending.get("market") and pending.get("cancel_requested"):
            rows = {order_id: item for order_id, item in book.items() if instrument(item) == key}
            if self.execution_net(state, cycle, key, rows)[0] != expected:
                return False
        fresh_book, fresh_exposure = self.snapshot()
        if (delta < 0 or expected * pending["net"] < 0
                or fresh_book != book or fresh_exposure != exposure
                or fresh_exposure.get(key, 0) != expected):
            return False
        if filled and not pending.get("entry"):
            if pending.get("protection"):
                source = "EXCHANGE_STOP"
            elif pending.get("market") or not pending.get("cancel_requested"):
                source = "MARKET"
            else:
                source = "UNKNOWN"
            state["fills"][oid] = {"key": list(key), "source": source}
        if key_id is None:
            cycle["pending"] = None
        else:
            del cycle["pending_by_key"][key_id]
        cycle["confirmed"][oid] = [list(key), filled, row["orderStatus"]]
        cycle["exposure"][oid] = [list(key), expected]
        self.save(state)
        return True

    def submit(self, state, cycle, key, net, qty, *, protection=None, entry=None, key_id=None):
        previous = deepcopy(cycle) if self.budget is not None else None
        security, exchange, product = key
        side = entry["transaction_type"] if entry else ("SELL" if net > 0 else "BUY")
        tag = uuid4().hex[:24]
        pending = dict(oid=None, tag=tag, key=list(key), net=net, side=side,
                       quantity=qty, filled=0, protection=protection is not None, entry=entry is not None,
                       market=protection is None and entry is None)
        if entry is not None:
            pending["requested_type"] = entry["order_type"]
        if protection is not None:
            pending.update(requested_type=self.broker.SL, prices=protection)
        cycle["exposure"] = {
            oid: pair for oid, pair in cycle["exposure"].items() if tuple(pair[0]) != key
        }
        if key_id is None:
            cycle["pending"] = pending
        else:
            cycle["pending_by_key"][key_id] = pending
        self.save(state)
        params = dict(security_id=security, exchange_segment=exchange, product_type=product,
                      transaction_type=side, quantity=qty, tag=tag)
        if entry is not None:
            self.check_entry_allowed()
            try:
                reply = self.broker.place_order(**entry, tag=tag)
            except Exception:
                return None
        elif protection is None:
            self.check_deadline_after_intent(state, cycle, previous)
            try:
                reply = self.broker._place_exit_order(**params, order_type=self.broker.MARKET, price=0)
            except Exception:
                return None
        else:
            try:
                reply = self.broker.place_order(**params, order_type=self.broker.SL, **protection)
            except Exception:
                return None
        if isinstance(reply, dict) and reply.get("status") == "success":
            try:
                pending["oid"] = identifier(reply["data"]["orderId"])
            except (KeyError, TypeError, ValueError):
                return None
            self.save(state)
        return pending["oid"]

    def check_deadline_after_intent(self, state, cycle, previous):
        try:
            self.check_deadline()
        except MutationBlocked:
            cycle.clear()
            cycle.update(previous)
            self.save(state)
            raise

    def cancel(self, state, cycle, oid, row, detail, exposure, *, key_id, prior=None):
        previous = deepcopy(cycle) if self.budget is not None else None
        key = instrument(row)
        side = row["transactionType"]
        if side not in {"BUY", "SELL"}:
            raise ValueError("Invalid cancellation side")
        prior = prior or {}
        pending = {**prior, "oid": oid, "key": list(key), "net": exposure.get(key, 0),
                   "side": side, "quantity": integer(row["quantity"]), "filled": fill(row),
                   "cancel_requested": True, "cancel_rejected": False,
                   "cancel_attempts": prior.get("cancel_attempts", 0) + 1}
        if not self.matches_pending(pending, row, detail):
            raise ValueError("Cancellation identity changed")
        for field in ("orderType", "correlationId"):
            if field in row or field in detail:
                pending[field] = row[field] if field in row else detail[field]
        cycle["exposure"] = {
            oid: pair for oid, pair in cycle["exposure"].items() if tuple(pair[0]) != key
        }
        cycle["pending_by_key"][key_id] = pending
        self.save(state)
        self.check_deadline_after_intent(state, cycle, previous)
        try:
            reply = self.broker.cancel_order(oid)
        except Exception:
            return
        if not isinstance(reply, dict):
            return
        data = reply.get("data")
        explicit_reject = isinstance(data, dict) and data.get("orderStatus") == "REJECTED" and data.get("orderId") == oid
        remarks = reply.get("remarks")
        if explicit_reject or (not isinstance(data, dict) and reply.get("status") == "failure" and isinstance(remarks, str)
                               and re.fullmatch(r"(?:.* )?explicit reject(?:ion)?[.!]?", remarks, re.IGNORECASE)):
            pending["cancel_rejected"] = True
            self.save(state)

    def inspect_intent(self, pending, book):
        matches = [oid for oid, row in book.items()
                   if oid == pending["oid"] or pending["tag"] and row.get("correlationId") == pending["tag"]]
        if len(matches) != 1 or pending["oid"] not in (None, matches[0]):
            raise ValueError("Order identity unavailable or ambiguous")
        oid, = matches
        row = book[oid]
        detail = self.detail(oid)
        if not self.matches_pending(pending, row, detail):
            raise ValueError("Order evidence disagrees with intent")
        if (fill(row) < pending.get("verified_fill", 0)
                or pending.get("verified_terminal", row["orderStatus"]) != row["orderStatus"]):
            raise ValueError("Order evidence regressed")
        return oid, row, detail

    def remember_intent(self, state, pending, oid, row):
        evidence = {"oid": oid, "verified_fill": fill(row)}
        if row["orderStatus"] in TERMINAL:
            evidence["verified_terminal"] = row["orderStatus"]
        if any(pending.get(field) != value for field, value in evidence.items()):
            pending.update(evidence)
            self.save(state)

    @staticmethod
    def stop_prices(row):
        price, trigger = (float(row[field]) for field in ("price", "triggerPrice"))
        if not all(math.isfinite(value) and value > 0 for value in (price, trigger)):
            raise ValueError("Invalid stop prices")
        return {"price": price, "trigger_price": trigger}

    def inspect_stop(self, pending, book, exposure, *, state=None):
        oid, row, detail = self.inspect_intent(pending, book)
        if state is not None:
            self.remember_intent(state, pending, oid, row)
        key = tuple(pending["key"])
        filled = fill(row)
        signed = filled if pending["side"] == "BUY" else -filled
        if exposure.get(key, 0) != pending["net"] + signed:
            raise ValueError("Position has not corroborated the stop execution")
        status = row["orderStatus"]
        if status in TERMINAL:
            return oid, status, None
        if filled or status == "PART_TRADED":
            return oid, "PARTIAL", None
        if (status not in {"PENDING", "TRIGGER_PENDING", "CONFIRM"}
                or row.get("orderType") != self.broker.SL or detail.get("orderType") != self.broker.SL):
            raise ValueError("Stop is not confirmed resting")
        prices = self.stop_prices(row)
        if prices != self.stop_prices(detail):
            raise ValueError("Stop prices disagree")
        if pending["quantity"] != abs(pending["net"]):
            raise ValueError("Stop quantity differs from exposure")
        return oid, "RESTING", prices

    def protect(self, key, transaction_type, quantity, prices):
        """Place or recover one stop intent against independently verified exposure."""
        result = {"placed": None, "rejected": False, "filled_qty": 0, "status": "UNKNOWN"}
        with locked(self.path) as state:
            self.deliver(state)
            book, exposure = self.snapshot()
            cycle = state["cycle"]
            if cycle and (cycle["flatten"] or cycle.get("entry_workflow")):
                return result
            pending = cycle["pending"] if cycle else None
            if not pending:
                net = exposure.get(key, 0)
                if (not net or transaction_type != ("SELL" if net > 0 else "BUY")
                        or quantity != abs(net) or any(row["orderStatus"] in WORKING for row in book.values())
                        or self.execution_net(state, {}, key, {
                            oid: row for oid, row in book.items() if instrument(row) == key
                        })[0] != net or self.snapshot() != (book, exposure)):
                    return result
                cycle = cycle or self.start(state, flatten=False)
                self.submit(state, cycle, key, net, quantity, protection=prices)
                pending = cycle["pending"]
                book, exposure = self.snapshot()
            if (not pending["protection"] or tuple(pending["key"]) != key
                    or pending["side"] != transaction_type):
                return result
            try:
                oid, status, actual = self.inspect_stop(pending, book, exposure, state=state)
                if self.snapshot() != (book, exposure):
                    raise ValueError("Stop snapshot changed")
            except (KeyError, TypeError, ValueError, RuntimeError, OSError):
                alert_id = cycle["id"] + ":standalone-stop"
                if not cycle.get("stop_alerted"):
                    cycle["stop_alerted"] = True
                    state["alerts"][alert_id] = f"Current protection could not be confirmed for {key}, intent {pending['tag']}."
                    self.save(state)
                self.deliver(state)
                return result
            result.update(status=status, rejected=status == "REJECTED", filled_qty=abs(exposure.get(key, 0)))
            if status == "RESTING" and actual == pending["prices"] == prices:
                pending["oid"] = oid
                cycle["protected"] = True
                self.save(state)
                result["placed"] = oid
            return result

    def closure(self, state, *, settle=True):
        """Read a flat, quiet account and the verified exit IDs under the same lock."""
        executions = state.get("replay_executions", {})
        checking = bool(executions) and "replay_check" not in state
        if checking:
            # Fence replay before observing the broker. If this write fails,
            # no observation is made. A crash or later failed write leaves
            # the fence in place for the next process.
            state["replay_check"] = "verification_required"
            self.save(state)

        cycle = state["cycle"]
        if cycle and (cycle["flatten"] or not settle):
            return None
        book, exposure = self.snapshot()
        if any(exposure.values()):
            return self._contradicted_closure(state)
        if any(row["orderStatus"] in WORKING for row in book.values()):
            return self._contradicted_closure(state)
        for row in book.values():
            if row["orderStatus"] not in TERMINAL:
                raise ValueError("Unverified order status")
        for oid, evidence in executions.items():
            if oid in book:
                listed = execution_evidence(oid, book[oid])
                detail = self.detail(oid)
                if (not same_execution(book[oid], detail)
                        or listed != execution_evidence(oid, detail)):
                    return None
                if listed != evidence:
                    return self._contradicted_closure(state)
        if cycle:
            if not cycle["pending"] or not self.settle(state, cycle, book, exposure):
                return None
            state["flat_book"] = (state["flat_book"] or {}) | {oid: fill(row) for oid, row in book.items()}
            state["cycle"] = None
            self.save(state)
        else:
            grouped = defaultdict(dict)
            baseline = state["flat_book"] or {}
            for oid, row in book.items():
                if oid in baseline and fill(row) != baseline[oid]:
                    if same_execution(row, self.detail(oid)):
                        return self._contradicted_closure(state)
                    return None
                grouped[instrument(row)][oid] = row
            for key, rows in grouped.items():
                if self.execution_net(state, {}, key, rows)[0]:
                    return self._contradicted_closure(state)
        if executions.keys() - book.keys():
            return None
        if executions:
            fresh_book, fresh_exposure = self.snapshot()
            if (any(fresh_exposure.values())
                    or any(row["orderStatus"] in WORKING for row in fresh_book.values())):
                return self._contradicted_closure(state)
            if fresh_book != book:
                return None
        if checking:
            del state["replay_check"]
            self.save(state)
        return book, state["fills"]

    def _contradicted_closure(self, state):
        if state.get("replay_executions") and state.get("replay_check") != "contradicted":
            state["replay_check"] = "contradicted"
            self.save(state)
        return None

    def enter(self, params, *, workflow=None):
        with locked(self.path) as state:
            self.deliver(state)
            if state["cycle"] or "replay_check" in state:
                return None
            book, exposure = self.snapshot()
            if any(exposure.values()) or any(row["orderStatus"] in WORKING for row in book.values()):
                return None
            key = (str(params["security_id"]), params["exchange_segment"], params["product_type"])
            if (self.execution_net(state, {}, key, {
                    oid: row for oid, row in book.items() if instrument(row) == key
                })[0] or self.snapshot() != (book, exposure)):
                return None
            previous = state.copy()
            cycle = self.start(state, flatten=False)
            if workflow is not None:
                cycle["entry_workflow"] = workflow
            try:
                return self.submit(state, cycle, key, 0, params["quantity"], entry=params)
            except MutationBlocked:
                self.save(previous)
                return None

    def modify(self, oid, *, entry=False, **kwargs):
        with locked(self.path) as state:
            cycle = state["cycle"]
            if cycle and cycle["flatten"]:
                return None
            if entry and (not cycle or not cycle["pending"] or cycle["pending"]["oid"] != oid):
                return None
            book, exposure = self.snapshot()
            row = book.get(oid, {})
            if row.get("orderStatus") not in WORKING or not same_execution(row, order_detail(self.broker, oid)):
                return None
            if fill(row):
                return None
            if not entry:
                net = exposure.get(instrument(row), 0)
                if (not net or row["transactionType"] != ("SELL" if net > 0 else "BUY")
                        or any(other != oid and order["orderStatus"] in WORKING for other, order in book.items())):
                    return None
                quantity = integer(row["quantity"])
                if quantity > abs(net):
                    return None
                kwargs["quantity"] = quantity
            if entry:
                try:
                    self.check_entry_allowed()
                except MutationBlocked:
                    return None
            return self.broker.modify_order(order_id=oid, **kwargs)
