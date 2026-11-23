"""Persistent coordination for Survivor's position-reducing orders."""
from collections import defaultdict
from contextlib import contextmanager
import fcntl
import json
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
        if (set(state) != {"cycle", "alerts", "fills", "flat_book"}
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
    def __init__(self, message, *, flat=False):
        super().__init__([message])
        self.flat = flat


class ExitCoordinator:
    def __init__(self, broker, path, notify, *, polls, interval, deadline):
        self.broker = broker
        self.path = path
        self.notify = notify
        self.polls = polls
        self.interval = interval
        self.deadline = deadline

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

    def start(self, state, *, flatten):
        state["cycle"] = {
            "id": uuid4().hex, "deadline": time.time() + self.deadline,
            "pending": None, "flatten": flatten, "escalated": False, "confirmed": {}, "exposure": {},
        }
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
        before = orders(self.broker)
        exposure = positions(self.broker)
        if orders(self.broker) != before:
            raise ValueError("Order book changed during position read")
        return before, exposure

    def flatten(self, max_retries):
        with locked(self.path) as state:
            self.deliver(state)
            cycle = state["cycle"] or self.start(state, flatten=True)
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
            try:
                for _ in range(self.polls + max_retries):
                    verified = {}
                    book, exposure = self.snapshot()
                    grouped = defaultdict(dict)
                    for oid, row in book.items():
                        grouped[instrument(row)][oid] = row
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
                        if key_id in deferred or key in reserved:
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
                    if (not pending and not any(exposure.values()) and
                            not any(row["orderStatus"] in WORKING for row in book.values()) and
                            all(value == 0 for value in verified.values()) and
                            self.snapshot() == (book, exposure)):
                        if cycle.get("unresolved"):
                            self.escalate(state, message, verified)
                        state["flat_book"] = (state["flat_book"] or {}) | {oid: fill(row) for oid, row in book.items()}
                        state["cycle"] = None
                        self.save(state)
                        return FlattenResult("Broker confirmed flat with no outstanding orders", flat=True)
                    time.sleep(self.interval)
            except Exception as exc:
                verified = {key: "UNKNOWN" for key in verified}
                message = f"Exit verification interrupted ({type(exc).__name__})"
            if not cycle.get("unresolved"):
                cycle["unresolved"] = True
                self.save(state)
            self.escalate(state, message, verified)
            return FlattenResult(f"Flatten INCOMPLETE: {message}")

    def detail(self, oid):
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
            if (not same_execution(row, detail) or row["transactionType"] != pending["side"]
                    or integer(row["quantity"]) != pending["quantity"]):
                return False
            if pending.get("tag") and any(candidate.get("correlationId", pending["tag"]) != pending["tag"]
                                          for candidate in (row, detail)):
                return False
            cycle.setdefault("observed", {})[oid] = [list(key), fill(row)]
            if row["orderStatus"] in TERMINAL:
                return self.settle(state, cycle, book, exposure, key_id=key_id)
            if pending.get("cancel_requested"):
                if pending.get("cancel_rejected") and pending.get("cancel_attempts", 1) < 2:
                    if fill(row) != pending["filled"] or net != pending["net"]:
                        return False
                    signed, _ = self.execution_net(state, cycle, key, rows)
                    if signed != net or self.snapshot() != (book, exposure):
                        return False
                    self.cancel(state, cycle, oid, row, exposure, key_id=key_id, prior=pending)
                    if cycle["pending_by_key"][key_id].get("cancel_rejected"):
                        deferred.add(key_id)
                    return True
            elif pending.get("protection") or pending.get("entry"):
                expected = pending["net"] + (fill(row) if pending["side"] == "BUY" else -fill(row))
                if net == expected and self.snapshot() == (book, exposure):
                    self.cancel(state, cycle, oid, row, exposure, key_id=key_id, prior=pending)
                    if cycle["pending_by_key"][key_id].get("cancel_rejected"):
                        deferred.add(key_id)
                    return True
            return False

        signed, working = self.execution_net(state, cycle, key, rows)
        if working:
            oid, row = working[0]
            # Cancellation is safe for an identified order with no established
            # fill discrepancy. Reconcile its terminal execution before exiting.
            if (signed == net
                    and same_execution(row, self.detail(oid))
                    and self.snapshot() == (book, exposure)):
                self.cancel(state, cycle, oid, row, exposure, key_id=key_id)
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
        if (not same_execution(row, listed) or instrument(row) != key
                or row["transactionType"] != pending["side"]
                or integer(row["quantity"]) != pending["quantity"]):
            return False
        cycle["observed"] = cycle.get("observed", {}) | {oid: [list(key), filled]}
        delta = filled - pending["filled"]
        expected = pending["net"] + (delta if pending["side"] == "BUY" else -delta)
        fresh_book, fresh_exposure = self.snapshot()
        if (delta < 0 or expected * pending["net"] < 0
                or fresh_book != book or fresh_exposure != exposure
                or fresh_exposure.get(key, 0) != expected):
            return False
        if filled and not pending.get("entry"):
            if pending.get("protection"):
                source = "EXCHANGE_STOP"
            elif not pending.get("cancel_requested"):
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
        security, exchange, product = key
        side = entry["transaction_type"] if entry else ("SELL" if net > 0 else "BUY")
        tag = uuid4().hex[:24]
        pending = dict(oid=None, tag=tag, key=list(key), net=net, side=side,
                       quantity=qty, filled=0, protection=protection is not None, entry=entry is not None)
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
            reply = self.broker.place_order(**entry, tag=tag)
        elif protection is None:
            try:
                reply = self.broker._place_exit_order(**params, order_type=self.broker.MARKET, price=0)
            except Exception:
                return None
        else:
            reply = self.broker.place_order(**params, order_type=self.broker.SL, **protection)
        if isinstance(reply, dict) and reply.get("status") == "success":
            try:
                pending["oid"] = identifier(reply["data"]["orderId"])
            except (KeyError, TypeError, ValueError):
                return None
            self.save(state)
        return pending["oid"]

    def cancel(self, state, cycle, oid, row, exposure, *, key_id, prior=None):
        key = instrument(row)
        side = row["transactionType"]
        if side not in {"BUY", "SELL"}:
            raise ValueError("Invalid cancellation side")
        cycle["exposure"] = {
            oid: pair for oid, pair in cycle["exposure"].items() if tuple(pair[0]) != key
        }
        prior = prior or {}
        pending = dict(oid=oid, key=list(key), net=exposure.get(key, 0),
                       side=side, quantity=integer(row["quantity"]), filled=fill(row),
                       cancel_requested=True, protection=prior.get("protection", False),
                       entry=prior.get("entry", False),
                       cancel_attempts=prior.get("cancel_attempts", 0) + 1)
        cycle["pending_by_key"][key_id] = pending
        self.save(state)
        try:
            reply = self.broker.cancel_order(oid)
        except Exception:
            return
        if not isinstance(reply, dict):
            return
        data = reply.get("data")
        explicit_reject = isinstance(data, dict) and data.get("orderStatus") == "REJECTED" and data.get("orderId") == oid
        remarks = reply.get("remarks")
        if explicit_reject or (reply.get("status") == "failure" and isinstance(remarks, str)
                               and re.fullmatch(r"(?:.* )?explicit reject(?:ion)?[.!]?", remarks, re.IGNORECASE)):
            pending["cancel_rejected"] = True
            self.save(state)

    def protect(self, key, transaction_type, quantity, prices):
        result = {"placed": None, "rejected": True, "filled_qty": 0}
        with locked(self.path) as state:
            self.deliver(state)
            book, exposure = self.snapshot()
            cycle = state["cycle"]
            if cycle:
                pending = cycle["pending"]
                if (cycle["flatten"] or not pending or not pending.get("entry")
                        or tuple(pending["key"]) != key or not self.settle(state, cycle, book, exposure)):
                    return result
                state["cycle"] = None
                self.save(state)
            net = exposure.get(key, 0)
            result["filled_qty"] = abs(net)
            if (not net or transaction_type != ("SELL" if net > 0 else "BUY")
                    or any(row["orderStatus"] in WORKING for row in book.values())):
                return result
            cycle = self.start(state, flatten=False)
            oid = self.submit(state, cycle, key, net, min(abs(net), quantity), protection=prices)
            if oid:
                row = order_detail(self.broker, oid)
                if (row["orderStatus"] in WORKING and instrument(row) == key
                        and row["transactionType"] == transaction_type
                        and integer(row["quantity"]) == min(abs(net), quantity)
                        and fill(row) == 0):
                    cycle["protected"] = True
                    self.save(state)
                    result.update(placed=oid, rejected=False)
            return result

    def closure(self, state, *, settle=True):
        """Read a flat, quiet account and the verified exit IDs under the same lock."""
        cycle = state["cycle"]
        if cycle and (cycle["flatten"] or not settle):
            return None
        book, exposure = self.snapshot()
        if any(exposure.values()) or any(row["orderStatus"] in WORKING for row in book.values()):
            return None
        if cycle:
            if not cycle["pending"] or not self.settle(state, cycle, book, exposure):
                return None
            state["flat_book"] = (state["flat_book"] or {}) | {oid: fill(row) for oid, row in book.items()}
            state["cycle"] = None
            self.save(state)
        else:
            grouped = defaultdict(dict)
            for oid, row in book.items():
                grouped[instrument(row)][oid] = row
            if any(self.execution_net(state, {}, key, rows)[0] for key, rows in grouped.items()):
                return None
        return book, state["fills"]

    def enter(self, params):
        with locked(self.path) as state:
            self.deliver(state)
            if state["cycle"]:
                return None
            book, exposure = self.snapshot()
            if any(exposure.values()) or any(row["orderStatus"] in WORKING for row in book.values()):
                return None
            key = (str(params["security_id"]), params["exchange_segment"], params["product_type"])
            cycle = self.start(state, flatten=False)
            return self.submit(state, cycle, key, 0, params["quantity"], entry=params)

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
            return self.broker.modify_order(order_id=oid, **kwargs)
