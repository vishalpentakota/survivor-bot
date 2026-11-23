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
        if oid in result or row["orderStatus"] not in STATUSES:
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
    if (quantity <= 0 or not 0 <= filled <= quantity
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

    def escalate(self, state, message):
        cycle = state["cycle"]
        if cycle and not cycle["escalated"] and time.time() >= cycle["deadline"]:
            cycle["escalated"] = True
            pending = cycle["pending"] or {}
            identity = pending.get("oid") or pending.get("tag")
            state["alerts"][cycle["id"]] = f"{message}. Pending order or intent {identity or 'unverified'}"
            self.save(state)
            self.deliver(state)

    def snapshot(self):
        before = orders(self.broker)
        exposure = positions(self.broker)
        if orders(self.broker) != before:
            raise ValueError("Order book changed during position read")
        return before, exposure

    def settle(self, state, cycle, book, exposure):
        pending = cycle["pending"]
        oid = pending["oid"]
        if oid is None:
            matches = [key for key, row in book.items() if row.get("correlationId") == pending["tag"]]
            if len(matches) != 1:
                return False
            oid = pending["oid"] = matches[0]
            self.save(state)
        if oid not in book or book[oid]["orderStatus"] not in TERMINAL:
            return False
        row = order_detail(self.broker, oid)
        listed = book[oid]
        key = tuple(pending["key"])
        filled = fill(row)
        if (row["orderStatus"] != listed["orderStatus"]
                or instrument(row) != key or instrument(listed) != key
                or row["transactionType"] != pending["side"] or listed["transactionType"] != pending["side"]
                or integer(row["quantity"]) != pending["quantity"]
                or integer(listed["quantity"]) != pending["quantity"] or fill(listed) != filled):
            return False
        delta = filled - pending["filled"]
        expected = pending["net"] + (delta if pending["side"] == "BUY" else -delta)
        fresh_book, fresh_exposure = self.snapshot()
        if (delta < 0 or expected * pending["net"] < 0
                or fresh_book != book or fresh_exposure != exposure
                or fresh_exposure.get(key, 0) != expected):
            return False
        if filled and not pending.get("entry"):
            state["fills"][oid] = list(key)
        cycle["pending"] = None
        cycle["confirmed"][oid] = [list(key), filled, row["orderStatus"]]
        cycle["exposure"][oid] = [list(key), expected]
        self.save(state)
        return True

    def submit(self, state, cycle, key, net, qty, *, protection=None, entry=None):
        security, exchange, product = key
        side = entry["transaction_type"] if entry else ("SELL" if net > 0 else "BUY")
        tag = uuid4().hex[:24]
        pending = dict(oid=None, tag=tag, key=list(key), net=net, side=side,
                       quantity=qty, filled=0, protection=protection is not None, entry=entry is not None)
        cycle["exposure"] = {
            oid: pair for oid, pair in cycle["exposure"].items() if tuple(pair[0]) != key
        }
        cycle["pending"] = pending
        self.save(state)
        params = dict(security_id=security, exchange_segment=exchange, product_type=product,
                      transaction_type=side, quantity=qty, tag=tag)
        if entry is not None:
            reply = self.broker.place_order(**entry, tag=tag)
        elif protection is None:
            reply = self.broker._place_exit_order(**params, order_type=self.broker.MARKET, price=0)
        else:
            reply = self.broker.place_order(**params, order_type=self.broker.SL, **protection)
        if isinstance(reply, dict) and reply.get("status") == "success":
            pending["oid"] = identifier(reply["data"]["orderId"])
            self.save(state)
        return pending["oid"]

    def cancel(self, state, cycle, oid, row, exposure):
        key = instrument(row)
        side = row["transactionType"]
        if side not in {"BUY", "SELL"}:
            raise ValueError("Invalid cancellation side")
        cycle["exposure"] = {
            oid: pair for oid, pair in cycle["exposure"].items() if tuple(pair[0]) != key
        }
        cycle["pending"] = dict(oid=oid, key=list(key), net=exposure.get(key, 0),
                                side=side, quantity=integer(row["quantity"]), filled=fill(row),
                                cancel_requested=True, protection=False, entry=cycle["pending"].get("entry", False)
                                if cycle["pending"] else False)
        self.save(state)
        self.broker.cancel_order(oid)

    def after_flat(self, baseline, book, exposure):
        if baseline is None:
            return
        changes = defaultdict(int)
        for oid, row in book.items():
            if row["orderStatus"] not in TERMINAL:
                continue
            if oid in baseline:
                if fill(row) != baseline[oid]:
                    raise ValueError("A previously terminal fill changed")
                continue
            filled = fill(row)
            if filled:
                side = row["transactionType"]
                if side not in {"BUY", "SELL"}:
                    raise ValueError("Invalid execution side")
                changes[instrument(row)] += filled if side == "BUY" else -filled
        if any(changes[key] != exposure.get(key, 0) for key in changes.keys() | exposure.keys()):
            raise ValueError("Broker fills conflict with the last verified flat position")

    def flatten(self, max_retries):
        with locked(self.path) as state:
            self.deliver(state)
            cycle = state["cycle"] or self.start(state, flatten=True)
            if not cycle["flatten"]:
                cycle["flatten"] = True
                if cycle.get("protected"):
                    cycle["deadline"] = time.time() + self.deadline
                self.save(state)
            message = "Exit has no verified terminal order and consistent broker position"
            try:
                attempts = 0
                for _ in range(self.polls + max_retries):
                    book, exposure = self.snapshot()
                    for key, expected in cycle["exposure"].values():
                        if exposure.get(tuple(key), 0) != expected:
                            raise ValueError("Previously settled position changed")
                    for oid, (key, filled, status) in cycle["confirmed"].items():
                        row = book.get(oid, {})
                        if row.get("orderStatus") != status or instrument(row) != tuple(key) or fill(row) != filled:
                            raise ValueError("Previously settled order changed")
                    pending = cycle["pending"]
                    if pending:
                        # A known protective order can be cancelled during recovery after
                        # verifying its identity and current exposure. Market orders settle first.
                        oid = pending["oid"]
                        row = book.get(oid, {})
                        if pending.get("sparse"):
                            if row.get("orderStatus") not in TERMINAL:
                                break
                            row = order_detail(self.broker, oid)
                            if (not same_execution(row, book[oid]) or fill(row)
                                    or any(exposure.values())):
                                break
                            cycle["confirmed"][oid] = [list(instrument(row)), 0, row["orderStatus"]]
                            cycle["pending"] = None
                            self.save(state)
                            continue
                        if (pending["protection"] or pending.get("entry")) and row.get("orderStatus") in WORKING and not pending.get("cancel_requested"):
                            key = tuple(pending["key"])
                            filled = fill(row)
                            expected = pending["net"] + (filled if pending["side"] == "BUY" else -filled)
                            if (instrument(row) == key and row["transactionType"] == pending["side"]
                                    and integer(row["quantity"]) == pending["quantity"]
                                    and exposure.get(key, 0) == expected
                                    and same_execution(row, order_detail(self.broker, oid))):
                                self.cancel(state, cycle, oid, row, exposure)
                                continue
                        if not self.settle(state, cycle, book, exposure):
                            time.sleep(self.interval)
                            continue
                    working = [(oid, row) for oid, row in book.items() if row["orderStatus"] in WORKING]
                    if working:
                        oid, row = working[0]
                        # Sparse legacy order book rows can be cancelled when exposure is
                        # verified flat. They cannot establish settlement or authorize an exit.
                        if not all(field in row for field in ("securityId", "exchangeSegment", "productType",
                                                              "quantity", "transactionType")):
                            if not any(exposure.values()) and "securityId" not in row:
                                cycle["pending"] = {"oid": oid, "sparse": True}
                                self.save(state)
                                self.broker.cancel_order(oid)
                            break
                        if not same_execution(row, order_detail(self.broker, oid)):
                            raise ValueError("Working order detail disagrees with the order book")
                        self.cancel(state, cycle, oid, row, exposure)
                    elif not any(exposure.values()):
                        self.after_flat(state["flat_book"], book, exposure)
                        if self.snapshot() != (book, exposure):
                            raise ValueError("Broker state changed before flat confirmation")
                        if cycle.get("unresolved"):
                            self.escalate(state, message)
                        old = state["flat_book"] or {}
                        state["flat_book"] = old | {oid: fill(row) for oid, row in book.items()}
                        state["cycle"] = None
                        self.save(state)
                        return FlattenResult("Broker confirmed flat with no outstanding orders", flat=True)
                    elif attempts >= max_retries:
                        message = "Exit attempts exhausted with remaining broker exposure"
                        break
                    else:
                        key, net = next((key, net) for key, net in exposure.items() if net)
                        self.after_flat(state["flat_book"], book, exposure)
                        self.submit(state, cycle, key, net, abs(net))
                        attempts += 1
            except Exception as exc:
                message = f"Exit verification interrupted ({type(exc).__name__})"
            if not cycle.get("unresolved"):
                cycle["unresolved"] = True
                self.save(state)
            self.escalate(state, message)
            return FlattenResult(f"Flatten INCOMPLETE: {message}")

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
            self.after_flat(state["flat_book"], book, exposure)
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
