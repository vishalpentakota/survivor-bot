"""Entry settlement and initial protection under the Survivor exit-ledger lock."""

from decimal import Decimal, ROUND_FLOOR
import math
import re
import time

from verified_exit import TERMINAL, WORKING, fill, instrument, locked, same_execution


def round_tick(price, tick, *, floor=False):
    step = Decimal(str(tick))
    units = Decimal(str(price)) / step
    units = units.to_integral_value(rounding=ROUND_FLOOR if floor else None)
    return float(units * step)


def stop_prices(trigger, tick, side):
    trigger = max(tick, round_tick(trigger, tick))
    cushion = 0.98 if side == "SELL" else 1.02
    return {"trigger_price": trigger, "price": max(tick, round_tick(trigger * cushion, tick))}


def opposite(side):
    return "SELL" if side == "BUY" else "BUY"


def signed(side, quantity):
    return quantity if side == "BUY" else -quantity


def alert(coordinator, state, owner, alert_id, message):
    if alert_id not in owner.get("alerted", []):
        owner.setdefault("alerted", []).append(alert_id)
        state["alerts"][alert_id] = message
        coordinator.save(state)
    coordinator.deliver(state)


def recover(coordinator, publish, chase, *, polls, interval, allow_protection):
    """Return idle, flat, pending, ready or close. Reopen from disk on each tick."""
    with locked(coordinator.path) as state:
        cycle = state["cycle"]
        work = cycle and cycle.get("entry_workflow")
        if not work or cycle["flatten"] or "replay_check" in state:
            return "idle"
        coordinator.deliver(state)
        if advance(coordinator, state, cycle, work, publish, chase, polls, interval, allow_protection):
            return work["outcome"]
        identity = cycle.get("pending") or work["entry"]
        alert(coordinator, state, work, cycle["id"],
              f"Entry/protection unresolved for {identity['key']}, intent {identity['tag']}. "
              "Current protection could not be confirmed.")
        return "pending"


def advance(coordinator, state, cycle, work, publish, chase, polls, interval, allow_protection):
    trade = work["trade"]
    wait = 0
    # Each network write consumes a distinct durable phase. Poll limits also
    # bound the operation when sleep is collapsed or a broker reply is stale.
    for _ in range(4 * polls + 8):
        try:
            book, exposure = coordinator.snapshot()
            pending = cycle["pending"]
            entry = work.get("entry") or pending
            key = tuple(entry["key"])
            oid, row, _ = coordinator.inspect_intent(entry, book)
            coordinator.remember_intent(state, entry, oid, row)
            quantity = fill(row)
            net = signed(entry["side"], quantity)
            entry_terminal = row["orderStatus"] in TERMINAL
            stop = pending if pending and pending["protection"] else None
            allowed = {oid}
            if stop:
                stop_id, stop_status, actual = coordinator.inspect_stop(stop, book, exposure, state=state)
                allowed.add(stop_id)
            rows = {order_id: item for order_id, item in book.items() if instrument(item) == key}
            executions, _ = coordinator.execution_net(state, cycle, key, rows)
            if (any(order_id not in allowed and item["orderStatus"] in WORKING
                    for order_id, item in book.items())
                    or any(value for other, value in exposure.items() if other != key)
                    or executions != exposure.get(key, 0)
                    or not stop and net != executions or coordinator.snapshot() != (book, exposure)):
                raise ValueError("Entry executions and position have not converged")
            if work.get("entry") and (not entry_terminal or quantity != work["filled"]
                                      or float(row["averageTradedPrice"]) != work["entry_price"]):
                raise ValueError("Confirmed entry evidence changed")
        except (KeyError, TypeError, ValueError, RuntimeError, OSError):
            wait += 1
            if wait >= polls:
                return False
            time.sleep(interval)
            continue

        if not entry_terminal:
            wait += 1
            if wait < polls:
                time.sleep(interval)
                continue
            if (not quantity and not work.get("chase_attempted") and not entry.get("cancel_requested")
                    and chase is not None):
                work["chase_attempted"] = True
                coordinator.save(state)
                try:
                    chased = chase(work, entry, oid, book, exposure)
                except Exception:
                    chased = True
                if chased:
                    wait = 0
                    continue
            if entry.get("cancel_attempts", 0) < (2 if entry.get("cancel_rejected") else 1):
                entry.update(oid=oid, cancel_requested=True,
                             cancel_attempts=entry.get("cancel_attempts", 0) + 1, cancel_rejected=False)
                coordinator.save(state)
                try:
                    reply = coordinator.broker.cancel_order(oid)
                except Exception:
                    reply = None
                data = reply.get("data") if isinstance(reply, dict) else None
                if (isinstance(data, dict) and data.get("orderId") == oid and data.get("orderStatus") == "REJECTED"
                        or isinstance(reply, dict) and reply.get("status") == "failure"):
                    entry["cancel_rejected"] = True
                    coordinator.save(state)
                wait = 0
                continue
            return False

        if not quantity:
            state["flat_book"] = (state["flat_book"] or {}) | {oid: 0}
            state["cycle"] = None
            work["outcome"] = "flat"
            coordinator.save(state)
            return True

        average = float(row["averageTradedPrice"])
        if not math.isfinite(average) or average <= 0:
            return False
        if not work.get("entry"):
            entry["oid"] = oid
            work.update(entry=entry, filled=quantity, entry_price=average)
            cycle["pending"] = None
            coordinator.save(state)

        active = dict(trade, order_id=oid, quantity=quantity, entry_price=average,
                      entry_cycle=cycle["id"], entry_side=entry["side"], product_type=key[2])
        publish(active)
        side = opposite(entry["side"])
        desired = stop_prices(work["trigger"], trade["tick_size"], side)
        close = not allow_protection() or quantity % trade["lot_size"] != 0
        if stop:
            if stop_status == "RESTING" and actual != stop["prices"]:
                return False
            close |= stop_status != "RESTING"

        if not stop and not close:
            coordinator.submit(state, cycle, key, net, quantity, protection=desired)
            wait = 0
            continue

        if stop and stop_status == "RESTING":
            active.update(sl_order_id=stop_id, stop_loss=actual["trigger_price"], sl_limit_price=actual["price"])
            cycle["protected"] = True
        elif stop:
            active["stop_execution_id"] = stop_id
        if close:
            active.update(flatten_pending=True, flatten_reason="UNPROTECTED_POSITION_EXIT")
        publish(active)
        del cycle["entry_workflow"]
        if not stop:
            # An entry with no stop still needs the verified exit path.
            cycle["confirmed"][oid] = [list(key), quantity, row["orderStatus"]]
            cycle["exposure"][oid] = [list(key), net]
        coordinator.save(state)
        work["outcome"] = "close" if close else "ready"
        return True


def amendment_rejected(reply, oid):
    if not isinstance(reply, dict) or reply.get("status") != "failure":
        return False
    data = reply.get("data")
    if isinstance(data, dict):
        return str(data.get("orderId")) == oid and data.get("orderStatus") == "REJECTED"
    remarks = reply.get("remarks")
    return isinstance(remarks, str) and bool(
        re.fullmatch(r"(?:.* )?explicit reject(?:ion)?[.!]?", remarks, re.IGNORECASE))


def monitor_stop(coordinator, active, save, *, tick, trigger=None):
    """Verify the resting stop and resolve a durable amendment, if present."""
    with locked(coordinator.path) as state:
        coordinator.deliver(state)
        cycle = state["cycle"]
        if cycle and cycle["flatten"]:
            return "UNKNOWN"
        key = (str(active["security_id"]), active["exchange_segment"], active.get("product_type", "INTRADAY"))
        side = active.get("entry_side") or (
            "BUY" if active["weapon"] == "OPTIONS" or active["direction"] == "LONG" else "SELL")
        quantity = active["quantity"]
        oid = str(active["sl_order_id"])
        pending = cycle and cycle["pending"]
        if not pending and not active.get("entry_cycle"):
            pending = dict(oid=oid, tag=None, key=list(key), quantity=quantity, net=signed(side, quantity),
                           side=opposite(side), protection=True, requested_type=coordinator.broker.SL)
        try:
            if not pending or not pending.get("protection"):
                raise ValueError("No verified stop intent")
            book, exposure = coordinator.snapshot()
            observed_id, status, actual = coordinator.inspect_stop(pending, book, exposure, state=state if cycle else None)
            if (observed_id != oid or tuple(pending["key"]) != key or pending["side"] != opposite(side)
                    or pending["quantity"] != quantity
                    or any(order_id != oid and row["orderStatus"] in WORKING for order_id, row in book.items())):
                raise ValueError("Active stop evidence is inconsistent")
            if active.get("entry_cycle"):
                entry_id = str(active["order_id"])
                opening = book[entry_id]
                if (instrument(opening) != key or opening["transactionType"] != side
                        or opening["orderStatus"] not in TERMINAL or fill(opening) != quantity
                        or float(opening["averageTradedPrice"]) != active["entry_price"]
                        or not same_execution(opening, coordinator.detail(entry_id))):
                    raise ValueError("Opening execution has changed")
            if coordinator.snapshot() != (book, exposure):
                raise ValueError("Stop snapshot changed")
        except (KeyError, TypeError, ValueError, RuntimeError, OSError):
            status = "UNKNOWN"
        if status == "RESTING":
            old = {"price": active.get("sl_limit_price", actual["price"]), "trigger_price": active["stop_loss"]}
            amendment = active.get("stop_amendment")
            if amendment and actual == amendment["prices"]:
                active.update(stop_loss=actual["trigger_price"], sl_limit_price=actual["price"])
                active.pop("stop_amendment")
                save()
                return "RESTING"
            if actual != old:
                status = "UNKNOWN"
            elif amendment and not amendment.get("rejected"):
                stop_alert(coordinator, state, cycle, active, save, "amend",
                           f"Stop amendment unresolved for {key}, order {oid}. Last verified stop retained.")
                return "RESTING"
            elif amendment:
                active.pop("stop_amendment")
                save()
                return "RESTING"
            elif trigger is not None:
                prices = stop_prices(trigger, tick, pending["side"])
                active["stop_amendment"] = {"prices": prices}
                save()
                try:
                    reply = coordinator.broker.modify_order(
                        order_id=oid, order_type=coordinator.broker.SL, leg_name="NA",
                        quantity=pending["quantity"], disclosed_quantity=0,
                        validity=coordinator.broker.DAY, **prices,
                    )
                except Exception:
                    reply = None
                if amendment_rejected(reply, oid):
                    active["stop_amendment"]["rejected"] = True
                    save()
                return "SUBMITTED"
        if status == "UNKNOWN":
            stop_alert(coordinator, state, cycle, active, save, "stop",
                       f"Current protection could not be confirmed for {key}, order {oid}.")
        return status


def stop_alert(coordinator, state, cycle, active, save, suffix, message):
    if cycle:
        alert(coordinator, state, cycle, cycle["id"] + ":" + suffix, message)
    elif suffix not in active.get("protection_alerts", []):
        alert_id = f"{active['order_id']}:{active['entry_time']}:{suffix}"
        state["alerts"][alert_id] = message
        coordinator.save(state)
        active.setdefault("protection_alerts", []).append(suffix)
        save()
        coordinator.deliver(state)
