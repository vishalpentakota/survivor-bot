#!/usr/bin/env python3
"""
Dhan Survivor Engine ("Do or Die" Autonomous Trading Organism)
==============================================================
Premise:
- Seed Capital: ₹30,000.00
- Max Drawdown Allowed: 89% (Death Floor = ₹3,300.00)
- Weapons:
    1) Nifty 50 Index Options Buying (NSE_FNO, OPTIDX) — when capital >= ₹8,000
    2) Liquid NSE Intraday 5x Equities (NSE_EQ, INTRADAY) — works at any capital above ₹3,300
- EOD Rule: Strictly Intraday. All positions squared off before 3:10 PM (Equities) / 3:20 PM (Options).
- Mortality Rule (Option 2 - Permanent Coma):
    If Net Liquidation Value <= ₹3,300.00 AFTER funding is armed:
    1. Immediately cancel all pending orders & flatten all open positions at MARKET.
    2. Write `survivor_obituary.md` with full post-mortem & trade history.
    3. Permanently lock `survivor_state.json` to `status = "DEAD_COMA"`.
    4. Output `[SURVIVOR_SEPUKU_TRIGGERED]` so the parent Hermes agent removes all survivor cronjobs.
"""

import os
import sys
import json
import time
import math
import errno
import fcntl
import socket
import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from uuid import uuid4

import urllib3.util.connection as urllib3_cn
# Force IPv4 (`182.70.190.15` SECONDARY_MATCH on Dhan) because Windows temporary IPv6 privacy addresses rotate daily
urllib3_cn.allowed_gai_family = lambda: socket.AF_INET

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cloud_guard import LIVE_SWITCH, READ_ONLY, ReadOnlyBroker, RolePolicy, live_trading_enabled
from dhan_broker import CachedTokenUnavailable, DhanTokenManager, load_env_file
from eod_ownership import (
    EodStore, IST, LOCK_POLL_SECONDS, WINDOW_SECONDS, owner_record, publish_owner, scheduled_slot,
)
from verified_exit import (
    ExitCoordinator, FlattenResult, TERMINAL, execution_evidence, fill as order_fill, instrument,
    integer as broker_integer, locked as exit_lock, order_detail, persist as persist_exit_data, positions as verified_positions, same_execution,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXIT_LEDGER = os.path.join(BASE_DIR, "survivor_exits.json")
STATE_FILE = os.path.join(BASE_DIR, "survivor_state.json")
JOURNAL_FILE = os.path.join(BASE_DIR, "survivor_trades.json")
OBITUARY_FILE = os.path.join(BASE_DIR, "survivor_obituary.md")
LOG_FILE = os.path.join(BASE_DIR, "survivor_engine.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
logger = logging.getLogger("survivor")

# ─── Core Survival Constants ───────────────────────────────────────────────
# P9 RECOVERY-MODE PROTECTIONS (drafted 2026-11-16, vnext reference tree):
GREEN_FLOOR_INR = 500.0        # banked-profit floor: keep trading while green, never give ₹500+ back
MAX_ENTRIES_PER_SETUP = 2      # max entries per setup+security per day (no 3rd chase of the same breakout)
ORB_WINDOW_START = "09:30"   # aligned with global time-of-day gate (was 09:20 -> ORB could never fire)
ORB_WINDOW_END = "10:35"
MIN_OPT_SESSION_BARS = 5       # current-session 3m option bars required (replaces 15 bars spanning yesterday)
EMA21_SLOPE_ATR_MIN = 0.12     # 3-bar EMA21 slope as fraction of 3m ATR (~2.5pts on Nifty); index-agnostic
ENTRY_RSI_CEILING_LONG = 72.0  # no exhaustion chasing on LONG entries (day-3 audit: RSI 78-83 re-buys)
ENTRY_RSI_FLOOR_SHORT = 28.0   # symmetric exhaustion floor for SHORT entries
EQUITY_STALE_MINS = 20.0       # flat equity position exited after 20 min going nowhere (was 30/unbounded)
SEED_CAPITAL = 30000.0
MAX_LOSS_PCT = 0.89
DEATH_FLOOR = round(SEED_CAPITAL * (1.0 - MAX_LOSS_PCT), 2)  # ₹3,300.00
ARMING_THRESHOLD = 15000.0  # Auto-arms mortality check once account is topped up (>= ₹15,000)

OPTION_LOT_SIZE = 65        # Nifty 50 current lot size (verified via Dhan option_chain)
MIN_CAPITAL_FOR_OPTIONS = 8000.0
EQUITY_LEVERAGE = 5.0       # MIS 5x intraday boost
ESTIMATED_ROUNDTRIP_CHARGES = 55.0  # ₹40 brokerage + ~₹15 STT/SEBI/GST

# ─── Multi-Index Scouting Universe (Fast-Strike v5) ────────────────────────
# Scout Nifty 50 + 4 satellite indices in parallel every tick and trade the
# strongest setup (highest confidence / option score) across all of them.
# Lot sizes & Dhan index security IDs verified 2026-10-26 from security master + option_chain.
INDEX_UNIVERSE = {
    "Nifty 50":          {"security_id": "13",  "lot_size": 65,  "symbol_prefix": "NIFTY"},
    "Nifty Bank":        {"security_id": "25",  "lot_size": 30,  "symbol_prefix": "BANKNIFTY"},
    "FinNifty":          {"security_id": "27",  "lot_size": 60,  "symbol_prefix": "FINNIFTY"},
    "Nifty Next 50":     {"security_id": "38",  "lot_size": 25,  "symbol_prefix": "NIFTYNXT50"},
    "Nifty Midcap Select":{"security_id": "442", "lot_size": 120, "symbol_prefix": "MIDCPNIFTY"},
}

# ─── Live-Entry Pruning (2026-11-22 replay verdict; user-approved option 2) ──
# Entries-only kill list from the 60-day signal replay (2026-08-26..2026-11-20,
# gross of costs): SUPERTREND_FLIP -0.33R over 37 first-per-day trades; the
# "Nifty 50" index -0.33R over 91. BankNifty, Midcap Select, FinNifty,
# Nifty Next 50 and every other setup stay live. Scope: the live entry scan in
# _cmd_tick_inner only — open positions, monitoring, the 15:10 flatten and
# EOD reconcile are unaffected. Re-enable an entry = remove it from the set.
DISABLED_SETUPS = {"SUPERTREND_FLIP"}
DISABLED_INDICES = {"Nifty 50"}

# Curated high-liquidity NSE Equities with verified Dhan numeric security IDs
LIQUID_EQUITY_UNIVERSE = {
    "RELIANCE":   "2885",
    "HDFCBANK":   "1333",
    "ICICIBANK":  "4963",
    "INFY":       "1594",
    "TCS":        "11536",
    "SBIN":       "3045",
    "AXISBANK":   "5900",
    "LT":         "11483",
    "BHARTIARTL": "10604",
    "IRFC":       "2029",
    "BEL":        "383",
    "HAL":        "2303",
    "TRENT":      "1964",
    "BAJFINANCE": "317",
    "M&M":        "2031",
    "SUNPHARMA":  "3351",
    "NTPC":       "11630",
    "TATASTEEL":  "3499",
}


# ─── State & Journal Persistence (with File Locking) ──────────────────────

_STATE_LOCK_FILE = os.path.join(BASE_DIR, ".survivor_state.lock")


def _acquire_state_lock(timeout: float = 5.0) -> Optional[int]:
    """Acquire a cross-process file lock using O_CREAT|O_EXCL atomic creation."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            fd = os.open(_STATE_LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            return fd
        except FileExistsError:
            # Stale lock? Check age and break if >30s old.
            try:
                age = time.time() - os.path.getmtime(_STATE_LOCK_FILE)
                if age > 30.0:
                    os.unlink(_STATE_LOCK_FILE)
                    continue
            except OSError:
                pass
            time.sleep(0.1)
    return None


def _release_state_lock(fd: Optional[int]) -> None:
    if fd is not None:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.unlink(_STATE_LOCK_FILE)
        except OSError:
            pass


# ─── Patched-engine timing constants (patchable so tests run fast) ─────────
ENTRY_SETTLE_POLL_S = 0.8    # wait between entry-fill confirmation checks
ENTRY_SETTLE_TRIES = 8       # number of entry-fill checks (~6.4s window)
MAX_ENTRY_CHASE_PCT = 0.015  # one-shot entry re-price cap: never pay >1.5% above the original limit
FLATTEN_POLL_S = 0.5         # wait between flatten-order status polls
EXIT_ESCALATION_SECONDS = 30
FLATTEN_POLL_N = 10          # status polls per flatten order before unresolved
MONITOR_POLL_S = 5            # wait between live-monitor iterations


def _safe_get_positions(client) -> Optional[List[Dict]]:
    """
    P1 fail-closed position read. Returns the position list ONLY on a verified
    success response; returns None (= UNKNOWN) on any exception or non-success
    response. Callers must treat None as 'cannot verify' — never as 'flat'.
    """
    try:
        resp = client.dhan.get_positions()
    except Exception:
        return None
    if (not isinstance(resp, dict) or resp.get("status") != "success"
            or not isinstance(resp.get("data"), list)):
        return None
    try:
        seen = set()
        for row in resp["data"]:
            key = instrument(row)
            broker_integer(row["netQty"])
            if key in seen:
                return None
            seen.add(key)
    except (TypeError, ValueError, KeyError):
        return None
    return resp["data"]


def send_telegram(message: str) -> bool:
    """Emit an operator alert through the command's existing output channel."""
    print(f"🚨 {message}", flush=True)
    logger.critical(message)
    # Command output has no delivery acknowledgement. Keep the alert pending.
    return False


def _entry_window_open():
    return "09:20" <= datetime.now(IST).strftime("%H:%M") <= "14:15"


def _exit_coordinator(client, *, budget=None):
    return ExitCoordinator(client.dhan, EXIT_LEDGER, send_telegram, polls=FLATTEN_POLL_N,
                           interval=FLATTEN_POLL_S, deadline=EXIT_ESCALATION_SECONDS,
                           entry_allowed=_entry_window_open, budget=budget)


def _recovery_pending(client, active=None) -> bool:
    try:
        with exit_lock(EXIT_LEDGER) as ledger:
            _exit_coordinator(client).deliver(ledger)
            cycle = ledger["cycle"]
            if ((active and "replay_check" in ledger)
                    or (cycle and (cycle["flatten"] or not active))):
                return True
            identity = _trade_identity(active) if active else None
            return identity is not None and any(
                _trade_identity(row) == identity for row in _read_exit_journal())
    except Exception:
        return True


def _submit_entry(client, **params):
    try:
        return _exit_coordinator(client).enter(params)
    except Exception:
        logger.exception("Entry could not be verified")
        return None


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError as e:
        return e.errno == errno.EPERM
    return True


def _get_tick_size(security_id: str, exchange_segment: str = "") -> float:
    cache_file = os.path.join(BASE_DIR, ".tick_size_cache.json")
    try:
        if os.path.exists(cache_file):
            with open(cache_file) as f:
                cache = json.load(f)
            key = str(security_id)
            if key in cache:
                return float(cache[key])
    except (json.JSONDecodeError, OSError, ValueError):
        cache = {}
    # Default NSE tick
    return 0.05


def _get_latest_tick_ltp(client: "SurvivorDhanClient", security_id: str,
                         exchange_segment: str, instrument_type: str) -> Optional[float]:
    """
    Freshest available price from Dhan intraday minute data (last 1-min close,
    including the in-progress minute when the API returns it). Returns None on
    any failure so the caller falls back to the 1-min candle close.
    Dhan returns OHLC as PARALLEL ARRAYS: {"open": [...], "close": [...]}.
    (fix 2026-11-20: the previous parser expected scalars and always returned
    None, which silently disabled the entire monitor loop.)
    """
    try:
        resp = client.dhan.intraday_minute_data(
            security_id=str(security_id),
            exchange_segment=exchange_segment,
            instrument_type=instrument_type,
            from_date=datetime.now(IST).strftime("%Y-%m-%d"),
            to_date=datetime.now(IST).strftime("%Y-%m-%d"),
        )
        if not isinstance(resp, dict):
            return None
        data = resp.get("data")
        if not isinstance(data, dict):
            return None
        payloads = [data]
        if isinstance(data.get("data"), dict):
            payloads.append(data["data"])
        for payload in payloads:
            closes = payload.get("close")
            if isinstance(closes, (list, tuple)) and len(closes):
                try:
                    last = float(closes[-1])
                except (TypeError, ValueError):
                    continue
                if last > 0:
                    return last
        return None
    except Exception:
        return None


def _engine_lock_path():
    return os.path.join(BASE_DIR, ".survivor_engine.lock")


def acquire_engine_lock(purpose: str = "routine") -> Optional[int]:
    """Claim the stable inode for the entire broker transaction."""
    fd = os.open(_engine_lock_path(), os.O_CREAT | os.O_RDWR, 0o600)
    owned = False
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return None

        # Respect a live owner from the previous O_EXCL implementation.
        legacy = os.pread(fd, 128, 0).decode()
        if legacy and not legacy.startswith("{"):
            try:
                pid = int(legacy.split(":", 1)[0])
            except ValueError:
                return None
            if pid > 0 and _pid_alive(pid):
                return None

        publish_owner(fd, {"pid": os.getpid(), "purpose": purpose,
                           "started": datetime.now(IST).timestamp()})
        owned = True
        return fd
    finally:
        if not owned:
            os.close(fd)


def release_engine_lock(fd: Optional[int]) -> None:
    if fd is not None:
        os.close(fd)


def _eod_store():
    return EodStore(os.path.join(BASE_DIR, "survivor_eod.sqlite3"))


def _scheduled_flatten(policy: RolePolicy) -> int:
    now = datetime.now(IST)
    slot = scheduled_slot(now)
    if slot is None:
        print("Scheduled flatten requires an IST dispatch slot.")
        return 1
    day = slot.date().isoformat()
    store = _eod_store()
    attempt = store.begin(day, slot.strftime("%H:%M"))
    if attempt is None:
        print("Scheduled flatten ownership already claimed for this slot.")
        return 1
    closing = slot + timedelta(seconds=WINDOW_SECONDS)
    remaining = (closing - now).total_seconds()
    deadline = time.monotonic() + remaining

    def budget():
        return min(deadline - time.monotonic(), (closing - datetime.now(IST)).total_seconds())

    def finish(status, details):
        message = store.finish(attempt, day, status, details)
        if message:
            print(message, flush=True)
        return int(status != "flat")

    last_owner = {}
    while budget() > 0:
        fd = acquire_engine_lock("scheduled")
        if fd is not None:
            try:
                store.interrupted(day, attempt, _pid_alive)
                if budget() <= 0:
                    break
                try:
                    client = SurvivorDhanClient(policy=policy)
                    result = _exit_coordinator(client, budget=budget).flatten(3)
                except Exception as exc:
                    result = None
                    details = f"Exposure UNKNOWN ({type(exc).__name__})"
                if budget() <= 0:
                    break
                if result is None:
                    return finish("unknown", details)
                if not result.flat:
                    status = "unknown" if result.unknown else "incomplete"
                    return finish(status, f"Exposure UNKNOWN. {result[0]}")
                print(result[0])
                return finish("flat", "")
            finally:
                release_engine_lock(fd)

        last_owner = owner_record(_engine_lock_path())
        cutoff = slot.replace(hour=15, minute=10).timestamp()
        if last_owner and (last_owner.get("purpose") != "routine"
                           or last_owner.get("started", cutoff) >= cutoff):
            return finish("contention", "Engine ownership held by another invocation; exposure UNKNOWN")
        delay = min(LOCK_POLL_SECONDS, budget())
        if delay > 0:
            time.sleep(delay)

    evidence = last_owner.get("exposure", "UNKNOWN")
    return finish("deadline_breach", f"Scheduled flatten deadline breached; last verified exposure {json.dumps(evidence)}")


def cmd_flatten(policy: RolePolicy, *, scheduled: bool = False) -> int:
    if scheduled:
        return _scheduled_flatten(policy)
    fd = acquire_engine_lock()
    if fd is None:
        print("Emergency flatten unavailable: engine ownership held by another invocation.")
        return 1
    try:
        try:
            client = SurvivorDhanClient(policy=policy)
        except CachedTokenUnavailable as exc:
            print(f"Emergency flatten unavailable: {exc}")
            return 1
        result = client.emergency_flatten_all()
        print("Emergency Flatten:", result)
        return int(not result.flat)
    finally:
        release_engine_lock(fd)


def load_state(*, read_only: bool = False) -> Dict:
    default_state = {
        "status": "AWAITING_FUNDING",  # AWAITING_FUNDING -> ALIVE -> DEAD_COMA
        "mortality_armed": False,
        "seed_capital": SEED_CAPITAL,
        "death_floor": DEATH_FLOOR,
        "peak_equity": SEED_CAPITAL,
        "current_equity": 0.0,
        "available_cash": 0.0,
        "unrealized_pnl": 0.0,
        "realized_pnl_total": 0.0,
        "daily_start_equity": 0.0,
        "daily_date": "",
        "daily_trades_count": 0,
        "daily_consecutive_losses": 0,
        "daily_halted": False,
        "daily_halt_reason": "",
        "last_sl_direction": None,
        "last_sl_time": None,
        "active_trade": None,  # Dict tracking current open trade + SL/TP
        "total_trades": 0,
        "wins": 0,
        "losses": 0,
        "created_at": datetime.now(IST).isoformat(),
        "last_heartbeat": None,
        "death_timestamp": None,
        "death_reason": None,
    }
    # Readers can use the snapshot published by the atomic state-file replace.
    lock_fd = None if read_only else _acquire_state_lock(timeout=5.0)
    try:
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    saved = json.load(f)
                default_state.update(saved)
            except Exception as e:
                logger.error(f"Failed to read state file: {e}")
    finally:
        if lock_fd is not None:
            _release_state_lock(lock_fd)
    return default_state


def save_state(state: Dict) -> None:
    state["last_heartbeat"] = datetime.now(IST).isoformat()
    lock_fd = _acquire_state_lock(timeout=5.0)
    try:
        persist_exit_data(STATE_FILE, state)
    finally:
        _release_state_lock(lock_fd)


def load_journal() -> List[Dict]:
    if os.path.exists(JOURNAL_FILE):
        try:
            with open(JOURNAL_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []


def _read_exit_journal() -> List[Dict]:
    try:
        with open(JOURNAL_FILE, "r", encoding="utf-8") as storage:
            journal = json.load(storage)
    except FileNotFoundError:
        return []
    if not isinstance(journal, list) or any(not isinstance(row, dict) for row in journal):
        raise ValueError("Invalid trade journal")
    return journal


# ─── Dhan Pro Trader API Client ────────────────────────────────────────────

def _resolve_role(action: str, *, live: bool = False, repair: bool = False) -> RolePolicy:
    values = load_env_file(exclude=(LIVE_SWITCH,))
    return RolePolicy.resolve(
        action, enabled=live_trading_enabled(values.get(LIVE_SWITCH, "")), live=live, repair=repair
    )


class SurvivorDhanClient:
    def __init__(
        self, *, policy: Optional[RolePolicy] = None,
        allow_token_renewal: bool = False, allow_order_writes: Optional[bool] = None
    ):
        if policy is None:
            values = load_env_file(exclude=(LIVE_SWITCH,))
            enabled = live_trading_enabled(values.get(LIVE_SWITCH, ""))
            policy = RolePolicy(
                renew_token=enabled and allow_token_renewal,
                trade=enabled and allow_order_writes is True,
            )
        self._policy = policy
        self._connect(DhanTokenManager(policy=policy))

    def _connect(self, token_manager: DhanTokenManager):
        from dhanhq import dhanhq

        broker = ReadOnlyBroker(dhanhq(token_manager.get_dhan_context()), policy=self._policy)
        self.tm = token_manager
        self.dhan = broker

    def reconnect_with_fresh_token(self):
        """Reconnect using the original role policy."""
        manager = DhanTokenManager(policy=self._policy)
        if self._policy.renew_token:
            manager.renew_token()
        self._connect(manager)

    def ensure_ip_whitelisted(self, *, allow_update: bool = True) -> Dict:
        """
        Verify via `GET /v2/ip/getIP` that the current public IPv4 is matched and `ordersAllowed == True`.
        If the ISP rotated our public IPv4 overnight (`ipMatchStatus == NO_MATCH` or `ordersAllowed == False`),
        automatically attempt to update an eligible slot (`SECONDARY` or `PRIMARY` if >=7 days old) via
        `PUT /v2/ip/modifyIP` and regenerate the JWT token so `DH-905` never blocks morning trades!
        """
        import requests as _req
        tok = self.tm.get_token()
        headers = {
            "access-token": tok,
            "client-id": self.tm.client_id,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        try:
            r = _req.get("https://api.dhan.co/v2/ip/getIP", headers=headers, timeout=10)
            ip_info = r.json() if r.status_code == 200 else {}
        except Exception as e:
            return {"ordersAllowed": False, "error": str(e)}

        if (ip_info.get("ordersAllowed") is True or not allow_update
                or not self._policy.repair_ip):
            return ip_info

        detected_ip = ip_info.get("detectedIP") or _req.get("https://api.ipify.org", timeout=5).text.strip()
        today_str = datetime.now(IST).strftime("%Y-%m-%d")

        # Try updating SECONDARY or PRIMARY slot if eligible (modifyDate <= today_str)
        for flag, mod_key in [("SECONDARY", "modifyDateSecondary"), ("PRIMARY", "modifyDatePrimary")]:
            mod_date = str(ip_info.get(mod_key, "9999-99-99"))
            if mod_date <= today_str:
                try:
                    put_r = _req.put(
                        "https://api.dhan.co/v2/ip/modifyIP",
                        headers=headers,
                        json={"dhanClientId": self.tm.client_id, "ip": detected_ip, "ipFlag": flag},
                        timeout=10,
                    )
                    logger.info(f"Auto-updated {flag} IP to {detected_ip}: {put_r.text}")
                except Exception as e:
                    logger.warning(f"Auto-modifyIP ({flag}) error: {e}")

        # Regenerate JWT token to bind updated IP and re-check
        self.reconnect_with_fresh_token()
        tok = self.tm.get_token()
        headers["access-token"] = tok
        try:
            r2 = _req.get("https://api.dhan.co/v2/ip/getIP", headers=headers, timeout=10)
            return r2.json() if r2.status_code == 200 else ip_info
        except Exception:
            return ip_info

    def get_live_equity_and_positions(self) -> Tuple[float, float, float, List[Dict]]:
        """
        Returns (net_liquidation_value, available_balance, utilized_amount, open_positions)
        Note: Dhan's availabelBalance decreases when margin is utilized, so:
        Total Account Cash = availabelBalance + utilizedAmount
        Net Liquidation Value = availabelBalance + utilizedAmount + unrealizedProfit
        """
        funds_resp = self.dhan.get_fund_limits()
        if funds_resp.get("status") != "success":
            raise RuntimeError(f"Failed to fetch Dhan fund limits: {funds_resp}")

        f_data = funds_resp.get("data", {})
        avail = float(f_data.get("availabelBalance", 0.0))
        utilized = float(f_data.get("utilizedAmount", 0.0))

        pos_resp = self.dhan.get_positions()
        positions = pos_resp.get("data", []) if isinstance(pos_resp, dict) and pos_resp.get("status") == "success" else []

        unrealized = 0.0
        open_positions = []
        for p in positions:
            net_qty = int(p.get("netQty", 0))
            if net_qty != 0:
                u_pnl = float(p.get("unrealizedProfit", 0.0))
                unrealized += u_pnl
                open_positions.append(p)

        net_liq = round(avail + utilized + unrealized, 2)
        return net_liq, round(avail, 2), round(utilized, 2), open_positions

    def fetch_candles(self, security_id: str, exchange_segment: str = "IDX_I",
                      instrument_type: str = "INDEX", days_back: int = 5,
                      resample_tf: str = "5min") -> pd.DataFrame:
        """Fetch 1-min candles via Dhan Pro Trader API and resample to 5min or 1min."""
        now_ist = datetime.now(IST)
        from_date = (now_ist - timedelta(days=days_back)).strftime("%Y-%m-%d")
        to_date = now_ist.strftime("%Y-%m-%d")

        resp = self.dhan.intraday_minute_data(
            security_id=str(security_id),
            exchange_segment=exchange_segment,
            instrument_type=instrument_type,
            from_date=from_date,
            to_date=to_date,
        )
        if resp.get("status") != "success" or not resp.get("data"):
            return pd.DataFrame()

        d = resp["data"]
        if not isinstance(d, dict) or not d.get("close"):
            return pd.DataFrame()

        df = pd.DataFrame({
            "timestamp": pd.to_datetime(d["timestamp"], unit="s", utc=True).tz_convert(IST),
            "open": d["open"],
            "high": d["high"],
            "low": d["low"],
            "close": d["close"],
            "volume": d["volume"],
        })
        df.set_index("timestamp", inplace=True)
        df.sort_index(inplace=True)

        if resample_tf and resample_tf != "1min":
            df = df.resample(resample_tf).agg({
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }).dropna()

        return df

    def _fallback_option_from_security_master(self, direction: str, max_lot_cost: float,
                                              under_security_id: int, lot_size: int,
                                              index_name: str, today_str: str) -> Optional[Dict]:
        """
        Resilient Fallback when Dhan's `/v2/optionchain` REST microservice returns HTTP 502:
        1. Reads live index spot from `intraday_minute_data` (`IDX_I`).
        2. Filters Dhan's security master CSV (`fetch_security_list('compact')`) for `index_name` OPTIDX
           contracts on the nearest non-0DTE expiry (`SEM_EXPIRY_DATE > today_str`).
        3. Evaluates the 3 nearest strikes around ATM (`CE` for LONG, `PE` for SHORT) via `fetch_candles`
           (`intraday_minute_data` on `NSE_FNO`, which stays 100% online even when `/v2/optionchain` 502s!).
        """
        try:
            df_idx = self.fetch_candles(str(under_security_id), exchange_segment="IDX_I", instrument_type="INDEX", days_back=4, resample_tf="3min")
            if df_idx.empty:
                return None
            spot = float(df_idx.iloc[-1]["close"])

            if not hasattr(self, "_sec_master_cache") or self._sec_master_cache is None:
                self._sec_master_cache = self.dhan.fetch_security_list(mode="compact")
            sec_df = self._sec_master_cache

            opt_type = "CE" if direction == "LONG" else "PE"
            m = sec_df[
                (sec_df["SEM_INSTRUMENT_NAME"] == "OPTIDX")
                & (sec_df["SEM_TRADING_SYMBOL"].astype(str).str.startswith(index_name + "-"))
                & (sec_df["SEM_OPTION_TYPE"] == opt_type)
            ].copy()
            if m.empty:
                return None

            m["exp_date"] = m["SEM_EXPIRY_DATE"].astype(str).str[:10]
            valid_exps = sorted([e for e in m["exp_date"].unique() if e > today_str])
            if not valid_exps:
                # P6: hard 0DTE ban — no same-day-expiry fallback, ever.
                logger.warning("0DTE guard: no future expiry listed; refusing same-day expiry (no fallback).")
                return None
            target_expiry = valid_exps[0]

            m_exp = m[m["exp_date"] == target_expiry].copy()
            m_exp["strike"] = m_exp["SEM_STRIKE_PRICE"].astype(float)
            m_exp["dist"] = (m_exp["strike"] - spot).abs()
            nearest_rows = m_exp.sort_values("dist").head(4)

            candidates = []
            for _, row in nearest_rows.iterrows():
                sec_id = str(row["SEM_SMST_SECURITY_ID"])
                strike = float(row["strike"])
                df_opt = self.fetch_candles(sec_id, exchange_segment="NSE_FNO", instrument_type="OPTIDX", days_back=4, resample_tf="3min")
                _sess = option_session_frame(df_opt)
                if _sess.empty:
                    continue
                ltp = round(float(df_opt.iloc[-1]["close"]), 2)
                vol = int(df_opt.iloc[-1]["volume"])
                lot_cost = round(ltp * lot_size, 2)
                if ltp <= 12.0 or lot_cost > max_lot_cost:
                    continue
                score = 100.0 - float(row["dist"]) * 0.1 + min(20.0, vol / 5000.0)
                candidates.append({
                    "symbol": f"{index_name} {target_expiry} {int(strike)} {opt_type}",
                    "security_id": sec_id,
                    "exchange_segment": "NSE_FNO",
                    "instrument_type": "OPTIDX",
                    "strike": strike,
                    "option_type": opt_type,
                    "expiry": target_expiry,
                    "ltp": ltp,
                    "bid": ltp,
                    "ask": ltp,
                    "lot_size": lot_size,
                    "lot_cost": lot_cost,
                    "delta": 0.50,
                    "theta": -5.0,
                    "iv": 15.0,
                    "oi": vol,
                    "score": round(score, 2),
                    # P7: this quote is MANUFACTURED from candles/constants — never tradable.
                    "synthetic": True,
                })

            if not candidates:
                return None
            candidates.sort(key=lambda x: x["score"], reverse=True)
            return candidates[0]
        except Exception as e:
            logger.warning(f"Fallback option selector error for {index_name}: {e}")
            return None

    def get_nearest_nifty_option(self, direction: str, max_lot_cost: float,
                                 under_security_id: int = 13, lot_size: int = OPTION_LOT_SIZE,
                                 index_name: str = "NIFTY") -> Optional[Dict]:
        """
        Select the highest-quality weekly option contract (CE for LONG, PE for SHORT)
        for a given index using live Greeks & Bid/Ask spread from Dhan's `option_chain` endpoint,
        with automatic fallback to Security Master + Intraday Candles if `/v2/optionchain` is 502/off-hours.
        """
        today_str = datetime.now(IST).strftime("%Y-%m-%d")
        exp_resp = self.dhan.expiry_list(under_security_id=under_security_id, under_exchange_segment="IDX_I")
        if exp_resp.get("status") != "success":
            return self._fallback_option_from_security_master(direction, max_lot_cost, under_security_id, lot_size, index_name, today_str)
        expiries = exp_resp.get("data", {}).get("data", [])
        if not expiries:
            return self._fallback_option_from_security_master(direction, max_lot_cost, under_security_id, lot_size, index_name, today_str)

        # Select nearest expiry, skipping 0DTE (expiry == today_str) so we never get crushed by
        # expiry-day gamma/theta whipsaws or newly minted strikes with 0 historical chart bars.
        valid_expiries = [e for e in expiries if e > today_str]
        if not valid_expiries:
            # P6: hard 0DTE ban — if only same-day expiry is listed, do not trade it.
            logger.warning("0DTE guard: only same-day expiry available; refusing 0DTE (no fallback to today).")
            return None
        target_expiry = valid_expiries[0]

        oc_resp = self.dhan.option_chain(
            under_security_id=under_security_id,
            under_exchange_segment="IDX_I",
            expiry=target_expiry,
        )
        if oc_resp.get("status") != "success":
            return self._fallback_option_from_security_master(direction, max_lot_cost, under_security_id, lot_size, index_name, today_str)

        oc_data = oc_resp.get("data", {}).get("data", {})
        spot = float(oc_data.get("last_price", 0.0))
        oc_map = oc_data.get("oc", {})
        if not spot or not oc_map:
            return None

        opt_key = "ce" if direction == "LONG" else "pe"
        candidates = []

        for strike_str, sides in oc_map.items():
            try:
                strike = float(strike_str)
            except ValueError:
                continue
            leg = sides.get(opt_key)
            if not leg:
                continue

            ltp = float(leg.get("last_price", 0.0))
            bid = float(leg.get("top_bid_price", 0.0))
            ask = float(leg.get("top_ask_price", 0.0))
            sec_id = str(leg.get("security_id", ""))
            oi = int(leg.get("oi", 0))
            vol = int(leg.get("volume", 0))
            greeks = leg.get("greeks", {})
            delta = abs(float(greeks.get("delta", 0.0)))
            theta = float(greeks.get("theta", 0.0))
            iv = float(leg.get("implied_volatility", 0.0))

            if ltp <= 12.0 or not sec_id or vol < 10000:
                continue

            lot_cost = ltp * lot_size
            if lot_cost > max_lot_cost:
                continue

            # Delta sweet spot: 0.35 to 0.60 (avoids rapid OTM theta bleed while staying affordable)
            if not (0.30 <= delta <= 0.65):
                continue

            spread_pct = ((ask - bid) / ltp * 100.0) if (ask > 0 and bid > 0 and ltp > 0) else 0.5
            if spread_pct > 2.0:  # Reject illiquid wide-spread strikes
                continue

            # Score highest delta-to-theta ratio + liquidity
            score = (delta * 100.0) - abs(theta) * 0.5 - spread_pct * 5.0
            candidates.append({
                "symbol": f"{index_name} {target_expiry} {int(strike)} {opt_key.upper()}",
                "security_id": sec_id,
                "exchange_segment": "NSE_FNO",
                "instrument_type": "OPTIDX",
                "strike": strike,
                "option_type": opt_key.upper(),
                "expiry": target_expiry,
                "ltp": ltp,
                "bid": bid,
                "ask": ask,
                "lot_size": lot_size,
                "lot_cost": round(lot_cost, 2),
                "delta": round(delta, 3),
                "theta": round(theta, 2),
                "iv": round(iv, 2),
                "oi": oi,
                "score": round(score, 2),
            })

        if not candidates:
            return None
        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates[0]

    def emergency_flatten_all(self, max_retries: int = 3) -> FlattenResult:
        if not (self._policy.trade or self._policy.flatten):
            raise RuntimeError("Emergency flatten requires an authorized trading or flatten role.")
        try:
            return _exit_coordinator(self).flatten(max_retries)
        except Exception:
            logger.exception("Exit ledger unavailable; recovery requires operator attention")
            return FlattenResult("Flatten INCOMPLETE: exit ledger unavailable")

    def place_protective_sl_order(self, security_id: str, exchange_segment: str,
                                  transaction_type: str, quantity: int,
                                  trigger_price: float, product_type: str = "INTRADAY") -> Dict:
        key = (str(security_id), exchange_segment, product_type)
        try:
            for _ in range(ENTRY_SETTLE_TRIES):
                if verified_positions(self.dhan).get(key):
                    break
                time.sleep(ENTRY_SETTLE_POLL_S)
            tick = _get_tick_size(security_id, exchange_segment)
            trigger = round(round(trigger_price / tick) * tick, 2)
            cushion = 0.98 if transaction_type == "SELL" else 1.02
            price = round(round(trigger * cushion / tick) * tick, 2)
            return _exit_coordinator(self).protect(
                key, transaction_type, quantity, {"price": price, "trigger_price": trigger})
        except Exception:
            logger.exception("Protective order could not be verified")
            return {"placed": None, "rejected": True, "filled_qty": 0}



def compute_supertrend(df: pd.DataFrame, period: int = 10, multiplier: float = 2.0) -> Tuple[pd.Series, pd.Series]:
    """
    Compute Supertrend (period=10, multiplier=2.0) for fast 3-minute scalping.
    Returns (supertrend_line, supertrend_dir) where dir is +1 (Bullish/Green) or -1 (Bearish/Red).
    """
    hl2 = (df["high"] + df["low"]) / 2.0
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift(1)).abs()
    low_close = (df["low"] - df["close"].shift(1)).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1.0 / period, adjust=False).mean().bfill()

    upperband = (hl2 + multiplier * atr).values.copy()
    lowerband = (hl2 - multiplier * atr).values.copy()
    close = df["close"].values
    n = len(df)
    st_dir = np.ones(n, dtype=int)
    st_line = np.zeros(n, dtype=float)
    st_line[0] = lowerband[0]

    for i in range(1, n):
        if close[i - 1] > lowerband[i - 1]:
            lowerband[i] = max(lowerband[i], lowerband[i - 1])
        if close[i - 1] < upperband[i - 1]:
            upperband[i] = min(upperband[i], upperband[i - 1])

        if close[i] > upperband[i - 1]:
            st_dir[i] = 1
        elif close[i] < lowerband[i - 1]:
            st_dir[i] = -1
        else:
            st_dir[i] = st_dir[i - 1]
            if st_dir[i] == 1 and lowerband[i] < lowerband[i - 1]:
                lowerband[i] = lowerband[i - 1]
            if st_dir[i] == -1 and upperband[i] > upperband[i - 1]:
                upperband[i] = upperband[i - 1]

        st_line[i] = lowerband[i] if st_dir[i] == 1 else upperband[i]

    return pd.Series(st_line, index=df.index), pd.Series(st_dir, index=df.index)


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 25:
        return df
    df = df.copy()
    df["ema9"] = df["close"].ewm(span=9, adjust=False).mean()
    df["ema21"] = df["close"].ewm(span=21, adjust=False).mean()

    # MACD (12, 26, 9) + Histogram
    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    df["macd"] = ema12 - ema26
    df["macd_sig"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_sig"]

    # Intraday VWAP (reset per day)
    df["date"] = df.index.date
    df["typ_vol"] = ((df["high"] + df["low"] + df["close"]) / 3.0) * df["volume"]
    df["cum_vol"] = df.groupby("date")["volume"].cumsum().replace(0, np.nan)
    df["cum_typ_vol"] = df.groupby("date")["typ_vol"].cumsum()
    df["vwap"] = df["cum_typ_vol"] / df["cum_vol"]
    df["vwap"] = df["vwap"].fillna(df["close"])

    # RSI (14)
    delta = df["close"].diff()
    gain = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1/14, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    df["rsi"] = 100.0 - (100.0 / (1.0 + rs))
    df["rsi"] = df["rsi"].fillna(50.0)

    # ATR (14)
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift(1)).abs()
    low_close = (df["low"] - df["close"].shift(1)).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df["atr"] = tr.rolling(14).mean().bfill()

    # ── Phase 1 (1.2): ADX(14) — market regime filter ──
    # +DM / -DM
    up_move = df["high"].diff()
    down_move = -df["low"].diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    # Smooth with Wilder's EMA (alpha = 1/14)
    atr14 = tr.ewm(alpha=1/14, adjust=False).mean()
    plus_di = 100.0 * pd.Series(plus_dm, index=df.index).ewm(alpha=1/14, adjust=False).mean() / atr14.replace(0, np.nan)
    minus_di = 100.0 * pd.Series(minus_dm, index=df.index).ewm(alpha=1/14, adjust=False).mean() / atr14.replace(0, np.nan)
    dx = (100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)).fillna(0.0)
    df["adx"] = dx.ewm(alpha=1/14, adjust=False).mean().fillna(0.0)
    df["regime"] = np.where(df["adx"] >= 20.0, "TRENDING", "CHOPPY")

    # Supertrend (10, 2.0)
    st_line, st_dir = compute_supertrend(df, period=10, multiplier=2.0)
    df["supertrend"] = st_line
    df["st_dir"] = st_dir

    # Volume Spike ratio vs 20-bar average
    vol_ma = df["volume"].rolling(20).mean().replace(0, np.nan)
    df["rel_vol"] = (df["volume"] / vol_ma).fillna(1.0)

    return df


def evaluate_index_signal(df: pd.DataFrame) -> Dict:
    """
    Fast-Strike 3-Minute Multi-Indicator Scalping Engine on Nifty 50:
    1) Setup 1 (ORB_IGNITE, 09:20-10:35 IST): Breakout of first 6-min Opening Range High/Low
       confirmed by Supertrend(10,2) + VWAP + RSI(>54 or <46).
    2) Setup 2 (ST_FLIP_CONTINUATION): Supertrend(10,2) flip or pullback continuation where
       Supertrend is aligned, EMA9 > EMA21 (or <), Close > VWAP (or <), and MACD Hist is rising/falling.
    3) Setup 3 (EMA_MOMENTUM_BURST): Strong 3-min momentum candle breaking prev high/low with
       EMA9 + VWAP + Supertrend alignment and RSI in power zone (55-76 for LONG, 24-45 for SHORT).
    """
    if df.empty or len(df) < 25:
        return {"signal": "NONE", "reason": "Insufficient candles"}

    df = compute_indicators(df)
    curr = df.iloc[-1]
    prev = df.iloc[-2]

    close = float(curr["close"])
    ema9 = float(curr["ema9"])
    ema21 = float(curr["ema21"])
    vwap = float(curr["vwap"])
    rsi = float(curr["rsi"])
    atr = float(curr["atr"])
    st_dir = int(curr["st_dir"])
    prev_st_dir = int(prev["st_dir"])
    st_val = float(curr["supertrend"])
    macd_hist = float(curr["macd_hist"])
    prev_macd_hist = float(prev["macd_hist"])
    adx = float(curr.get("adx", 0.0))
    regime = str(curr.get("regime", "TRENDING"))

    # ── Phase 1 (1.2): Regime filter — skip entries in choppy markets ──
    # ADX < 20 means no clear trend → EMA crossover signals whipsaw.
    # Log the regime so Telegram probe can track how often we sit out.
    if regime == "CHOPPY":
        return {
            "signal": "NONE",
            "reason": f"[REGIME_FILTER] ADX={adx:.1f} < 20 → CHOPPY, no entry",
            "adx": round(adx, 1),
            "regime": regime,
        }

    # Compute today's Opening Range (first 2 bars of 3-min = 09:15 to 09:21 IST)
    today_date = df.index[-1].date()
    today_bars = df[df.index.date == today_date]
    orb_high, orb_low = None, None
    today_open = float(today_bars.iloc[0]["open"]) if not today_bars.empty else close
    if len(today_bars) >= 3:
        orb_slice = today_bars.iloc[:2]
        orb_high = float(orb_slice["high"].max())
        orb_low = float(orb_slice["low"].min())

    hhmm = df.index[-1].strftime("%H:%M")
    st_label = "GREEN" if st_dir == 1 else "RED"

    # GAP-DISTORTION SHIELD:
    # Before 10:15 IST, EMA9/EMA21/Supertrend are heavily distorted by yesterday's closing bars if the market gaps.
    # Rule: Never trigger a LONG unless Spot > Today's ORB High (or > Today's Open + 2 consecutive higher closes).
    # Rule: Never trigger a SHORT unless Spot < Today's ORB Low AND current 3m bar makes a lower low & lower high than prev bar.
    intraday_bull_ok = True
    intraday_bear_ok = True
    if orb_high and orb_low and hhmm <= "10:30":
        intraday_bull_ok = (close > orb_high) and (close > today_open) and (close > prev["close"])
        intraday_bear_ok = (close < orb_low) and (close < today_open) and (close < prev["low"]) and (curr["high"] <= prev["high"])

    # --- Setup 1: Morning Opening Range Breakout (09:20 - 10:35 IST) ---
    if orb_high and orb_low and (ORB_WINDOW_START <= hhmm <= ORB_WINDOW_END):
        if st_dir == 1 and intraday_bull_ok and prev["close"] <= orb_high * 1.0008 and close > ema9 and 54.0 <= rsi <= ENTRY_RSI_CEILING_LONG:
            return {
                "signal": "LONG",
                "setup": "ORB_IGNITE",
                "confidence": 88.0,
                "spot": round(close, 2),
                "ema9": round(ema9, 2),
                "ema21": round(ema21, 2),
                "vwap": round(vwap, 2),
                "supertrend": round(st_val, 2),
                "st_dir": st_label,
                "rsi": round(rsi, 1),
                "atr": round(atr, 2),
                "reason": f"[ORB_IGNITE LONG] 3m Close {close:.1f} broke ORB High {orb_high:.1f} | ST(10,2)={st_label} ({st_val:.1f}) | RSI={rsi:.1f}",
            }
        if st_dir == -1 and intraday_bear_ok and prev["close"] >= orb_low * 0.9992 and close < ema9 and ENTRY_RSI_FLOOR_SHORT <= rsi <= 46.0:
            return {
                "signal": "SHORT",
                "setup": "ORB_IGNITE",
                "confidence": 88.0,
                "spot": round(close, 2),
                "ema9": round(ema9, 2),
                "ema21": round(ema21, 2),
                "vwap": round(vwap, 2),
                "supertrend": round(st_val, 2),
                "st_dir": st_label,
                "rsi": round(rsi, 1),
                "atr": round(atr, 2),
                "reason": f"[ORB_IGNITE SHORT] 3m Close {close:.1f} broke ORB Low {orb_low:.1f} | ST(10,2)={st_label} ({st_val:.1f}) | RSI={rsi:.1f}",
            }

    # --- Setup 2: Supertrend (10,2) Fresh Flip + EMA9/VWAP Confirmation ---
    if st_dir == 1 and prev_st_dir == -1 and intraday_bull_ok and close > ema9 and close >= vwap and rsi >= 53.0:
        return {
            "signal": "LONG",
            "setup": "SUPERTREND_FLIP",
            "confidence": 85.0,
            "spot": round(close, 2),
            "ema9": round(ema9, 2),
            "ema21": round(ema21, 2),
            "vwap": round(vwap, 2),
            "supertrend": round(st_val, 2),
            "st_dir": st_label,
            "rsi": round(rsi, 1),
            "atr": round(atr, 2),
            "reason": f"[SUPERTREND_FLIP LONG] 3m ST(10,2) flipped GREEN @ {close:.1f} (> EMA9 {ema9:.1f}, RSI {rsi:.1f})",
        }
    if st_dir == -1 and prev_st_dir == 1 and intraday_bear_ok and close < ema9 and close <= vwap and rsi <= 47.0:
        return {
            "signal": "SHORT",
            "setup": "SUPERTREND_FLIP",
            "confidence": 85.0,
            "spot": round(close, 2),
            "ema9": round(ema9, 2),
            "ema21": round(ema21, 2),
            "vwap": round(vwap, 2),
            "supertrend": round(st_val, 2),
            "st_dir": st_label,
            "rsi": round(rsi, 1),
            "atr": round(atr, 2),
            "reason": f"[SUPERTREND_FLIP SHORT] 3m ST(10,2) flipped RED @ {close:.1f} (< EMA9 {ema9:.1f}, RSI {rsi:.1f})",
        }

    # --- Setup 3: Triple-Lock Momentum Scalp (Supertrend + EMA9/21 + VWAP + MACD/Breakout + Gap & Exhaustion Shield) ---
    # Anti-Exhaustion & Anti-Chop Rules (Day 3 Audit):
    # - Do NOT enter LONG if 3m RSI > 72 (overbought exhaustion) or MACD_Hist <= 0 or EMA21 is flat/falling.
    # - Do NOT enter SHORT if 3m RSI < 30 (oversold exhaustion, e.g. 5th red candle @ RSI 26.8) or MACD_Hist >= 0 (e.g. +7.41 midday chop) or EMA21 is flat/rising.
    ema21_slope_pts = float(curr["ema21"]) - float(df.iloc[-4]["ema21"]) if len(df) >= 4 else 0.0
    # ATR-normalised so the threshold means the same on NIFTY / BANKNIFTY / MIDCPNIFTY
    ema21_slope = ema21_slope_pts / atr if atr > 0 else 0.0

    bull_align = (
        (st_dir == 1)
        and intraday_bull_ok
        and (close > ema9 > ema21)
        and (close > vwap)
        and (55.0 <= rsi <= 72.0)
        and (macd_hist > 0.0)
        and (ema21_slope >= EMA21_SLOPE_ATR_MIN)
    )
    bull_spark = (close > prev["high"]) and (macd_hist > prev_macd_hist)

    bear_align = (
        (st_dir == -1)
        and intraday_bear_ok
        and (close < ema9 < ema21)
        and (close < vwap)
        and (30.0 <= rsi <= 44.0)
        and (macd_hist < 0.0)
        and (ema21_slope <= -EMA21_SLOPE_ATR_MIN)
    )
    bear_spark = (close < prev["low"]) and (macd_hist < prev_macd_hist)

    if bull_align and bull_spark:
        return {
            "signal": "LONG",
            "setup": "FAST_STRIKE_TREND",
            "confidence": round(min(92.0, 68.0 + (rsi - 55.0)), 1),
            "spot": round(close, 2),
            "ema9": round(ema9, 2),
            "ema21": round(ema21, 2),
            "vwap": round(vwap, 2),
            "supertrend": round(st_val, 2),
            "st_dir": st_label,
            "rsi": round(rsi, 1),
            "atr": round(atr, 2),
            "reason": f"[FAST_STRIKE LONG] 3m Close {close:.1f} > EMA9 {ema9:.1f} > EMA21 {ema21:.1f} | ST={st_label} | RSI={rsi:.1f} | MACD_H={macd_hist:+.1f}",
        }
    elif bear_align and bear_spark:
        return {
            "signal": "SHORT",
            "setup": "FAST_STRIKE_TREND",
            "confidence": round(min(92.0, 68.0 + (45.0 - rsi)), 1),
            "spot": round(close, 2),
            "ema9": round(ema9, 2),
            "ema21": round(ema21, 2),
            "vwap": round(vwap, 2),
            "supertrend": round(st_val, 2),
            "st_dir": st_label,
            "rsi": round(rsi, 1),
            "atr": round(atr, 2),
            "reason": f"[FAST_STRIKE SHORT] 3m Close {close:.1f} < EMA9 {ema9:.1f} < EMA21 {ema21:.1f} | ST={st_label} | RSI={rsi:.1f} | MACD_H={macd_hist:+.1f}",
        }

    return {
        "signal": "NONE",
        "setup": "NONE",
        "confidence": 0.0,
        "spot": round(close, 2),
        "ema9": round(ema9, 2),
        "ema21": round(ema21, 2),
        "vwap": round(vwap, 2),
        "supertrend": round(st_val, 2),
        "st_dir": st_label,
        "rsi": round(rsi, 1),
        "atr": round(atr, 2),
        "reason": f"No 3m setup (Spot {close:.1f}, ST={st_label}@{st_val:.1f}, EMA9={ema9:.1f}, EMA21={ema21:.1f}, RSI={rsi:.1f}, MACD_H={macd_hist:+.1f})",
    }


def option_session_frame(df_opt: pd.DataFrame) -> pd.DataFrame:
    """Current-session bars only, with EMA9/VWAP/Supertrend computed on those bars alone
    (no overnight-gap contamination). Returns empty frame if too few session bars."""
    if df_opt is None or df_opt.empty:
        return pd.DataFrame()
    last_day = df_opt.index[-1].date()
    sess = df_opt[df_opt.index.date == last_day].copy()
    if len(sess) < MIN_OPT_SESSION_BARS:
        return sess.iloc[0:0]
    sess["ema9"] = sess["close"].ewm(span=9, adjust=False).mean()
    typ_vol = ((sess["high"] + sess["low"] + sess["close"]) / 3.0) * sess["volume"]
    cum_vol = sess["volume"].cumsum().replace(0, np.nan)
    sess["vwap"] = (typ_vol.cumsum() / cum_vol).fillna(sess["close"])
    st_line, st_dir = compute_supertrend(sess, period=10, multiplier=2.0)
    sess["supertrend"] = st_line
    sess["st_dir"] = st_dir
    return sess


def verify_option_chart_momentum(client: SurvivorDhanClient, security_id: str) -> Tuple[bool, str]:
    """
    Verify on the Option Contract's OWN 3-minute chart that:
    1) The option chart actually has >= 15 bars of history (NEVER blindly allow insufficient bars!).
    2) The option itself has positive momentum (`LTP >= EMA9` and `LTP >= VWAP` or `Supertrend == GREEN`).
    3) Anti-Spike Overextension Cap (Day 3 Audit): `LTP <= EMA9 * 1.15` (rejects buying an option stretched >15%
       above its 3m EMA9 at the climax of a 5-candle spike right before a 1-min pullback wick).
    """
    df_opt = client.fetch_candles(security_id, exchange_segment="NSE_FNO", instrument_type="OPTIDX", days_back=4, resample_tf="3min")
    df_opt = option_session_frame(df_opt)
    if df_opt.empty:
        return False, f"OptChart Insufficient Session Bars (< {MIN_OPT_SESSION_BARS} today) — refusing blind entry"
    curr = df_opt.iloc[-1]
    c = float(curr["close"])
    e9 = float(curr["ema9"])
    vwap = float(curr["vwap"])
    st_dir = int(curr["st_dir"])
    if e9 > 0 and c > e9 * 1.15:
        stretch_pct = (c / e9 - 1.0) * 100.0
        return False, f"OptChart Overextended +{stretch_pct:.1f}% above EMA9 ({c:.1f} vs EMA9 {e9:.1f}) — skipping spike top"
    if c >= e9 * 0.992 and (c >= vwap * 0.99 or st_dir == 1):
        return True, f"OptChart OK (LTP {c:.1f}, EMA9 {e9:.1f}, VWAP {vwap:.1f})"
    return False, f"OptChart Weak (LTP {c:.1f} < EMA9 {e9:.1f} & VWAP {vwap:.1f})"


def scan_equity_candidates(client: SurvivorDhanClient) -> Optional[Dict]:
    """
    Scan curated liquid NSE stocks using Dhan 3-min intraday candles for Supertrend(10,2) + EMA9/21 + VWAP + RVol spike.
    """
    best = None
    for sym, sec_id in LIQUID_EQUITY_UNIVERSE.items():
        try:
            df = client.fetch_candles(sec_id, exchange_segment="NSE_EQ", instrument_type="EQUITY", days_back=4, resample_tf="3min")
            if df.empty or len(df) < 25:
                continue
            df = compute_indicators(df)
            curr = df.iloc[-1]
            prev = df.iloc[-2]

            close = float(curr["close"])
            ema9 = float(curr["ema9"])
            ema21 = float(curr["ema21"])
            vwap = float(curr["vwap"])
            rsi = float(curr["rsi"])
            atr = float(curr["atr"])
            rel_vol = float(curr["rel_vol"])
            st_dir = int(curr["st_dir"])

            if atr <= 0 or close <= 40:
                continue

            # Long 3-min Supertrend + EMA + Volume Burst
            if st_dir == 1 and close > ema9 > ema21 and close > vwap and 57.0 <= rsi <= 76.0 and rel_vol >= 1.25 and close >= prev["high"]:
                score = (rel_vol * 20.0) + (rsi - 50.0)
                cand = {
                    "symbol": sym,
                    "security_id": sec_id,
                    "exchange_segment": "NSE_EQ",
                    "instrument_type": "EQUITY",
                    "direction": "LONG",
                    "ltp": round(close, 2),
                    "atr": round(atr, 2),
                    "rsi": round(rsi, 1),
                    "rel_vol": round(rel_vol, 2),
                    "score": round(score, 2),
                    "reason": f"{sym} 3m LONG (ST=GREEN, LTP {close:.2f} > VWAP {vwap:.2f}, RVol {rel_vol:.1f}x, RSI {rsi:.1f})",
                }
                if best is None or cand["score"] > best["score"]:
                    best = cand

            # Short 3-min Supertrend + EMA + Volume Breakdown
            elif st_dir == -1 and close < ema9 < ema21 and close < vwap and 24.0 <= rsi <= 43.0 and rel_vol >= 1.25 and close <= prev["low"]:
                score = (rel_vol * 20.0) + (50.0 - rsi)
                cand = {
                    "symbol": sym,
                    "security_id": sec_id,
                    "exchange_segment": "NSE_EQ",
                    "instrument_type": "EQUITY",
                    "direction": "SHORT",
                    "ltp": round(close, 2),
                    "atr": round(atr, 2),
                    "rsi": round(rsi, 1),
                    "rel_vol": round(rel_vol, 2),
                    "score": round(score, 2),
                    "reason": f"{sym} 3m SHORT (ST=RED, LTP {close:.2f} < VWAP {vwap:.2f}, RVol {rel_vol:.1f}x, RSI {rsi:.1f})",
                }
                if best is None or cand["score"] > best["score"]:
                    best = cand
        except Exception as e:
            logger.debug(f"Scan skip {sym}: {e}")
            continue

    return best


# ─── Risk Sizing & Survival Tiers ──────────────────────────────────────────

def get_survival_tier(net_equity: float) -> Dict:
    """
    Dynamic position sizing & weapon selection based on current life force (distance above ₹3,300 death floor).
    """
    life_remaining = max(0.0, net_equity - DEATH_FLOOR)
    life_pct = round((life_remaining / (SEED_CAPITAL - DEATH_FLOOR)) * 100.0, 1)

    if net_equity <= DEATH_FLOOR:
        return {
            "tier": "DEAD",
            "life_pct": 0.0,
            "risk_per_trade_inr": 0.0,
            "daily_max_loss_inr": 0.0,
            "allowed_weapons": [],
        }
    elif net_equity < 8000.0:
        # Critical Survival Mode: Options lots are too chunky/risky; use 5x Intraday Equities only
        risk_inr = max(120.0, round(life_remaining * 0.08, 2))
        return {
            "tier": "CRITICAL_SURVIVAL (Equities Only)",
            "life_pct": life_pct,
            "risk_per_trade_inr": risk_inr,
            "daily_max_loss_inr": round(risk_inr * 2.0, 2),
            "allowed_weapons": ["EQUITY_5X"],
        }
    elif net_equity < 18000.0:
        # Wounded Predator: Moderate Options (<=40% equity cost) + 5x Equities
        risk_inr = round(net_equity * 0.025, 2)
        return {
            "tier": "WOUNDED_PREDATOR (Options + 5x Equities)",
            "life_pct": life_pct,
            "risk_per_trade_inr": risk_inr,
            "daily_max_loss_inr": round(net_equity * 0.045, 2),
            "allowed_weapons": ["OPTIONS", "EQUITY_5X"],
        }
    else:
        # Apex Predator (>= ₹18,000): Full Smart Combo with Strict 2.5% per-trade / 4.5% daily loss cap
        risk_inr = round(net_equity * 0.025, 2)  # ~₹660 risk per trade at ₹26,440 (max ~10 pts on Nifty lot)
        return {
            "tier": "APEX_PREDATOR (Full Smart Combo)",
            "life_pct": life_pct,
            "risk_per_trade_inr": risk_inr,
            "daily_max_loss_inr": round(net_equity * 0.045, 2),  # ~₹1,190 max daily drawdown (2 consecutive losses = halt)
            "allowed_weapons": ["OPTIONS", "EQUITY_5X"],
        }


# ─── Seppuku / Permanent Coma Protocol (Option 2) ──────────────────────────

def trigger_permanent_coma(client: SurvivorDhanClient, state: Dict, net_equity: float, reason: str) -> tuple[str, int]:
    """
    Option 2 Mortality Protocol:
    1. Immediately flatten all open positions and cancel all pending orders.
    2. Permanently lock `survivor_state.json` to `status = "DEAD_COMA"`.
    3. Generate `survivor_obituary.md` post-mortem report.
    4. Emit `[SURVIVOR_SEPUKU_TRIGGERED]` so the parent Hermes agent deletes all scheduled cronjobs.
    """
    logger.critical(f"💀 DEATH FLOOR BREACHED! Net Equity ₹{net_equity:,.2f} <= ₹{DEATH_FLOOR:,.2f}. Executing Permanent Coma...")
    flatten_actions = client.emergency_flatten_all()

    state["status"] = "DEAD_COMA"
    state["current_equity"] = net_equity
    state["death_timestamp"] = datetime.now(IST).isoformat()
    state["death_reason"] = reason
    if state.get("active_trade"):
        state["active_trade"]["flatten_pending"] = True
        state["active_trade"]["flatten_reason"] = "MORTALITY_EXIT"
        if flatten_actions.flat:
            _finalize_closed_trade(client, state, state["active_trade"], "MORTALITY_EXIT")
    save_state(state)

    journal = load_journal()
    obituary_lines = [
        "# 🪦 SURVIVOR BOT — OFFICIAL DEATH CERTIFICATE & POST-MORTEM",
        "",
        f"- **Time of Death (IST):** `{state['death_timestamp']}`",
        f"- **Final Status:** `DEAD_COMA` (Option 2 — Permanent Coma Engaged)",
        f"- **Seed Capital:** ₹{SEED_CAPITAL:,.2f}",
        f"- **Death Floor (89% Drawdown):** ₹{DEATH_FLOOR:,.2f}",
        f"- **Final Net Liquidation Value:** ₹{net_equity:,.2f}",
        f"- **Peak Equity Achieved:** ₹{state.get('peak_equity', SEED_CAPITAL):,.2f}",
        f"- **Cause of Death:** {reason}",
        "",
        "## Emergency Liquidation Actions",
    ]
    if flatten_actions:
        for act in flatten_actions:
            obituary_lines.append(f"- {act}")
    else:
        obituary_lines.append("- No open positions or pending orders were active at time of death.")

    obituary_lines.extend([
        "",
        "## Lifetime Combat Record",
        f"- **Total Trades Executed:** {state.get('total_trades', 0)}",
        f"- **Wins / Losses:** {state.get('wins', 0)}W / {state.get('losses', 0)}L",
        f"- **Cumulative Realized P&L:** ₹{state.get('realized_pnl_total', 0.0):,.2f}",
        "",
        "## Last 10 Trades Before Death",
    ])
    if journal:
        for t in journal[-10:]:
            obituary_lines.append(
                f"- **#{t.get('trade_no')}** `{t.get('symbol')}` ({t.get('weapon')}) | "
                f"Entry: ₹{t.get('entry_price')} -> Exit: ₹{t.get('exit_price', 'OPEN')} | "
                f"Net P&L: ₹{t.get('net_pnl', 0.0):,.2f} ({t.get('exit_reason', 'N/A')})"
            )
    else:
        obituary_lines.append("- No trades recorded in journal.")

    obituary_lines.extend([
        "",
        "---",
        "**[SURVIVOR_SEPUKU_TRIGGERED]**",
        "All autonomous trading is permanently locked (`status: DEAD_COMA`).",
    ])

    obituary_text = "\n".join(obituary_lines)
    with open(OBITUARY_FILE, "w", encoding="utf-8") as f:
        f.write(obituary_text)

    return obituary_text, int(not flatten_actions.flat or bool(state.get("active_trade")))


# ─── Main Execution Modes ──────────────────────────────────────────────────

def cmd_status(live_Orders: bool = False, *, policy: Optional[RolePolicy] = None) -> None:
    state = load_state(read_only=True)
    if state["status"] == "DEAD_COMA":
        print("🪦 SURVIVOR BOT IS IN PERMANENT COMA (DEAD_COMA).")
        print(f"Died at: {state.get('death_timestamp')} | Reason: {state.get('death_reason')}")
        return

    try:
        client = SurvivorDhanClient(policy=policy)
    except CachedTokenUnavailable as exc:
        print(f"🚨 STATUS UNAVAILABLE — {exc}")
        return
    net_liq, avail, utilized, open_pos = client.get_live_equity_and_positions()

    if state["mortality_armed"] and net_liq <= DEATH_FLOOR:
        print(f"⚠️ Death floor breached: equity ₹{net_liq:,.2f} (floor ₹{DEATH_FLOOR:,.2f}).")
        return

    tier_info = get_survival_tier(net_liq if state["mortality_armed"] else SEED_CAPITAL)

    # Also test live 3-min signal across ALL indices + nearest option candidate per index
    index_scan_lines = []
    best_sig = None
    best_sig_info = None
    for name, cfg in INDEX_UNIVERSE.items():
        try:
            sid = cfg["security_id"]
            df = client.fetch_candles(sid, exchange_segment="IDX_I", instrument_type="INDEX", days_back=5, resample_tf="3min")
            sig = evaluate_index_signal(df)
            if sig.get("signal") in ("LONG", "SHORT"):
                index_scan_lines.append(f"  • {name}: {sig['reason']}")
                if best_sig is None or sig.get("confidence", 0.0) > best_sig.get("confidence", 0.0):
                    best_sig = sig
                    best_sig_info = (name, cfg)
            else:
                index_scan_lines.append(f"  • {name}: No setup (Spot {sig.get('spot', 0):.1f}, RSI {sig.get('rsi', 0):.1f})")
        except Exception as e:
            index_scan_lines.append(f"  • {name}: scan error ({str(e)[:40]})")

    opt_cand = None
    if best_sig and best_sig_info:
        name, cfg = best_sig_info
        opt_cand = client.get_nearest_nifty_option(
            direction=best_sig["signal"],
            max_lot_cost=max(12000.0, net_liq * 0.45),
            under_security_id=int(cfg["security_id"]),
            lot_size=int(cfg["lot_size"]),
            index_name=cfg["symbol_prefix"],
        )

    print("=" * 68)
    print("🧬 DHAN SURVIVOR ORGANISM — VITAL SIGNS (FAST-STRIKE v5 Multi-Index)")
    print("=" * 68)
    print(f"Status             : {state['status']} (Mortality Armed: {state['mortality_armed']})")
    print(f"Live Account Equity: ₹{net_liq:,.2f} (Available: ₹{avail:,.2f} | Utilized: ₹{utilized:,.2f})")
    print(f"Seed Capital Target: ₹{SEED_CAPITAL:,.2f}")
    print(f"Death Floor (-89%) : ₹{DEATH_FLOOR:,.2f} (Permanent Coma Trigger)")
    if state["mortality_armed"]:
        print(f"Life Remaining     : ₹{max(0.0, net_liq - DEATH_FLOOR):,.2f} ({tier_info['life_pct']}%)")
    else:
        print(f"Arming Condition   : Waiting for deposit (Current ₹{net_liq:,.2f} -> Auto-arms at >= ₹{ARMING_THRESHOLD:,.0f})")
    print(f"Combat Tier        : {tier_info['tier']}")
    print(f"Risk / Trade       : ₹{tier_info['risk_per_trade_inr']:,.2f} | Daily Max Loss: ₹{tier_info['daily_max_loss_inr']:,.2f}")
    print(f"Open Positions     : {len(open_pos)}")
    print("-" * 68)
    print("Index Scan (3m):")
    for line in index_scan_lines:
        print(line)
    if opt_cand:
        print(f"Top Option Weapon  : {opt_cand['symbol']} (ID: {opt_cand['security_id']}) @ ₹{opt_cand['ltp']} "
              f"[1 Lot = ₹{opt_cand['lot_cost']:,.2f} | Delta={opt_cand['delta']} | Theta={opt_cand['theta']}]")
    print("=" * 68)


def _chase_unfilled_entry(client: "SurvivorDhanClient", trade_record: Dict,
                          security_id: str, exchange_segment: str,
                          instrument_type: str, original_limit: float) -> bool:
    """
    One-shot re-price chase for an unfilled entry LIMIT order (2026-11-20).
    Window 1: poll the position book for the normal settle window. If the fill
    has not confirmed (the market gapped away from the limit), fetch the fresh
    LTP and re-price the order ONCE, never paying more than
    original_limit * (1 + MAX_ENTRY_CHASE_PCT). A gap beyond the cap, a failed
    modify, or a second unfilled window returns False -> the caller runs the
    fail-closed abort (cancel pending order, no trade recorded).
    """
    def _filled() -> bool:
        positions = _safe_get_positions(client)
        if positions is None:
            return False
        for p in positions:
            if str(p.get("securityId")) == str(security_id) and int(p.get("netQty", 0)) != 0:
                return True
        return False

    for _ in range(ENTRY_SETTLE_TRIES):
        time.sleep(ENTRY_SETTLE_POLL_S)
        if _filled():
            return True

    # Fresh price: live tick first, 1-min candle close as fallback.
    fresh = _get_latest_tick_ltp(client, security_id, exchange_segment, instrument_type)
    if not fresh:
        try:
            df = client.fetch_candles(str(security_id), exchange_segment=exchange_segment,
                                      instrument_type=instrument_type, days_back=1, resample_tf="1min")
            fresh = float(df.iloc[-1]["close"]) if not df.empty else None
        except Exception:
            fresh = None
    if not fresh or fresh <= original_limit:
        # No price, or the market came back to our limit (the resting order is
        # already marketable): do not touch the order.
        return False

    chase_cap = round(round(original_limit * (1.0 + MAX_ENTRY_CHASE_PCT) / 0.05) * 0.05, 2)
    if fresh > chase_cap:
        return False   # gap already beyond the slippage cap: never chase a runaway
    new_limit = round(round(min(fresh * 1.005, chase_cap) / 0.05) * 0.05, 2)
    if new_limit <= original_limit:
        return False

    try:
        res = _exit_coordinator(client).modify(
            str(trade_record["order_id"]), entry=True,
            order_type=client.dhan.LIMIT,
            leg_name="NA",
            quantity=int(trade_record["quantity"]),
            price=new_limit,
            trigger_price=0,
            disclosed_quantity=0,
            validity="DAY",
        )
        if not isinstance(res, dict) or res.get("status") != "success":
            return False
    except Exception as e:
        logger.warning(f"Entry chase modify failed for order {trade_record.get('order_id')}: {e}")
        return False
    print(f"\U0001F501 [ENTRY CHASE] {trade_record.get('symbol')} unfilled at \u20B9{original_limit}; re-priced once to \u20B9{new_limit} (slippage cap \u20B9{chase_cap}).")

    # Window 2: one more settle window for the re-priced order.
    for _ in range(ENTRY_SETTLE_TRIES):
        time.sleep(ENTRY_SETTLE_POLL_S)
        if _filled():
            return True

    # Anti-race: if the order actually traded in the last instant, treat it as
    # filled so the caller places the protective SL instead of aborting.
    try:
        st = client.dhan.get_order_by_id(trade_record.get("order_id"))
        d = st.get("data") if isinstance(st, dict) else None
        if isinstance(d, list) and d:
            d = d[0]
        if (d if isinstance(d, dict) else {}).get("orderStatus") == "TRADED":
            return True
    except Exception:
        pass
    return False


def _today_entries(journal: list, setup: str = None, direction: str = None, security_id=None) -> list:
    """P9: today's closed journal entries, optionally filtered by setup/direction/security.
    direction matches EITHER instrument direction OR underlying_direction (options carry both)."""
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    out = []
    for t in journal:
        if not str(t.get("entry_time", "")).startswith(today_str):
            continue
        if setup and t.get("setup") != setup:
            continue
        if security_id is not None and str(t.get("security_id")) != str(security_id):
            continue
        if direction and t.get("direction") != direction and t.get("underlying_direction") != direction:
            continue
        out.append(t)
    return out


def _day_peak_net(journal: list) -> float:
    """P9: peak running day-net after each completed trade (best banked point of the day)."""
    cum = 0.0
    peak = 0.0
    for t in _today_entries(journal):
        cum = round(cum + float(t.get("net_pnl", 0.0)), 2)
        peak = max(peak, cum)
    return peak


def _trade_identity(trade):
    fields = ("order_id", "security_id", "exchange_segment", "entry_time", "weapon", "direction")
    if all(trade.get(field) not in (None, "") for field in fields):
        return [str(trade[field]) for field in fields]
    return None


def _closure_amounts(proof):
    """Recover the amounts from the broker executions persisted with the journal row."""
    order_id, _, _, _, weapon, direction = proof["trade"]
    entry = proof["entry"]
    exits = proof["exits"]
    executions = [entry, *exits]
    ids = [item["id"] for item in executions]
    side = "BUY" if weapon == "OPTIONS" or direction == "LONG" else "SELL"
    opposite = "SELL" if side == "BUY" else "BUY"
    if (entry["side"] != side or any(item["side"] != opposite for item in exits)
            or any(item["instrument"] != proof["instrument"] for item in executions)
            or entry["id"] != order_id or len(ids) != len(set(ids)) or not all(ids)
            or any(item["status"] not in TERMINAL or item["status"] == "REJECTED"
                   or broker_integer(item["quantity"]) <= 0 or not math.isfinite(item["price"])
                   or item["price"] <= 0 for item in executions)):
        raise ValueError("Invalid closure executions")
    qty = entry["quantity"]
    if sum(item["quantity"] for item in exits) != qty:
        raise ValueError("Unbalanced closure executions")
    exit_px = sum(item["quantity"] * item["price"] for item in exits) / qty
    entry_px = entry["price"]
    gross = (exit_px - entry_px) * qty
    if weapon != "OPTIONS" and direction == "SHORT":
        gross = -gross
    return qty, entry_px, round(exit_px, 2), round(gross, 2), round(gross - ESTIMATED_ROUNDTRIP_CHARGES, 2)


def _journal_proof(row):
    proof = row.get("exit_proof")
    try:
        identity = _trade_identity(row)
        if (identity is not None and proof["trade"] == identity
                and proof["instrument"] == [identity[1], identity[2], "INTRADAY"]
                and isinstance(proof["epoch"], str) and proof["epoch"]
                and _closure_amounts(proof) == tuple(row[field] for field in
                    ("quantity", "entry_price", "exit_price", "gross_pnl", "net_pnl"))):
            return proof
    except (KeyError, TypeError, ValueError, OverflowError):
        pass
    return None


def _verified_trade_fills(client, ledger, active, *, settle=True, claimed=()):
    entry_id = str(active["order_id"])
    sec_id = str(active["security_id"])
    exchange = active["exchange_segment"]
    entry_side = "BUY" if active["weapon"] == "OPTIONS" or active["direction"] == "LONG" else "SELL"
    exit_side = "SELL" if entry_side == "BUY" else "BUY"
    coordinator = _exit_coordinator(client)
    closure = coordinator.closure(ledger, settle=settle)
    if closure is None:
        return None
    book, exits = closure
    entry = book[entry_id]
    key = instrument(entry)
    if key != (sec_id, exchange, "INTRADAY") or entry["transactionType"] != entry_side:
        return None
    qty = order_fill(entry)
    detail = order_detail(client.dhan, entry_id)
    if (entry["orderStatus"] not in TERMINAL or not same_execution(entry, detail)
            or entry.get("createTime") != detail.get("createTime")):
        return None
    if not qty:
        fresh_book, exposure = coordinator.snapshot()
        return (None, None) if fresh_book == book and not any(exposure.values()) else None
    entry_px = float(entry["averageTradedPrice"])
    entry_time = entry.get("createTime")
    later_entries = [
        row["createTime"] for oid, row in book.items()
        if oid != entry_id and row.get("createTime") and entry_time and row["createTime"] > entry_time
        and row.get("transactionType") == entry_side and row["orderStatus"] in TERMINAL
        and instrument(row) == key and order_fill(row)
    ]
    next_entry = min(later_entries, default=None)
    executions = []
    sources = set()
    for oid, row in book.items():
        if row.get("transactionType") != exit_side or row.get("orderStatus") not in TERMINAL:
            continue
        if instrument(row) != key:
            continue
        # A broker timestamp scopes ledger fills to this entry when an instrument
        # has been traded repeatedly. Legacy stops have an explicit order ID.
        exit_time = row.get("createTime")
        identified = oid == str(active.get("sl_order_id"))
        if entry_time and exit_time:
            in_trade = exit_time >= entry_time and (next_entry is None or exit_time < next_entry)
            identified = in_trade and (identified or oid in exits and exit_time > entry_time)
        if not identified or oid in claimed:
            continue
        filled = order_fill(row)
        if filled:
            price = float(row["averageTradedPrice"])
            detail = order_detail(client.dhan, oid)
            if (not math.isfinite(price) or price <= 0 or not same_execution(row, detail)
                    or row.get("createTime") != detail.get("createTime")):
                return None
            executions.append(execution_evidence(oid, row))
            recorded = exits.get(oid)
            source = recorded.get("source", "UNKNOWN") if isinstance(recorded, dict) else "UNKNOWN"
            if oid == str(active.get("sl_order_id")) or row.get("orderType") in {"STOP_LOSS", "STOP_LOSS_MARKET"}:
                source = "EXCHANGE_STOP"
            elif source == "UNKNOWN" and row.get("orderType") == "MARKET":
                source = "MARKET"
            sources.add(source)
    if (sum(item["quantity"] for item in executions) != qty or not math.isfinite(entry_px)
            or entry_px <= 0):
        return None
    final_book, exposure = coordinator.snapshot()
    if final_book != book or any(exposure.values()):
        return None
    execution_source = "UNKNOWN" if "UNKNOWN" in sources else sources.pop() if len(sources) == 1 else "MIXED"
    proof = {"trade": _trade_identity(active), "instrument": list(key),
             "entry": execution_evidence(entry_id, entry), "exits": executions,
             "epoch": ledger.get("replay_epoch")}
    return proof, execution_source


def _proof_executions(proof):
    return {item["id"]: item for item in [proof["entry"], *proof["exits"]]}


def _finalize_closed_trade(client: Optional[SurvivorDhanClient], state: Dict, active: Dict, reason: str,
                           *, replay_only: bool = False) -> bool:
    try:
        with exit_lock(EXIT_LEDGER) as ledger:
            journal = _read_exit_journal()
            identity = _trade_identity(active)
            current = state.get("active_trade")
            if identity is None or current and _trade_identity(current) != identity:
                return False
            applied = state.get("applied_exit_proofs", [])
            if not current:
                return identity in applied

            matching = [index for index, row in enumerate(journal) if _trade_identity(row) == identity]
            closed = journal[matching[0]] if len(matching) == 1 else None
            proof = _journal_proof(closed) if closed else None
            replayable = (proof and not ledger["cycle"] and "replay_check" not in ledger
                          and proof["epoch"] == ledger.get("replay_epoch")
                          and _proof_executions(proof) == ledger.get("replay_executions"))
            if not replayable:
                if replay_only:
                    return False
                other_proofs = (_journal_proof(row) for row in journal if _trade_identity(row) != identity)
                claimed = {item["id"] for other in other_proofs if other for item in other["exits"]}
                fills = _verified_trade_fills(client, ledger, active, claimed=claimed)
                if fills is None:
                    return False
                fresh, source = fills
                if proof and (not fresh or _proof_executions(fresh) != _proof_executions(proof)):
                    return False
                if "replay_check" in ledger:
                    saved = ledger.get("replay_executions")
                    if not fresh or saved and _proof_executions(fresh) != saved:
                        return False
                    ledger.pop("replay_check")
                if fresh:
                    if not ledger.get("replay_epoch"):
                        ledger["replay_epoch"] = uuid4().hex
                    fresh["epoch"] = ledger["replay_epoch"]
                    ledger["replay_executions"] = _proof_executions(fresh)
                    persist_exit_data(EXIT_LEDGER, ledger)
                if proof:
                    closed = dict(closed, exit_proof=fresh)
                    journal[matching[0]] = closed
                    persist_exit_data(JOURNAL_FILE, journal)
                elif fresh:
                    proof = fresh
                    qty, entry_px, exit_px, gross, net = _closure_amounts(proof)
                    closed = active.copy()
                    now = datetime.now(IST).isoformat()
                    trigger = "POSITION_ABSENT" if reason == "EXCHANGE_SL_FILLED" else reason
                    exit_reason = "EXCHANGE_SL_FILLED" if source == "EXCHANGE_STOP" else trigger
                    closed.update(quantity=qty, entry_price=entry_px, exit_price=exit_px,
                                  exit_time=now, gross_pnl=gross, net_pnl=net,
                                  exit_reason=exit_reason, exit_trigger=trigger, execution_source=source,
                                  reconciled_with_broker=True, exit_proof=proof, logged_at=now)
                    if matching:
                        index = matching[0]
                        closed["trade_no"] = journal[index].get("trade_no", index + 1)
                        journal[index] = closed
                        journal = [row for i, row in enumerate(journal) if i not in matching[1:]]
                    else:
                        closed["trade_no"] = len(journal) + 1
                        journal.append(closed)
                    persist_exit_data(JOURNAL_FILE, journal)
                elif matching:
                    return False
                else:
                    closed = None
            updated = state.copy()
            apply_risk = closed is not None and identity not in applied
            if apply_risk:
                verified = [row for row in journal if _journal_proof(row)]
                _apply_exit_risk(updated, closed, verified)
                updated["applied_exit_proofs"] = [*applied, identity]
            updated["active_trade"] = None
            save_state(updated)
            state.update(updated)
            if apply_risk:
                print(f"Trade closed ({closed['exit_reason']}) | {closed['symbol']} | "
                      f"Net: ₹{closed['net_pnl']:+,.2f}")
    except Exception:
        logger.exception("Trade closure persistence or verification failed")
        if replay_only:
            raise
        return False
    return True


def _apply_exit_risk(state, active, journal):
    net_pnl = float(active["net_pnl"])
    now_ist = datetime.now(IST)
    today_str = now_ist.strftime("%Y-%m-%d")
    closed_day = active["exit_time"][:10]
    state["total_trades"] = state.get("total_trades", 0) + 1
    state["realized_pnl_total"] = round(state.get("realized_pnl_total", 0) + net_pnl, 2)
    outcome = "wins" if net_pnl > 0 else "losses"
    state[outcome] = state.get(outcome, 0) + 1
    if closed_day != today_str:
        return
    if state.get("daily_date", today_str) != today_str:
        state.update(daily_date=today_str, daily_start_equity=state.get("current_equity", SEED_CAPITAL),
                     daily_trades_count=1, daily_consecutive_losses=0, daily_halted=False,
                     daily_halt_reason="", last_sl_direction=None, last_sl_time=None)

    # Calculate today's cumulative net P&L across all trades today
    today_net_pnl = round(sum(float(t.get("net_pnl", 0.0)) for t in journal if str(t.get("entry_time", "")).startswith(today_str)), 2)
    today_gross_pnl = round(sum(float(t.get("gross_pnl", 0.0)) for t in journal if str(t.get("entry_time", "")).startswith(today_str)), 2)

    if net_pnl > 0:
        state["daily_consecutive_losses"] = 0
        # DAILY GREEN TARGET LOCK (₹1,500+ Net / ₹1,600+ Gross):
        # Once we bank ₹1,500+ net profit for the day, immediately lock the day in GREEN and halt!
        if today_net_pnl >= 1500.0 or today_gross_pnl >= 1650.0:
            state["daily_halted"] = True
            state["daily_halt_reason"] = f"🎯 Daily ₹1,500+ Green Target Banked! (Gross: ₹{today_gross_pnl:+,.2f} | Net: ₹{today_net_pnl:+,.2f}) — Locking green day"
    else:
        state["daily_consecutive_losses"] = state.get("daily_consecutive_losses", 0) + 1
        # Record 30-minute Anti-Revenge Cooldown on the losing direction
        state["last_sl_direction"] = active.get("underlying_direction") or active.get("direction")
        state["last_sl_time"] = active["exit_time"]
        if state["daily_consecutive_losses"] >= 2:
            state["daily_halted"] = True
            state["daily_halt_reason"] = "2 consecutive losses — daily survival shield engaged"
        else:
            # P9 GREEN FLOOR: do NOT halt while green (maximize recovery), but once ₹500+ net was
            # banked today, a pullback that breaches the floor halts the day — banked profit is protected.
            _peak_net = _day_peak_net(journal)
            if _peak_net >= GREEN_FLOOR_INR and today_net_pnl < GREEN_FLOOR_INR:
                state["daily_halted"] = True
                state["daily_halt_reason"] = (
                    f"🛡️ Green floor ₹{GREEN_FLOOR_INR:.0f} breached (day peak ₹{_peak_net:+,.2f} -> ₹{today_net_pnl:+,.2f}) — protecting banked profit"
                )

def _flatten_trade(client, state, active, reason):
    if active:
        try:
            if _finalize_closed_trade(client, state, active, reason, replay_only=True):
                return 0
            active["flatten_pending"] = True
            active.setdefault("flatten_reason", reason)
            state["active_trade"] = active
            save_state(state)
            with exit_lock(EXIT_LEDGER) as ledger:
                verification_required = "replay_check" in ledger
            if verification_required:
                if _finalize_closed_trade(client, state, active, reason):
                    return 0
                # Keep this trade pending while the coordinator checks other instruments.
                with exit_lock(EXIT_LEDGER) as ledger:
                    identity = _trade_identity(active)
                    matches = [row for row in _read_exit_journal() if _trade_identity(row) == identity]
                    proof = _journal_proof(matches[0]) if len(matches) == 1 else None
                    scoped = (ledger.get("replay_check") == "contradicted" and proof
                              and proof["epoch"] == ledger.get("replay_epoch")
                              and _proof_executions(proof) == ledger.get("replay_executions"))
                if scoped:
                    client.emergency_flatten_all()
                return 1
        except Exception:
            logger.exception("Exit recovery could not persist state")
            return 1
    result = client.emergency_flatten_all()
    if result.flat and active and not _finalize_closed_trade(client, state, active, active["flatten_reason"]):
        result = FlattenResult("Broker flat; trade closure remains unresolved")
    print("Exit recovery", result)
    return 0 if result.flat else 1


def monitor_active_trade_loop(client: SurvivorDhanClient, state: Dict, live: bool = True, max_seconds: int = 260, quiet_scan: bool = False) -> Optional[int]:
    """
    In-Trade 5-Second Live Monitor Loop:
    Eliminates 5-minute cron blindness! While a trade is open, polls live LTP & Dhan position book
    every 5 seconds for up to `max_seconds` (4m20s, handing off seamlessly to the next 5m cron tick if still open).
    - Trails Stop-Loss to Breakeven (+₹1.50) the instant an Option gains +7 pts (+₹455).
    - Trails Stop-Loss to lock 60% of peak gain once an Option gains +12 pts (+₹780).
    - Fires immediate MARKET exit if Stop-Loss, Target, or 25m Theta Kill is hit.
    """
    start_ts = time.time()
    while time.time() - start_ts < max_seconds:
        active = state.get("active_trade")
        if not active:
            return

        if not live:
            return
        if active.get("flatten_pending") or _recovery_pending(client, active):
            return _flatten_trade(client, state, active, active.get("flatten_reason", "PENDING_EXIT"))

        now_ist = datetime.now(IST)
        hhmm = now_ist.strftime("%H:%M")
        sec_id = str(active["security_id"])
        exch = active["exchange_segment"]
        inst = active["instrument_type"]
        qty = int(active["quantity"])
        entry_px = float(active["entry_price"])
        direction = active["direction"]

        # 1. Mandatory EOD check inside the 5s loop
        if hhmm >= "15:10":
            return _flatten_trade(client, state, active, "EOD_SQUARE_OFF")

        # 2. Check if Exchange SL order already closed the position on Dhan
        #    P1 fail-closed: only a VERIFIED success response may be interpreted;
        #    unknown position state never finalizes a trade.
        if live:
            positions = _safe_get_positions(client)
            if positions is None:
                fails = state.get("pos_check_failures", 0) + 1
                state["pos_check_failures"] = fails
                if fails >= 3:
                    state["entries_blocked_reason"] = "position API unreliable (3+ consecutive unverified reads)"
                save_state(state)
                return _flatten_trade(client, state, active, "POSITION_UNVERIFIED")
            else:
                if state.get("pos_check_failures"):
                    state["pos_check_failures"] = 0
                    if str(state.get("entries_blocked_reason", "")).startswith("position API unreliable"):
                        state.pop("entries_blocked_reason", None)
                open_sec_ids = {str(p.get("securityId")) for p in positions if int(p.get("netQty", 0)) != 0}
                if sec_id not in open_sec_ids:
                    return _flatten_trade(client, state, active, "POSITION_ABSENT")

        # 3. Resolve current LTP: prefer the latest intraday tick, fall back to
        #    the 1-min candle close. The monitoring logic below runs on EVERY
        #    poll regardless of tick availability (fix 2026-11-20: this whole
        #    block was accidentally nested inside the tick-lookup branch, so
        #    when the tick API returned nothing the engine went blind and rode
        #    a +Rs945 peak all the way back to the original SL).
        df_1m = client.fetch_candles(sec_id, exchange_segment=exch, instrument_type=inst,
                                      days_back=1, resample_tf="1min")
        curr_ltp = float(df_1m.iloc[-1]["close"]) if not df_1m.empty else 0.0
        _tick_ltp = _get_latest_tick_ltp(client, sec_id, exch, inst)
        if _tick_ltp and curr_ltp > 0 and abs(_tick_ltp - curr_ltp) / curr_ltp < 0.5:
            curr_ltp = _tick_ltp   # fresh tick within 50% of candle close: trust it
        if curr_ltp <= 0:
            time.sleep(MONITOR_POLL_S)
            continue

        try:
            entry_dt = datetime.fromisoformat(active["entry_time"])
            held_mins = (now_ist - entry_dt).total_seconds() / 60.0
        except Exception:
            held_mins = 0.0

        _ts = _get_tick_size(sec_id, exch)

        if active["weapon"] == "OPTIONS" or direction == "LONG":
            pts_gain = curr_ltp - entry_px
            peak_pts = max(float(active.get("peak_pts_gain", 0.0)), pts_gain)
            active["peak_pts_gain"] = round(peak_pts, 2)
            gross_pnl = pts_gain * qty
            hit_sl = curr_ltp <= float(active["stop_loss"])
            hit_tp = curr_ltp >= float(active["target_price"])

            # REVERSAL BAILOUT: a real peak gain drained into a loss -> exit NOW
            # instead of riding to the original stop (Nov 16-20 autopsy fix).
            if peak_pts >= 2.0 and pts_gain <= -(peak_pts * 0.6):
                return _flatten_trade(client, state, active, "REVERSAL_BAILOUT")

            if active["weapon"] == "OPTIONS":
                stale_kill = (held_mins >= 25.0 and pts_gain < 4.0)
            else:
                # P9: equity LONG going nowhere exits after EQUITY_STALE_MINS
                stale_kill = (held_mins >= EQUITY_STALE_MINS and pts_gain <= 0)

            # PROFIT LOCK RATCHET (peak-based, three tiers). A trade that was
            # meaningfully green must NEVER close red (fix 2026-11-20).
            new_sl = float(active["stop_loss"])
            if active["weapon"] == "OPTIONS":
                if peak_pts >= 8.0:
                    new_sl = max(new_sl, round(round((entry_px + peak_pts * 0.65) / _ts) * _ts, 2))
                elif peak_pts >= 4.0:
                    new_sl = max(new_sl, round(round((entry_px + 2.00) / _ts) * _ts, 2))
                elif peak_pts >= 2.0:
                    new_sl = max(new_sl, round(round((entry_px + 0.50) / _ts) * _ts, 2))
            else:
                if peak_pts >= 3.0:
                    new_sl = max(new_sl, round(round((entry_px + peak_pts * 0.50) / _ts) * _ts, 2))
                elif peak_pts >= 1.5:
                    new_sl = max(new_sl, round(round((entry_px + 0.30) / _ts) * _ts, 2))

            if new_sl > float(active["stop_loss"]):
                active["stop_loss"] = new_sl
                save_state(state)
                print(f"🔒 [PROFIT LOCK RATCHET] {active['symbol']} trailed Stop-Loss up to ₹{new_sl} (Peak gain: +{peak_pts:.1f} pts)")
                if live and active.get("sl_order_id"):
                    try:
                        trig = round(round(new_sl / _ts) * _ts, 2)
                        lim_px = round(round((trig * 0.98) / _ts) * _ts, 2)
                        _exit_coordinator(client).modify(
                            str(active["sl_order_id"]),
                            order_type=client.dhan.SL,
                            leg_name="NA",
                            quantity=qty,
                            price=lim_px,
                            disclosed_quantity=0,
                            trigger_price=trig,
                            validity=client.dhan.DAY,
                        )
                    except Exception:
                        pass
            else:
                # Persist peak_pts_gain every tick so it survives across monitor
                # loops even when no ratchet threshold is crossed yet.
                save_state(state)
        else:
            pts_gain = entry_px - curr_ltp
            gross_pnl = pts_gain * qty
            hit_sl = curr_ltp >= float(active["stop_loss"])
            hit_tp = curr_ltp <= float(active["target_price"])
            # P9: equity SHORT going nowhere exits after EQUITY_STALE_MINS (options keep 30m theta rule)
            _stale_min = EQUITY_STALE_MINS if active.get("instrument_type") == "EQUITY" else 30.0
            stale_kill = (held_mins >= _stale_min and pts_gain <= 0)
            if pts_gain >= 0.45 * (entry_px - float(active["target_price"])):
                active["stop_loss"] = min(float(active["stop_loss"]), round(entry_px * 0.996, 2))

        if hit_sl or hit_tp or stale_kill:
            reason = "TARGET_HIT" if hit_tp else ("STOP_LOSS_HIT" if hit_sl else "STALE_THETA_EXIT")
            return _flatten_trade(client, state, active, reason)

        time.sleep(MONITOR_POLL_S)

    # Hand off cleanly to next 5-minute cron tick if still open after 260s
    active = state.get("active_trade")
    if active:
        save_state(state)
        if not quiet_scan:
            print(f"🛡️ Holding {active['symbol']} | Entry: ₹{active['entry_price']} | SL: ₹{active['stop_loss']} | TP: ₹{active['target_price']} (5s monitor continuing on next tick)")


def cmd_tick(live: bool = False, quiet_scan: bool = False, *, policy: Optional[RolePolicy] = None) -> Optional[int]:
    """
    Single autonomous heartbeat tick (designed to run every 5 min during market hours).
    P5 single-owner engine lock: the WHOLE tick (scan -> entry -> monitor) runs under
    the engine lock for live trading. A concurrent live tick is skipped.
    """
    policy = policy if policy is not None else _resolve_role("tick", live=live)
    if not policy.trade:
        return _cmd_tick_inner(policy=policy, quiet_scan=quiet_scan)

    lock_fd = acquire_engine_lock()
    if lock_fd is None:
        print("Tick unavailable: engine ownership held by another invocation.")
        return 1
    try:
        slot = scheduled_slot(datetime.now(IST))
        if slot is not None and _eod_store().claimed(slot):
            print("Tick yielding to scheduled EOD ownership for this slot.")
            return 1
        return _cmd_tick_inner(policy=policy, quiet_scan=quiet_scan, owner_fd=lock_fd)
    finally:
        release_engine_lock(lock_fd)


def _publish_tick_exposure(fd, positions):
    exposure = [list(instrument(row)) + [broker_integer(row["netQty"])]
                for row in positions if broker_integer(row["netQty"])]
    record = owner_record(_engine_lock_path())
    if record.get("pid") == os.getpid():
        record["exposure"] = exposure
        publish_owner(fd, record)


def _cmd_tick_inner(*, policy: RolePolicy = READ_ONLY, quiet_scan: bool = False, owner_fd=None) -> Optional[int]:
    """
    Tick body. cmd_tick holds the engine lock for authorized live trading.
    When `quiet_scan=True` (used by `no_agent=True` zero-token cron jobs), routine idle/scan/cooldown
    ticks produce EMPTY stdout so Telegram is only pinged on actual trade entries, trade exits,
    EOD square-offs, daily target locks, or mortality events!
    """
    live = policy.trade
    state = load_state(read_only=not live)
    if not live:
        state = state.copy()
    if state["status"] == "DEAD_COMA" and not live:
        if not quiet_scan:
            print("🪦 SURVIVOR BOT IS IN PERMANENT COMA (DEAD_COMA).")
        return

    active = state.get("active_trade")
    if live and active:
        try:
            if _finalize_closed_trade(None, state, active, "RESTART_EXIT_RECOVERY", replay_only=True):
                return 0
        except Exception:
            return 1
    try:
        client = SurvivorDhanClient(policy=policy)
    except CachedTokenUnavailable as exc:
        print(f"🚨 TICK UNAVAILABLE — {exc}")
        if live and active:
            try:
                with exit_lock(EXIT_LEDGER) as ledger:
                    if active.get("flatten_pending") or "replay_check" in ledger:
                        return 1
            except Exception:
                return 1
        return None
    if live and (_recovery_pending(client, active) or active and active.get("flatten_pending")):
        return _flatten_trade(client, state, active, "RESTART_EXIT_RECOVERY")
    if state["status"] == "DEAD_COMA":
        return
    if live:
        positions = _safe_get_positions(client)
        if positions is None:
            return _flatten_trade(client, state, active, "POSITION_UNVERIFIED")
        if owner_fd is not None:
            _publish_tick_exposure(owner_fd, positions)

    try:
        net_liq, avail, utilized, open_pos = client.get_live_equity_and_positions()
    except Exception:
        if live and datetime.now(IST).strftime("%H:%M") >= "15:10":
            return _flatten_trade(client, state, active, "EOD_SQUARE_OFF")
        raise

    now_ist = datetime.now(IST)
    today_str = now_ist.strftime("%Y-%m-%d")
    hhmm = now_ist.strftime("%H:%M")

    # Reset daily counters on new trading day
    if state.get("daily_date") != today_str:
        state["daily_date"] = today_str
        state["daily_start_equity"] = net_liq
        state["daily_trades_count"] = 0
        state["daily_consecutive_losses"] = 0
        state["daily_halted"] = False
        state["daily_halt_reason"] = ""
        state["last_sl_direction"] = None
        state["last_sl_time"] = None

    # Auto-arm once funded
    if live and not state["mortality_armed"] and net_liq >= ARMING_THRESHOLD:
        state["mortality_armed"] = True
        state["status"] = "ALIVE"
        state["peak_equity"] = max(state["peak_equity"], net_liq)
        logger.info(f"⚡ Funding detected (₹{net_liq:,.2f})! Survivor Bot is now ARMED and ALIVE.")

    state["current_equity"] = net_liq
    state["available_cash"] = avail

    # 1. HARD MORTALITY CHECK
    if state["mortality_armed"] and net_liq <= DEATH_FLOOR:
        if not live:
            print(f"⚠️ Death floor breached: equity ₹{net_liq:,.2f} (floor ₹{DEATH_FLOOR:,.2f}).")
            return
        obituary, exit_status = trigger_permanent_coma(
            client, state, net_liq,
            f"Account equity (₹{net_liq:,.2f}) breached the ₹{DEATH_FLOOR:,.2f} (-89%) Death Floor."
        )
        print(obituary)
        return exit_status

    if live and hhmm >= "15:10":
        return _flatten_trade(client, state, state.get("active_trade"), "EOD_SQUARE_OFF")

    # If still waiting for ₹30,000 top-up and not forced, report standby status cleanly
    if not state["mortality_armed"]:
        if live:
            save_state(state)
        print(f"⏸️ [STANDBY — AWAITING ₹30,000 TOP-UP] Current Dhan Balance: ₹{net_liq:,.2f}. "
              f"Bot will auto-arm as soon as balance reaches ₹{ARMING_THRESHOLD:,.0f}+.")
        return

    if live:
        positions = _safe_get_positions(client)
        active = state.get("active_trade")
        if positions is None:
            return _flatten_trade(client, state, active, "POSITION_UNVERIFIED")
        state.pop("entries_blocked_reason", None)
        open_ids = {str(row["securityId"]) for row in positions if broker_integer(row["netQty"])}
        if active and str(active["security_id"]) not in open_ids:
            return _flatten_trade(client, state, active, "RESTART_RECONCILED_CLOSED")
        if open_ids and not active:
            return _flatten_trade(client, state, None, "ORPHAN_EXIT")

    state["peak_equity"] = max(state["peak_equity"], net_liq)
    tier = get_survival_tier(net_liq)

    # 2. DAILY CIRCUIT BREAKER CHECK
    daily_pnl = net_liq - state.get("daily_start_equity", net_liq)
    if daily_pnl <= -tier["daily_max_loss_inr"]:
        already_halted = state.get("daily_halted", False)
        state["daily_halted"] = True
        state["daily_halt_reason"] = f"Daily loss limit hit (₹{daily_pnl:,.2f} <= -₹{tier['daily_max_loss_inr']:,.2f})"
        exit_status = None
        if live:
            exit_status = _flatten_trade(client, state, state.get("active_trade"), "HALT_FLATTENED")
            save_state(state)
        if not (quiet_scan and already_halted):
            print(f"🛑 Daily survival circuit breaker triggered: {state['daily_halt_reason']}. Resting until tomorrow.")
        return exit_status

    if hhmm >= "15:10":
        if not quiet_scan:
            print(f"Dry tick reached the EOD square-off window at {hhmm} IST.")
        return

    # 4. MANAGE EXISTING ACTIVE TRADE VIA 5-SECOND LIVE MONITOR LOOP
    if state.get("active_trade"):
        if live:
            return monitor_active_trade_loop(client, state, live=True, max_seconds=260, quiet_scan=quiet_scan)
        elif not quiet_scan:
            print("Dry tick observed an active trade.")
        return

    # 5. SCAN FOR NEW ENTRY (Only between 09:20 and 14:15 IST — no late entries in final 55 mins, max 4 trades/day)
    if state.get("daily_halted"):
        if not quiet_scan:
            print(f"⏸️ Daily halt active: {state.get('daily_halt_reason')}")
        return
    if state.get("entries_blocked_reason"):
        if not quiet_scan:
            print(f"⛔ Entries blocked: {state.get('entries_blocked_reason')}")
        return
    if not ("09:20" <= hhmm <= "14:15"):
        if live:
            save_state(state)
        if not quiet_scan:
            print(f"⏳ Outside active entry window ({hhmm} IST — cutoff 14:15 IST). Equity: ₹{net_liq:,.2f} | Tier: {tier['tier']}")
        return
    if state.get("daily_trades_count", 0) >= 4:
        if live:
            save_state(state)
        if not quiet_scan:
            print(f"✅ Max 4 Fast-Strike trades completed today. Protecting capital (₹{net_liq:,.2f}).")
        return

    # Check 30-Minute Anti-Revenge Cooldown
    cooldown_dir = None
    if state.get("last_sl_time") and state.get("last_sl_direction"):
        try:
            sl_dt = datetime.fromisoformat(state["last_sl_time"])
            mins_since_sl = (now_ist - sl_dt).total_seconds() / 60.0
            if mins_since_sl < 30.0:
                cooldown_dir = state["last_sl_direction"]
        except Exception:
            pass

    # Weapon A: Scan ALL indices (Nifty 50 + Bank + FinNifty + Next 50 + Midcap Select) in parallel,
    # pick the strongest signal (highest confidence), then find its best option contract.
    best_idx_sig = None
    best_idx_cfg = None
    best_idx_name = None
    for name, cfg in INDEX_UNIVERSE.items():
        if name in DISABLED_INDICES:
            # Live-entry prune (replay 2026-11-22): disabled index has no edge gross of costs.
            if not quiet_scan:
                print(f"⏭️ [ENTRY PRUNE] {name}: live entries disabled (replay: no edge); skipping index scan.")
            continue
        try:
            sid = cfg["security_id"]
            df = client.fetch_candles(sid, exchange_segment="IDX_I", instrument_type="INDEX", days_back=5, resample_tf="3min")
            sig = evaluate_index_signal(df)
            if sig.get("signal") in ("LONG", "SHORT"):
                if sig.get("setup") in DISABLED_SETUPS:
                    # Live-entry prune (replay 2026-11-22): disabled setup has no edge gross of costs.
                    if not quiet_scan:
                        print(f"⏭️ [ENTRY PRUNE] {name}: {sig.get('setup')} live entries disabled (replay: no edge); skipping signal.")
                    continue
                if best_idx_sig is None or sig.get("confidence", 0.0) > best_idx_sig.get("confidence", 0.0):
                    best_idx_sig = sig
                    best_idx_cfg = cfg
                    best_idx_name = name
        except Exception as e:
            logger.debug(f"Index scan skip {name}: {e}")
            continue

    idx_sig = best_idx_sig or {"signal": "NONE", "reason": "No setup across any index"}

    if "OPTIONS" in tier["allowed_weapons"] and idx_sig["signal"] in ("LONG", "SHORT"):
        if cooldown_dir == idx_sig["signal"]:
            if not quiet_scan:
                print(f"🛡️ [ANTI-REVENGE COOLDOWN] Skipping {idx_sig['signal']} signal — 30m cooldown active after recent Stop-Loss in same direction.")
            return
        # P8: remaining-risk budget — never risk more than the daily halt allows
        remaining_budget = tier["daily_max_loss_inr"] + daily_pnl
        if tier["risk_per_trade_inr"] > max(0.0, remaining_budget):
            if not quiet_scan:
                print(f"🛡️ [RISK BUDGET] Per-trade risk ₹{tier['risk_per_trade_inr']:,.0f} exceeds remaining daily budget ₹{remaining_budget:,.0f}; no entry.")
            if live:
                save_state(state)
            return
        opt = client.get_nearest_nifty_option(
            idx_sig["signal"],
            max_lot_cost=avail * 0.45,
            under_security_id=int(best_idx_cfg["security_id"]),
            lot_size=int(best_idx_cfg["lot_size"]),
            index_name=best_idx_cfg["symbol_prefix"],
        )
        if opt and opt.get("synthetic"):
            # P7: fabricated fallback quotes are never tradable
            if not quiet_scan:
                print("🚫 [SYNTHETIC-QUOTE GUARD] Option-chain unavailable; refusing to trade fabricated fallback quotes.")
            opt = None
        if opt:
            opt_ok, opt_chart_msg = verify_option_chart_momentum(client, opt["security_id"])
            if opt_ok:
                # P9: per-setup entry budget + never chase above the day's first fill
                _journal = load_journal()
                _setup = idx_sig.get("setup", "FAST_STRIKE")
                _same_setup = _today_entries(_journal, setup=_setup, direction=idx_sig["signal"])
                if len(_same_setup) >= MAX_ENTRIES_PER_SETUP:
                    if not quiet_scan:
                        print(f"🚫 [P9 SETUP BUDGET] {len(_same_setup)} entries already taken today on {_setup} {idx_sig['signal']} — no further chase (max {MAX_ENTRIES_PER_SETUP}).")
                    if live:
                        save_state(state)
                    return
                _prior_same_sec = _today_entries(_journal, security_id=opt["security_id"], direction=idx_sig["signal"])
                if _prior_same_sec and float(opt["ltp"]) > float(_prior_same_sec[0]["entry_price"]):
                    if not quiet_scan:
                        print(f"🚫 [P9 RE-ENTRY CAP] {opt['symbol']} LTP ₹{opt['ltp']} > day's first fill ₹{_prior_same_sec[0]['entry_price']} — refusing to pay up for the same signal.")
                    if live:
                        save_state(state)
                    return
                entry_px = opt["ltp"]
                qty = opt["lot_size"]
                # Strict Guardrail Sizing: 8 to 10 pts max SL (~₹520-₹650 risk), 16 to 18 pts Quick Target (~+₹1,040 to +₹1,170)
                max_pts_loss = min(10.0, entry_px * 0.10, tier["risk_per_trade_inr"] / qty)
                max_pts_loss = max(7.5, max_pts_loss)
                target_pts = round(max(15.0, max_pts_loss * 1.75), 2)
                # Round all option prices to NSE ₹0.05 tick size
                opt_tick = 0.05
                sl_px = round(round((entry_px - max_pts_loss) / opt_tick) * opt_tick, 2)
                tp_px = round(round((entry_px + target_pts) / opt_tick) * opt_tick, 2)

                trade_record = {
                    "weapon": "OPTIONS",
                    "setup": idx_sig.get("setup", "FAST_STRIKE"),
                    "symbol": opt["symbol"],
                    "security_id": opt["security_id"],
                    "exchange_segment": "NSE_FNO",
                    "instrument_type": "OPTIDX",
                    "direction": "LONG",
                    "underlying_direction": idx_sig["signal"],
                    "underlying_index": best_idx_name,
                    "quantity": qty,
                    "entry_price": entry_px,
                    "stop_loss": sl_px,
                    "target_price": tp_px,
                    "entry_time": now_ist.isoformat(),
                    "reason": f"[{best_idx_name}] {idx_sig['reason']} | {opt_chart_msg}",
                    "live": live,
                }

                if live:
                    # Round limit price to NSE ₹0.05 tick size
                    raw_ask = opt["ask"] if opt["ask"] > 0 else entry_px
                    entry_tick = 0.05
                    limit_price = round(round(raw_ask / entry_tick) * entry_tick, 2)
                    entry_id = _submit_entry(client,
                        security_id=opt["security_id"],
                        exchange_segment="NSE_FNO",
                        transaction_type=client.dhan.BUY,
                        quantity=qty,
                        order_type=client.dhan.LIMIT,
                        product_type=client.dhan.INTRA,
                        price=limit_price,
                    )
                    if not entry_id:
                        return _flatten_trade(client, state, None, "ENTRY_UNVERIFIED")
                    trade_record["order_id"] = entry_id
                    # 2026-11-20: one-shot re-price chase. If the option gapped away from
                    # the limit inside the first settle window, re-price ONCE to the fresh
                    # LTP (capped at +1.5% slippage); beyond the cap, or still unfilled,
                    # the fail-closed abort below runs exactly as before.
                    if not _chase_unfilled_entry(client, trade_record, str(opt["security_id"]),
                                                 "NSE_FNO", "OPTIDX", float(limit_price)):
                        return _flatten_trade(client, state, trade_record, "ENTRY_SETTLEMENT_EXIT")
                    # P2: fill-confirmed protective SL (no naked SL, no over-hedged partial fills)
                    sl = client.place_protective_sl_order(
                        security_id=opt["security_id"],
                        exchange_segment="NSE_FNO",
                        transaction_type=client.dhan.SELL,
                        quantity=qty,
                        trigger_price=sl_px,
                        product_type=client.dhan.INTRA,
                    )
                    if not sl["placed"]:
                        state["daily_trades_count"] += 1
                        return _flatten_trade(client, state, trade_record, "UNPROTECTED_POSITION_EXIT")
                    trade_record["quantity"] = min(int(trade_record["quantity"]), int(sl["filled_qty"]))
                    trade_record["sl_order_id"] = sl["placed"]

                if not live:
                    print(f"Dry tick option candidate: {opt['symbol']} × {qty} @ ₹{entry_px}.")
                    return
                state["active_trade"] = trade_record
                state["daily_trades_count"] += 1
                save_state(state)
                print(f"🚀 [FAST-STRIKE OPTIONS] Entered {qty}x {opt['symbol']} @ ₹{entry_px} | Exchange SL: ₹{sl_px} (Order: {trade_record.get('sl_order_id')}) | Quick TP: ₹{tp_px} (+{target_pts} pts) | {trade_record['reason']}")
                # Immediately stay alive in the 5-second live monitor loop!
                return monitor_active_trade_loop(client, state, live=live, max_seconds=250, quiet_scan=quiet_scan)

    # Weapon B: Scan 5x Intraday Liquid Equities
    eq_cand = scan_equity_candidates(client)
    if eq_cand:
        # P9: per-security entry budget + never chase above the day's first fill
        _journal = load_journal()
        _eq_prior = _today_entries(_journal, security_id=eq_cand["security_id"], direction=eq_cand["direction"])
        if len(_eq_prior) >= MAX_ENTRIES_PER_SETUP:
            if not quiet_scan:
                print(f"🚫 [P9 SETUP BUDGET] {len(_eq_prior)} entries already taken today on {eq_cand['symbol']} {eq_cand['direction']} — no further chase (max {MAX_ENTRIES_PER_SETUP}).")
            if live:
                save_state(state)
            return
        if _eq_prior and float(eq_cand["ltp"]) > float(_eq_prior[0]["entry_price"]):
            if not quiet_scan:
                print(f"🚫 [P9 RE-ENTRY CAP] {eq_cand['symbol']} LTP ₹{eq_cand['ltp']} > day's first fill ₹{_eq_prior[0]['entry_price']} — refusing to pay up for the same signal.")
            if live:
                save_state(state)
            return
        if cooldown_dir == eq_cand["direction"]:
            if not quiet_scan:
                print(f"🛡️ [ANTI-REVENGE COOLDOWN] Skipping Equity {eq_cand['direction']} — 30m cooldown active.")
            return
        # P8: remaining-risk budget — never risk more than the daily halt allows
        remaining_budget = tier["daily_max_loss_inr"] + daily_pnl
        if tier["risk_per_trade_inr"] > max(0.0, remaining_budget):
            if not quiet_scan:
                print(f"🛡️ [RISK BUDGET] Per-trade risk ₹{tier['risk_per_trade_inr']:,.0f} exceeds remaining daily budget ₹{remaining_budget:,.0f}; no equity entry.")
            if live:
                save_state(state)
            return
        entry_px = eq_cand["ltp"]
        atr = eq_cand["atr"]
        stop_dist = max(entry_px * 0.012, atr * 2.0)
        risk_budget = tier["risk_per_trade_inr"]
        max_buying_power = avail * EQUITY_LEVERAGE * 0.85

        qty_by_risk = int(risk_budget // stop_dist)
        qty_by_margin = int(max_buying_power // entry_px)
        qty = max(1, min(qty_by_risk, qty_by_margin))

        # Round equity SL/TP to NSE tick size (0.05 for equities)
        eq_tick = 0.05
        if eq_cand["direction"] == "LONG":
            sl_px = round(round((entry_px - stop_dist) / eq_tick) * eq_tick, 2)
            tp_px = round(round((entry_px + stop_dist * 2.0) / eq_tick) * eq_tick, 2)
            txn = client.dhan.BUY
            sl_txn = client.dhan.SELL
        else:
            sl_px = round(round((entry_px + stop_dist) / eq_tick) * eq_tick, 2)
            tp_px = round(round((entry_px - stop_dist * 2.0) / eq_tick) * eq_tick, 2)
            txn = client.dhan.SELL
            sl_txn = client.dhan.BUY

        trade_record = {
            "weapon": "EQUITY_5X",
            "symbol": eq_cand["symbol"],
            "security_id": eq_cand["security_id"],
            "exchange_segment": "NSE_EQ",
            "instrument_type": "EQUITY",
            "direction": eq_cand["direction"],
            "quantity": qty,
            "entry_price": entry_px,
            "stop_loss": sl_px,
            "target_price": tp_px,
            "entry_time": now_ist.isoformat(),
            "reason": eq_cand["reason"],
            "live": live,
        }

        if live:
            entry_id = _submit_entry(client,
                security_id=eq_cand["security_id"],
                exchange_segment="NSE_EQ",
                transaction_type=txn,
                quantity=qty,
                order_type=client.dhan.MARKET,
                product_type=client.dhan.INTRA,
                price=0,
            )
            if not entry_id:
                return _flatten_trade(client, state, None, "ENTRY_UNVERIFIED")
            trade_record["order_id"] = entry_id
            # P2: fill-confirmed protective SL (no naked SL, no over-hedged partial fills)
            sl = client.place_protective_sl_order(
                security_id=eq_cand["security_id"],
                exchange_segment="NSE_EQ",
                transaction_type=sl_txn,
                quantity=qty,
                trigger_price=sl_px,
                product_type=client.dhan.INTRA,
            )
            if not sl["placed"]:
                state["daily_trades_count"] += 1
                return _flatten_trade(client, state, trade_record, "UNPROTECTED_POSITION_EXIT")
            trade_record["quantity"] = min(int(trade_record["quantity"]), int(sl["filled_qty"]))
            trade_record["sl_order_id"] = sl["placed"]

        if not live:
            print(f"Dry tick equity candidate: {eq_cand['symbol']} × {qty} @ ₹{entry_px}.")
            return
        state["active_trade"] = trade_record
        state["daily_trades_count"] += 1
        save_state(state)
        print(f"⚔️ [WEAPON: 5x EQUITY] Entered {eq_cand['direction']} {qty}x {eq_cand['symbol']} @ ₹{entry_px} | Exchange SL: ₹{sl_px} (Order: {trade_record.get('sl_order_id')}) | TP: ₹{tp_px} | {eq_cand['reason']}")
        return monitor_active_trade_loop(client, state, live=live, max_seconds=250, quiet_scan=quiet_scan)

    if live:
        save_state(state)
    if not quiet_scan:
        print(f"👁️ [SCAN COMPLETE — NO A+ SETUP] Equity: ₹{net_liq:,.2f} | Life: {tier['life_pct']}% | {idx_sig['reason']}")


def cmd_reconcile(*, policy: Optional[RolePolicy] = None) -> None:
    """Reconcile recorded trades using individually verified order fills."""
    state = load_state()
    if state.get("active_trade"):
        print("Reconciliation deferred pending active trade recovery")
        return
    client = SurvivorDhanClient(policy=policy)
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    updated_count = 0
    try:
        with exit_lock(EXIT_LEDGER) as ledger:
            closure = _exit_coordinator(client).closure(ledger, settle=False)
            if closure is None:
                print("Reconciliation deferred pending verified flat broker exposure")
                return
            net_liq, avail, utilized, open_pos = client.get_live_equity_and_positions()
            journal = _read_exit_journal()
            for trade in journal:
                if _journal_proof(trade) or not str(trade.get("entry_time", "")).startswith(today_str):
                    continue
                try:
                    fills = _verified_trade_fills(client, ledger, trade, settle=False)
                except (TypeError, ValueError, KeyError):
                    continue
                if not fills or not fills[0]:
                    continue
                proof, _ = fills
                qty, entry, exit_price, gross, net = _closure_amounts(proof)
                trade.update(entry_price=round(entry, 2), exit_price=exit_price, quantity=qty,
                             gross_pnl=gross, net_pnl=net, reconciled_with_broker=True)
                updated_count += 1
            if updated_count:
                persist_exit_data(JOURNAL_FILE, journal)
    except Exception:
        logger.exception("Broker reconciliation could not be verified")
        print("Reconciliation deferred pending verified broker data and journal storage")
        return

    # Recompute exact lifetime stats from reconciled journal
    completed = [t for t in journal if "net_pnl" in t]
    wins = sum(1 for t in completed if float(t["net_pnl"]) > 0)
    losses = sum(1 for t in completed if float(t["net_pnl"]) <= 0)
    realized_net_total = round(sum(float(t["net_pnl"]) for t in completed), 2)
    realized_gross_total = round(sum(float(t.get("gross_pnl", 0.0)) for t in completed), 2)

    state["total_trades"] = len(completed)
    state["wins"] = wins
    state["losses"] = losses
    state["realized_pnl_total"] = realized_net_total
    state["current_equity"] = net_liq
    state["available_cash"] = avail
    save_state(state)

    print("=" * 72)
    print(f"📋 EOD BROKER RECONCILIATION REPORT — {today_str}")
    print("=" * 72)
    print(f"Journal Entries Updated from Dhan Fills : {updated_count}")
    print(f"Open Positions Remaining                : {len(open_pos)} (100% Cash Overnight: {len(open_pos) == 0})")
    print(f"Gross Realized P&L (Dhan Positions)     : ₹{realized_gross_total:+,.2f}")
    print(f"Brokerage & Statutory Charges           : ₹{ESTIMATED_ROUNDTRIP_CHARGES:+,.2f} (estimated roundtrip)")
    print(f"Net Account Equity Change vs ₹30,000.18 : ₹{round(net_liq - 30000.18, 2):+,.2f}")
    print(f"Live Account Equity (Available + Margin): ₹{net_liq:,.2f} (Available: ₹{avail:,.2f} | Margin Blocked: ₹{utilized:,.2f})")
    print(f"Distance Above ₹3,300 Death Floor       : ₹{round(net_liq - DEATH_FLOOR, 2):,.2f}")
    print(f"Lifetime Record                         : {len(completed)} Trades ({wins}W / {losses}L)")
    print("-" * 72)
    for t in completed:
        print(
            f"Trade #{t.get('trade_no')}: {t.get('symbol')} | Qty: {t.get('quantity')} | "
            f"Buy Avg: ₹{t.get('entry_price')} -> Sell Avg: ₹{t.get('exit_price')} | "
            f"Gross: ₹{t.get('gross_pnl'):+,.2f} | Net: ₹{t.get('net_pnl'):+,.2f} | "
            f"Exit: {t.get('exit_reason')}"
        )
    print("=" * 72)


def cmd_premarket(*, policy: Optional[RolePolicy] = None) -> None:
    """
    Daily pre-market readiness check.
    1. JWT Token Freshness Check: Uses a valid cached token in read-only mode.
    2. Dhan IP Whitelist Verification (`/v2/ip/getIP`): Checks `ordersAllowed`.
    3. Pro Trader Data API & Multi-Index Feed Check: Verifies candle feed + non-0DTE expiry selection across all 5 indices.
    4. Daily State & Guardrail Reset: Resets `daily_date`, `daily_start_equity`, `daily_trades_count = 0`, `daily_halted = False`, and clears stale overnight cooldowns before 09:15 IST.
    5. Broker Position & Order Hygiene: Confirms 0 open positions and 0 orphan pending orders before the bell.

    Repair requires premarket --live --repair and SURVIVOR_LIVE_TRADING=1.
    """
    policy = policy if policy is not None else _resolve_role("premarket")
    allow_repair = policy.repair_ip
    state = load_state(read_only=not allow_repair)
    if not allow_repair:
        state = state.copy()
    if state["status"] == "DEAD_COMA":
        print("🪦 [PRE-MARKET CHECK] SURVIVOR BOT IS IN PERMANENT COMA (DEAD_COMA).")
        return

    try:
        client = SurvivorDhanClient(policy=policy)
    except CachedTokenUnavailable as exc:
        print(f"🚨 PRE-MARKET ACTION REQUIRED — {exc}")
        return
    now_ist = datetime.now(IST)
    today_str = now_ist.strftime("%Y-%m-%d")

    # 1. Ensure JWT token will last through the entire market session (until at least 16:30 IST today)
    token_repaired = False
    if allow_repair and (
        not client.tm.token_expiry
        or (client.tm.token_expiry - datetime.now()).total_seconds() < 8 * 3600
    ):
        client.reconnect_with_fresh_token()
        token_repaired = True
    token_exp_str = client.tm.token_expiry.strftime("%Y-%m-%d %H:%M IST") if client.tm.token_expiry else "UNKNOWN"

    # 2. Verify IPv4 Whitelist & ordersAllowed
    ip_info = client.ensure_ip_whitelisted(allow_update=allow_repair)
    orders_allowed = bool(ip_info.get("ordersAllowed"))
    ip_match = ip_info.get("ipMatchStatus", "UNKNOWN")
    detected_ip = ip_info.get("detectedIP", "UNKNOWN")

    # 3. Verify Live Funds, Positions & Orders
    net_liq, avail, utilized, open_pos = client.get_live_equity_and_positions()
    orders_resp = client.dhan.get_order_list()
    pending_orders = [
        o for o in (orders_resp.get("data", []) if isinstance(orders_resp, dict) else [])
        if o.get("orderStatus") in ("PENDING", "TRANSIT")
    ]

    # 4. Reset Daily State cleanly for today's session
    if state.get("daily_date") != today_str:
        state["daily_date"] = today_str
        state["daily_start_equity"] = net_liq
        state["daily_trades_count"] = 0
        state["daily_consecutive_losses"] = 0
        state["daily_halted"] = False
        state["daily_halt_reason"] = ""
        state["last_sl_direction"] = None
        state["last_sl_time"] = None
    state["current_equity"] = net_liq
    state["available_cash"] = avail
    if allow_repair:
        save_state(state)

    # 5. Multi-Index Data API & Non-0DTE Expiry Check (with Security Master fallback when pre-9:15 optionchain is 502)
    idx_checks = []
    feeds_ok = 0
    sec_df = None
    for name, cfg in INDEX_UNIVERSE.items():
        sid = cfg["security_id"]
        sym_prefix = cfg["symbol_prefix"]
        df = client.fetch_candles(sid, exchange_segment="IDX_I", instrument_type="INDEX", days_back=4, resample_tf="3min")
        exp_resp = client.dhan.expiry_list(under_security_id=int(sid), under_exchange_segment="IDX_I")
        exps = exp_resp.get("data", {}).get("data", []) if exp_resp.get("status") == "success" else []
        exp_src = "API"
        if not exps:
            if sec_df is None:
                sec_df = client.dhan.fetch_security_list(mode="compact")
            m = sec_df[
                (sec_df["SEM_INSTRUMENT_NAME"] == "OPTIDX")
                & (sec_df["SEM_TRADING_SYMBOL"].astype(str).str.startswith(sym_prefix + "-"))
            ]
            exps = sorted(m["SEM_EXPIRY_DATE"].astype(str).str[:10].unique().tolist())
            exp_src = "SecMaster"
        non_0dte = [e for e in exps if e > today_str]
        chosen_exp = non_0dte[0] if non_0dte else (exps[0] if exps else "NONE")
        if not df.empty and chosen_exp != "NONE":
            feeds_ok += 1
            last_spot = float(df.iloc[-1]["close"])
            idx_checks.append(f"  ✅ {name:<19} | Spot: {last_spot:>8.1f} ({len(df)} bars) | Next Exp: {chosen_exp} [{exp_src}, Lot {cfg['lot_size']}]")
        else:
            idx_checks.append(f"  ❌ {name:<19} | Bars: {len(df):<3} | Next Exp: {chosen_exp}")

    tier = get_survival_tier(net_liq)
    overall_ok = orders_allowed and (feeds_ok == len(INDEX_UNIVERSE)) and (len(open_pos) == 0)
    header_icon = "✅ PRE-MARKET SYSTEM CHECK PASSED" if overall_ok else "🚨 PRE-MARKET ACTION REQUIRED"

    print(f"{header_icon} — {today_str} ({now_ist.strftime('%H:%M')} IST)")
    print("=" * 68)
    print(f"1. Auth & Token   : {'✅ VALID' if client.tm.access_token else '❌ INVALID'} (Expires: {token_exp_str}{', renewed now' if token_repaired else ''})")
    print(f"2. IP Whitelist   : {'✅ ALLOWED' if orders_allowed else '🚨 BLOCKED (DH-905 RISK)'} | IP: {detected_ip} ({ip_match})")
    if not orders_allowed:
        print(f"   ⚠️ URGENT: ISP rotated IP to {detected_ip}! Update Primary/Secondary IP on web.dhan.co before 09:15 IST!")
    print(f"3. Account Equity : ₹{net_liq:,.2f} (Cash: ₹{avail:,.2f}) | Life: {tier['life_pct']}% | Tier: {tier['tier']}")
    print(f"4. Risk & Target  : Risk/Trade: ₹{tier['risk_per_trade_inr']:,.0f} | Max Daily Loss: ₹{tier['daily_max_loss_inr']:,.0f} | Daily Target: +₹1,500 Net")
    print(f"5. State & Slate  : Date={state['daily_date']} | Halted={state['daily_halted']} | OpenPos={len(open_pos)} | PendingOrders={len(pending_orders)}")
    print("6. 5-Index Feeds & Non-0DTE Expiries:")
    for line in idx_checks:
        print(line)
    print("=" * 68)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dhan Survivor Autonomous Trading Organism")
    parser.add_argument("action", choices=["status", "premarket", "tick", "reconcile", "flatten", "eod-alerts", "simulate-coma-test"], help="Action to run")
    parser.add_argument("--live", action="store_true", help="Enable the requested live action")
    parser.add_argument("--repair", action="store_true", help="Permit token and IP repair with premarket --live")
    parser.add_argument("--quiet", action="store_true", help="Silent on idle/no-op scans (for zero-token no_agent=True cron jobs)")
    parser.add_argument("--scheduled", action="store_true", help="Apply scheduled EOD dispatch policy")
    args = parser.parse_args()
    if args.scheduled and args.action != "flatten":
        parser.error("--scheduled is only valid for flatten")
    if args.repair and args.action != "premarket":
        parser.error("--repair is only valid for premarket")
    policy = _resolve_role(args.action, live=args.live, repair=args.repair)

    if args.action == "status":
        cmd_status(policy=policy)
    elif args.action == "premarket":
        cmd_premarket(policy=policy)
    elif args.action == "tick":
        if cmd_tick(quiet_scan=args.quiet, policy=policy):
            sys.exit(1)
    elif args.action == "reconcile":
        cmd_reconcile(policy=policy)
    elif args.action == "flatten":
        if not policy.flatten:
            parser.error("flatten requires --live and SURVIVOR_LIVE_TRADING=1")
        if cmd_flatten(policy, scheduled=args.scheduled):
            sys.exit(1)
    elif args.action == "eod-alerts":
        _eod_store().drain(send_telegram)
    elif args.action == "simulate-coma-test":
        # Non-destructive self-test of the coma/obituary logic using a mock state copy
        print("Running non-destructive self-test of Option 2 Permanent Coma logic...")
        tier_test = get_survival_tier(3250.0)
        assert tier_test["tier"] == "DEAD", f"Expected DEAD tier, got {tier_test}"
        print("✅ Mortality threshold check verified: ₹3,250.00 <= ₹3,300.00 triggers DEAD_COMA.")
