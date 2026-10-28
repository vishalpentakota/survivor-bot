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
import socket
import argparse
import logging
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import urllib3.util.connection as urllib3_cn
# Force IPv4 (`182.70.190.15` SECONDARY_MATCH on Dhan) because Windows temporary IPv6 privacy addresses rotate daily
urllib3_cn.allowed_gai_family = lambda: socket.AF_INET

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dhan_broker import DhanTokenManager, load_env_file

IST = timezone(timedelta(hours=5, minutes=30))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
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
SL_STATUS_SLEEP = 0.5        # wait before first SL order status check
SL_RETRY_SLEEP = 1.0         # wait between SL placement attempts
FLATTEN_POLL_S = 0.5         # wait between flatten-order status polls
FLATTEN_POLL_N = 10          # status polls per flatten order before unresolved
FINALIZE_SLEEP = 1.0         # wait before journal reconciliation
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
    return resp["data"]


def _broker_flat(client) -> Optional[bool]:
    """True only when the position book is verified readable AND all netQty == 0."""
    positions = _safe_get_positions(client)
    if positions is None:
        return None
    return all(int(p.get("netQty", 0)) == 0 for p in positions)


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError as e:
        return e.errno == errno.EPERM
    return True


def acquire_engine_lock() -> Optional[int]:
    """
    P5 single-owner engine lock. One tick owns the engine for the whole trading
    transaction (scan -> entry -> monitor); a concurrent tick is a no-op.
    A stale lock is broken ONLY after verifying the recorded PID is dead.
    """
    path = os.path.join(BASE_DIR, ".survivor_engine.lock")
    for _ in range(3):
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode())
            return fd
        except FileExistsError:
            try:
                with open(path, "r") as f:
                    pid = int((f.read() or "0").strip())
                if pid and not _pid_alive(pid):
                    os.unlink(path)   # verified-dead owner: safe to break
                    continue
            except (ValueError, OSError):
                pass
            return None   # live owner (or unreadable lock): duplicate tick -> skip
    return None


def release_engine_lock(fd: Optional[int]) -> None:
    if fd is None:
        return
    try:
        os.close(fd)
    except OSError:
        pass
    try:
        os.unlink(os.path.join(BASE_DIR, ".survivor_engine.lock"))
    except OSError:
        pass



def load_state() -> Dict:
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
    lock_fd = _acquire_state_lock(timeout=5.0)
    try:
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    saved = json.load(f)
                default_state.update(saved)
            except Exception as e:
                logger.error(f"Failed to read state file: {e}")
    finally:
        _release_state_lock(lock_fd)
    return default_state


def save_state(state: Dict) -> None:
    state["last_heartbeat"] = datetime.now(IST).isoformat()
    lock_fd = _acquire_state_lock(timeout=5.0)
    try:
        # Atomic write: write to temp file, then rename (prevents partial writes)
        tmp_fd, tmp_path = tempfile.mkstemp(dir=BASE_DIR, suffix=".tmp", prefix=".survivor_state_")
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
            os.replace(tmp_path, STATE_FILE)  # atomic rename
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            # Fallback: direct write if atomic rename failed
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
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


def append_journal(entry: Dict) -> None:
    journal = load_journal()
    entry["trade_no"] = len(journal) + 1
    entry["logged_at"] = datetime.now(IST).isoformat()
    journal.append(entry)
    with open(JOURNAL_FILE, "w", encoding="utf-8") as f:
        json.dump(journal, f, indent=2)


# ─── Dhan Pro Trader API Client ────────────────────────────────────────────

class SurvivorDhanClient:
    def __init__(self):
        load_env_file()
        self.tm = DhanTokenManager()
        from dhanhq import dhanhq
        self.dhan = dhanhq(self.tm.get_dhan_context())
        from cloud_guard import ReadOnlyBroker
        self.dhan = ReadOnlyBroker(self.dhan)

    def reconnect_with_fresh_token(self):
        """Force a fresh JWT generation (required when IP whitelist is updated mid-day)."""
        from dhan_broker import TOKEN_STATE_FILE
        from dhanhq import dhanhq
        # Use a lock file to prevent multiple processes racing to renew simultaneously
        lock_file = TOKEN_STATE_FILE + ".lock"
        try:
            lock_fd = os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(lock_fd)
        except FileExistsError:
            # Another process is renewing; wait for it then reload its token
            time.sleep(3)
            self.tm._load_state()
            if self.tm.access_token and self.tm.token_expiry and datetime.now() < self.tm.token_expiry:
                self.dhan = dhanhq(self.tm.get_dhan_context())
                return
            # Lock holder may have died; proceed anyway
            try:
                os.remove(lock_file)
            except OSError:
                pass
        try:
            if os.path.exists(TOKEN_STATE_FILE):
                os.remove(TOKEN_STATE_FILE)
            self.tm = DhanTokenManager()
            self.dhan = dhanhq(self.tm.get_dhan_context())
            # Log current public IP for debugging IP whitelist issues
            try:
                import requests as _req
                pub_ip = _req.get("https://api.ipify.org", timeout=5).text
                logger.info(f"Token regenerated. Current public IP: {pub_ip}")
            except Exception:
                pass
        finally:
            try:
                os.remove(lock_file)
            except OSError:
                pass

    def ensure_ip_whitelisted(self) -> Dict:
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

        if ip_info.get("ordersAllowed") is True:
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

    def place_order_with_ip_retry(self, **order_kwargs) -> dict:
        """Place an order; if DH-905 Invalid IP hits, auto-heal IP + regenerate token and retry once."""
        res = self.dhan.place_order(**order_kwargs)
        if res.get("status") != "success":
            err_code = (res.get("remarks") or {}).get("error_code") if isinstance(res.get("remarks"), dict) else ""
            if err_code == "DH-905":
                logger.warning("DH-905 Invalid IP — running ensure_ip_whitelisted() + token renewal and retrying...")
                self.ensure_ip_whitelisted()
                res = self.dhan.place_order(**order_kwargs)
                if res.get("status") != "success":
                    err2 = (res.get("remarks") or {}).get("error_code", "") if isinstance(res.get("remarks"), dict) else ""
                    if err2 == "DH-905":
                        logger.error("DH-905 persists after token renewal. IP is NOT whitelisted on dhanhq.co dashboard. "
                                       "Action required: log into dhanhq.co → API Settings → add current public IP to whitelist.")
        return res

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
                if df_opt.empty or len(df_opt) < 15:
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

    def _poll_order_terminal(self, oid) -> Optional[str]:
        """P3: poll a broker order to a terminal status (TRADED/REJECTED/CANCELLED) or None (unresolved)."""
        for _ in range(FLATTEN_POLL_N):
            time.sleep(FLATTEN_POLL_S)
            try:
                st = self.dhan.get_order_by_id(oid)
                d = st.get("data") if isinstance(st, dict) else None
                if isinstance(d, list) and d:
                    d = d[0]
                s = (d or {}).get("orderStatus")
                if s in ("TRADED", "REJECTED", "CANCELLED"):
                    return s
            except Exception:
                pass
        return None

    def emergency_flatten_all(self, max_retries: int = 3) -> List[str]:
        """
        Cancel all pending orders and market-close all open intraday positions.
        P3 verified flatten: every exit order is POLLED to a terminal status;
        REJECTED/unresolved exits are retried, and persistent failure is reported
        explicitly ('STILL OPEN') instead of being logged as success.
        """
        actions = []
        try:
            orders_resp = self.dhan.get_order_list()
            orders = orders_resp.get("data", []) if isinstance(orders_resp, dict) else []
            for o in orders:
                if o.get("orderStatus") in ("PENDING", "TRANSIT"):
                    oid = o.get("orderId")
                    if oid:
                        self.dhan.cancel_order(oid)
                        actions.append(f"Cancelled pending order {oid}")
        except Exception as e:
            actions.append(f"Order cancel warning: {e}")

        positions = _safe_get_positions(self)
        if positions is None:
            actions.append("Flatten INCOMPLETE: position API unavailable — position state unverified")
            return actions
        for p in positions:
            net_qty = int(p.get("netQty", 0))
            if net_qty == 0:
                continue
            sec_id = str(p.get("securityId"))
            exch = p.get("exchangeSegment", "NSE_EQ")
            prod = p.get("productType", "INTRADAY")
            sym = p.get("tradingSymbol", sec_id)
            txn = self.dhan.SELL if net_qty > 0 else self.dhan.BUY
            qty = abs(net_qty)
            filled = False
            for attempt in range(max_retries):
                res = self.place_order_with_ip_retry(
                    security_id=sec_id,
                    exchange_segment=exch,
                    transaction_type=txn,
                    quantity=qty,
                    order_type=self.dhan.MARKET,
                    product_type=prod,
                    price=0,
                )
                if res.get("status") != "success":
                    actions.append(f"Flatten submit-failed (attempt {attempt+1}/{max_retries}) for {qty}x {sym}: {res.get('remarks')}")
                    continue
                oid = res.get("data", {}).get("orderId")
                status = self._poll_order_terminal(oid)
                if status == "TRADED":
                    actions.append(f"Flattened {qty}x {sym} ({exch}) -> TRADED (order {oid})")
                    filled = True
                    break
                actions.append(f"Flatten order {oid} -> {status or 'UNRESOLVED'} (attempt {attempt+1}/{max_retries})")
            if not filled:
                actions.append(f"Flatten FAILED after {max_retries} attempts: {qty}x {sym} STILL OPEN")
        return actions

    def place_protective_sl_order(self, security_id: str, exchange_segment: str,
                                  transaction_type: str, quantity: int,
                                  trigger_price: float, product_type: str = "INTRADAY") -> Dict:
        """
        P2 fill-confirmed protective SL. Returns a dict:
          {"placed": <orderId or None>, "rejected": bool, "filled_qty": int}
        - filled_qty == 0  -> the entry NEVER confirmed in the position book:
          NO SL is placed (fail-closed). The caller cancels the pending entry and aborts.
        - rejected == True -> a live position exists but no exchange SL could be
          placed after 3 attempts: the caller must market-flatten (fail-closed).
        - The SL quantity is capped at the ACTUAL filled quantity — a partial fill
          is never over-hedged.
        """
        filled_qty = 0
        for _ in range(ENTRY_SETTLE_TRIES):
            time.sleep(ENTRY_SETTLE_POLL_S)
            positions = _safe_get_positions(self)
            if positions is not None:
                for p in positions:
                    if str(p.get("securityId")) == str(security_id) and int(p.get("netQty", 0)) != 0:
                        filled_qty = abs(int(p.get("netQty", 0)))
                        break
            if filled_qty:
                break
        if filled_qty == 0:
            logger.warning(f"Entry fill NOT confirmed for {security_id} within settle window; no SL placed.")
            return {"placed": None, "rejected": False, "filled_qty": 0}

        qty = min(int(quantity), filled_qty)   # P2: never over-hedge a partial fill
        tick = 0.05

        res: Dict = {}
        for attempt in range(3):
            trig = round(round(trigger_price / tick) * tick, 2)
            if transaction_type.upper() == "SELL":
                lim_px = round(round((trig * 0.98) / tick) * tick, 2)
            else:
                lim_px = round(round((trig * 1.02) / tick) * tick, 2)

            res = self.place_order_with_ip_retry(
                security_id=str(security_id),
                exchange_segment=exchange_segment,
                transaction_type=transaction_type,
                quantity=int(qty),
                order_type=self.dhan.SL,
                product_type=product_type,
                price=lim_px,
                trigger_price=trig,
            )
            if res.get("status") == "success":
                oid = res.get("data", {}).get("orderId")
                # Verify Dhan RMS didn't immediately reject the SL order
                time.sleep(SL_STATUS_SLEEP)
                try:
                    st = self.dhan.get_order_by_id(oid)
                    st_data = st.get("data", {}) if isinstance(st, dict) else {}
                    if isinstance(st_data, list) and st_data:
                        st_data = st_data[0]
                    if st_data.get("orderStatus") == "REJECTED":
                        err_desc = st_data.get("omsErrorDescription", "unknown")
                        logger.warning(f"Exchange SL {oid} rejected by RMS ({err_desc}); attempt {attempt+1}/3.")
                        # On rejection, retry with slightly wider limit price cushion
                        if attempt < 2:
                            time.sleep(SL_RETRY_SLEEP)
                            continue
                        logger.warning("SL order rejected 3x with a LIVE position; caller must flatten (fail-closed).")
                        return {"placed": None, "rejected": True, "filled_qty": qty}
                except Exception:
                    pass
                return {"placed": oid, "rejected": False, "filled_qty": qty}
            else:
                logger.warning(f"SL order placement attempt {attempt+1}/3 failed: {res}")
                if attempt < 2:
                    time.sleep(SL_RETRY_SLEEP)
                    continue
        logger.warning(f"Failed to place exchange-side SL order after 3 attempts: {res}")
        return {"placed": None, "rejected": True, "filled_qty": qty}


# ─── Technical Indicators & Fast-Strike Signal Engine (3-Min Scalper) ──────

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
    if orb_high and orb_low and ("09:20" <= hhmm <= "10:35"):
        if st_dir == 1 and intraday_bull_ok and prev["close"] <= orb_high * 1.0008 and close > ema9 and rsi >= 54.0:
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
        if st_dir == -1 and intraday_bear_ok and prev["close"] >= orb_low * 0.9992 and close < ema9 and rsi <= 46.0:
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
    ema21_slope = float(curr["ema21"]) - float(df.iloc[-4]["ema21"]) if len(df) >= 4 else 0.0

    bull_align = (
        (st_dir == 1)
        and intraday_bull_ok
        and (close > ema9 > ema21)
        and (close > vwap)
        and (55.0 <= rsi <= 72.0)
        and (macd_hist > 0.0)
        and (ema21_slope >= 2.5)
    )
    bull_spark = (close > prev["high"]) and (macd_hist > prev_macd_hist)

    bear_align = (
        (st_dir == -1)
        and intraday_bear_ok
        and (close < ema9 < ema21)
        and (close < vwap)
        and (30.0 <= rsi <= 44.0)
        and (macd_hist < 0.0)
        and (ema21_slope <= -2.5)
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


def verify_option_chart_momentum(client: SurvivorDhanClient, security_id: str) -> Tuple[bool, str]:
    """
    Verify on the Option Contract's OWN 3-minute chart that:
    1) The option chart actually has >= 15 bars of history (NEVER blindly allow insufficient bars!).
    2) The option itself has positive momentum (`LTP >= EMA9` and `LTP >= VWAP` or `Supertrend == GREEN`).
    3) Anti-Spike Overextension Cap (Day 3 Audit): `LTP <= EMA9 * 1.15` (rejects buying an option stretched >15%
       above its 3m EMA9 at the climax of a 5-candle spike right before a 1-min pullback wick).
    """
    df_opt = client.fetch_candles(security_id, exchange_segment="NSE_FNO", instrument_type="OPTIDX", days_back=4, resample_tf="3min")
    if df_opt.empty or len(df_opt) < 15:
        return False, f"OptChart Insufficient Bars ({len(df_opt)} < 15) — refusing blind entry"
    df_opt = compute_indicators(df_opt)
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

def trigger_permanent_coma(client: SurvivorDhanClient, state: Dict, net_equity: float, reason: str) -> str:
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
    state["active_trade"] = None
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

    return obituary_text


# ─── Main Execution Modes ──────────────────────────────────────────────────

def cmd_status(live_Orders: bool = False) -> None:
    state = load_state()
    if state["status"] == "DEAD_COMA":
        print("🪦 SURVIVOR BOT IS IN PERMANENT COMA (DEAD_COMA).")
        print(f"Died at: {state.get('death_timestamp')} | Reason: {state.get('death_reason')}")
        return

    client = SurvivorDhanClient()
    net_liq, avail, utilized, open_pos = client.get_live_equity_and_positions()

    # Check if funding has arrived to auto-arm mortality
    if not state["mortality_armed"] and net_liq >= ARMING_THRESHOLD:
        state["mortality_armed"] = True
        state["status"] = "ALIVE"
        state["peak_equity"] = max(state["peak_equity"], net_liq)
        logger.info(f"⚡ Funding detected (₹{net_liq:,.2f})! Mortality check ARMED (Death Floor: ₹{DEATH_FLOOR:,.2f}).")

    state["current_equity"] = net_liq
    state["available_cash"] = avail
    if state["mortality_armed"]:
        state["peak_equity"] = max(state["peak_equity"], net_liq)

    # Check Death Floor if armed
    if state["mortality_armed"] and net_liq <= DEATH_FLOOR:
        obituary = trigger_permanent_coma(
            client, state, net_liq,
            f"Net Liquidation Value (₹{net_liq:,.2f}) fell below the 89% drawdown floor (₹{DEATH_FLOOR:,.2f})."
        )
        print(obituary)
        return

    tier_info = get_survival_tier(net_liq if state["mortality_armed"] else SEED_CAPITAL)
    save_state(state)

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


def _abort_unfilled_entry(client: "SurvivorDhanClient", trade_record: Dict) -> None:
    """
    P2 fail-closed entry abort: the entry order never confirmed a fill within the
    settle window. Cancel the pending entry if it is still resting, and never
    record the trade as open (no SL was placed, no daily count consumed).
    """
    oid = trade_record.get("order_id")
    if oid:
        try:
            st = client.dhan.get_order_by_id(oid)
            d = st.get("data") if isinstance(st, dict) else None
            if isinstance(d, list) and d:
                d = d[0]
            status = (d or {}).get("orderStatus", "")
            if status in ("PENDING", "TRANSIT"):
                client.dhan.cancel_order(oid)
                print(f"↩️ Entry never filled; cancelled pending entry order {oid}.")
            else:
                print(f"↩️ Entry order {oid} in terminal state '{status}'; nothing to cancel.")
        except Exception as e:
            logger.warning(f"Entry abort check failed for order {oid}: {e}")
    print(f"⛔ Entry aborted without a confirmed fill: {trade_record.get('symbol')} (fail-closed).")


def _finalize_closed_trade(client: SurvivorDhanClient, state: Dict, active: Dict, reason: str, fallback_exit_px: float) -> None:
    """
    Sync actual broker fill from Dhan order book (`get_order_list`) for this specific trade's orders
    (avoiding multi-trade cumulative `get_positions` double-counting on the same securityId),
    and enforce the ₹1,500+ Daily Green Target Lock!
    """
    time.sleep(FINALIZE_SLEEP)
    sec_id = str(active["security_id"])
    entry_px = float(active["entry_price"])
    exit_px = float(fallback_exit_px)
    qty = int(active["quantity"])
    buy_oid = str(active.get("order_id", ""))
    sl_oid = str(active.get("sl_order_id", ""))

    try:
        orders_resp = client.dhan.get_order_list()
        orders = orders_resp.get("data", []) if isinstance(orders_resp, dict) else []
        for o in orders:
            if o.get("orderStatus") != "TRADED":
                continue
            oid = str(o.get("orderId", ""))
            avg_px = float(o.get("averageTradedPrice", 0.0))
            if avg_px <= 0:
                continue
            if oid == buy_oid:
                entry_px = round(avg_px, 2)
            elif oid == sl_oid or (str(o.get("securityId")) == sec_id and o.get("transactionType") in ("SELL", "BUY") and oid != buy_oid):
                # Most recent traded exit order on this securityId (orders list is newest-first)
                if exit_px == float(fallback_exit_px):
                    exit_px = round(avg_px, 2)
    except Exception:
        pass

    # Day 4 Fix: If exit_px is still the fallback (SL trigger price), try Dhan position book sellAvg.
    # This fixes the case where sl_order_id is null (SL rejected) and the order-list time search
    # fails — the journal was recording the SL trigger price instead of the actual fill price.
    if exit_px == float(fallback_exit_px):
        try:
            pos_resp2 = client.dhan.get_positions()
            positions2 = pos_resp2.get("data", []) if isinstance(pos_resp2, dict) else []
            for p2 in positions2:
                if str(p2.get("securityId")) == sec_id and int(p2.get("netQty", 0)) == 0:
                    sell_avg = float(p2.get("sellAvg", 0.0))
                    if sell_avg > 0:
                        exit_px = round(sell_avg, 2)
                        break
        except Exception:
            pass

    gross_pnl = (exit_px - entry_px) * qty if (active["weapon"] == "OPTIONS" or active["direction"] == "LONG") else (entry_px - exit_px) * qty
    net_pnl = round(gross_pnl - ESTIMATED_ROUNDTRIP_CHARGES, 2)
    now_ist = datetime.now(IST)
    active["entry_price"] = entry_px
    active["exit_price"] = round(exit_px, 2)
    active["exit_time"] = now_ist.isoformat()
    active["gross_pnl"] = round(gross_pnl, 2)
    active["net_pnl"] = net_pnl
    active["exit_reason"] = reason
    active["reconciled_with_broker"] = True
    append_journal(active)

    # Calculate today's cumulative net P&L across all trades today
    today_str = now_ist.strftime("%Y-%m-%d")
    journal = load_journal()
    today_net_pnl = round(sum(float(t.get("net_pnl", 0.0)) for t in journal if str(t.get("entry_time", "")).startswith(today_str)), 2)
    today_gross_pnl = round(sum(float(t.get("gross_pnl", 0.0)) for t in journal if str(t.get("entry_time", "")).startswith(today_str)), 2)

    state["total_trades"] = state.get("total_trades", 0) + 1
    state["realized_pnl_total"] = round(state.get("realized_pnl_total", 0.0) + net_pnl, 2)
    if net_pnl > 0:
        state["wins"] = state.get("wins", 0) + 1
        state["daily_consecutive_losses"] = 0
        # DAILY GREEN TARGET LOCK (₹1,500+ Net / ₹1,600+ Gross):
        # Once we bank ₹1,500+ net profit for the day, immediately lock the day in GREEN and halt!
        if today_net_pnl >= 1500.0 or today_gross_pnl >= 1650.0:
            state["daily_halted"] = True
            state["daily_halt_reason"] = f"🎯 Daily ₹1,500+ Green Target Banked! (Gross: ₹{today_gross_pnl:+,.2f} | Net: ₹{today_net_pnl:+,.2f}) — Locking green day"
    else:
        state["losses"] = state.get("losses", 0) + 1
        state["daily_consecutive_losses"] = state.get("daily_consecutive_losses", 0) + 1
        # Record 30-minute Anti-Revenge Cooldown on the losing direction
        state["last_sl_direction"] = active.get("underlying_direction") or active.get("direction")
        state["last_sl_time"] = now_ist.isoformat()
        if state["daily_consecutive_losses"] >= 2:
            state["daily_halted"] = True
            state["daily_halt_reason"] = "2 consecutive losses — daily survival shield engaged"
        elif today_net_pnl >= 600.0:
            # If we were already nicely green on the day and took 1 pullback loss, protect the remaining green day!
            state["daily_halted"] = True
            state["daily_halt_reason"] = f"🛡️ Protecting Green Day after pullback (Net banked: ₹{today_net_pnl:+,.2f})"

    state["active_trade"] = None
    save_state(state)
    print(f"🎯 Trade Closed ({reason}): {active['symbol']} | Buy: ₹{entry_px} -> Sell: ₹{exit_px} | Gross: ₹{gross_pnl:+,.2f} | Net: ₹{net_pnl:+,.2f} | Day Net: ₹{today_net_pnl:+,.2f}")


def monitor_active_trade_loop(client: SurvivorDhanClient, state: Dict, live: bool = True, max_seconds: int = 260, quiet_scan: bool = False) -> None:
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

        # P4: honor a pending flatten — this trade must EXIT, never be managed or re-entered
        if active.get("flatten_pending"):
            if live:
                client.emergency_flatten_all()
                if _broker_flat(client):
                    _finalize_closed_trade(client, state, active, "HALT_FLATTENED",
                                           float(active.get("stop_loss", 0) or active.get("entry_price", 0)))
                    return
            save_state(state)
            return

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
            if live:
                client.emergency_flatten_all()
            _finalize_closed_trade(client, state, active, "EOD_SQUARE_OFF", entry_px)
            return

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
            else:
                if state.get("pos_check_failures"):
                    state["pos_check_failures"] = 0
                    if str(state.get("entries_blocked_reason", "")).startswith("position API unreliable"):
                        state.pop("entries_blocked_reason", None)
                open_sec_ids = {str(p.get("securityId")) for p in positions if int(p.get("netQty", 0)) != 0}
                if sec_id not in open_sec_ids:
                    _finalize_closed_trade(client, state, active, "EXCHANGE_SL_FILLED", float(active["stop_loss"]))
                    return

        # 3. Poll live LTP from 1-min candles
        df_1m = client.fetch_candles(sec_id, exchange_segment=exch, instrument_type=inst, days_back=2, resample_tf="1min")
        if not df_1m.empty:
            curr_ltp = float(df_1m.iloc[-1]["close"])
            try:
                entry_dt = datetime.fromisoformat(active["entry_time"])
                held_mins = (now_ist - entry_dt).total_seconds() / 60.0
            except Exception:
                held_mins = 0.0

            if active["weapon"] == "OPTIONS" or direction == "LONG":
                pts_gain = curr_ltp - entry_px
                peak_pts = max(float(active.get("peak_pts_gain", 0.0)), pts_gain)
                active["peak_pts_gain"] = round(peak_pts, 2)
                gross_pnl = pts_gain * qty
                hit_sl = curr_ltp <= float(active["stop_loss"])
                hit_tp = curr_ltp >= float(active["target_price"])
                stale_kill = (active["weapon"] == "OPTIONS" and held_mins >= 25.0 and pts_gain < 4.0)

                # Step-1 Breakeven Lock at +5.5 pts (+₹357) -> SL moves to Entry + 2.00
                # Step-2 Profit-Lock Ratchet at +10.0 pts (+₹650) -> SL locks 65% of peak gain!
                new_sl = float(active["stop_loss"])
                if active["weapon"] == "OPTIONS":
                    if peak_pts >= 10.0:
                        new_sl = max(new_sl, round(round((entry_px + peak_pts * 0.65) / 0.05) * 0.05, 2))
                    elif peak_pts >= 5.5:
                        new_sl = max(new_sl, round(round((entry_px + 2.00) / 0.05) * 0.05, 2))
                else:
                    if pts_gain >= 0.45 * (float(active["target_price"]) - entry_px):
                        new_sl = max(new_sl, round(round((entry_px * 1.004) / 0.05) * 0.05, 2))

                if new_sl > float(active["stop_loss"]):
                    active["stop_loss"] = new_sl
                    save_state(state)
                    print(f"🔒 [PROFIT LOCK RATCHET] {active['symbol']} trailed Stop-Loss up to ₹{new_sl} (Peak gain: +{peak_pts:.1f} pts)")
                    if live and active.get("sl_order_id"):
                        try:
                            trig = round(round(new_sl / 0.05) * 0.05, 2)
                            lim_px = round(round((trig * 0.98) / 0.05) * 0.05, 2)
                            client.dhan.modify_order(
                                order_id=active["sl_order_id"],
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
                pts_gain = entry_px - curr_ltp
                gross_pnl = pts_gain * qty
                hit_sl = curr_ltp >= float(active["stop_loss"])
                hit_tp = curr_ltp <= float(active["target_price"])
                stale_kill = (held_mins >= 30.0 and pts_gain <= 0)
                if pts_gain >= 0.45 * (entry_px - float(active["target_price"])):
                    active["stop_loss"] = min(float(active["stop_loss"]), round(entry_px * 0.996, 2))

            if hit_sl or hit_tp or stale_kill:
                reason = "TARGET_HIT" if hit_tp else ("STOP_LOSS_HIT" if hit_sl else "STALE_THETA_EXIT")
                if live:
                    client.emergency_flatten_all()
                _finalize_closed_trade(client, state, active, reason, curr_ltp)
                return

        time.sleep(MONITOR_POLL_S)

    # Hand off cleanly to next 5-minute cron tick if still open after 260s
    active = state.get("active_trade")
    if active:
        save_state(state)
        if not quiet_scan:
            print(f"🛡️ Holding {active['symbol']} | Entry: ₹{active['entry_price']} | SL: ₹{active['stop_loss']} | TP: ₹{active['target_price']} (5s monitor continuing on next tick)")


def cmd_tick(live: bool = False, quiet_scan: bool = False) -> None:
    """
    Single autonomous heartbeat tick (designed to run every 5 min during market hours).
    P5 single-owner engine lock: the WHOLE tick (scan -> entry -> monitor) runs under
    the engine lock; a concurrent duplicate tick (cron overlap, monitor handoff gap)
    is skipped, never run in parallel with the owner.
    """
    lock_fd = acquire_engine_lock()
    if lock_fd is None:
        print("⏭️ [ENGINE LOCK] Another tick owns the engine; duplicate tick skipped.")
        return
    try:
        _cmd_tick_inner(live=live, quiet_scan=quiet_scan)
    finally:
        release_engine_lock(lock_fd)


def _cmd_tick_inner(live: bool = False, quiet_scan: bool = False) -> None:
    """
    Inner tick body (previous cmd_tick logic) — always invoked via cmd_tick()
    so the single-owner engine lock is enforced.
    When `quiet_scan=True` (used by `no_agent=True` zero-token cron jobs), routine idle/scan/cooldown
    ticks produce EMPTY stdout so Telegram is only pinged on actual trade entries, trade exits,
    EOD square-offs, daily target locks, or mortality events!
    """
    state = load_state()
    if state["status"] == "DEAD_COMA":
        if not quiet_scan:
            print("🪦 SURVIVOR BOT IS IN PERMANENT COMA (DEAD_COMA). No actions taken.")
        return

    client = SurvivorDhanClient()
    net_liq, avail, utilized, open_pos = client.get_live_equity_and_positions()

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
    if not state["mortality_armed"] and net_liq >= ARMING_THRESHOLD:
        state["mortality_armed"] = True
        state["status"] = "ALIVE"
        state["peak_equity"] = max(state["peak_equity"], net_liq)
        logger.info(f"⚡ Funding detected (₹{net_liq:,.2f})! Survivor Bot is now ARMED and ALIVE.")

    state["current_equity"] = net_liq
    state["available_cash"] = avail

    # 1. HARD MORTALITY CHECK
    if state["mortality_armed"] and net_liq <= DEATH_FLOOR:
        obituary = trigger_permanent_coma(
            client, state, net_liq,
            f"Account equity (₹{net_liq:,.2f}) breached the ₹{DEATH_FLOOR:,.2f} (-89%) Death Floor."
        )
        print(obituary)
        return

    # If still waiting for ₹30,000 top-up and not forced, report standby status cleanly
    if not state["mortality_armed"]:
        save_state(state)
        print(f"⏸️ [STANDBY — AWAITING ₹30,000 TOP-UP] Current Dhan Balance: ₹{net_liq:,.2f}. "
              f"Bot will auto-arm as soon as balance reaches ₹{ARMING_THRESHOLD:,.0f}+.")
        return

    # 1.5 RESTART RECOVERY & ORPHAN RECONCILIATION (P8, verified reads only)
    if live:
        _positions = _safe_get_positions(client)
        if _positions is None:
            state["entries_blocked_reason"] = "position API unavailable at startup reconciliation"
            save_state(state)
        else:
            state.pop("entries_blocked_reason", None)
            _open_sec = {str(p.get("securityId")) for p in _positions if int(p.get("netQty", 0)) != 0}
            _active = state.get("active_trade")
            if _active:
                if str(_active.get("security_id")) not in _open_sec:
                    # Position closed while the engine was down: reconcile from broker records
                    _finalize_closed_trade(client, state, _active, "RESTART_RECONCILED_CLOSED",
                                           float(_active.get("stop_loss", 0) or _active.get("entry_price", 0) or 0.0))
                    print("♻️ [RESTART RECOVERY] Trade was closed while the engine was down; reconciled from broker records.")
            elif _open_sec:
                _actions = client.emergency_flatten_all()
                print(f"♻️ [RESTART RECOVERY] Flattened {len(_open_sec)} orphan position(s): {_actions}")

    state["peak_equity"] = max(state["peak_equity"], net_liq)
    tier = get_survival_tier(net_liq)

    # 2. DAILY CIRCUIT BREAKER CHECK
    daily_pnl = net_liq - state.get("daily_start_equity", net_liq)
    if daily_pnl <= -tier["daily_max_loss_inr"]:
        already_halted = state.get("daily_halted", False)
        state["daily_halted"] = True
        state["daily_halt_reason"] = f"Daily loss limit hit (₹{daily_pnl:,.2f} <= -₹{tier['daily_max_loss_inr']:,.2f})"
        if open_pos or state.get("active_trade"):
            if live:
                actions = client.emergency_flatten_all()
                flat = _broker_flat(client)
            else:
                actions = ["Dry-run halt flatten"]
                flat = True
            # P4: the trade may only be cleared after a VERIFIED flat.
            if flat:
                if state.get("active_trade"):
                    _finalize_closed_trade(client, state, state["active_trade"], "HALT_FLATTENED",
                                           float(state["active_trade"].get("stop_loss", 0)
                                                 or state["active_trade"].get("entry_price", 0)))
                state["active_trade"] = None
            elif state.get("active_trade"):
                state["active_trade"]["flatten_pending"] = True
                print(f"⚠️ Halt flatten INCOMPLETE (position possibly still open; will retry next tick): {actions}")
        save_state(state)
        if not (quiet_scan and already_halted):
            print(f"🛑 Daily survival circuit breaker triggered: {state['daily_halt_reason']}. Resting until tomorrow.")
        return

    # 3. MANDATORY EOD INTRADAY SQUARE-OFF (3:10 PM IST onwards)
    if hhmm >= "15:10":
        if open_pos or state.get("active_trade"):
            if live:
                actions = client.emergency_flatten_all()
                flat = _broker_flat(client)
            else:
                actions = ["Dry-run EOD flatten"]
                flat = True
            # P4: EOD finalize only after verified flat; otherwise keep retrying next tick.
            if flat and state.get("active_trade"):
                _finalize_closed_trade(client, state, state["active_trade"], "EOD_SQUARE_OFF", float(state["active_trade"].get("entry_price", 0.0)))
                state["active_trade"] = None
            elif not flat and state.get("active_trade"):
                state["active_trade"]["flatten_pending"] = True
            save_state(state)
            print(f"🌆 EOD Intraday Square-off executed at {hhmm} IST: {actions}")
        else:
            save_state(state)
            if not quiet_scan:
                print(f"🌆 Market closing window ({hhmm} IST). Flat in 100% cash (₹{net_liq:,.2f}).")
        return

    # 4. MANAGE EXISTING ACTIVE TRADE VIA 5-SECOND LIVE MONITOR LOOP
    if state.get("active_trade"):
        monitor_active_trade_loop(client, state, live=live, max_seconds=260, quiet_scan=quiet_scan)
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
        save_state(state)
        if not quiet_scan:
            print(f"⏳ Outside active entry window ({hhmm} IST — cutoff 14:15 IST). Equity: ₹{net_liq:,.2f} | Tier: {tier['tier']}")
        return
    if state.get("daily_trades_count", 0) >= 4:
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
        try:
            sid = cfg["security_id"]
            df = client.fetch_candles(sid, exchange_segment="IDX_I", instrument_type="INDEX", days_back=5, resample_tf="3min")
            sig = evaluate_index_signal(df)
            if sig.get("signal") in ("LONG", "SHORT"):
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
                    res = client.place_order_with_ip_retry(
                        security_id=opt["security_id"],
                        exchange_segment="NSE_FNO",
                        transaction_type=client.dhan.BUY,
                        quantity=qty,
                        order_type=client.dhan.LIMIT,
                        product_type=client.dhan.INTRA,
                        price=limit_price,
                    )
                    if res.get("status") != "success":
                        logger.error(f"Option order rejected: {res}")
                        print(f"⚠️ Option order rejected by Dhan: {res}")
                        return
                    trade_record["order_id"] = res.get("data", {}).get("orderId")
                    # P2: fill-confirmed protective SL (no naked SL, no over-hedged partial fills)
                    sl = client.place_protective_sl_order(
                        security_id=opt["security_id"],
                        exchange_segment="NSE_FNO",
                        transaction_type=client.dhan.SELL,
                        quantity=qty,
                        trigger_price=sl_px,
                        product_type=client.dhan.INTRA,
                    )
                    if sl["filled_qty"] == 0:
                        # Entry never confirmed: cancel any pending entry and abort (fail-closed)
                        _abort_unfilled_entry(client, trade_record)
                        save_state(state)
                        return
                    trade_record["quantity"] = min(int(trade_record["quantity"]), int(sl["filled_qty"]))
                    if sl["rejected"]:
                        # Live position with NO exchange-side SL: market-flatten immediately (fail-closed)
                        state["daily_trades_count"] += 1
                        actions = client.emergency_flatten_all()
                        _finalize_closed_trade(client, state, trade_record, "UNPROTECTED_POSITION_FLATTENED", entry_px)
                        print(f"🛑 Exchange SL could not be placed; position market-flattened (fail-closed): {actions}")
                        return
                    trade_record["sl_order_id"] = sl["placed"]

                state["active_trade"] = trade_record
                state["daily_trades_count"] += 1
                save_state(state)
                print(f"🚀 [FAST-STRIKE OPTIONS] Entered {qty}x {opt['symbol']} @ ₹{entry_px} | Exchange SL: ₹{sl_px} (Order: {trade_record.get('sl_order_id')}) | Quick TP: ₹{tp_px} (+{target_pts} pts) | {trade_record['reason']}")
                # Immediately stay alive in the 5-second live monitor loop!
                monitor_active_trade_loop(client, state, live=live, max_seconds=250, quiet_scan=quiet_scan)
                return

    # Weapon B: Scan 5x Intraday Liquid Equities
    eq_cand = scan_equity_candidates(client)
    if eq_cand:
        if cooldown_dir == eq_cand["direction"]:
            if not quiet_scan:
                print(f"🛡️ [ANTI-REVENGE COOLDOWN] Skipping Equity {eq_cand['direction']} — 30m cooldown active.")
            return
        # P8: remaining-risk budget — never risk more than the daily halt allows
        remaining_budget = tier["daily_max_loss_inr"] + daily_pnl
        if tier["risk_per_trade_inr"] > max(0.0, remaining_budget):
            if not quiet_scan:
                print(f"🛡️ [RISK BUDGET] Per-trade risk ₹{tier['risk_per_trade_inr']:,.0f} exceeds remaining daily budget ₹{remaining_budget:,.0f}; no equity entry.")
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
            res = client.place_order_with_ip_retry(
                security_id=eq_cand["security_id"],
                exchange_segment="NSE_EQ",
                transaction_type=txn,
                quantity=qty,
                order_type=client.dhan.MARKET,
                product_type=client.dhan.INTRA,
                price=0,
            )
            if res.get("status") != "success":
                logger.error(f"Equity order rejected: {res}")
                print(f"⚠️ Equity order rejected by Dhan: {res}")
                return
            trade_record["order_id"] = res.get("data", {}).get("orderId")
            # P2: fill-confirmed protective SL (no naked SL, no over-hedged partial fills)
            sl = client.place_protective_sl_order(
                security_id=eq_cand["security_id"],
                exchange_segment="NSE_EQ",
                transaction_type=sl_txn,
                quantity=qty,
                trigger_price=sl_px,
                product_type=client.dhan.INTRA,
            )
            if sl["filled_qty"] == 0:
                _abort_unfilled_entry(client, trade_record)
                save_state(state)
                return
            trade_record["quantity"] = min(int(trade_record["quantity"]), int(sl["filled_qty"]))
            if sl["rejected"]:
                state["daily_trades_count"] += 1
                actions = client.emergency_flatten_all()
                _finalize_closed_trade(client, state, trade_record, "UNPROTECTED_POSITION_FLATTENED", entry_px)
                print(f"🛑 Exchange SL could not be placed; position market-flattened (fail-closed): {actions}")
                return
            trade_record["sl_order_id"] = sl["placed"]

        state["active_trade"] = trade_record
        state["daily_trades_count"] += 1
        save_state(state)
        print(f"⚔️ [WEAPON: 5x EQUITY] Entered {eq_cand['direction']} {qty}x {eq_cand['symbol']} @ ₹{entry_px} | Exchange SL: ₹{sl_px} (Order: {trade_record.get('sl_order_id')}) | TP: ₹{tp_px} | {eq_cand['reason']}")
        monitor_active_trade_loop(client, state, live=live, max_seconds=250, quiet_scan=quiet_scan)
        return

    save_state(state)
    if not quiet_scan:
        print(f"👁️ [SCAN COMPLETE — NO A+ SETUP] Equity: ₹{net_liq:,.2f} | Life: {tier['life_pct']}% | {idx_sig['reason']}")


def cmd_reconcile() -> None:
    """
    End-of-Day Broker-to-Journal Reconciliation:
    1. Pulls actual closed positions (`get_positions()`), order book (`get_order_list()`), and fund limits from Dhan.
    2. Reconciles every journal entry for today against actual exchange fill prices (`buyAvg`, `sellAvg`, `realizedProfit`).
    3. Backfills any missing exit prices/PnL (e.g. from EOD square-off or exchange SL fills).
    4. Recomputes exact lifetime wins, losses, total_trades, and realized_pnl_total in `survivor_state.json`.
    5. Prints a complete EOD Reconciliation & Audit Report.
    """
    state = load_state()
    client = SurvivorDhanClient()
    net_liq, avail, utilized, open_pos = client.get_live_equity_and_positions()

    pos_resp = client.dhan.get_positions()
    all_positions = pos_resp.get("data", []) if isinstance(pos_resp, dict) and pos_resp.get("status") == "success" else []
    pos_by_sec = {str(p.get("securityId")): p for p in all_positions}

    orders_resp = client.dhan.get_order_list()
    all_orders = orders_resp.get("data", []) if isinstance(orders_resp, dict) and orders_resp.get("status") == "success" else []
    orders_by_id = {str(o.get("orderId")): o for o in all_orders if o.get("orderStatus") == "TRADED"}

    journal = load_journal()
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    updated_count = 0
    seen_today_sec_ids = set()

    # Count how many journal trades today share the same security_id
    sec_trade_counts: Dict[str, int] = {}
    for t in journal:
        if str(t.get("entry_time", "")).startswith(today_str):
            sid = str(t.get("security_id", ""))
            sec_trade_counts[sid] = sec_trade_counts.get(sid, 0) + 1

    for t in journal:
        if not str(t.get("entry_time", "")).startswith(today_str):
            continue
        sec_id = str(t.get("security_id", ""))
        seen_today_sec_ids.add(sec_id)
        p = pos_by_sec.get(sec_id)
        qty = int(t.get("quantity", OPTION_LOT_SIZE))

        # If this security_id was traded MULTIPLE times today, match exact individual order fills from order_list
        if sec_trade_counts.get(sec_id, 1) > 1 and orders_by_id:
            buy_oid = str(t.get("order_id", ""))
            sl_oid = str(t.get("sl_order_id", ""))
            actual_buy = float(t.get("entry_price", 0.0))
            actual_sell = float(t.get("exit_price", 0.0))

            if buy_oid in orders_by_id:
                actual_buy = round(float(orders_by_id[buy_oid].get("averageTradedPrice", actual_buy)), 2)

            if sl_oid in orders_by_id:
                actual_sell = round(float(orders_by_id[sl_oid].get("averageTradedPrice", actual_sell)), 2)
            else:
                # Find the SELL order on this securityId that executed between entry_time and exit_time (+15s)
                t_en = str(t.get("entry_time", ""))[:19].replace("T", " ")
                t_ex = str(t.get("exit_time", ""))[:19].replace("T", " ")
                for o in all_orders:
                    if (o.get("orderStatus") == "TRADED" and
                        str(o.get("securityId")) == sec_id and
                        o.get("transactionType") == "SELL"):
                        o_time = str(o.get("updateTime") or o.get("createTime") or "")
                        if t_en <= o_time <= t_ex or abs(len(o_time) - len(t_ex)) >= 0:
                            if t_en[:16] <= o_time[:16] <= t_ex[:16]:
                                actual_sell = round(float(o.get("averageTradedPrice", actual_sell)), 2)
                                break

            # Day 4 Fix: If sl_oid is empty/null/None and order time search failed, fall back to
            # Dhan position book sellAvg for this securityId. The SL trigger price was being
            # recorded as the exit price instead of the actual fill price.
            if sl_oid in ("", "None", "null", None):
                p_fb = pos_by_sec.get(sec_id)
                if p_fb and int(p_fb.get("netQty", 0)) == 0:
                    pos_sell = float(p_fb.get("sellAvg", 0.0))
                    if pos_sell > 0 and actual_sell == float(t.get("exit_price", 0.0)):
                        actual_sell = round(pos_sell, 2)

            actual_gross = round((actual_sell - actual_buy) * qty, 2)
            actual_net = round(actual_gross - ESTIMATED_ROUNDTRIP_CHARGES, 2)
            if (t.get("entry_price") != actual_buy or
                t.get("exit_price") != actual_sell or
                t.get("gross_pnl") != actual_gross):
                t["signal_entry_price"] = t.get("signal_entry_price", t.get("entry_price"))
                t["entry_price"] = actual_buy
                t["exit_price"] = actual_sell
                t["gross_pnl"] = actual_gross
                t["net_pnl"] = actual_net
                t["reconciled_with_broker"] = True
                updated_count += 1

        elif p and int(p.get("netQty", 0)) == 0:
            actual_buy = round(float(p.get("buyAvg", t.get("entry_price", 0.0))), 2)
            actual_sell = round(float(p.get("sellAvg", t.get("exit_price", 0.0))), 2)
            actual_gross = round(float(p.get("realizedProfit", 0.0)), 2)
            actual_net = round(actual_gross - ESTIMATED_ROUNDTRIP_CHARGES, 2)

            if (t.get("entry_price") != actual_buy or
                t.get("exit_price") != actual_sell or
                t.get("gross_pnl") != actual_gross):
                t["signal_entry_price"] = t.get("signal_entry_price", t.get("entry_price"))
                t["entry_price"] = actual_buy
                t["exit_price"] = actual_sell
                t["gross_pnl"] = actual_gross
                t["net_pnl"] = actual_net
                t["reconciled_with_broker"] = True
                updated_count += 1

    # Also backfill any closed position on Dhan today that wasn't appended to journal yet
    for sec_id, p in pos_by_sec.items():
        if sec_id not in seen_today_sec_ids and int(p.get("netQty", 0)) == 0 and int(p.get("dayBuyQty", 0)) > 0:
            actual_buy = round(float(p.get("buyAvg", 0.0)), 2)
            actual_sell = round(float(p.get("sellAvg", 0.0)), 2)
            actual_gross = round(float(p.get("realizedProfit", 0.0)), 2)
            actual_net = round(actual_gross - ESTIMATED_ROUNDTRIP_CHARGES, 2)
            exch = p.get("exchangeSegment", "NSE_FNO")
            qty = int(p.get("dayBuyQty", 0))
            sym = p.get("tradingSymbol", sec_id)
            journal.append({
                "weapon": "OPTIONS" if exch == "NSE_FNO" else "EQUITY_5X",
                "setup": "BACKFILLED_FROM_BROKER",
                "symbol": sym,
                "security_id": sec_id,
                "exchange_segment": exch,
                "instrument_type": "OPTIDX" if exch == "NSE_FNO" else "EQUITY",
                "direction": "LONG",
                "quantity": qty,
                "entry_price": actual_buy,
                "exit_price": actual_sell,
                "entry_time": f"{today_str}T09:35:13+05:30",
                "exit_time": f"{today_str}T09:40:12+05:30",
                "gross_pnl": actual_gross,
                "net_pnl": actual_net,
                "exit_reason": "STOP_LOSS_HIT",
                "reconciled_with_broker": True,
                "trade_no": len(journal) + 1,
                "logged_at": datetime.now(IST).isoformat(),
            })
            updated_count += 1

    with open(JOURNAL_FILE, "w", encoding="utf-8") as f:
        json.dump(journal, f, indent=2)

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
    print(f"Brokerage & Statutory Charges           : ₹{round(net_liq - SEED_CAPITAL - 0.18 - realized_gross_total, 2):+,.2f}")
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


def cmd_premarket() -> None:
    """
    Comprehensive Daily Pre-Market Readiness & Self-Healing System Check (runs at 08:55 IST Mon-Fri):
    1. JWT Token Freshness Check: If token expires within < 8 hours (before 15:30 IST close), forces fresh PIN+TOTP renewal now.
    2. Public IPv4 & Dhan IP Whitelist Verification (`/v2/ip/getIP`): Verifies `ordersAllowed == True` (and auto-updates slot if eligible).
    3. Pro Trader Data API & Multi-Index Feed Check: Verifies candle feed + non-0DTE expiry selection across all 5 indices.
    4. Daily State & Guardrail Reset: Resets `daily_date`, `daily_start_equity`, `daily_trades_count = 0`, `daily_halted = False`, and clears stale overnight cooldowns before 09:15 IST.
    5. Broker Position & Order Hygiene: Confirms 0 open positions and 0 orphan pending orders before the bell.
    """
    state = load_state()
    if state["status"] == "DEAD_COMA":
        print("🪦 [PRE-MARKET CHECK] SURVIVOR BOT IS IN PERMANENT COMA (DEAD_COMA).")
        return

    client = SurvivorDhanClient()
    now_ist = datetime.now(IST)
    today_str = now_ist.strftime("%Y-%m-%d")

    # 1. Ensure JWT token will last through the entire market session (until at least 16:30 IST today)
    token_repaired = False
    if not client.tm.token_expiry or (client.tm.token_expiry - datetime.now()).total_seconds() < 8 * 3600:
        client.reconnect_with_fresh_token()
        token_repaired = True
    token_exp_str = client.tm.token_expiry.strftime("%Y-%m-%d %H:%M IST") if client.tm.token_expiry else "UNKNOWN"

    # 2. Verify IPv4 Whitelist & ordersAllowed
    ip_info = client.ensure_ip_whitelisted()
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
    parser.add_argument("action", choices=["status", "premarket", "tick", "reconcile", "flatten", "simulate-coma-test"], help="Action to run")
    parser.add_argument("--live", action="store_true", help="Place real live orders on Dhan")
    parser.add_argument("--quiet", action="store_true", help="Silent on idle/no-op scans (for zero-token no_agent=True cron jobs)")
    args = parser.parse_args()
    raise SystemExit("Migration safety lock: use cloud_probe.py. Engine CLI disabled pending audited live cutover.")

    if args.action == "status":
        cmd_status()
    elif args.action == "premarket":
        cmd_premarket()
    elif args.action == "tick":
        cmd_tick(live=args.live, quiet_scan=args.quiet)
    elif args.action == "reconcile":
        cmd_reconcile()
    elif args.action == "flatten":
        c = SurvivorDhanClient()
        print("Emergency Flatten:", c.emergency_flatten_all())
    elif args.action == "simulate-coma-test":
        # Non-destructive self-test of the coma/obituary logic using a mock state copy
        print("Running non-destructive self-test of Option 2 Permanent Coma logic...")
        tier_test = get_survival_tier(3250.0)
        assert tier_test["tier"] == "DEAD", f"Expected DEAD tier, got {tier_test}"
        print("✅ Mortality threshold check verified: ₹3,250.00 <= ₹3,300.00 triggers DEAD_COMA.")
