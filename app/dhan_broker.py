"""
Dhan Broker Integration
Handles authentication (with auto-renewal for 24h tokens),
order placement, position management, and market data.
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger("dhan_broker")

# ─── Credentials ────────────────────────────────────────────────────
# Set these as environment variables or in a .env file
# NEVER commit real credentials to version control.
DHAN_CLIENT_ID = os.environ.get("DHAN_CLIENT_ID", "")
DHAN_ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN", "")
DHAN_PIN = os.environ.get("DHAN_PIN", "")
DHAN_TOTP = os.environ.get("DHAN_TOTP", "")
DHAN_APP_ID = os.environ.get("DHAN_APP_ID", "")
DHAN_APP_SECRET = os.environ.get("DHAN_APP_SECRET", "")

# State file to persist token across runs
TOKEN_STATE_FILE = os.path.join(os.path.dirname(__file__), ".dhan_token_state.json")


class CachedTokenUnavailable(RuntimeError):
    """A read-only connection requires an unexpired cached token."""


class DhanTokenManager:
    """
    Manages Dhan access tokens with automatic renewal.
    The free API token expires every 24 hours.
    """

    def __init__(self, client_id: str = "", pin: str = "", totp: str = "",
                 app_id: str = "", app_secret: str = "", *, allow_renewal: bool = True):
        # Read from os.environ at runtime (not import-time constants)
        # so that load_env_file() works correctly before token generation.
        self.client_id = client_id or os.environ.get("DHAN_CLIENT_ID", DHAN_CLIENT_ID)
        self.pin = pin or os.environ.get("DHAN_PIN", DHAN_PIN)
        self.totp = totp or os.environ.get("DHAN_TOTP", DHAN_TOTP)
        self.app_id = app_id or os.environ.get("DHAN_APP_ID", DHAN_APP_ID)
        self.app_secret = app_secret or os.environ.get("DHAN_APP_SECRET", DHAN_APP_SECRET)
        self.allow_renewal = allow_renewal
        self.access_token = None
        self.token_expiry = None

        # Try to load cached token
        self._load_state()

    def _load_state(self):
        """Load cached token if still valid."""
        if os.path.exists(TOKEN_STATE_FILE):
            try:
                with open(TOKEN_STATE_FILE, 'r') as f:
                    state = json.load(f)
                # Support both camelCase (Dhan API) and snake_case (our format)
                self.access_token = state.get("access_token") or state.get("accessToken")
                self.token_expiry = datetime.fromisoformat(state.get("expiry", "2000-01-01"))
                renewal_margin = timedelta(hours=1) if self.allow_renewal else timedelta()
                if datetime.now() >= self.token_expiry - renewal_margin:
                    logger.info("Cached token is too close to expiry for this connection.")
                    self.access_token = None
            except (json.JSONDecodeError, ValueError, KeyError):
                self.access_token = None

    def _save_state(self):
        """Persist token to disk."""
        state = {
            "access_token": self.access_token,
            "expiry": self.token_expiry.isoformat() if self.token_expiry else None,
        }
        with open(TOKEN_STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)

    def get_token(self) -> str:
        """
        Get a valid access token. Renew when required and permitted.
        Returns the token string.
        """
        if self.access_token and self.token_expiry and datetime.now() < self.token_expiry:
            return self.access_token

        if not self.allow_renewal:
            raise CachedTokenUnavailable("No valid cached Dhan token for this read-only connection.")
        return self._renew_token()

    def _renew_token(self) -> str:
        """Generate a new token using PIN + TOTP flow."""
        try:
            import requests
            import pyotp

            if self.pin and self.totp:
                url = "https://auth.dhan.co/app/generateAccessToken"
                last_err_msg = ""

                # Retry up to 3 times across TOTP 30s window boundaries or Dhan 2-min rate-limit cooldowns
                for attempt in range(3):
                    # Re-check disk state first in case another process just renewed the token
                    self._load_state()
                    if self.access_token and self.token_expiry and datetime.now() < self.token_expiry:
                        return self.access_token

                    # Wait away from the exact 30s TOTP boundary if <2s remaining
                    rem_in_window = 30 - (int(time.time()) % 30)
                    if rem_in_window <= 2:
                        time.sleep(rem_in_window + 1)

                    totp_code = pyotp.TOTP(self.totp).now()
                    params = {
                        "dhanClientId": self.client_id,
                        "pin": self.pin,
                        "totp": totp_code,
                    }
                    response = requests.post(url, params=params, timeout=15)
                    token_data = response.json()

                    if response.status_code == 200:
                        self.access_token = token_data.get("accessToken") or token_data.get("access_token")
                        if self.access_token:
                            self.token_expiry = datetime.now() + timedelta(hours=23)
                            self._save_state()
                            logger.info(f"Token renewed successfully. Expires: {self.token_expiry}")
                            logger.info(f"Account: {token_data.get('dhanClientName', 'N/A')} ({token_data.get('dhanClientId', 'N/A')})")
                            return self.access_token
                        else:
                            last_err_msg = token_data.get("message") or str(token_data)
                            logger.warning(f"Token renewal attempt {attempt + 1}/3 returned no token ({last_err_msg}); waiting 15s...")
                            time.sleep(15)
                    else:
                        last_err_msg = token_data.get("message", f"HTTP {response.status_code}")
                        logger.warning(f"Token renewal attempt {attempt + 1}/3 HTTP {response.status_code} ({last_err_msg}); waiting 15s...")
                        time.sleep(15)

                raise Exception(f"Token renewal failed after 3 attempts: {last_err_msg}")

            elif self.app_id and self.app_secret:
                from dhanhq import DhanLogin
                dhan_login = DhanLogin(self.client_id)
                logger.warning("OAuth flow requires manual browser login. Use PIN+TOTP for automation.")
                consent_id = dhan_login.generate_login_session(self.app_id, self.app_secret)
                raise Exception(
                    "OAuth flow: Complete browser login and provide token_id manually. "
                    f"Consent ID: {consent_id}"
                )
            else:
                raise Exception(
                    "No credentials configured. Set DHAN_PIN and DHAN_TOTP "
                    "environment variables for automated token renewal."
                )

        except ImportError as e:
            raise Exception(f"Required package not installed: {e}. Run: pip install requests pyotp")

    def get_dhan_context(self):
        """Return a DhanContext object for use with dhanhq classes."""
        from dhanhq import DhanContext
        token = self.get_token()
        return DhanContext(self.client_id, token)


# ─── Index Security ID Mapping ──────────────────────────────────────
# Dhan uses numeric security IDs instead of ticker symbols.
# These are the NSE FNO segment IDs for index futures.
# You may need to update these based on the current expiry contract.
DHAN_INDEX_SECURITY_IDS = {
    "Nifty 50":     {"security_id": "13",   "exchange": "IDX_I",  "fno_security_id": ""},
    "Nifty Bank":   {"security_id": "25",   "exchange": "IDX_I",  "fno_security_id": ""},
    "Nifty IT":     {"security_id": "24",   "exchange": "IDX_I",  "fno_security_id": ""},
    "Nifty Auto":   {"security_id": "41",   "exchange": "IDX_I",  "fno_security_id": ""},
    "Nifty Pharma": {"security_id": "19",   "exchange": "IDX_I",  "fno_security_id": ""},
    "Nifty Metal":  {"security_id": "46",   "exchange": "IDX_I",  "fno_security_id": ""},
    "Nifty FMCG":   {"security_id": "18",   "exchange": "IDX_I",  "fno_security_id": ""},
}


class DhanBroker:
    """
    Dhan broker adapter implementing the BrokerAdapter interface.
    Handles orders, positions, and market data via DhanHQ v2.2.0 API.
    """

    def __init__(self, token_manager: DhanTokenManager = None):
        self.token_manager = token_manager or DhanTokenManager()
        self._dhan = None
        self._initialized = False

    def _ensure_connected(self):
        """Lazy-initialize the Dhan client."""
        if not self._initialized:
            from dhanhq import dhanhq
            context = self.token_manager.get_dhan_context()
            self._dhan = dhanhq(context)
            self._initialized = True

    @property
    def dhan(self):
        self._ensure_connected()
        return self._dhan

    # ─── Order Management ───────────────────────────────────────────

    def place_order(self, symbol: str, direction: str, qty: int, price: float,
                    sl: float = 0, product_type: str = "INTRADAY",
                    order_type: str = "MARKET", exchange_segment: str = "NSE_FNO",
                    security_id: str = None) -> dict:
        """
        Place an order via Dhan API.

        Args:
            symbol: Human-readable index/stock name
            direction: 'LONG' or 'SHORT'
            qty: Quantity
            price: Price (0 for market orders)
            sl: Stop-loss price (0 = no SL)
            product_type: INTRADAY, CNC, MARGIN, CO, BO
            order_type: MARKET, LIMIT, STOP_LOSS, STOP_LOSS_MARKET
            exchange_segment: NSE_EQ, NSE_FNO, BSE_EQ, etc.
            security_id: Override security ID (auto-detected if None)

        Returns:
            Order response dict with orderId and orderStatus
        """
        self._ensure_connected()

        # Map direction to Dhan transaction type
        txn_type = "BUY" if direction == "LONG" else "SELL"

        # Resolve security ID
        if security_id is None:
            sec_info = DHAN_INDEX_SECURITY_IDS.get(symbol, {})
            security_id = sec_info.get("fno_security_id") or sec_info.get("security_id", "")

        if not security_id:
            logger.error(f"No security ID found for {symbol}. Cannot place order.")
            return {"error": f"No security ID for {symbol}"}

        # Build order params
        order_params = {
            "security_id": str(security_id),
            "exchange_segment": exchange_segment,
            "transaction_type": txn_type,
            "quantity": qty,
            "order_type": order_type,
            "product_type": product_type,
            "price": price,
        }

        # Add SL if provided
        if sl > 0 and order_type in ("STOP_LOSS", "STOP_LOSS_MARKET"):
            order_params["trigger_price"] = sl

        try:
            logger.info(f"Placing {txn_type} order: {qty}x {symbol} @ {price} (SL: {sl})")
            response = self.dhan.place_order(**order_params)
            logger.info(f"Order placed: {response}")
            return response
        except Exception as e:
            logger.error(f"Order placement failed: {e}")
            return {"error": str(e)}

    def modify_order(self, order_id: str, **kwargs) -> dict:
        """Modify a pending order."""
        self._ensure_connected()
        try:
            response = self.dhan.modify_order(order_id=order_id, **kwargs)
            return response
        except Exception as e:
            logger.error(f"Order modification failed: {e}")
            return {"error": str(e)}

    def cancel_order(self, order_id: str) -> dict:
        """Cancel a pending order."""
        self._ensure_connected()
        try:
            response = self.dhan.cancel_order(order_id)
            return response
        except Exception as e:
            logger.error(f"Order cancellation failed: {e}")
            return {"error": str(e)}

    def get_order_list(self) -> list:
        """Get all orders for the day."""
        self._ensure_connected()
        try:
            return self.dhan.get_order_list()
        except Exception as e:
            logger.error(f"Failed to fetch order list: {e}")
            return []

    def get_order_status(self, order_id: str) -> dict:
        """Get status of a specific order."""
        self._ensure_connected()
        try:
            return self.dhan.get_order_by_id(order_id)
        except Exception as e:
            logger.error(f"Failed to fetch order status: {e}")
            return {"error": str(e)}

    # ─── Position Management ────────────────────────────────────────

    def get_positions(self) -> list:
        """Get all open positions."""
        self._ensure_connected()
        try:
            return self.dhan.get_positions()
        except Exception as e:
            logger.error(f"Failed to fetch positions: {e}")
            return []

    def get_holdings(self) -> list:
        """Get all holdings."""
        self._ensure_connected()
        try:
            return self.dhan.get_holdings()
        except Exception as e:
            logger.error(f"Failed to fetch holdings: {e}")
            return []

    # ─── Market Data ────────────────────────────────────────────────

    def get_intraday_data(self, security_id: str, exchange_segment: str = "IDX_I",
                          instrument_type: str = "INDEX",
                          from_date: str = None, to_date: str = None) -> dict:
        """
        Fetch intraday minute data from Dhan.
        Useful as a replacement for Yahoo Finance during live trading.

        Args:
            security_id: Dhan security ID
            exchange_segment: IDX_I for indices, NSE_EQ for stocks, NSE_FNO for F&O
            instrument_type: INDEX, EQUITY, FUTCOM, etc.
            from_date: Start date (YYYY-MM-DD HH:MM:SS format)
            to_date: End date (YYYY-MM-DD HH:MM:SS format)

        Returns:
            OHLCV data dict
        """
        self._ensure_connected()

        if from_date is None:
            from_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S")
        if to_date is None:
            to_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            data = self.dhan.intraday_minute_data(
                security_id=security_id,
                exchange_segment=exchange_segment,
                instrument_type=instrument_type,
                from_date=from_date,
                to_date=to_date
            )
            return data
        except Exception as e:
            logger.error(f"Failed to fetch intraday data: {e}")
            return {"error": str(e)}

    def get_historical_data(self, security_id: str, exchange_segment: str = "IDX_I",
                            instrument_type: str = "INDEX",
                            from_date: str = None, to_date: str = None) -> dict:
        """Fetch daily historical data."""
        self._ensure_connected()

        if from_date is None:
            from_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        if to_date is None:
            to_date = datetime.now().strftime("%Y-%m-%d")

        try:
            data = self.dhan.historical_daily_data(
                security_id=security_id,
                exchange_segment=exchange_segment,
                instrument_type=instrument_type,
                from_date=from_date,
                to_date=to_date
            )
            return data
        except Exception as e:
            logger.error(f"Failed to fetch historical data: {e}")
            return {"error": str(e)}

    # ─── Account Info ───────────────────────────────────────────────

    def get_fund_limits(self) -> dict:
        """Get available funds and margins."""
        self._ensure_connected()
        try:
            return self.dhan.get_fund_limits()
        except Exception as e:
            logger.error(f"Failed to fetch fund limits: {e}")
            return {"error": str(e)}

    # ─── Rate Limiting ──────────────────────────────────────────────
    # Dhan limits: 10 orders/sec, 250/min, 1000/hr, 7000/day
    _last_order_time = 0

    def _rate_limit_check(self):
        """Ensure we don't exceed Dhan's rate limits."""
        elapsed = time.time() - self._last_order_time
        if elapsed < 0.1:  # 10 orders/sec = 100ms between orders
            time.sleep(0.1 - elapsed)
        self._last_order_time = time.time()


def load_env_file(env_path: str = None):
    """Load .env file into environment variables."""
    if env_path is None:
        env_path = os.path.join(os.path.dirname(__file__), ".env")

    if not os.path.exists(env_path):
        return False

    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key.strip()] = value.strip().strip('"').strip("'")
    return True
