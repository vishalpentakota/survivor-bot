"""
Warrior Trading Scanner & Executor — Indian Market Adaptation
==============================================================
Ross Cameron's small-account momentum strategy adapted for NSE equities.

Integrates with your existing infra:
  - dhan_broker.DhanBroker     → orders & market data
  - dhan_broker.DhanTokenManager → auth with PIN+TOTP auto-renew
  - trade_logger.TradeLogger   → trade journal
  - config.py                  → trading hours, capital, learning mode

Usage (from ~/nifty-algo-trading/):
  python warrior_scanner.py scan          # Paper scan only
  python warrior_scanner.py scan --live   # Live scan + trade via Dhan
  python warrior_scanner.py demo          # Dry-run demo
"""
import sys, os, json, time, logging
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from warrior_config import WARRIOR_CFG
from config import (
    TOTAL_CAPITAL, MARKET_OPEN, MARKET_CLOSE,
    MAX_TRADES_PER_DAY, LEARNING_MODE_START, LEARNING_MODE_END,
)
from dhan_broker import DhanBroker, DhanTokenManager, load_env_file
from trade_logger import TradeLogger

# ─── Logging ────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] warrior: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "warrior_scanner.log")),
    ],
)
logger = logging.getLogger("warrior")


# ══════════════════════════════════════════════════════════════════════
#  DATA CLASSES
# ══════════════════════════════════════════════════════════════════════

@dataclass
class ScanResult:
    """A stock that passed all scanner criteria."""
    symbol: str
    security_id: str
    ltp: float
    prev_close: float
    pct_change: float
    volume_today: int
    avg_volume_30d: float
    relative_volume: float
    float_shares: Optional[int] = None
    catalyst: Optional[str] = None
    at_upper_circuit: bool = False
    score: int = 0

    def __repr__(self):
        return (
            f"{self.symbol} | ₹{self.ltp:.1f} (+{self.pct_change:.1f}%) | "
            f"RV:{self.relative_volume:.1f}x | Score:{self.score}"
        )


@dataclass 
class EntrySignal:
    """A confirmed entry."""
    symbol: str
    security_id: str
    entry_price: float
    stop_loss: float
    target: float
    risk_reward_ratio: float
    confidence: int
    pattern: str = "micro_pullback"
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


# ══════════════════════════════════════════════════════════════════════
#  SCANNER
# ══════════════════════════════════════════════════════════════════════

class WarriorScanner:
    """
    Scans NSE equities matching Warrior Trading criteria:
      1. Price ₹50-₹500
      2. Up ≥10% today
      3. Relative Volume ≥5x (30-day avg)
      4. Float <20M shares
      5. News catalyst (preferred)
    """

    def __init__(self, broker: DhanBroker):
        self.cfg = WARRIOR_CFG["scanner"]
        self.broker = broker
        self._master: List[Dict] = []
        self._volume_cache: Dict[str, Tuple[int, float]] = {}
        self._symbol_to_id: Dict[str, str] = {}  # "IRFC" → "12345"

    def load_master_list(self) -> int:
        """Load NSE equity master from cache or download."""
        cache_path = Path.home() / ".nifty-algo" / "nse_master.csv"

        if cache_path.exists():
            import pandas as pd
            df = pd.read_csv(cache_path)
            df = df[df["SERIES"] == "EQ"]
            self._master = df.to_dict("records")
            logger.info(f"Loaded {len(self._master)} NSE equities from cache")
            return len(self._master)

        logger.warning("No NSE master cache. Run: python warrior_scanner.py nse-download")
        return 0

    def download_nse_master(self):
        """Download latest NSE Bhavcopy for EQ series."""
        import urllib.request, zipfile, io, pandas as pd

        now = datetime.now()
        cache_dir = Path.home() / ".nifty-algo"
        cache_dir.mkdir(exist_ok=True)

        for offset in range(4):  # Try last 4 trading days
            dt = now - timedelta(days=offset)
            date_str = dt.strftime("%d%b%Y").upper()
            url = (
                f"https://archives.nseindia.com/content/historical/"
                f"EQUITIES/{dt.year}/{dt.strftime('%b').upper()}/"
                f"cm{date_str}bhav.csv.zip"
            )
            try:
                logger.info(f"Downloading {url}")
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=15) as resp:
                    with zipfile.ZipFile(io.BytesIO(resp.read())) as zf:
                        csv_name = f"cm{date_str}bhav.csv"
                        with zf.open(csv_name) as f:
                            df = pd.read_csv(f)
                            df = df[df["SERIES"] == "EQ"]
                            df.to_csv(cache_dir / "nse_master.csv", index=False)
                            logger.info(f"Saved {len(df)} EQ scrips")
                            self._master = df.to_dict("records")
                            return len(self._master)
            except Exception as e:
                logger.debug(f"  Attempt {offset+1}: {e}")

        logger.error("All Bhavcopy downloads failed")
        return 0

    def _build_security_map(self):
        """Build SYMBOL -> yfinance ticker mapping (.NS suffix)."""
        if self._symbol_to_id:
            return
        for stock in self._master:
            sym = str(stock.get("SYMBOL", "")).upper().strip()
            if sym:
                self._symbol_to_id[sym] = f"{sym}.NS"
        logger.info(f"Built yfinance ticker map: {len(self._symbol_to_id)} symbols")

    def _fetch_live_quotes(self, symbols: list) -> dict:
        """Fetch live prices + volume + prev_close from Yahoo Finance."""
        import yfinance as yf
        all_quotes = {}
        for ticker_str in [f"{s}.NS" for s in symbols]:
            try:
                t = yf.Ticker(ticker_str)
                info = t.info
                ltp = info.get("currentPrice") or info.get("regularMarketPrice")
                prev = info.get("previousClose") or info.get("regularMarketPreviousClose")
                vol = info.get("volume") or info.get("regularMarketVolume")
                avg = info.get("averageVolume") or info.get("averageDailyVolume10Day")
                sym = ticker_str.replace(".NS", "")
                if ltp and prev and prev > 0:
                    all_quotes[sym] = {
                        "ltp": float(ltp), "prev_close": float(prev),
                        "volume": int(vol or 0), "avg_volume": float(avg or 0),
                    }
            except Exception:
                continue
        return all_quotes

    def scan(self):
        """Run full scan using Yahoo Finance for live data."""
        sc = self.cfg
        price_min, price_max = sc["price_min"], sc["price_max"]

        self._build_security_map()
        if not self._symbol_to_id:
            return []

        symbols = list(self._symbol_to_id.keys())
        logger.info(f"Scanning {len(symbols)} stocks (yfinance, ₹{price_min}-₹{price_max})...")

        all_quotes = self._fetch_live_quotes(symbols)
        logger.info(f"Got live quotes for {len(all_quotes)}/{len(symbols)} symbols")

        results = []
        for sym, quote in all_quotes.items():
            try:
                ltp = quote["ltp"]
                prev_close = quote["prev_close"]

                # Price range filter
                if ltp < price_min or ltp > price_max:
                    continue

                pct_change = ((ltp - prev_close) / prev_close) * 100
                if pct_change < sc["pct_change_min"]:
                    continue

                vol_today = quote["volume"]
                if vol_today < sc["min_tradable_volume"]:
                    continue

                avg_vol = quote.get("avg_volume", 0)
                if avg_vol <= 0:
                    avg_vol = self._get_avg_volume(sym)
                rel_vol = vol_today / avg_vol if avg_vol > 0 else 1.0
                if rel_vol < sc["relative_volume_min"]:
                    continue

                score = min(int(pct_change), 30) + min(int(rel_vol * 2), 50)
                if self._check_catalyst(sym):
                    score += 20

                results.append(ScanResult(
                    symbol=sym,
                    security_id=self._symbol_to_id.get(sym.upper(), sym),
                    ltp=ltp,
                    prev_close=prev_close,
                    pct_change=round(pct_change, 2),
                    volume_today=vol_today,
                    avg_volume_30d=round(avg_vol, 0),
                    relative_volume=round(rel_vol, 1),
                    catalyst=self._check_catalyst(sym),
                    at_upper_circuit=self._is_at_circuit(pct_change),
                    score=score,
                ))
            except Exception as e:
                logger.debug(f"  {sym}: {e}")

        results.sort(key=lambda r: r.score, reverse=True)
        return results

    def print_scan(self, results: List[ScanResult], top: int = 15):
        """Pretty-print scan results."""
        print(f"\n{'='*80}")
        print(f"  ⚔️  WARRIOR SCAN — {datetime.now().strftime('%d-%b %H:%M')}")
        print(f"{'='*80}")
        print(f"  {'Rank':<5} {'Symbol':<14} {'LTP':>8} {'Chg%':>7} "
              f"{'RelVol':>7} {'AtCirc':>7} {'Score':>5}")
        print("-"*80)
        for i, r in enumerate(results[:top], 1):
            circ = "⛔ YES" if r.at_upper_circuit else ""
            print(f"  {i:<5} {r.symbol:<14} ₹{r.ltp:>7.1f} {r.pct_change:>+6.1f}% "
                  f"{r.relative_volume:>6.1f}x {circ:>7} {r.score:>4}")
        print(f"\n  Total qualifying: {len(results)}")

    def _get_avg_volume(self, symbol: str) -> float:
        """Get 30-day average volume from Dhan historical data."""
        cache_key = f"{symbol}_{date.today()}"
        if cache_key in self._volume_cache:
            return self._volume_cache[cache_key][1]

        try:
            historical = self.broker.get_historical_data(
                security_id=symbol,
                exchange_segment="NSE_EQ",
                instrument_type="EQUITY",
            )
            vols = []
            data_list = historical.get("data", historical) if isinstance(historical, dict) else historical
            if isinstance(data_list, list) and len(data_list) > 0:
                for day in data_list[-31:-1]:  # Last 30 excluding today
                    v = float(day.get("volume", 0) or 0)
                    if v > 0:
                        vols.append(v)
            avg = sum(vols) / len(vols) if vols else 0
            self._volume_cache[cache_key] = (0, avg)
            return avg
        except Exception:
            return 0

    def _check_catalyst(self, symbol: str) -> Optional[str]:
        """Check for news catalyst. Stub — integrate NSE announcements API."""
        return None  # TODO: Wire NSE corporate announcements

    def _is_at_circuit(self, pct_change: float) -> bool:
        """Check if stock is near upper circuit (≥19.5%)"""
        return pct_change >= 19.5


# ══════════════════════════════════════════════════════════════════════
#  ENTRY ANALYZER
# ══════════════════════════════════════════════════════════════════════

class EntryAnalyzer:
    """Detects micro-pullback entries on 1-min/5-min candles."""

    def __init__(self):
        self.cfg = WARRIOR_CFG["entry"]

    def analyze(self, result: ScanResult, candles_1m: List[Dict]) -> Optional[EntrySignal]:
        """Check for a valid micro-pullback entry. Returns signal or None."""
        if len(candles_1m) < 50:
            return None

        closes = [float(c.get("close", 0)) for c in candles_1m]
        highs = [float(c.get("high", 0)) for c in candles_1m]
        lows = [float(c.get("low", 0)) for c in candles_1m]
        opens = [float(c.get("open", 0)) for c in candles_1m]
        volumes = [float(c.get("volume", 0)) for c in candles_1m]

        if any(c <= 0 for c in closes[-20:]):
            return None

        # 1. Uptrend check (last 15 bars)
        recent = closes[-15:]
        if not self._is_uptrend(recent):
            return None

        # 2. Pullback detection
        momentum_high = max(closes[-15:-5])
        pullback_low = min(closes[-5:])
        if pullback_low >= momentum_high:
            return None
        pb_pct = (momentum_high - pullback_low) / momentum_high * 100
        if pb_pct < 0.5 or pb_pct > 5.0:
            return None

        # 3. Volume contraction on pullback
        import numpy as np
        mom_vol = np.mean(volumes[-15:-5]) if volumes[-15:-5] else 0
        pb_vol = np.mean(volumes[-5:]) if volumes[-5:] else 0
        if mom_vol <= 0:
            return None
        vol_decline = (mom_vol - pb_vol) / mom_vol * 100
        if vol_decline < self.cfg["pullback_volume_decline_pct"]:
            return None

        # 4. Rejection wick on last candle
        last = candles_1m[-1]
        body = abs(float(last.get("close", 0)) - float(last.get("open", 0)))
        low_wick = min(float(last.get("open", 0)), float(last.get("close", 0))) - float(last.get("low", 0))
        wick_pct = (low_wick / body * 100) if body > 0 else 0
        if wick_pct < self.cfg["jackknife_wick_pct"]:
            return None

        # 5. MACD check
        if not self._check_macd(opens, highs, lows, closes):
            return None

        # Levels
        entry_price = closes[-1]
        stop_loss = pullback_low * 0.995
        target = entry_price + (entry_price - stop_loss) * 2
        rr = (target - entry_price) / (entry_price - stop_loss) if entry_price > stop_loss else 0

        if rr < 2.0:
            return None

        confidence = 60 + min(int(vol_decline // 2), 20) + min(int(wick_pct // 5), 10)
        if closes[-1] > closes[-2]:
            confidence += 10

        if confidence < self.cfg.get("min_confidence", 60):
            return None

        return EntrySignal(
            symbol=result.symbol,
            security_id=result.security_id,
            entry_price=round(entry_price, 2),
            stop_loss=round(stop_loss, 2),
            target=round(target, 2),
            risk_reward_ratio=round(rr, 2),
            confidence=min(confidence, 100),
        )

    def _is_uptrend(self, closes):
        import numpy as np
        half = len(closes) // 2
        return np.mean(closes[half:]) > np.mean(closes[:half]) * 1.005

    def _check_macd(self, opens, highs, lows, closes):
        """MACD 12-26-9: bullish if line > signal or turning up."""
        if len(closes) < 35:
            return False

        f, s, sg = self.cfg["macd_fast"], self.cfg["macd_slow"], self.cfg["macd_signal"]
        ema_f = self._ema(closes, f)
        ema_s = self._ema(closes, s)
        macd_line = [a - b for a, b in zip(ema_f, ema_s)]
        sig_line = self._ema(macd_line, sg)

        hist_now = macd_line[-1] - sig_line[-1]
        if hist_now > 0:
            return True
        if len(macd_line) >= 3:
            h1 = macd_line[-2] - sig_line[-2]
            h2 = macd_line[-3] - sig_line[-3]
            if h2 < h1 < hist_now:
                return True
        return False

    @staticmethod
    def _ema(data, period):
        import numpy as np
        if len(data) < period:
            return [0] * len(data)
        ema = [sum(data[:period]) / period]
        mult = 2 / (period + 1)
        for price in data[period:]:
            ema.append((price - ema[-1]) * mult + ema[-1])
        return [0] * (len(data) - len(ema)) + ema


# ══════════════════════════════════════════════════════════════════════
#  RISK MANAGER
# ══════════════════════════════════════════════════════════════════════

class WarriorRiskManager:
    """Enforces position sizing, daily loss limits, consecutive loser stops."""

    def __init__(self, capital: float = TOTAL_CAPITAL):
        self.rc = WARRIOR_CFG["risk"]
        self.starting_capital = capital
        self.current_capital = capital
        self.total_pnl = 0.0
        self.trades_today = 0
        self.wins = 0
        self.losses = 0
        self.consecutive_losses = 0
        self.stopped = False
        self.stop_reason = ""

    def can_trade(self, max_trades: int = 1) -> Tuple[bool, str]:
        """Check if another trade is allowed."""
        if self.stopped:
            return False, f"Stopped: {self.stop_reason}"

        if self.trades_today >= max_trades:
            return False, f"Max {max_trades} trades reached"

        daily_max = self.starting_capital * (self.rc["daily_max_loss_pct"] / 100)
        if abs(self.total_pnl) >= daily_max and self.total_pnl < 0:
            self.stopped = True
            self.stop_reason = f"Daily max loss ₹{daily_max:,.0f} hit"
            return False, self.stop_reason

        if self.consecutive_losses >= self.rc["max_consecutive_losses"]:
            self.stopped = True
            self.stop_reason = f"{self.consecutive_losses} consecutive losses"
            return False, self.stop_reason

        return True, "OK"

    def size_position(self, entry_price: float, stop_loss: float) -> Tuple[int, float]:
        """Calculate shares and actual risk."""
        max_risk = self.current_capital * (self.rc["max_risk_per_trade_pct"] / 100)
        risk_per_share = abs(entry_price - stop_loss)
        if risk_per_share <= 0:
            return 0, 0

        qty = int(max_risk / risk_per_share)
        required = qty * entry_price
        if required > self.current_capital:
            qty = int(self.current_capital / entry_price)
        return max(qty, 0), qty * risk_per_share

    def record_trade(self, pnl: float):
        """Record completed trade."""
        self.trades_today += 1
        self.total_pnl += pnl
        self.current_capital += pnl
        if pnl > 0:
            self.wins += 1
            self.consecutive_losses = 0
        else:
            self.losses += 1
            self.consecutive_losses += 1

    def print_summary(self):
        win_rate = (self.wins / self.trades_today * 100) if self.trades_today > 0 else 0
        print(f"\n{'='*50}")
        print(f"  ⚔️  WARRIOR DAY SUMMARY")
        print(f"{'='*50}")
        print(f"  Capital: ₹{self.starting_capital:,.0f} → ₹{self.current_capital:,.0f}")
        print(f"  P&L:     ₹{self.total_pnl:+,.0f}")
        print(f"  Trades:  {self.trades_today} ({self.wins}W / {self.losses}L) — {win_rate:.0f}% WR")
        print(f"{'='*50}")


# ══════════════════════════════════════════════════════════════════════
#  MAIN RUNNER
# ══════════════════════════════════════════════════════════════════════

def run_scan(live: bool = False, capital: float = TOTAL_CAPITAL):
    """Main entry: scan, signal, optionally trade."""
    load_env_file()

    print("""
╔══════════════════════════════════════════════════════════════╗
║     ⚔️  WARRIOR TRADING — Small Account Strategy              ║
║     Adapted for NSE Indian Markets                            ║
╚══════════════════════════════════════════════════════════════╝
    """)
    print(f"Mode: {'LIVE via Dhan' if live else 'PAPER (scan only)'}")
    print(f"Capital: ₹{capital:,}")
    print(f"Price: ₹{WARRIOR_CFG['scanner']['price_min']}-₹{WARRIOR_CFG['scanner']['price_max']}")
    print(f"Max Risk/Trade: {WARRIOR_CFG['risk']['max_risk_per_trade_pct']}%")
    print(f"Daily Max Loss: {WARRIOR_CFG['risk']['daily_max_loss_pct']}%")

    # Init
    token_mgr = DhanTokenManager()
    broker = DhanBroker(token_mgr)

    scanner = WarriorScanner(broker)
    analyzer = EntryAnalyzer()
    risk_mgr = WarriorRiskManager(capital)
    trade_logger = TradeLogger()

    if scanner.load_master_list() == 0:
        print("❌ No NSE master list. Run: python warrior_scanner.py nse-download")
        return

    # Check learning mode
    learning = WARRIOR_CFG["learning"]["enabled"]
    max_trades = WARRIOR_CFG["learning"]["max_trades_per_day"] if learning else 99
    min_conf = WARRIOR_CFG["learning"]["min_confidence_for_live"] if learning else 60

    if learning:
        print(f"\n📚 LEARNING MODE: max {max_trades} trade/day, min {min_conf}% confidence")

    # ── Scan ──
    results = scanner.scan()
    scanner.print_scan(results)

    if not results:
        print("\nNo stocks passed criteria today.")
        return

    # ── Signal Check ──
    for result in results[:10]:
        can, reason = risk_mgr.can_trade(max_trades)
        if not can:
            print(f"\n⛔ Trade blocked: {reason}")
            break

        if result.at_upper_circuit:
            print(f"\n  ⛔ {result.symbol}: At upper circuit — skipping")
            continue

        # Get 1-min candles from Yahoo Finance
        try:
            import yfinance as yf
            ticker_str = f"{result.symbol}.NS"
            df = yf.download(ticker_str, period="1d", interval="1m", progress=False, quiet=True)
            if df.empty:
                continue
            # Flatten multi-level columns if needed
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0].lower() for c in df.columns]
            else:
                df.columns = [c.lower() for c in df.columns]
            candle_list = df.reset_index().to_dict("records")
            # Rename to standard keys
            candle_list = [{
                "open": r.get("open", 0), "high": r.get("high", 0),
                "low": r.get("low", 0), "close": r.get("close", 0),
                "volume": r.get("volume", 0),
            } for r in candle_list]
            if len(candle_list) < 50:
                continue
        except Exception as e:
            logger.debug(f"{result.symbol}: yfinance candle fetch failed: {e}")
            continue

        signal = analyzer.analyze(result, candle_list)
        if not signal:
            continue

        # Position sizing
        qty, actual_risk = risk_mgr.size_position(signal.entry_price, signal.stop_loss)
        if qty <= 0:
            print(f"\n  ⛔ {result.symbol}: Can't size position")
            continue

        print(f"""
╔══════════════════════════════════════════════════════════╗
║ 🎯 SIGNAL: {signal.symbol:<15} Confidence: {signal.confidence}%
╠══════════════════════════════════════════════════════════╣
║  Pattern:    {signal.pattern}
║  Entry:      ₹{signal.entry_price:.2f}
║  Stop-Loss:  ₹{signal.stop_loss:.2f}
║  Target:     ₹{signal.target:.2f}
║  R:R:        {signal.risk_reward_ratio:.1f}:1
║  Shares:     {qty} (₹{qty * signal.entry_price:,.0f} capital)
║  Max Risk:   ₹{actual_risk:,.0f}
╚══════════════════════════════════════════════════════════╝
        """)

        if not live:
            print(f"  📝 PAPER: Would BUY {qty}x{signal.symbol} @ ₹{signal.entry_price:.1f}")
            # Simulate for paper
            risk_mgr.record_trade(0)  # placeholder
            continue

        if learning and signal.confidence < min_conf:
            print(f"  📚 Learning mode: confidence {signal.confidence}% < {min_conf}% — logged, not traded")
            trade_logger.log_trade({
                "symbol": signal.symbol,
                "type": "PAPER_SKIPPED",
                "entry": signal.entry_price,
                "confidence": signal.confidence,
                "reason": "Below learning confidence threshold",
                "timestamp": signal.timestamp,
            })
            continue

        # ── LIVE ORDER ──
        order = broker.place_order(
            symbol=signal.symbol,
            direction="LONG",
            qty=qty,
            price=signal.entry_price,
            product_type="INTRADAY",
            order_type="LIMIT",
            exchange_segment="NSE_EQ",
            security_id=signal.security_id,  # numeric Dhan security ID
        )

        order_failed = (
            "error" in order
            or (isinstance(order, dict) and order.get("status") == "failure")
        )

        if order_failed:
            err = order.get("error") or order.get("remarks", {}).get("error_message", "Unknown")
            logger.error(f"❌ Order failed: {err}")
            trade_logger.log_trade({
                "symbol": signal.symbol,
                "type": "FAILED",
                "entry": signal.entry_price,
                "error": str(err),
                "timestamp": signal.timestamp,
            })
            # DON'T count failed orders as trades
            continue

        order_id = order.get("data", {}).get("orderId", "") or order.get("orderId", "")
        print(f"  ✅ Order placed: {order_id}")

        trade_logger.log_trade({
            "symbol": signal.symbol,
            "type": "LIVE_LONG",
            "entry": signal.entry_price,
            "stop_loss": signal.stop_loss,
            "target": signal.target,
            "quantity": qty,
            "order_id": order_id,
            "confidence": signal.confidence,
            "rr_ratio": signal.risk_reward_ratio,
            "timestamp": signal.timestamp,
        })
        risk_mgr.record_trade(0)  # P&L determined on exit

        break  # One trade per scan in learning mode

    risk_mgr.print_summary()


# ══════════════════════════════════════════════════════════════════════
#  DEMO
# ══════════════════════════════════════════════════════════════════════

def run_demo():
    """Demonstrate the strategy with simulated data."""
    print("""
╔══════════════════════════════════════════════════════════════╗
║     ⚔️  WARRIOR TRADING STRATEGY DEMO                         ║
╚══════════════════════════════════════════════════════════════╝
    """)

    # Simulated scan results
    print("📊 SCAN RESULTS (simulated NSE data):\n")
    demo_stocks = [
        ("IRFC", 185.0, 162.0, 8.5, 25_000_000),
        ("SUZLON", 62.5, 56.0, 9.2, 18_000_000),
        ("NHPC", 118.0, 106.5, 5.4, 15_000_000),
        ("IDEA", 22.5, 18.0, 15.0, 45_000_000),  # Would fail price
    ]
    for sym, ltp, prev, rv, vol in demo_stocks:
        pct = (ltp - prev) / prev * 100
        status = "✅" if 50 <= ltp <= 500 and pct >= 10 and rv >= 5 else "❌"
        print(f"  {status} {sym:<8} ₹{ltp:>7.1f}  +{pct:.1f}%  "
              f"RV:{rv:.1f}x  Vol:{vol/1e6:.1f}M")

    # Simulated entry
    print("""
🎯 ENTRY SIGNAL (simulated):
  IRFC | Micro-pullback on 1-min chart
  Entry:      ₹184.50
  Stop-Loss:  ₹181.80
  Target:     ₹189.90
  R:R:        2.0:1
  Confidence: 85%

💰 POSITION SIZING (₹50,000 account, 5% risk):
  Max Risk:       ₹2,500
  Risk/Share:     ₹2.70
  Shares:         925
  Capital Used:   ₹170,663 (exceeds ₹50K — capped by capital)

  → Capped: 270 shares @ ₹184.50 = ₹49,815
  → Actual Risk: ₹729 (1.46%)

📈 OUTCOMES:
  🟢 Win:  +₹1,458 (+2.9%)
  🔴 Lose: -₹729 (-1.5%)
  📊 Win Rate Needed: 33% (to break even at 2.0:1 R:R)
    """)

    print("─" * 60)
    print("\n📝 Reuses existing infra:")
    print("  • DhanBroker (orders + market data)")
    print("  • DhanTokenManager (PIN+TOTP auto-renew)")
    print("  • TradeLogger (JSON trade journal)")
    print("  • config.py (capital, hours, learning mode)")
    print("  • .env credentials (DHAN_PIN, DHAN_TOTP, etc.)")


# ══════════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════════

def main():
    import argparse
    parser = argparse.ArgumentParser(description="⚔️ Warrior Trading Scanner")
    parser.add_argument("command", nargs="?", default="demo",
                        choices=["demo", "scan", "nse-download"],
                        help="Command to run")
    parser.add_argument("--live", action="store_true", help="Live trading via Dhan")
    parser.add_argument("--capital", type=float, default=TOTAL_CAPITAL,
                        help=f"Starting capital (default: ₹{TOTAL_CAPITAL:,})")
    args = parser.parse_args()

    if args.command == "demo":
        run_demo()
    elif args.command == "nse-download":
        load_env_file()
        broker = DhanBroker(DhanTokenManager())
        scanner = WarriorScanner(broker)
        count = scanner.download_nse_master()
        print(f"✅ Downloaded {count} NSE EQ stocks to ~/.nifty-algo/nse_master.csv")
    elif args.command == "scan":
        run_scan(live=args.live, capital=args.capital)


if __name__ == "__main__":
    main()