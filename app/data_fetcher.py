"""
Data Fetcher - Pulls historical 15-min candle data from yfinance.
For live trading, this module can be swapped to use broker APIs.
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from config import UNIVERSE, BACKTEST_DAYS


def fetch_historical_data(ticker: str, days: int = BACKTEST_DAYS, interval: str = "15m") -> pd.DataFrame:
    """
    Fetch historical OHLCV data for a given ticker.
    yfinance limits 15m data to 60 days, so we fetch 60d max per call
    and concatenate if needed.
    """
    # yfinance allows max 60 days for 15m interval
    if days <= 60:
        period = f"{days}d"
        df = yf.download(ticker, period=period, interval=interval, progress=False)
    else:
        # Fetch in two windows: recent 60d + prior window
        df_recent = yf.download(ticker, period="60d", interval=interval, progress=False)
        # For older data, try 5m (max 60d) or daily fallback
        remaining = days - 60
        if remaining > 0:
            end_date = datetime.now() - timedelta(days=60)
            start_date = end_date - timedelta(days=remaining)
            try:
                df_older = yf.download(
                    ticker,
                    start=start_date.strftime("%Y-%m-%d"),
                    end=end_date.strftime("%Y-%m-%d"),
                    interval="15m",
                    progress=False
                )
                df = pd.concat([df_older, df_recent])
            except Exception:
                df = df_recent
                print(f"  [WARN] Could only fetch last 60 days for {ticker}")

    if df is not None and not df.empty:
        # Flatten multi-level columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
        df.dropna(inplace=True)
    return df


def fetch_all_indices(days: int = BACKTEST_DAYS) -> dict:
    """Fetch data for all indices in the universe."""
    data = {}
    for name, ticker in UNIVERSE.items():
        print(f"  Fetching {name} ({ticker})...")
        df = fetch_historical_data(ticker, days=days)
        if df is not None and not df.empty:
            data[name] = df
            print(f"    ✓ {len(df)} candles fetched ({df.index[0]} → {df.index[-1]})")
        else:
            print(f"    ✗ No data for {name}")
    return data


if __name__ == "__main__":
    print("=== Fetching historical data for all NSE indices ===")
    data = fetch_all_indices()
    print(f"\nTotal indices fetched: {len(data)}")
    for name, df in data.items():
        print(f"  {name}: {len(df)} candles, range {df.index[0]} → {df.index[-1]}")
