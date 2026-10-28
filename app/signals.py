"""
Signal Generator - EMA Crossover + RSI confirmation + ATR volatility.
"""

import pandas as pd
import numpy as np
from config import EMA_FAST, EMA_SLOW, RSI_PERIOD, RSI_LONG_THRESHOLD, RSI_SHORT_THRESHOLD


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add EMA, RSI, and ATR columns to the dataframe."""
    df = df.copy()

    # ── EMA ──
    df['EMA_Fast'] = df['Close'].ewm(span=EMA_FAST, adjust=False).mean()
    df['EMA_Slow'] = df['Close'].ewm(span=EMA_SLOW, adjust=False).mean()

    # ── RSI (14) ──
    delta = df['Close'].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / RSI_PERIOD, min_periods=RSI_PERIOD).mean()
    avg_loss = loss.ewm(alpha=1 / RSI_PERIOD, min_periods=RSI_PERIOD).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df['RSI'] = 100 - (100 / (1 + rs))
    df['RSI'] = df['RSI'].fillna(50)  # Warmup period → neutral (no signal)

    # ── ATR (14) ──
    high_low = df['High'] - df['Low']
    high_close = (df['High'] - df['Close'].shift()).abs()
    low_close = (df['Low'] - df['Close'].shift()).abs()
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df['ATR'] = true_range.ewm(span=RSI_PERIOD, adjust=False).mean()

    # ── EMA Cross signals ──
    df['EMA_Cross_Up'] = (df['EMA_Fast'] > df['EMA_Slow']) & (df['EMA_Fast'].shift(1) <= df['EMA_Slow'].shift(1))
    df['EMA_Cross_Down'] = (df['EMA_Fast'] < df['EMA_Slow']) & (df['EMA_Fast'].shift(1) >= df['EMA_Slow'].shift(1))

    return df


def generate_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate LONG/SHORT signals:
      LONG  = EMA9 crosses above EMA26 AND RSI > 50
      SHORT = EMA9 crosses below EMA26 AND RSI < 50
    """
    df = compute_indicators(df)
    df['Signal'] = 0
    df.loc[df['EMA_Cross_Up'] & (df['RSI'] > RSI_LONG_THRESHOLD), 'Signal'] = 1    # LONG
    df.loc[df['EMA_Cross_Down'] & (df['RSI'] < RSI_SHORT_THRESHOLD), 'Signal'] = -1  # SHORT

    return df
