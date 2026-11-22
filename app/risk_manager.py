"""
Risk Manager - Position sizing, stop-loss, trailing take-profit, kill switch.
"""

import pandas as pd
import numpy as np
from config import TOTAL_CAPITAL, MAX_RISK_PER_TRADE, ATR_MULTIPLIER_SL, TRAILING_TRIGGER, DAILY_LOSS_LIMIT


def calculate_position_size(capital: float, atr: float, risk_pct: float = MAX_RISK_PER_TRADE) -> int:
    """
    Dynamic position sizing: max risk = 1% of capital.
    Position size = (Capital × risk%) / (ATR × ATR_multiplier)
    Returns integer lot size.
    """
    risk_amount = capital * risk_pct
    sl_points = atr * ATR_MULTIPLIER_SL
    if sl_points <= 0:
        return 0
    qty = int(risk_amount / sl_points)
    return max(qty, 0)


class TradeSimulator:
    """
    Simulates trades with stop-loss, trailing take-profit, and daily kill switch.
    """

    def __init__(self, capital: float = TOTAL_CAPITAL):
        self.initial_capital = capital
        self.capital = capital
        self.trades = []
        self.daily_pnl = {}
        self.daily_start_capital = capital
        self.kill_switch_active = False

    def run_backtest(self, df: pd.DataFrame, index_name: str) -> list:
        """
        Run backtest on a signal-enriched dataframe.
        Returns list of trade records.
        """
        trades = []
        position = None  # {'type': 'LONG'/'SHORT', 'entry': price, 'qty': n, 'sl': sl, 'peak': peak, 'date': date}

        for i in range(1, len(df)):
            row = df.iloc[i]
            prev = df.iloc[i - 1]
            timestamp = df.index[i]
            date_key = timestamp.date() if hasattr(timestamp, 'date') else str(timestamp)[:10]

            # ── Daily kill switch check ──
            if date_key not in self.daily_pnl:
                self.daily_pnl[date_key] = 0.0
                self.daily_start_capital = self.capital
                self.kill_switch_active = False

            daily_dd = (self.daily_start_capital - self.capital) / self.daily_start_capital
            if daily_dd >= DAILY_LOSS_LIMIT:
                self.kill_switch_active = True

            if self.kill_switch_active:
                continue

            # ── Manage open position ──
            if position is not None:
                pnl_points = 0
                exit_reason = None

                if position['type'] == 'LONG':
                    # Update peak
                    if row['High'] > position['peak']:
                        position['peak'] = row['High']

                    # Check stop-loss
                    if row['Low'] <= position['sl']:
                        pnl_points = position['sl'] - position['entry']
                        exit_reason = 'SL'
                    # Check trailing TP
                    elif position['peak'] - position['entry'] > TRAILING_TRIGGER:
                        trailing_sl = position['peak'] - TRAILING_TRIGGER
                        if trailing_sl > position['sl']:
                            position['sl'] = trailing_sl
                        if row['Low'] <= position['sl']:
                            pnl_points = position['sl'] - position['entry']
                            exit_reason = 'TrailingTP'
                    # Exit on opposite signal
                    elif row.get('Signal', 0) == -1:
                        pnl_points = row['Close'] - position['entry']
                        exit_reason = 'OppositeSignal'

                elif position['type'] == 'SHORT':
                    # Update peak (lowest for short)
                    if row['Low'] < position['peak']:
                        position['peak'] = row['Low']

                    # Check stop-loss
                    if row['High'] >= position['sl']:
                        pnl_points = position['entry'] - position['sl']
                        exit_reason = 'SL'
                    # Check trailing TP
                    elif position['entry'] - position['peak'] > TRAILING_TRIGGER:
                        trailing_sl = position['peak'] + TRAILING_TRIGGER
                        if trailing_sl < position['sl']:
                            position['sl'] = trailing_sl
                        if row['High'] >= position['sl']:
                            pnl_points = position['entry'] - position['sl']
                            exit_reason = 'TrailingTP'
                    # Exit on opposite signal
                    elif row.get('Signal', 0) == 1:
                        pnl_points = position['entry'] - row['Close']
                        exit_reason = 'OppositeSignal'

                if exit_reason:
                    trade_pnl = pnl_points * position['qty']
                    self.capital += trade_pnl
                    self.daily_pnl[date_key] = self.daily_pnl.get(date_key, 0) + trade_pnl

                    trades.append({
                        'Index': index_name,
                        'Entry_Time': position['time'],
                        'Exit_Time': timestamp,
                        'Type': position['type'],
                        'Entry_Price': round(position['entry'], 2),
                        'Exit_Price': round(position['entry'] + pnl_points if position['type'] == 'LONG' else position['entry'] - pnl_points, 2),
                        'Qty': position['qty'],
                        'PnL_Points': round(pnl_points, 2),
                        'PnL_Rs': round(trade_pnl, 2),
                        'Exit_Reason': exit_reason,
                        'Capital_After': round(self.capital, 2),
                    })
                    position = None

            # ── Enter new position ──
            if position is None and row.get('Signal', 0) != 0:
                qty = calculate_position_size(self.capital, row['ATR'])
                if qty > 0:
                    if row['Signal'] == 1:  # LONG
                        sl = row['Close'] - (row['ATR'] * ATR_MULTIPLIER_SL)
                        position = {
                            'type': 'LONG',
                            'entry': row['Close'],
                            'qty': qty,
                            'sl': sl,
                            'peak': row['Close'],
                            'time': timestamp,
                        }
                    elif row['Signal'] == -1:  # SHORT
                        sl = row['Close'] + (row['ATR'] * ATR_MULTIPLIER_SL)
                        position = {
                            'type': 'SHORT',
                            'entry': row['Close'],
                            'qty': qty,
                            'sl': sl,
                            'peak': row['Close'],
                            'time': timestamp,
                        }

        # Close any open position at last candle
        if position is not None:
            last_row = df.iloc[-1]
            if position['type'] == 'LONG':
                pnl_points = last_row['Close'] - position['entry']
            else:
                pnl_points = position['entry'] - last_row['Close']
            trade_pnl = pnl_points * position['qty']
            self.capital += trade_pnl

            trades.append({
                'Index': index_name,
                'Entry_Time': position['time'],
                'Exit_Time': df.index[-1],
                'Type': position['type'],
                'Entry_Price': round(position['entry'], 2),
                'Exit_Price': round(last_row['Close'], 2),
                'Qty': position['qty'],
                'PnL_Points': round(pnl_points, 2),
                'PnL_Rs': round(trade_pnl, 2),
                'Exit_Reason': 'EndOfData',
                'Capital_After': round(self.capital, 2),
            })

        return trades
