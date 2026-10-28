"""
Backtester - Runs the strategy on historical data and computes performance metrics.
"""

import pandas as pd
import numpy as np
from signals import generate_signals
from risk_manager import TradeSimulator
from config import TOTAL_CAPITAL, BACKTEST_DAYS


def compute_metrics(trades: list, initial_capital: float) -> dict:
    """Compute Sharpe Ratio, Max Drawdown, Win Rate from trade list."""
    if not trades:
        return {
            'Total Trades': 0,
            'Win Rate': 'N/A',
            'Sharpe Ratio': 'N/A',
            'Max Drawdown (%)': 'N/A',
            'Net PnL (₹)': 0,
            'Net PnL (%)': '0.00%',
            'Avg Trade PnL (₹)': 0,
        }

    df = pd.DataFrame(trades)
    pnl_list = df['PnL_Rs'].values

    # Win rate
    wins = sum(1 for p in pnl_list if p > 0)
    total = len(pnl_list)
    win_rate = (wins / total * 100) if total > 0 else 0

    # Net PnL
    net_pnl = sum(pnl_list)
    net_pnl_pct = (net_pnl / initial_capital * 100)

    # Sharpe Ratio (annualized, assuming ~252 trading days, 15-min bars)
    # Using per-trade returns
    if len(pnl_list) > 1:
        returns = pnl_list / initial_capital
        mean_ret = np.mean(returns)
        std_ret = np.std(returns, ddof=1)
        if std_ret > 0:
            # Approximate annualization: ~16 trades/day * 252 days
            trades_per_year = total / (BACKTEST_DAYS / 252) if BACKTEST_DAYS > 0 else total
            sharpe = (mean_ret / std_ret) * np.sqrt(trades_per_year)
        else:
            sharpe = 0.0
    else:
        sharpe = 0.0

    # Max Drawdown
    cumulative = np.cumsum(pnl_list) + initial_capital
    running_max = np.maximum.accumulate(cumulative)
    drawdowns = (running_max - cumulative) / running_max
    max_dd = np.max(drawdowns) * 100 if len(drawdowns) > 0 else 0

    avg_pnl = net_pnl / total if total > 0 else 0

    return {
        'Total Trades': total,
        'Win Rate': f'{win_rate:.1f}%',
        'Sharpe Ratio': f'{sharpe:.2f}',
        'Max Drawdown (%)': f'{max_dd:.2f}%',
        'Net PnL (₹)': f'₹{net_pnl:,.0f}',
        'Net PnL (%)': f'{net_pnl_pct:.2f}%',
        'Avg Trade PnL (₹)': f'₹{avg_pnl:,.0f}',
    }


def run_full_backtest(data: dict) -> tuple:
    """
    Run backtest across all indices.
    Returns (summary_table_df, all_trades_df).
    """
    all_trades = []
    summary_rows = []

    for index_name, df in data.items():
        print(f"\n  Backtesting {index_name} ({len(df)} candles)...")

        # Reset capital for each index (independent backtests)
        sim = TradeSimulator(capital=TOTAL_CAPITAL)

        # Generate signals
        df_signals = generate_signals(df)

        # Run simulation
        trades = sim.run_backtest(df_signals, index_name)
        all_trades.extend(trades)

        # Compute metrics
        metrics = compute_metrics(trades, TOTAL_CAPITAL)
        metrics['Index'] = index_name
        summary_rows.append(metrics)

        print(f"    {metrics['Total Trades']} trades | Win: {metrics['Win Rate']} | "
              f"Sharpe: {metrics['Sharpe Ratio']} | MaxDD: {metrics['Max Drawdown (%)']} | "
              f"Net: {metrics['Net PnL (₹)']}")

    summary_df = pd.DataFrame(summary_rows)
    trades_df = pd.DataFrame(all_trades) if all_trades else pd.DataFrame()

    # Reorder summary columns
    cols = ['Index', 'Total Trades', 'Win Rate', 'Sharpe Ratio',
            'Max Drawdown (%)', 'Net PnL (₹)', 'Net PnL (%)', 'Avg Trade PnL (₹)']
    summary_df = summary_df[cols]

    return summary_df, trades_df
