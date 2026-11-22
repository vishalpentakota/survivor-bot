"""
Main Entry Point - Run the full backtest across all NSE indices.
Outputs a markdown summary table and detailed trade log.
"""

import sys
import os
import json
from datetime import datetime

# Ensure local modules are importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_fetcher import fetch_all_indices
from backtester import run_full_backtest
from config import TOTAL_CAPITAL, BACKTEST_DAYS


def main():
    print("=" * 70)
    print("  NIFTY ALGORITHMIC TRADING SYSTEM - BACKTEST")
    print(f"  Capital: ₹{TOTAL_CAPITAL:,.0f} | Lookback: {BACKTEST_DAYS} days")
    print(f"  Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # ── Step 1: Fetch data ──
    print("\n[1/3] Fetching historical data...")
    data = fetch_all_indices(BACKTEST_DAYS)

    if not data:
        print("ERROR: No data fetched. Exiting.")
        sys.exit(1)

    # ── Step 2: Run backtest ──
    print("\n[2/3] Running backtest...")
    summary_df, trades_df = run_full_backtest(data)

    # ── Step 3: Output results ──
    print("\n[3/3] Results")
    print("=" * 70)

    # Print summary table
    print("\n## Backtest Performance Summary\n")
    print(summary_df.to_markdown(index=False))

    # Save to files
    output_dir = os.path.dirname(os.path.abspath(__file__))
    summary_path = os.path.join(output_dir, "backtest_summary.md")
    trades_path = os.path.join(output_dir, "backtest_trades.csv")

    # Markdown report
    md_content = f"""# Nifty Algo Trading - Backtest Report

**Run Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**Capital:** ₹{TOTAL_CAPITAL:,.0f}  
**Lookback:** {BACKTEST_DAYS} days  
**Strategy:** EMA(9/26) Crossover + RSI(14) + ATR-based Risk Management

## Performance Summary

{summary_df.to_markdown(index=False)}

## Strategy Details
- **Entry LONG:** EMA9 crosses above EMA26 AND RSI > 50
- **Entry SHORT:** EMA9 crosses below EMA26 AND RSI < 50
- **Stop-Loss:** 1.5x ATR
- **Trailing TP:** 2000 pts behind peak
- **Max Risk/Trade:** 1% of capital
- **Daily Kill Switch:** 3% drawdown

## Notes
- Data source: Yahoo Finance (15-min candles)
- Backtest is per-index (independent capital allocation)
- Transaction costs and slippage NOT included
"""

    with open(summary_path, 'w') as f:
        f.write(md_content)
    print(f"\n  Summary saved to: {summary_path}")

    if not trades_df.empty:
        trades_df.to_csv(trades_path, index=False)
        print(f"  Trade log saved to: {trades_path}")
        print(f"\n  Total trades across all indices: {len(trades_df)}")

    print("\n" + "=" * 70)
    print("  BACKTEST COMPLETE")
    print("=" * 70)

    return summary_df, trades_df


if __name__ == "__main__":
    main()
