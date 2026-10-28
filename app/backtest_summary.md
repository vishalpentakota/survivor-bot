# Nifty Algo Trading - Backtest Report

**Run Date:** 2026-08-01 17:49:49  
**Capital:** ₹1,000,000  
**Lookback:** 90 days  
**Strategy:** EMA(9/26) Crossover + RSI(14) + ATR-based Risk Management

## Performance Summary

| Index        |   Total Trades | Win Rate   |   Sharpe Ratio | Max Drawdown (%)   | Net PnL (₹)   | Net PnL (%)   | Avg Trade PnL (₹)   |
|:-------------|---------------:|:-----------|---------------:|:-------------------|:--------------|:--------------|:--------------------|
| Nifty 50     |             51 | 31.4%      |           2.48 | 9.17%              | ₹296,334      | 29.63%        | ₹5,810              |
| Nifty Bank   |             57 | 28.1%      |           0.4  | 8.57%              | ₹34,789       | 3.48%         | ₹610                |
| Nifty Auto   |             57 | 21.1%      |          -0.71 | 20.08%             | ₹-55,284      | -5.53%        | ₹-970               |
| Nifty Pharma |             48 | 14.6%      |          -1.72 | 21.74%             | ₹-129,766     | -12.98%       | ₹-2,703             |
| Nifty IT     |             33 | 36.4%      |           1.8  | 8.58%              | ₹183,372      | 18.34%        | ₹5,557              |
| Nifty Metal  |             55 | 27.3%      |           0.55 | 13.01%             | ₹59,290       | 5.93%         | ₹1,078              |
| Nifty FMCG   |             57 | 21.1%      |          -4.18 | 21.49%             | ₹-211,889     | -21.19%       | ₹-3,717             |

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
