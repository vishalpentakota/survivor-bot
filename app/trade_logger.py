"""
Trade Logger - Records all trades with detailed metrics for learning.
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Optional

TRADE_LOG_FILE = os.path.join(os.path.dirname(__file__), "trades.json")


class TradeLogger:
    """Logs trades to JSON file for analysis and learning."""

    def __init__(self, log_file: str = TRADE_LOG_FILE):
        self.log_file = log_file
        self._ensure_log_file()

    def _ensure_log_file(self):
        """Create log file if it doesn't exist or is empty/invalid."""
        needs_init = True
        if os.path.exists(self.log_file):
            try:
                with open(self.log_file, 'r') as f:
                    data = json.load(f)
                if isinstance(data, list):
                    needs_init = False
            except (json.JSONDecodeError, ValueError):
                pass  # Corrupted file, will reinitialize

        if needs_init:
            with open(self.log_file, 'w') as f:
                json.dump([], f)

    def log_trade(self, trade_data: Dict) -> None:
        """
        Log a trade with all relevant details.

        Expected fields:
            - index_name: str
            - direction: str (LONG/SHORT)
            - entry_time: datetime
            - entry_price: float
            - quantity: int
            - stop_loss: float
            - signal_strength: dict (EMA, RSI, ATR values)
            - exit_time: datetime (optional)
            - exit_price: float (optional)
            - pnl: float (optional)
            - exit_reason: str (optional)
        """
        trades = self.load_trades()

        # Convert datetime objects to ISO format
        if 'entry_time' in trade_data and isinstance(trade_data['entry_time'], datetime):
            trade_data['entry_time'] = trade_data['entry_time'].isoformat()
        if 'exit_time' in trade_data and isinstance(trade_data['exit_time'], datetime):
            trade_data['exit_time'] = trade_data['exit_time'].isoformat()

        # Add metadata
        trade_data['logged_at'] = datetime.now().isoformat()
        trade_data['trade_id'] = len(trades) + 1

        trades.append(trade_data)

        with open(self.log_file, 'w') as f:
            json.dump(trades, f, indent=2)

    def update_trade(self, trade_id: int, updates: Dict) -> None:
        """Update an existing trade (e.g., add exit info)."""
        trades = self.load_trades()

        for trade in trades:
            if trade.get('trade_id') == trade_id:
                # Convert datetime if present
                if 'exit_time' in updates and isinstance(updates['exit_time'], datetime):
                    updates['exit_time'] = updates['exit_time'].isoformat()

                trade.update(updates)
                break

        with open(self.log_file, 'w') as f:
            json.dump(trades, f, indent=2)

    def load_trades(self) -> List[Dict]:
        """Load all trades from log file."""
        if not os.path.exists(self.log_file):
            return []

        with open(self.log_file, 'r') as f:
            return json.load(f)

    def get_todays_trades(self) -> List[Dict]:
        """Get all trades from today."""
        trades = self.load_trades()
        today = datetime.now().date().isoformat()

        return [
            t for t in trades
            if t.get('entry_time', '').startswith(today)
        ]

    def get_stats(self) -> Dict:
        """Calculate trading statistics."""
        trades = self.load_trades()
        completed = [t for t in trades if 'pnl' in t]

        if not completed:
            return {
                'total_trades': len(trades),
                'completed': 0,
                'win_rate': 0,
                'total_pnl': 0,
                'avg_pnl': 0,
                'best_trade': 0,
                'worst_trade': 0,
            }

        wins = [t for t in completed if t['pnl'] > 0]
        losses = [t for t in completed if t['pnl'] < 0]
        pnls = [t['pnl'] for t in completed]

        return {
            'total_trades': len(trades),
            'completed': len(completed),
            'wins': len(wins),
            'losses': len(losses),
            'win_rate': len(wins) / len(completed) * 100 if completed else 0,
            'total_pnl': sum(pnls),
            'avg_pnl': sum(pnls) / len(pnls),
            'best_trade': max(pnls),
            'worst_trade': min(pnls),
        }

    def generate_report(self) -> str:
        """Generate a markdown report of all trades."""
        trades = self.load_trades()
        stats = self.get_stats()

        report = []
        report.append("# Trading Report")
        report.append(f"\n**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

        # Stats
        report.append("## Summary")
        report.append(f"- Total Trades: {stats['total_trades']}")
        report.append(f"- Completed: {stats['completed']}")
        report.append(f"- Win Rate: {stats['win_rate']:.1f}%")
        report.append(f"- Total P&L: ₹{stats['total_pnl']:,.2f}")
        report.append(f"- Average P&L: ₹{stats['avg_pnl']:,.2f}")
        if stats['completed'] > 0:
            report.append(f"- Best Trade: ₹{stats['best_trade']:,.2f}")
            report.append(f"- Worst Trade: ₹{stats['worst_trade']:,.2f}")
        report.append("")

        # Trade details
        if trades:
            report.append("## Trade Log\n")
            for trade in reversed(trades):  # Most recent first
                report.append(f"### Trade #{trade.get('trade_id', '?')}")
                report.append(f"- **Index:** {trade.get('index_name', 'N/A')}")
                report.append(f"- **Direction:** {trade.get('direction', 'N/A')}")
                report.append(f"- **Entry:** {trade.get('entry_time', 'N/A')} @ ₹{trade.get('entry_price', 0):,.2f}")
                report.append(f"- **Quantity:** {trade.get('quantity', 0)}")
                report.append(f"- **Stop Loss:** ₹{trade.get('stop_loss', 0):,.2f}")

                if 'exit_time' in trade:
                    report.append(f"- **Exit:** {trade.get('exit_time', 'N/A')} @ ₹{trade.get('exit_price', 0):,.2f}")
                    report.append(f"- **P&L:** ₹{trade.get('pnl', 0):,.2f}")
                    report.append(f"- **Exit Reason:** {trade.get('exit_reason', 'N/A')}")

                if 'signal_strength' in trade:
                    sig = trade['signal_strength']
                    report.append(f"- **Signal:** EMA9={sig.get('ema_fast', 0):.2f}, EMA26={sig.get('ema_slow', 0):.2f}, RSI={sig.get('rsi', 0):.1f}, ATR={sig.get('atr', 0):.2f}")

                report.append("")

        return "\n".join(report)
