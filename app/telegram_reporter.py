"""
Telegram Reporter - Sends trading updates at scheduled times.
"""

import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import UNIVERSE, TOTAL_CAPITAL
from trade_logger import TradeLogger
from dhan_broker import DhanBroker, DhanTokenManager, load_env_file
from data_fetcher import fetch_historical_data
from signals import generate_signals


def generate_morning_brief() -> str:
    """Generate 9:15 AM market opening brief."""
    load_env_file()
    
    msg = []
    msg.append("🌅 *GOOD MORNING - MARKET OPENING*")
    msg.append(f"📅 {datetime.now().strftime('%A, %d %B %Y')}")
    msg.append(f"💰 Capital: ₹{TOTAL_CAPITAL:,}")
    msg.append("")
    
    # Learning mode status
    from live_trader import is_learning_mode
    if is_learning_mode():
        msg.append("🎓 *Learning Mode Active*")
        msg.append("📊 Max 1 trade today")
        msg.append("")
    
    # Yesterday's performance
    trade_logger = TradeLogger()
    stats = trade_logger.get_stats()
    
    if stats['completed'] > 0:
        msg.append("📈 *Yesterday's Performance*")
        msg.append(f"• Trades: {stats['completed']}")
        msg.append(f"• Win Rate: {stats['win_rate']:.1f}%")
        msg.append(f"• Total P&L: ₹{stats['total_pnl']:,.2f}")
        msg.append("")
    
    # Market sentiment (based on last available data)
    msg.append("🔍 *Market Scan*")
    for index_name, ticker in list(UNIVERSE.items())[:3]:  # Top 3 indices
        try:
            df = fetch_historical_data(ticker, days=1, interval="15m")
            if df is not None and not df.empty:
                df = generate_signals(df)
                latest = df.iloc[-1]
                
                # Determine trend
                if latest['EMA_Fast'] > latest['EMA_Slow']:
                    trend = "📈 Bullish"
                else:
                    trend = "📉 Bearish"
                
                msg.append(f"• {index_name}: {latest['Close']:.2f} {trend}")
        except:
            pass
    
    msg.append("")
    msg.append("⏰ *Today's Schedule*")
    msg.append("• 9:15 AM - Market opens")
    msg.append("• 1:00 PM - Mid-day check")
    msg.append("• 3:15 PM - Closing report")
    msg.append("")
    msg.append("🚀 Let's have a disciplined day!")
    
    return "\n".join(msg)


def generate_midday_update() -> str:
    """Generate 1:00 PM mid-day update."""
    load_env_file()
    
    msg = []
    msg.append("🕐 *MID-DAY UPDATE*")
    msg.append(f"📅 {datetime.now().strftime('%A, %d %B %Y')}")
    msg.append("")
    
    trade_logger = TradeLogger()
    today_trades = trade_logger.get_todays_trades()
    
    if today_trades:
        msg.append(f"📊 *Today's Trades ({len(today_trades)})*")
        for i, trade in enumerate(today_trades, 1):
            msg.append(f"\n*Trade #{i}*")
            msg.append(f"• Index: {trade.get('index_name')}")
            msg.append(f"• Direction: {trade.get('direction')}")
            msg.append(f"• Entry: ₹{trade.get('entry_price', 0):,.2f}")
            msg.append(f"• Stop Loss: ₹{trade.get('stop_loss', 0):,.2f}")
            
            if 'exit_time' in trade:
                msg.append(f"• Exit: ₹{trade.get('exit_price', 0):,.2f}")
                msg.append(f"• P&L: ₹{trade.get('pnl', 0):,.2f}")
            else:
                msg.append(f"• Status: Open")
    else:
        msg.append("😴 *No trades today*")
        msg.append("Waiting for the right setup...")
    
    msg.append("")
    msg.append("⏰ Next update: 3:15 PM")
    
    return "\n".join(msg)


def generate_closing_report() -> str:
    """Generate 3:15 PM closing report."""
    load_env_file()
    
    msg = []
    msg.append("🌆 *MARKET CLOSING REPORT*")
    msg.append(f"📅 {datetime.now().strftime('%A, %d %B %Y')}")
    msg.append("")
    
    trade_logger = TradeLogger()
    today_trades = trade_logger.get_todays_trades()
    stats = trade_logger.get_stats()
    
    # Today's summary
    msg.append("📊 *Today's Summary*")
    msg.append(f"• Trades: {len(today_trades)}")
    
    if today_trades:
        completed = [t for t in today_trades if 'pnl' in t]
        if completed:
            today_pnl = sum(t['pnl'] for t in completed)
            wins = len([t for t in completed if t['pnl'] > 0])
            msg.append(f"• P&L: ₹{today_pnl:,.2f}")
            msg.append(f"• Wins: {wins}/{len(completed)}")
        else:
            msg.append(f"• Open positions: {len(today_trades)}")
    else:
        msg.append("• No trades today")
    
    msg.append("")
    
    # Overall stats
    msg.append("📈 *Overall Performance*")
    msg.append(f"• Total Trades: {stats['total_trades']}")
    msg.append(f"• Win Rate: {stats['win_rate']:.1f}%")
    msg.append(f"• Total P&L: ₹{stats['total_pnl']:,.2f}")
    msg.append(f"• Best Trade: ₹{stats['best_trade']:,.2f}")
    msg.append(f"• Worst Trade: ₹{stats['worst_trade']:,.2f}")
    
    msg.append("")
    
    # Learning insights
    if stats['completed'] >= 5:
        msg.append("💡 *Learning Insights*")
        if stats['win_rate'] > 50:
            msg.append("✓ Good win rate! Strategy working well.")
        else:
            msg.append("⚠️ Low win rate. Review signal quality.")
        
        if stats['avg_pnl'] > 0:
            msg.append("✓ Positive average P&L. Risk management on point.")
        else:
            msg.append("⚠️ Negative average P&L. Check stop losses.")
        msg.append("")
    
    # Closing prices
    msg.append("📊 *Market Close*")
    for index_name, ticker in list(UNIVERSE.items())[:3]:
        try:
            df = fetch_historical_data(ticker, days=1, interval="15m")
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                msg.append(f"• {index_name}: {latest['Close']:.2f}")
        except:
            pass
    
    msg.append("")
    msg.append("✅ Market closed. See you tomorrow!")
    
    return "\n".join(msg)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Send Telegram trading updates")
    parser.add_argument("--type", choices=["morning", "midday", "closing"], required=True,
                        help="Type of update to send")
    args = parser.parse_args()
    
    if args.type == "morning":
        message = generate_morning_brief()
    elif args.type == "midday":
        message = generate_midday_update()
    elif args.type == "closing":
        message = generate_closing_report()
    
    print(message)
