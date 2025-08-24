#!/usr/bin/env python3
"""
Run the breakout strategy using the generic backtester.
"""

from pathlib import Path
from datetime import datetime
import pytz

from generic_backtester import GenericBacktester
from breakout import BreakoutStrategy
from indicators.momentum_mean_reversion_indicator import MomentumMeanReversionIndicator
from nautilus_trader.model.enums import OrderStatus


def export_trades_to_log(engine, filename="trades_log.txt"):
    """
    Extract filled orders from the engine cache and export to a log file.
    
    Args:
        engine: The BacktestEngine instance
        filename: Output filename for the trades log
    """
    # Get all orders from the cache
    orders = engine.kernel.cache.orders()
    
    # Filter for filled orders only
    filled_orders = [o for o in orders if o.status == OrderStatus.FILLED]
    
    # Sort by timestamp
    filled_orders.sort(key=lambda x: x.ts_last)
    
    # Format and write to file
    with open(filename, 'w') as f:
        f.write("# Trading Log\n")
        f.write(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("#\n")
        f.write("# Format: DATE TIME SIDE SYMBOL@PRICE x QUANTITY\n")
        f.write("#" + "="*70 + "\n\n")
        
        for order in filled_orders:
            # Convert nanosecond timestamp to datetime
            ts_seconds = order.ts_last / 1_000_000_000
            trade_time = datetime.fromtimestamp(ts_seconds, tz=pytz.UTC)
            
            # Format the trade entry
            date_str = trade_time.strftime('%Y-%m-%d')
            time_str = trade_time.strftime('%H:%M:%S')
            side = order.side.name  # BUY or SELL
            symbol = str(order.instrument_id.symbol)
            
            # Get the fill price (avg_px is the average fill price)
            price = order.avg_px if order.avg_px else 0.0
            quantity = int(order.quantity)
            
            # Write formatted trade
            trade_line = f"{date_str} {time_str} {side} {symbol}@{price:.2f} x {quantity}"
            f.write(trade_line + "\n")
        
        # Add summary at the end
        f.write("\n" + "#"*70 + "\n")
        f.write(f"# Total Trades: {len(filled_orders)}\n")
        
        # Count buy/sell orders
        buy_orders = len([o for o in filled_orders if o.side.name == 'BUY'])
        sell_orders = len([o for o in filled_orders if o.side.name == 'SELL'])
        
        f.write(f"# Buy Orders: {buy_orders}\n")
        f.write(f"# Sell Orders: {sell_orders}\n")


def main():
    # Initialize backtester
    catalog_path = Path("nautilus_catalog")
    backtester = GenericBacktester(catalog_path)
    
    # Set date range
    backtester.set_date_range(
        start_date=datetime(2020, 1, 1, tzinfo=pytz.UTC),
        end_date=datetime(2025, 1, 1, tzinfo=pytz.UTC)
    )
    
    # Set initial capital
    backtester.set_initial_capital(100_000)
    
    # Create indicator
    indicator = MomentumMeanReversionIndicator()
    
    # Create strategy with indicator
    strategy = BreakoutStrategy(indicator)
    
    # Run backtest
    print("Running breakout strategy backtest...")
    results = backtester.run_strategy(strategy, "BreakoutStrategy")
    
    # Print results
    if results['success']:
        print(f"\nBacktest Results:")
        print(f"  Total Return: {results['total_return_pct']:+.2f}%")
        print(f"  Total P&L: ${results['total_pnl']:,.2f}")
        print(f"  Final Balance: ${results['final_balance']:,.2f}")
        print(f"  Total Orders: {results['total_orders']}")
        print(f"  Filled Orders: {results['filled_orders']}")
        print(f"  Buy Orders: {results['buy_orders']}")
        print(f"  Sell Orders: {results['sell_orders']}")
        print(f"  Complete Trades: {results['trades']}")
        
        # Extract and export trades
        if 'engine' in results:
            export_trades_to_log(results['engine'], "trades_log.txt")
            print(f"\nTrades exported to trades_log.txt")
    else:
        print(f"Backtest failed: {results.get('error', 'Unknown error')}")


if __name__ == "__main__":
    main()