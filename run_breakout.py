#!/usr/bin/env python3
"""
Run the breakout strategy using the generic backtester.
"""

from pathlib import Path
from datetime import datetime
import pytz
import pandas as pd

from generic_backtester import GenericBacktester
from breakout import BreakoutStrategy
from indicators.momentum_mean_reversion_indicator import MomentumMeanReversionIndicator
from nautilus_trader.model.enums import OrderStatus


def export_pnl_over_time(engine, filename="pnl_over_time.csv", initial_capital=100000):
    """
    Calculate and export P&L over time from the backtest results.
    
    Args:
        engine: The BacktestEngine instance
        filename: Output filename for the P&L CSV
        initial_capital: Starting capital for P&L calculation
    """
    # Get all filled orders
    orders = engine.kernel.cache.orders()
    filled_orders = [o for o in orders if o.status == OrderStatus.FILLED]
    filled_orders.sort(key=lambda x: x.ts_last)
    
    if not filled_orders:
        print("No filled orders to calculate P&L")
        return
    
    # Track positions and P&L
    position_voo = 0
    position_sh = 0
    voo_avg_price = 0
    sh_avg_price = 0
    cash = initial_capital
    pnl_history = []
    
    for order in filled_orders:
        # Convert timestamp
        ts_seconds = order.ts_last / 1_000_000_000
        trade_time = datetime.fromtimestamp(ts_seconds, tz=pytz.UTC)
        
        symbol = str(order.instrument_id.symbol)
        side = order.side.name
        quantity = int(order.quantity)
        price = float(order.avg_px) if order.avg_px else 0.0
        
        # Calculate P&L and update positions
        if symbol == "VOO":
            if side == "BUY":
                # Buying VOO
                position_voo += quantity
                voo_avg_price = price
                cash -= quantity * price
            else:  # SELL
                # Selling VOO
                if position_voo > 0:
                    realized_pnl = quantity * (price - voo_avg_price)
                    cash += quantity * price
                    position_voo -= quantity
                    if position_voo == 0:
                        voo_avg_price = 0
        
        elif symbol == "SH":
            if side == "BUY":
                # Buying SH (going short)
                position_sh += quantity
                sh_avg_price = price
                cash -= quantity * price
            else:  # SELL
                # Selling SH (exiting short)
                if position_sh > 0:
                    realized_pnl = quantity * (price - sh_avg_price)
                    cash += quantity * price
                    position_sh -= quantity
                    if position_sh == 0:
                        sh_avg_price = 0
        
        # Calculate total equity (cash + value of positions)
        # For this we'd need current prices, but we'll use last trade prices as approximation
        voo_value = position_voo * voo_avg_price if position_voo > 0 else 0
        sh_value = position_sh * sh_avg_price if position_sh > 0 else 0
        total_equity = cash + voo_value + sh_value
        
        pnl_history.append({
            'datetime': trade_time,
            'symbol': symbol,
            'side': side,
            'quantity': quantity,
            'price': price,
            'cash': cash,
            'position_voo': position_voo,
            'position_sh': position_sh,
            'total_equity': total_equity,
            'pnl': total_equity - initial_capital,
            'pnl_pct': ((total_equity - initial_capital) / initial_capital) * 100
        })
    
    # Convert to DataFrame and save
    df = pd.DataFrame(pnl_history)
    df.to_csv(filename, index=False)
    
    print(f"P&L over time exported to {filename}")
    print(f"Final P&L: ${df.iloc[-1]['pnl']:,.2f} ({df.iloc[-1]['pnl_pct']:.2f}%)")
    
    return df



def main():
    # Initialize backtester
    catalog_path = Path("nautilus_catalog")
    backtester = GenericBacktester(catalog_path)
    
    # Set date range
    backtester.set_date_range(
        start_date=datetime(2014, 1, 1, tzinfo=pytz.UTC),
        end_date=datetime(2025, 1, 1, tzinfo=pytz.UTC)
    )
    
    # Set initial capital
    backtester.set_initial_capital(100_000)
    
    # Create indicator
    indicator = MomentumMeanReversionIndicator()

    config = {
        'long_entry_lookback': 4,
        'long_exit_lookback': 7,
        'short_entry_lookback': 4,
        'short_exit_lookback': 7,
    }
    
    # Create strategy with indicator
    strategy = BreakoutStrategy(indicator, config)
    
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
        
        # Export P&L over time
        if 'engine' in results:
            export_pnl_over_time(results['engine'], "pnl_over_time.csv", initial_capital=100_000)
    else:
        print(f"Backtest failed: {results.get('error', 'Unknown error')}")


if __name__ == "__main__":
    main()