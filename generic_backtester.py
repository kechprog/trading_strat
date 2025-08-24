#!/usr/bin/env python3
"""
Generic Backtester for Any Trading Strategy

This backtester accepts any strategy instance and runs backtests with clean P&L tracking.
Supports both single strategy testing and multi-strategy comparison with shared order book simulation.
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
import pytz

from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.models import FillModel
from nautilus_trader.config import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import Bar
from nautilus_trader.model.enums import AccountType, OmsType, OrderStatus
from nautilus_trader.model.identifiers import Symbol, TraderId, AccountId, Venue
from nautilus_trader.model.instruments import Equity
from nautilus_trader.model.objects import Money, Price, Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.trading.strategy import Strategy


logger = logging.getLogger(__name__)


class GenericBacktester:
    """
    Generic backtester that can test any trading strategy.
    
    This backtester:
    1. Accepts any Strategy instance
    2. Loads market data from catalog
    3. Runs backtest in isolated environment
    4. Returns clean performance metrics
    """
    
    def __init__(self, catalog_path: Path):
        """
        Initialize the generic backtester.
        
        Args:
            catalog_path: Path to the Nautilus data catalog
        """
        self.catalog_path = catalog_path
        self.catalog = ParquetDataCatalog(catalog_path)
        
        # Default date range (can be overridden)
        self.start_date = datetime(2022, 1, 1, tzinfo=pytz.UTC)
        self.end_date = datetime(2025, 1, 1, tzinfo=pytz.UTC)
        
        # Default capital
        self.initial_capital = 100_000
    
    def set_date_range(self, start_date: datetime, end_date: datetime):
        """Set the backtest date range."""
        self.start_date = start_date
        self.end_date = end_date
    
    def set_initial_capital(self, capital: float):
        """Set the initial capital amount."""
        self.initial_capital = capital
    
    def discover_instruments(self):
        """Discover available instruments in the catalog."""
        bars = self.catalog.bars()
        if not bars:
            raise ValueError("No bars found in catalog")
        
        # Get unique instruments
        instruments = set()
        for bar in bars:
            instruments.add(bar.bar_type.instrument_id)
        
        # Find VOO and SH instruments
        voo_instrument = None
        sh_instrument = None
        
        for instrument in instruments:
            symbol = str(instrument.symbol)
            if symbol == "VOO":
                voo_instrument = instrument
            elif symbol == "SH":
                sh_instrument = instrument
        
        if not voo_instrument:
            raise ValueError("VOO instrument not found in catalog")
        
        return bars, voo_instrument, sh_instrument
    
    def run_strategy(self, strategy: Strategy, strategy_name: str = None) -> Dict[str, Any]:
        """
        Run a single strategy and return performance metrics.
        
        Args:
            strategy: Strategy instance to test
            strategy_name: Optional name for the strategy (for logging)
            
        Returns:
            Dictionary with performance metrics
        """
        if strategy_name is None:
            strategy_name = strategy.__class__.__name__
        
        try:
            # Discover instruments and data
            bars, voo_instrument, sh_instrument = self.discover_instruments()
            
            # Configure backtest engine
            engine_config = BacktestEngineConfig(
                trader_id=TraderId(f"TEST-{strategy_name}"),
                logging=LoggingConfig(log_level="ERROR", log_colors=False),
            )
            
            engine = BacktestEngine(config=engine_config)
            
            # Add venue
            engine.add_venue(
                venue=voo_instrument.venue,
                oms_type=OmsType.HEDGING,
                account_type=AccountType.MARGIN,
                base_currency=USD,
                starting_balances=[Money(self.initial_capital, USD)],
                fill_model=FillModel()
            )
            
            # Add instruments
            voo_equity = Equity(
                instrument_id=voo_instrument,
                raw_symbol=Symbol(voo_instrument.symbol.value),
                currency=USD,
                price_precision=5,
                price_increment=Price.from_str("0.00001"),
                lot_size=Quantity.from_str("1"),
                ts_event=0,
                ts_init=0,
            )
            engine.add_instrument(voo_equity)
            
            if sh_instrument:
                sh_equity = Equity(
                    instrument_id=sh_instrument,
                    raw_symbol=Symbol(sh_instrument.symbol.value),
                    currency=USD,
                    price_precision=5,
                    price_increment=Price.from_str("0.00001"),
                    lot_size=Quantity.from_str("1"),
                    ts_event=0,
                    ts_init=0,
                )
                engine.add_instrument(sh_equity)
            
            # Add strategy to engine
            engine.add_strategy(strategy)
            
            # Filter and add data
            start_ns = int(self.start_date.timestamp() * 1_000_000_000)
            end_ns = int(self.end_date.timestamp() * 1_000_000_000)
            
            # Add VOO bars
            voo_bars = [bar for bar in bars if bar.bar_type.instrument_id == voo_instrument]
            voo_bars = [bar for bar in voo_bars if start_ns <= bar.ts_event < end_ns]
            voo_bars.sort(key=lambda x: x.ts_event)
            engine.add_data(voo_bars)
            
            # Add SH bars if available
            if sh_instrument:
                sh_bars = [bar for bar in bars if bar.bar_type.instrument_id == sh_instrument]
                sh_bars = [bar for bar in sh_bars if start_ns <= bar.ts_event < end_ns]
                sh_bars.sort(key=lambda x: x.ts_event)
                engine.add_data(sh_bars)
            
            # Run backtest
            engine.run()
            
            # Extract performance metrics and include engine reference
            metrics = self._extract_performance_metrics(engine, strategy_name)
            metrics['engine'] = engine  # Add engine reference for trade extraction
            return metrics
            
        except Exception as e:
            logger.error(f"Error running strategy {strategy_name}: {e}")
            return {
                'strategy_name': strategy_name,
                'success': False,
                'error': str(e),
                'total_pnl': 0.0,
                'total_return_pct': 0.0,
                'total_orders': 0,
                'filled_orders': 0,
            }
    
    def _extract_performance_metrics(self, engine: BacktestEngine, strategy_name: str) -> Dict[str, Any]:
        """Extract performance metrics from completed backtest."""
        # Account metrics
        accounts = engine.kernel.cache.accounts()
        total_pnl = 0.0
        total_return_pct = 0.0
        
        if accounts:
            account = accounts[0]
            starting_total = sum(money.as_double() for money in account.starting_balances().values())
            total_pnl = account.balance().total.as_double() - starting_total
            total_return_pct = (total_pnl / starting_total) * 100 if starting_total > 0 else 0.0
        
        # Order metrics
        orders = engine.kernel.cache.orders()
        filled_orders = [o for o in orders if o.status == OrderStatus.FILLED]
        buy_orders = len([o for o in filled_orders if o.side.name == 'BUY'])
        sell_orders = len([o for o in filled_orders if o.side.name == 'SELL'])
        
        # Position metrics
        positions = engine.kernel.cache.positions_open()
        
        return {
            'strategy_name': strategy_name,
            'success': True,
            'total_pnl': total_pnl,
            'total_return_pct': total_return_pct,
            'total_orders': len(orders),
            'filled_orders': len(filled_orders),
            'buy_orders': buy_orders,
            'sell_orders': sell_orders,
            'trades': min(buy_orders, sell_orders),  # Complete round trips
            'open_positions': len(positions),
            'initial_capital': self.initial_capital,
            'final_balance': account.balance().total.as_double() if accounts else self.initial_capital,
        }
    
    def run_multi_strategy_backtest(self, strategies: List[Tuple[Strategy, str]]) -> Dict[str, Any]:
        """
        Run multiple strategies in parallel with shared order book simulation.
        Each strategy gets its own venue with separate capital allocation.
        
        Args:
            strategies: List of (strategy_instance, strategy_name) tuples
            
        Returns:
            Dictionary with overall and per-strategy performance metrics
        """
        try:
            # Discover instruments and data
            bars, voo_instrument, sh_instrument = self.discover_instruments()
            
            # Configure backtest engine for multi-strategy
            engine_config = BacktestEngineConfig(
                trader_id=TraderId("MULTI-STRATEGY"),
                logging=LoggingConfig(log_level="ERROR", log_colors=False),
            )
            
            engine = BacktestEngine(config=engine_config)
            
            # Create separate venues for each strategy
            venue_strategy_map = {}
            for idx, (strategy, name) in enumerate(strategies):
                # Create unique venue for each strategy
                venue_name = f"SIM-{idx+1}"
                venue = Venue(venue_name)
                
                # Add venue with separate capital allocation
                engine.add_venue(
                    venue=venue,
                    oms_type=OmsType.HEDGING,
                    account_type=AccountType.MARGIN,
                    base_currency=USD,
                    starting_balances=[Money(self.initial_capital, USD)],
                    fill_model=FillModel()
                )
                
                # Map venue to strategy for later reference
                venue_strategy_map[venue_name] = (strategy, name)
                
                # Configure strategy to use its specific venue
                # This requires updating the strategy's instrument IDs
                strategy._venue = venue
            
            # Add instruments to all venues
            for idx in range(len(strategies)):
                venue = Venue(f"SIM-{idx+1}")
                
                # Create VOO instrument for this venue
                voo_id = voo_instrument.id.replace(str(voo_instrument.venue), str(venue))
                voo_equity = Equity(
                    instrument_id=voo_id,
                    raw_symbol=Symbol(voo_instrument.symbol.value),
                    currency=USD,
                    price_precision=5,
                    price_increment=Price.from_str("0.00001"),
                    lot_size=Quantity.from_str("1"),
                    ts_event=0,
                    ts_init=0,
                )
                engine.add_instrument(voo_equity)
                
                # Create SH instrument for this venue if available
                if sh_instrument:
                    sh_id = sh_instrument.id.replace(str(sh_instrument.venue), str(venue))
                    sh_equity = Equity(
                        instrument_id=sh_id,
                        raw_symbol=Symbol(sh_instrument.symbol.value),
                        currency=USD,
                        price_precision=5,
                        price_increment=Price.from_str("0.00001"),
                        lot_size=Quantity.from_str("1"),
                        ts_event=0,
                        ts_init=0,
                    )
                    engine.add_instrument(sh_equity)
            
            # Add all strategies to the engine
            for strategy, name in strategies:
                logger.info(f"Adding strategy: {name}")
                engine.add_strategy(strategy)
            
            # Filter and add data (shared across all venues)
            start_ns = int(self.start_date.timestamp() * 1_000_000_000)
            end_ns = int(self.end_date.timestamp() * 1_000_000_000)
            
            # Add VOO bars
            voo_bars = [bar for bar in bars if bar.bar_type.instrument_id == voo_instrument]
            voo_bars = [bar for bar in voo_bars if start_ns <= bar.ts_event < end_ns]
            voo_bars.sort(key=lambda x: x.ts_event)
            logger.info(f"Adding {len(voo_bars):,} VOO bars")
            
            # Duplicate bars for each venue
            for idx in range(len(strategies)):
                venue = Venue(f"SIM-{idx+1}")
                venue_bars = []
                for bar in voo_bars:
                    # Update bar to use the venue-specific instrument ID
                    new_bar_type = bar.bar_type.replace(venue=venue)
                    venue_bar = Bar(
                        bar_type=new_bar_type,
                        open=bar.open,
                        high=bar.high,
                        low=bar.low,
                        close=bar.close,
                        volume=bar.volume,
                        ts_event=bar.ts_event,
                        ts_init=bar.ts_init,
                    )
                    venue_bars.append(venue_bar)
                engine.add_data(venue_bars)
            
            # Add SH bars if available
            if sh_instrument:
                sh_bars = [bar for bar in bars if bar.bar_type.instrument_id == sh_instrument]
                sh_bars = [bar for bar in sh_bars if start_ns <= bar.ts_event < end_ns]
                sh_bars.sort(key=lambda x: x.ts_event)
                logger.info(f"Adding {len(sh_bars):,} SH bars")
                
                # Duplicate bars for each venue
                for idx in range(len(strategies)):
                    venue = Venue(f"SIM-{idx+1}")
                    venue_bars = []
                    for bar in sh_bars:
                        new_bar_type = bar.bar_type.replace(venue=venue)
                        venue_bar = Bar(
                            bar_type=new_bar_type,
                            open=bar.open,
                            high=bar.high,
                            low=bar.low,
                            close=bar.close,
                            volume=bar.volume,
                            ts_event=bar.ts_event,
                            ts_init=bar.ts_init,
                        )
                        venue_bars.append(venue_bar)
                    engine.add_data(venue_bars)
            
            # Run backtest with all strategies in parallel
            logger.info("Running multi-strategy backtest...")
            engine.run()
            
            # Extract performance metrics
            return self._extract_multi_strategy_metrics(engine, strategies, venue_strategy_map)
            
        except Exception as e:
            logger.error(f"Error running multi-strategy backtest: {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e),
                'strategies': {}
            }
    
    def _extract_multi_strategy_metrics(self, engine: BacktestEngine, 
                                      strategies: List[Tuple[Strategy, str]],
                                      venue_strategy_map: Dict[str, Tuple[Strategy, str]]) -> Dict[str, Any]:
        """Extract performance metrics from multi-strategy backtest."""
        # Get all accounts (one per venue/strategy)
        accounts = engine.kernel.cache.accounts()
        
        # Calculate overall metrics
        total_initial_capital = self.initial_capital * len(strategies)
        total_final_balance = 0.0
        total_pnl = 0.0
        
        # Per-strategy metrics
        strategy_metrics = {}
        
        for account in accounts:
            # Identify which strategy this account belongs to
            venue_name = str(account.id).split("-")[0] + "-" + str(account.id).split("-")[1]
            if venue_name in venue_strategy_map:
                strategy, name = venue_strategy_map[venue_name]
                
                # Calculate account metrics
                starting_balance = sum(money.as_double() for money in account.starting_balances().values())
                final_balance = account.balance().total.as_double()
                pnl = final_balance - starting_balance
                return_pct = (pnl / starting_balance) * 100 if starting_balance > 0 else 0.0
                
                total_final_balance += final_balance
                total_pnl += pnl
                
                # Get strategy-specific orders
                all_orders = engine.kernel.cache.orders()
                strategy_orders = [o for o in all_orders if o.strategy_id == strategy.id]
                filled_orders = [o for o in strategy_orders if o.status == OrderStatus.FILLED]
                buy_orders = len([o for o in filled_orders if o.side.name == 'BUY'])
                sell_orders = len([o for o in filled_orders if o.side.name == 'SELL'])
                
                # Get strategy-specific positions
                all_positions = engine.kernel.cache.positions()
                strategy_positions = [p for p in all_positions if p.strategy_id == strategy.id]
                open_positions = [p for p in strategy_positions if p.is_open]
                
                strategy_metrics[name] = {
                    'initial_capital': self.initial_capital,
                    'final_balance': final_balance,
                    'pnl': pnl,
                    'return_pct': return_pct,
                    'total_orders': len(strategy_orders),
                    'filled_orders': len(filled_orders),
                    'buy_orders': buy_orders,
                    'sell_orders': sell_orders,
                    'trades': min(buy_orders, sell_orders),
                    'open_positions': len(open_positions),
                    'venue': venue_name,
                }
        
        # Calculate overall return
        total_return_pct = (total_pnl / total_initial_capital) * 100 if total_initial_capital > 0 else 0.0
        
        return {
            'success': True,
            'overall': {
                'total_initial_capital': total_initial_capital,
                'total_final_balance': total_final_balance,
                'total_pnl': total_pnl,
                'total_return_pct': total_return_pct,
            },
            'strategies': strategy_metrics,
            'start_date': self.start_date.isoformat(),
            'end_date': self.end_date.isoformat(),
        }
    
    def compare_strategies(self, strategies: List[Tuple[Strategy, str]], 
                          parallel: bool = True) -> List[Dict[str, Any]]:
        """
        Compare multiple strategies.
        
        Args:
            strategies: List of (strategy_instance, strategy_name) tuples
            parallel: If True, runs strategies in parallel with separate venues. If False, runs separately.
            
        Returns:
            List of performance metrics dictionaries, sorted by return
        """
        if parallel:
            # Run all strategies together with separate venues
            print("Running multi-strategy backtest with separate venues...")
            multi_result = self.run_multi_strategy_backtest(strategies)
            
            if multi_result['success']:
                print(f"\nOverall Performance:")
                print(f"  Total Return: {multi_result['overall']['total_return_pct']:+.2f}%")
                print(f"  Total P&L: ${multi_result['overall']['total_pnl']:,.2f}")
                
                print(f"\nPer-Strategy Performance:")
                # Sort strategies by return
                sorted_strategies = sorted(
                    multi_result['strategies'].items(), 
                    key=lambda x: x[1]['return_pct'], 
                    reverse=True
                )
                
                for name, metrics in sorted_strategies:
                    print(f"  {name}:")
                    print(f"    - Return: {metrics['return_pct']:+.2f}%")
                    print(f"    - P&L: ${metrics['pnl']:,.2f}")
                    print(f"    - Orders: {metrics['filled_orders']}/{metrics['total_orders']} filled")
                    print(f"    - Trades: {metrics['trades']} complete round trips")
            
            return [multi_result]
        else:
            # Run strategies separately
            results = []
            
            for strategy, name in strategies:
                print(f"Testing strategy: {name}")
                result = self.run_strategy(strategy, name)
                results.append(result)
                
                if result['success']:
                    print(f"  -> {result['total_return_pct']:+.2f}% return")
                else:
                    print(f"  -> FAILED: {result.get('error', 'Unknown error')}")
            
            # Sort by return (descending)
            successful_results = [r for r in results if r['success']]
            failed_results = [r for r in results if not r['success']]
            
            successful_results.sort(key=lambda x: x['total_return_pct'], reverse=True)
            
            return successful_results + failed_results