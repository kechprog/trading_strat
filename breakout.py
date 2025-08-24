#!/usr/bin/env python3
"""
Breakout Strategy: Configurable Indicator-Based Trading Strategy

This strategy uses configurable trend indicators to generate trading signals
with breakout entry/exit logic based on daily candles.

The strategy:
1. Subscribes directly to daily bars from Nautilus
2. Calculates indicator values on daily data (once per day)  
3. Places stop orders at the start of each day based on daily breakout levels
4. Uses fixed position sizing (configurable number of contracts)
5. Supports swappable indicators via the TrendIndicator interface

Based on the outline in nautilus_impl/strategies/outline.md
"""

import logging
import numpy as np
from typing import Optional, Dict, Any

from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.objects import Quantity
from nautilus_trader.trading.strategy import Strategy

# Import indicator interface and default implementation
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))
from indicators import TrendIndicator


logger = logging.getLogger(__name__)




class BreakoutStrategy(Strategy):
    """
    A configurable breakout trading strategy that supports swappable trend indicators.
    
    The strategy operates on daily candles:
    1. Subscribes directly to daily bars from Nautilus
    2. Calculates indicator values on daily data (once per day)
    3. Places orders based on breakout levels and indicator signals
    4. Uses fixed position sizing (configurable number of contracts)
    5. Supports any indicator implementing the TrendIndicator interface
    """
    
    def __init__(self, indicator: TrendIndicator, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the breakout strategy with a pluggable indicator.
        
        Args:
            indicator: TrendIndicator instance to use for trend detection
            config: Strategy configuration parameters
        """
        super().__init__()
        self.voo_instrument = None  # Primary instrument (VOO)
        self.sh_instrument = None   # Inverse ETF instrument (SH)
        self.position = 0  # -1 for short (SH), 0 for neutral, 1 for long (VOO)
        self._bar_count = 0
        
        # Historical daily data for indicator calculation
        self._daily_closes = []
        self._daily_volumes = []
        self._daily_highs = []
        self._daily_lows = []
        
        # Strategy parameters (configurable)
        config = config or {}
        

        self.long_entry_lookback = config.get('long_entry_lookback', 1)   # Ultra-fast long entries
        self.long_exit_lookback = config.get('long_exit_lookback', 8)     # Very slow long exits  
        self.short_entry_lookback = config.get('short_entry_lookback', 6) # Cautious short entries
        self.short_exit_lookback = config.get('short_exit_lookback', 1)   # Quick short exits
        self.neutral_threshold = config.get('neutral_threshold', 1e-10)
        self.trade_size_pct = config.get('trade_size_pct', 1.0)  # Use 100% of available capital
        self.min_trade_size = config.get('min_trade_size', 1)  # Minimum shares
        
        # Store the pluggable indicator
        self.indicator = indicator
        
        # Daily signal tracking (calculated once per day)
        self._indicator_value = np.nan
        self._highest_high_long = np.nan
        self._lowest_low_long_exit = np.nan
        self._lowest_low_short = np.nan
        self._highest_high_short_exit = np.nan
        
    def on_start(self):
        """Called when the strategy is started."""
        logger.info(f"BreakoutStrategy started with parameters:")
        logger.info(f"  Indicator: {self.indicator.name}")
        logger.info(f"  Indicator config: {self.indicator.get_config()}")
        logger.info(f"  Long entry lookback: {self.long_entry_lookback}")
        logger.info(f"  Long exit lookback: {self.long_exit_lookback}")
        logger.info(f"  Short entry lookback: {self.short_entry_lookback}")
        logger.info(f"  Short exit lookback: {self.short_exit_lookback}")
        logger.info(f"  Neutral threshold: {self.neutral_threshold}")
        logger.info(f"  Trade size: {self.trade_size_pct*100:.0f}% of available capital")
        
        # Subscribe to both minute and daily bar data for all available instruments  
        for instrument in self.cache.instruments():
            # Subscribe to daily bars for indicator calculation
            daily_bar_type = BarType.from_str(f"{instrument.id}-1-DAY-LAST-EXTERNAL")
            logger.info(f"Subscribing to daily bars: {daily_bar_type}")
            self.subscribe_bars(daily_bar_type)
            
            # Subscribe to minute bars for trade execution
            minute_bar_type = BarType.from_str(f"{instrument.id}-1-MINUTE-LAST-EXTERNAL")
            logger.info(f"Subscribing to minute bars: {minute_bar_type}")
            self.subscribe_bars(minute_bar_type)
            
            # Identify VOO and SH instruments
            symbol = str(instrument.id.symbol)
            if symbol == "VOO":
                self.voo_instrument = instrument.id
                logger.info(f"VOO instrument identified: {self.voo_instrument}")
            elif symbol == "SH":
                self.sh_instrument = instrument.id
                logger.info(f"SH instrument identified: {self.sh_instrument}")
    
    def on_bar(self, bar: Bar):
        """Handle both minute and daily bar data."""
        # Determine if this is a daily or minute bar
        is_daily_bar = "DAY" in str(bar.bar_type)
        
        if is_daily_bar:
            # Handle daily bar for indicator calculation
            self._on_daily_bar(bar)
        else:
            # Handle minute bar for trade execution
            self._on_minute_bar(bar)
    
    def _on_daily_bar(self, bar: Bar):
        """Handle daily bar data for indicator calculation."""
        self._bar_count += 1
        
        # Only process VOO bars for indicator calculation
        # SH bars are used for trading inverse positions
        if str(bar.bar_type.instrument_id.symbol) != "VOO":
            return
        
        # Store daily bar data for indicator calculation (VOO only)
        self._daily_closes.append(float(bar.close))
        self._daily_volumes.append(float(bar.volume))
        self._daily_highs.append(float(bar.high))
        self._daily_lows.append(float(bar.low))
        
        # Keep only necessary history (maximum lookback window + indicator window)
        indicator_window = getattr(self.indicator, 'window', 28)  # Default to 28 if no window attribute
        max_history = max(indicator_window, self.long_entry_lookback,
                         self.short_entry_lookback, self.long_exit_lookback,
                         self.short_exit_lookback) + 10
        
        if len(self._daily_closes) > max_history:
            self._daily_closes = self._daily_closes[-max_history:]
            self._daily_volumes = self._daily_volumes[-max_history:]
            self._daily_highs = self._daily_highs[-max_history:]
            self._daily_lows = self._daily_lows[-max_history:]
        
        logger.info(f"Received daily bar #{self._bar_count}: "
                   f"O={bar.open} H={bar.high} L={bar.low} C={bar.close} V={bar.volume}")
        
        # Calculate daily signals with new bar
        self._calculate_daily_signals()
    
    def _on_minute_bar(self, bar: Bar):
        """Handle minute bar data for trade execution."""
        # Only process VOO minute bars for trading decisions
        if str(bar.bar_type.instrument_id.symbol) != "VOO":
            return
        
        # Check breakout conditions on minute bars for better entry/exit prices
        self._check_minute_breakouts(bar)
    
    def _calculate_daily_signals(self):
        """Calculate indicator value and breakout levels using daily bars."""
        # Need minimum data for indicator calculation
        min_required = getattr(self.indicator, 'window', 28) + 1  # Default to 28 if no window attribute
        if len(self._daily_closes) < min_required:
            logger.info(f"Not enough daily bars for calculation: {len(self._daily_closes)}/{min_required}")
            return
        
        # Calculate indicator value excluding current (incomplete) day
        # Use all data except the last bar (current day)
        self._indicator_value = self.indicator.calculate(
            np.array(self._daily_closes[:-1]), 
            np.array(self._daily_volumes[:-1])
        )
        
        # Calculate breakout levels from previous days (excluding current day)
        # This mimics shift(1) behavior from the original strategy
        if len(self._daily_highs) > self.long_entry_lookback:
            # Get the last N days BEFORE today
            self._highest_high_long = max(self._daily_highs[-(self.long_entry_lookback+1):-1])
        
        if len(self._daily_lows) > self.long_exit_lookback:
            # Get the last N days BEFORE today
            self._lowest_low_long_exit = min(self._daily_lows[-(self.long_exit_lookback+1):-1])
        
        # Calculate short signals (now enabled with SH)
        if len(self._daily_lows) > self.short_entry_lookback:
            # Get the last N days BEFORE today
            self._lowest_low_short = min(self._daily_lows[-(self.short_entry_lookback+1):-1])
        
        if len(self._daily_highs) > self.short_exit_lookback:
            # Get the last N days BEFORE today
            self._highest_high_short_exit = max(self._daily_highs[-(self.short_exit_lookback+1):-1])
        
        # Log the signals
        if not np.isnan(self._indicator_value):
            logger.info(f"Daily signals calculated: "
                       f"{self.indicator.name}={self._indicator_value:.6f}, "
                       f"LongEntry>{self._highest_high_long:.2f}, "
                       f"LongExit<{self._lowest_low_long_exit:.2f}, "
                       f"ShortEntry<{self._lowest_low_short:.2f}, "
                       f"ShortExit>{self._highest_high_short_exit:.2f}")
        else:
            logger.info(f"Daily signals calculated: {self.indicator.name}=nan (insufficient valid data)")
    
    def _check_minute_breakouts(self, current_bar: Bar):
        """Check for breakout conditions on minute bars and execute market orders immediately."""
        if self.voo_instrument is None or self.sh_instrument is None:
            return
        
        # Skip if we don't have indicator values yet
        if np.isnan(self._indicator_value):
            return
        
        # Get actual position from Nautilus cache
        voo_position_size = self._get_actual_position_size(self.voo_instrument)
        sh_position_size = self._get_actual_position_size(self.sh_instrument)
        
        # Update internal position tracking
        if voo_position_size > 0:
            self.position = 1  # Long VOO
        elif sh_position_size > 0:
            self.position = -1  # Short (via SH)
        else:
            self.position = 0  # Flat
        
        # Check for breakout conditions using current minute bar prices
        current_high = float(current_bar.high)
        current_low = float(current_bar.low)
        current_time = current_bar.ts_event
        
        # Determine market regime
        is_bullish = self._indicator_value > self.neutral_threshold
        is_bearish = self._indicator_value < -self.neutral_threshold
        
        # Check for exit conditions FIRST (independent of indicator state - exits don't care about indicator)
        if self.position == 1 and not np.isnan(self._lowest_low_long_exit):
            # Long exit: current low breaks below lowest low of last N days
            if current_low < self._lowest_low_long_exit:
                logger.info(f"✅ LONG EXIT on minute bar @ {current_time}: {current_low:.2f} < {self._lowest_low_long_exit:.2f}")
                self._execute_market_sell_exit()
                # Update position after exit
                self.position = 0
        
        # Short exit: current high breaks above highest high of last N days
        elif self.position == -1 and not np.isnan(self._highest_high_short_exit):
            if current_high > self._highest_high_short_exit:
                logger.info(f"✅ SHORT EXIT on minute bar @ {current_time}: {current_high:.2f} > {self._highest_high_short_exit:.2f}")
                self._execute_market_sell_sh_exit()
                # Update position after exit
                self.position = 0
        
        # Check for entry conditions (only when no position AND indicator agrees)
        # Entry requires BOTH breakout level AND indicator agreement
        elif self.position == 0:
            if is_bullish and not np.isnan(self._highest_high_long):
                # Long entry: current high breaks above highest high of last N days
                if current_high > self._highest_high_long:
                    logger.info(f"✅ LONG ENTRY on minute bar @ {current_time}: {current_high:.2f} > {self._highest_high_long:.2f}")
                    self._execute_market_buy_voo_entry()
            
            elif is_bearish and not np.isnan(self._lowest_low_short):
                # Short entry: current low breaks below lowest low of last N days (buy SH)
                if current_low < self._lowest_low_short:
                    logger.info(f"✅ SHORT ENTRY on minute bar @ {current_time}: {current_low:.2f} < {self._lowest_low_short:.2f}")
                    self._execute_market_buy_sh_entry()
    
    def _calculate_position_size(self, price: float) -> int:
        """Calculate position size using all available capital."""
        # Get available cash balance
        accounts = self.cache.accounts()
        if not accounts:
            logger.warning("Could not get any accounts, using minimum trade size")
            return self.min_trade_size
            
        # Use the first account (should be the only one in backtest)
        account = accounts[0]
        available_balance = account.balance_total().as_double()
        
        # Use all available capital
        shares = int(available_balance / price)
        
        # Ensure minimum trade size
        shares = max(shares, self.min_trade_size)
        
        logger.info(f"Position sizing: balance=${available_balance:.2f}, price=${price:.2f}, shares={shares} (${shares*price:.2f})")
        return shares

    def _execute_market_buy_voo_entry(self):
        """Execute immediate market buy order for VOO long entry."""
        # Get current price for position sizing (use previous close as approximation)
        current_price = float(self._daily_closes[-1]) if self._daily_closes else 400.0  # fallback price
        position_size = self._calculate_position_size(current_price)
        
        order = self.order_factory.market(
            instrument_id=self.voo_instrument,
            order_side=OrderSide.BUY,
            quantity=Quantity.from_str(str(position_size)),
        )
        
        self.submit_order(order)
        # Update position immediately in backtest since order executes immediately
        self.position = 1
        logger.info(f"Executed MARKET BUY VOO ENTRY order: qty={position_size} (${position_size*current_price:.2f}), position updated to LONG")
    
    def _execute_market_buy_sh_entry(self):
        """Execute immediate market buy order for SH short entry."""
        # Get current price for position sizing (use previous close as approximation)
        current_price = float(self._daily_closes[-1]) if self._daily_closes else 400.0  # fallback price for VOO
        # For SH, we'll use similar sizing but the price will be different
        sh_price = current_price * 0.25  # SH typically trades at about 1/4 of VOO price
        position_size = self._calculate_position_size(sh_price)
        
        order = self.order_factory.market(
            instrument_id=self.sh_instrument,
            order_side=OrderSide.BUY,
            quantity=Quantity.from_str(str(position_size)),
        )
        
        self.submit_order(order)
        # Update position immediately in backtest since order executes immediately
        self.position = -1
        logger.info(f"Executed MARKET BUY SH ENTRY order: qty={position_size} (${position_size*sh_price:.2f}), position updated to SHORT")
    
    def _execute_market_sell_exit(self):
        """Execute immediate market sell order for VOO long exit."""
        # Use Nautilus's close_position method if available, otherwise use market order
        positions = self.cache.positions_open(venue=None, instrument_id=self.voo_instrument)
        
        if not positions:
            logger.warning("Cannot exit VOO: no positions to close")
            return
            
        # Try to close all VOO positions
        for pos in positions:
            if pos.side.name == 'LONG':
                try:
                    # Use the built-in close_position method
                    self.close_position(pos)
                    logger.info(f"Closed VOO position using close_position(): {pos.id} qty={pos.quantity}")
                except AttributeError:
                    # Fallback to manual market order
                    order = self.order_factory.market(
                        instrument_id=self.voo_instrument,
                        order_side=OrderSide.SELL,
                        quantity=pos.quantity,
                    )
                    self.submit_order(order)
                    logger.info(f"Closed VOO position using market order: {pos.id} qty={pos.quantity}")
        
        # Update position immediately in backtest since order executes immediately  
        self.position = 0
        logger.info(f"Executed MARKET SELL VOO EXIT - all long positions closed, position updated to FLAT")
    
    def _execute_market_sell_sh_exit(self):
        """Execute immediate market sell order for SH short exit."""
        # Use Nautilus's close_position method if available, otherwise use market order
        positions = self.cache.positions_open(venue=None, instrument_id=self.sh_instrument)
        
        if not positions:
            logger.warning("Cannot exit SH: no positions to close")
            return
            
        # Try to close all SH positions
        for pos in positions:
            if pos.side.name == 'LONG':  # We buy SH to go short
                try:
                    # Use the built-in close_position method
                    self.close_position(pos)
                    logger.info(f"Closed SH position using close_position(): {pos.id} qty={pos.quantity}")
                except AttributeError:
                    # Fallback to manual market order
                    order = self.order_factory.market(
                        instrument_id=self.sh_instrument,
                        order_side=OrderSide.SELL,
                        quantity=pos.quantity,
                    )
                    self.submit_order(order)
                    logger.info(f"Closed SH position using market order: {pos.id} qty={pos.quantity}")
        
        # Update position immediately in backtest since order executes immediately  
        self.position = 0
        logger.info(f"Executed MARKET SELL SH EXIT - all short positions closed, position updated to FLAT")

    
    def _get_actual_position_size(self, instrument_id) -> int:
        """Get the actual position size from Nautilus cache for a specific instrument."""
        if instrument_id is None:
            return 0
            
        positions = self.cache.positions_open(venue=None, instrument_id=instrument_id)
        total_size = 0
        
        if positions:
            for pos in positions:
                if pos.side.name == 'LONG':
                    total_size += int(pos.quantity)
                elif pos.side.name == 'SHORT':
                    total_size -= int(pos.quantity)
        
        return total_size
    
    # Order cancellation no longer needed with immediate market orders
    
    def on_order_filled(self, event):
        """Handle order fills."""
        logger.info(f"Order filled: {event.client_order_id} - {event.last_qty} @ {event.last_px}")
        
        # Update position from actual Nautilus state
        voo_position_size = self._get_actual_position_size(self.voo_instrument)
        sh_position_size = self._get_actual_position_size(self.sh_instrument)
        
        if voo_position_size > 0:
            self.position = 1
            logger.info(f"📊 Position status: LONG VOO ({voo_position_size} shares)")
        elif sh_position_size > 0:
            self.position = -1
            logger.info(f"📊 Position status: SHORT via SH ({sh_position_size} shares)")
        else:
            self.position = 0
            logger.info(f"📊 Position status: FLAT")
    
    def on_stop(self):
        """Called when the strategy is stopped."""
        logger.info("BreakoutStrategy stopped")
        logger.info(f"Indicator used: {self.indicator.name}")
        logger.info(f"Total daily bars processed: {self._bar_count}")
        logger.info(f"Final position: {self.position}")
        
        # Log final position and P&L
        positions = self.cache.positions_open()
        if positions:
            for position in positions:
                logger.info(f"Final position: {position}")
        else:
            logger.info("No open positions at end of backtest")
