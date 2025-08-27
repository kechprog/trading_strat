#!/usr/bin/env python3
"""
Exponential Moving Average (EMA) Indicator

This indicator uses a 50-period EMA on hourly candles to generate mean-reversion signals:
- Long signal when price is below EMA (expecting reversion upward)
- Short signal when price is above EMA (expecting reversion downward)

The indicator follows a mean-reversion approach, betting on price returning to the average.
"""

import numpy as np
from typing import Dict, Any, Optional

from .base import TrendIndicator


class EMAIndicator(TrendIndicator):
    """
    EMA-based mean reversion indicator using hourly candles.
    
    Generates long signals when price < EMA and short signals when price > EMA.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        
        # EMA parameters
        self.ema_period = self.config.get('ema_period', 50)  # 50-hour lookback
        self.signal_strength = self.config.get('signal_strength', 100.0)  # Amplification factor
        
    def calculate(self, price_data: np.ndarray, volume_data: np.ndarray) -> float:
        """
        Calculate EMA-based mean reversion signal.
        
        Args:
            price_data: Array of hourly close prices
            volume_data: Array of hourly volumes (not used in EMA calculation)
            
        Returns:
            Float signal value:
            - Positive when price < EMA (long signal)
            - Negative when price > EMA (short signal)
            - Magnitude indicates distance from EMA
        """
        
        if len(price_data) < self.ema_period:
            # Not enough data for EMA calculation
            return 0.0
            
        # Calculate EMA
        ema_value = self._calculate_ema(price_data)
        
        if ema_value == 0:
            return 0.0
            
        # Current price
        current_price = price_data[-1]
        
        # Calculate deviation from EMA (as percentage)
        deviation = (current_price - ema_value) / ema_value
        
        # Generate mean reversion signal
        # When price < EMA (negative deviation), return positive signal (long)
        # When price > EMA (positive deviation), return negative signal (short)
        signal = -deviation * self.signal_strength
        
        return signal
    
    def _calculate_ema(self, prices: np.ndarray) -> float:
        """
        Calculate Exponential Moving Average.
        
        Args:
            prices: Array of price values
            
        Returns:
            EMA value
        """
        
        if len(prices) < self.ema_period:
            return 0.0
            
        # EMA multiplier (smoothing factor)
        multiplier = 2.0 / (self.ema_period + 1)
    
        # Calculate EMA for all prices
        # We recalculate from a reasonable starting point to handle data updates
        lookback = min(len(prices), self.ema_period * 2)  # Look back at most 2x the period
        start_idx = len(prices) - lookback
        
        # Start with SMA of first ema_period prices in our lookback window
        ema = np.mean(prices[start_idx:start_idx + self.ema_period])
        
        # Apply EMA formula to subsequent prices
        for i in range(start_idx + self.ema_period, len(prices)):
            ema = (prices[i] * multiplier) + (ema * (1 - multiplier))
            
        return ema
    
    @property
    def name(self) -> str:
        """Return the name of this indicator."""
        return f"EMAIndicator(period={self.ema_period})"