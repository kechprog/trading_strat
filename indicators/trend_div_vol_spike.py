#!/usr/bin/env python3
"""
Trend Divergence with Volume Spike Filter Indicator

This indicator implements the original trend divergence concept but adds a volume spike filter
to identify the end of divergence periods, as per the updated concept.
"""

from __future__ import annotations

from typing import List
from nautilus_trader.indicators.base.indicator import Indicator
from nautilus_trader.indicators.average.ema import ExponentialMovingAverage
from nautilus_trader.model import Bar
import math

class TrendDivVolSpikeIndicator(Indicator):
    """Trend divergence with volume spike filter indicator."""

    initialized: bool = False
    value: float | None = None  # Continuous signal

    def __init__(
        self,
        long_term_period: int = 100,       # EMA period for long-term trend (hours)
        short_term_period: int = 6,        # Period for short-term trend (days)
        volume_lookback: int = 20,         # Lookback window for volume statistics
        volume_spike_threshold: float = 1.5, # Z-score threshold for volume spike
        sensitivity: float = 3.5           # Signal sensitivity
    ) -> None:
        if long_term_period <= 1 or short_term_period <= 1 or volume_lookback <= 5:
            raise ValueError("Parameters must be reasonable")
            
        self.long_term_period = int(long_term_period)
        self.short_term_period = int(short_term_period)
        self.volume_lookback = int(volume_lookback)
        self.volume_spike_threshold = float(volume_spike_threshold)
        self.sensitivity = float(sensitivity)
        
        # EMA for long-term trend
        self._long_ema = ExponentialMovingAverage(long_term_period)
        
        # Data storage
        self._max_len = max(long_term_period, short_term_period, volume_lookback) + 50
        self._prices: List[float] = []
        self._emas: List[float] = []  # Store EMA values for regression
        self._volumes: List[float] = []
        self._log_volumes: List[float] = []
        
        # Track divergence state
        self._in_divergence = False
        self._divergence_type = 0  # 0 = no divergence, 1 = long up/short down, -1 = long down/short up
        
        self.initialized = False
        self.value = None

    def __repr__(self) -> str:
        return (
            f"TrendDivVolSpikeIndicator("
            f"long_term_period={self.long_term_period}, "
            f"short_term_period={self.short_term_period}, "
            f"volume_lookback={self.volume_lookback}, "
            f"volume_spike_threshold={self.volume_spike_threshold}, "
            f"sensitivity={self.sensitivity})"
        )

    def handle_quote_tick(self, tick) -> None:
        raise RuntimeError("TrendDivVolSpikeIndicator does not support quote ticks")

    def handle_trade_tick(self, tick) -> None:
        raise RuntimeError("TrendDivVolSpikeIndicator does not support trade ticks")

    def handle_bar(self, bar: Bar) -> None:
        # Update EMA
        self._long_ema.handle_bar(bar)
        
        # Add current data
        current_price = float(bar.close)
        current_volume = float(bar.volume)
        
        self._prices.append(current_price)
        self._volumes.append(current_volume)
        self._log_volumes.append(math.log(current_volume) if current_volume > 0 else 0)
        
        # Add EMA value if initialized
        if self._long_ema.initialized:
            self._emas.append(float(self._long_ema.value))
        
        # Maintain fixed-size buffers
        if len(self._prices) > self._max_len:
            self._prices.pop(0)
        if len(self._volumes) > self._max_len:
            self._volumes.pop(0)
        if len(self._log_volumes) > self._max_len:
            self._log_volumes.pop(0)
        if len(self._emas) > self._max_len:
            self._emas.pop(0)

        # Need sufficient data
        if len(self._prices) < self._max_len or len(self._emas) < self._max_len:
            return
            
        # Need EMA to be initialized
        if not self._long_ema.initialized:
            return

        # Update divergence state
        self._update_divergence_state()
        
        # Generate signal
        signal = self._generate_signal()
        
        self.value = signal * self.sensitivity
        self.initialized = True

    def reset(self) -> None:
        self._prices.clear()
        self._emas.clear()
        self._volumes.clear()
        self._log_volumes.clear()
        self._long_ema.reset()
        self._in_divergence = False
        self._divergence_type = 0
        self.value = None
        self.initialized = False

    def _set_has_inputs(self, setting: bool) -> None:
        raise NotImplementedError()

    def _set_initialized(self, setting: bool) -> None:
        self.initialized = setting

    def _reset(self) -> None:
        self.reset()

    def _update_divergence_state(self) -> None:
        """Update the divergence state tracking"""
        if len(self._prices) < self._max_len or len(self._emas) < self._max_len:
            return
            
        # Calculate long-term trend (linear regression on EMA values)
        long_trend = self._calculate_long_term_trend()
        
        # Calculate short-term trend (simple momentum)
        short_trend = self._calculate_short_term_trend()
        
        if long_trend == 0 or short_trend == 0:
            self._in_divergence = False
            self._divergence_type = 0
            return
            
        # Check for trend divergence
        trend_divergence = long_trend * short_trend < 0  # Opposite signs
        
        # Check for volume spike (end of divergence signal)
        volume_spike = self._detect_volume_spike()
        
        if not trend_divergence:
            # No divergence
            self._in_divergence = False
            self._divergence_type = 0
        elif volume_spike:
            # Volume spike during divergence - this marks end of divergence
            # Reset divergence state
            self._in_divergence = False
            self._divergence_type = 0
        else:
            # There is divergence and no volume spike, update state
            self._in_divergence = True
            if long_trend > 0 and short_trend < 0:  # Long-term up, short-term down
                self._divergence_type = 1
            elif long_trend < 0 and short_trend > 0:  # Long-term down, short-term up
                self._divergence_type = -1

    def _generate_signal(self) -> float:
        """Generate signal based on trend divergence and volume confirmation"""
        if len(self._prices) < self._max_len or len(self._emas) < self._max_len:
            return 0.0
            
        # Calculate long-term trend (linear regression on EMA values)
        long_trend = self._calculate_long_term_trend()
        
        # Calculate short-term trend (simple momentum)
        short_trend = self._calculate_short_term_trend()
        
        if long_trend == 0 or short_trend == 0:
            return short_trend  # Follow momentum when trends are flat
            
        # Check for trend divergence
        trend_divergence = long_trend * short_trend < 0  # Opposite signs
        
        # Check for volume spike
        volume_spike = self._detect_volume_spike()
        
        if not trend_divergence:
            # No divergence, follow momentum
            return short_trend
        elif volume_spike:
            # Volume spike during divergence - this marks end of divergence
            # Follow the short-term trend as this indicates a potential new trend
            return short_trend
        else:
            # Divergence without volume spike - expect reversion to long-term trend
            return long_trend

    def _calculate_long_term_trend(self) -> float:
        """Calculate long-term trend using linear regression on EMA values"""
        if len(self._emas) < 30:
            return 0.0
            
        # Use recent EMA values for regression
        ema_values = self._emas[-30:]
        
        # Linear regression to find slope
        n = len(ema_values)
        sum_x = sum(range(n))
        sum_y = sum(ema_values)
        sum_xy = sum(i * ema_values[i] for i in range(n))
        sum_xx = sum(i * i for i in range(n))
        
        denominator = n * sum_xx - sum_x * sum_x
        if denominator == 0:
            return 0.0
            
        slope = (n * sum_xy - sum_x * sum_y) / denominator
        
        # Normalize by recent price level
        avg_price = sum(ema_values) / len(ema_values) if ema_values else 1.0
        if avg_price != 0:
            return slope / avg_price * 1000  # Scale appropriately
        return 0.0

    def _calculate_short_term_trend(self) -> float:
        """Calculate short-term trend using recent momentum"""
        if len(self._prices) < self.short_term_period + 1:
            return 0.0
            
        # Calculate momentum over short term
        current_price = self._prices[-1]
        past_price = self._prices[-(self.short_term_period + 1)]
        
        if past_price == 0:
            return 0.0
            
        momentum = (current_price - past_price) / past_price * 100  # Percentage change
        return momentum

    def _detect_volume_spike(self) -> bool:
        """Detect if current volume is a spike compared to recent history"""
        if len(self._log_volumes) < self.volume_lookback:
            return False
            
        # Get recent log volumes
        recent_log_volumes = self._log_volumes[-self.volume_lookback:]
        
        # Calculate mean and std of log volumes
        mean_log_vol = sum(recent_log_volumes) / len(recent_log_volumes)
        
        if len(recent_log_volumes) < 2:
            return False
            
        # Calculate standard deviation
        variance = sum((lv - mean_log_vol) ** 2 for lv in recent_log_volumes) / (len(recent_log_volumes) - 1)
        std_log_vol = math.sqrt(variance) if variance > 0 else 1.0
        
        # Current log volume
        current_log_vol = self._log_volumes[-1]
        
        # Calculate z-score
        if std_log_vol > 0:
            z_score = (current_log_vol - mean_log_vol) / std_log_vol
            return z_score > self.volume_spike_threshold
        
        return False