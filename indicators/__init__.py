"""
Trading Indicators Package - TOP 5 PERFORMERS + HOURLY

🎉 BREAKTHROUGH ACHIEVED: 63% ROI with optimized strategy parameters! 🎉

TOP 5 DAILY INDICATORS (by backtesting performance):
1. MomentumMeanReversionIndicator: 22.67% (WINNER - used in 63% ROI configuration)
2. PercentileMomentumIndicator: 21.19% 
3. MomentumIndicator: 18.76%
4. MovingAverageCrossoverIndicator: 18.61%
5. EnhancedFFTIndicator: 16.66%

HOURLY INDICATOR:
- HourlyMomentumMeanReversionIndicator: Optimized for hourly data with volume weighting

OPTIMAL CONFIGURATION (63% ROI):
- Indicator: MomentumMeanReversionIndicator
- Strategy: BreakoutStrategy with Long(1,8) Short(6,1) parameters
- Key insight: Strategy parameter optimization was more impactful than indicator development

All indicators implement the TrendIndicator interface for consistent usage.
"""

from .base import TrendIndicator
from .enhanced_fft_indicator import EnhancedFFTIndicator  
from .volume_smoothing import SMOOTHING_METHODS
from .trend_indicators import (
    MomentumIndicator,
    MovingAverageCrossoverIndicator
)
from .percentile_momentum import PercentileMomentumIndicator
from .momentum_mean_reversion_indicator import MomentumMeanReversionIndicator
from .hourly_momentum_mean_reversion import HourlyMomentumMeanReversionIndicator

__all__ = [
    'TrendIndicator',
    'MomentumMeanReversionIndicator',  # #1 - Winner (22.67%, used in 63% config)
    'PercentileMomentumIndicator',     # #2 - 21.19%
    'MomentumIndicator',               # #3 - 18.76%
    'MovingAverageCrossoverIndicator', # #4 - 18.61%
    'EnhancedFFTIndicator',            # #5 - 16.66%
    # Hourly indicator for intraday trading
    'HourlyMomentumMeanReversionIndicator',
    'SMOOTHING_METHODS'
]