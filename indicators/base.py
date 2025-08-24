#!/usr/bin/env python3
"""
Base indicator interface for trading strategy indicators.

This module provides the abstract base class that all indicators should inherit from
to ensure a consistent interface for the breakout strategy.
"""

import numpy as np
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional


class TrendIndicator(ABC):
    """
    Abstract base class for trend indicators used in breakout strategies.
    
    All indicators should inherit from this class and implement the calculate method.
    The indicator should return a float value where:
    - Positive values indicate bullish trend
    - Negative values indicate bearish trend  
    - Values near zero indicate neutral/flat trend
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the indicator with configuration parameters."""
        self.config = config or {}
    
    @abstractmethod
    def calculate(self, price_data: np.ndarray, volume_data: np.ndarray) -> float:
        """
        Calculate the indicator value based on price and volume data.
        
        Args:
            price_data: Array of price values (typically close prices)
            volume_data: Array of volume values
            
        Returns:
            Float indicator value where:
            - Positive = bullish trend
            - Negative = bearish trend
            - Near zero = neutral trend
            - np.nan if calculation fails
        """
        pass
    
    @property
    def name(self) -> str:
        """Return the name of this indicator."""
        return self.__class__.__name__
    
    def get_config(self) -> Dict[str, Any]:
        """Return the current configuration."""
        return self.config.copy()