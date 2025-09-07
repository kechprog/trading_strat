#!/usr/bin/env python3
"""
EMA Signal Indicator (Nautilus)

Provides a discrete trend signal derived from an EMA:
- Returns 1 when close > EMA (bullish regime)
- Returns -1 when close <= EMA (bearish regime)

This is a Nautilus-compatible Indicator (see breakout.py for the pattern),
separate from the project-local TrendIndicator-based EMA implementation.
"""

from __future__ import annotations

from nautilus_trader.indicators.base.indicator import Indicator
from nautilus_trader.indicators.average.ema import ExponentialMovingAverage
from nautilus_trader.model import Bar
from nautilus_trader.model.enums import PriceType


class EMASignalIndicator(Indicator):
    """Discrete EMA regime signal for Nautilus pipelines."""

    initialized: bool = False
    value: int | None = None  # 1 (bullish) or -1 (bearish)

    def __init__(self, period: int = 50, price_type: PriceType = PriceType.LAST) -> None:
        if period <= 1:
            raise ValueError("period must be >= 2")

        self.period = int(period)
        self.price_type = price_type

        # Use Nautilus EMA under the hood for consistency with breakout.py
        self._ema = ExponentialMovingAverage(self.period, self.price_type)

        self.initialized = False
        self.value = None

    def __repr__(self) -> str:
        return f"EMASignalIndicator(period={self.period}, price_type={self.price_type})"

    # --- Indicator API -----------------------------------------------------

    def handle_quote_tick(self, tick) -> None:  # pragma: no cover - unsupported
        raise RuntimeError("EMASignalIndicator does not support quote ticks")

    def handle_trade_tick(self, tick) -> None:  # pragma: no cover - unsupported
        raise RuntimeError("EMASignalIndicator does not support trade ticks")

    def handle_bar(self, bar: Bar) -> None:
        # Update internal EMA first
        self._ema.handle_bar(bar)

        # Mirror initialization state from internal EMA
        self.initialized = bool(self._ema.initialized)
        if not self.initialized:
            return

        # Compute discrete regime signal
        try:
            close_val = float(bar.close)
            ema_val = float(self._ema.value)  # type: ignore
        except Exception:
            # If conversion fails, do not update signal
            return

        self.value = 1 if close_val > ema_val else -1

    def reset(self) -> None:
        self._ema.reset()
        self.value = None
        self.initialized = False

    def _set_has_inputs(self, setting: bool) -> None:
        # Not used by this indicator (mirrors breakout example pattern)
        raise NotImplementedError()

    def _set_initialized(self, setting: bool) -> None:
        self.initialized = setting

    def _reset(self) -> None:
        self.reset()

