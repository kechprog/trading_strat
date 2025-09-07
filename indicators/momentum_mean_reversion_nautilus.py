#!/usr/bin/env python3
"""
Momentum Mean Reversion Indicator (Nautilus)

This reimplements the existing MomentumMeanReversionIndicator, but as a
`nautilus_trader` `Indicator` subclass (see breakout.py for an example of
an `Indicator` implementation and lifecycle).

Behavior summary:
- Uses momentum for entries (volume-weighted short-term vs medium-term drift).
- Uses mean reversion for exits (strong deviation from recent mean).
- Prioritizes mean reversion signals over momentum when both present.

The indicator updates on bars only. Quote/trade tick handlers are unsupported.
`value` is a float signal (positive = bullish bias, negative = bearish bias).
"""

from __future__ import annotations

from typing import List, Optional

import math

from nautilus_trader.indicators.base.indicator import Indicator
from nautilus_trader.model import Bar


class MomentumMeanReversionNautilusIndicator(Indicator):
    """Nautilus-compatible momentum + mean reversion indicator.

    Parameters
    - reversion_window: lookback (bars) for mean reversion baseline.
    - momentum_peak_threshold: minimum magnitude of momentum to signal (percent, 0.001% = 0.001).
    - overbought_threshold: deviation threshold (in basis points) for strong reversion.
    - entry_amplifier: scale for momentum driven entries when no reversion signal.
    - exit_amplifier: scale for reversion driven exits when deviation present.
    """

    initialized: bool = False
    value: float | None = None

    def __init__(
        self,
        reversion_window: int = 15,
        momentum_peak_threshold: float = 1.0,
        overbought_threshold: float = 2.5,
        entry_amplifier: float = 2.0,
        exit_amplifier: float = 1.5,
    ) -> None:
        if reversion_window <= 1:
            raise ValueError("reversion_window must be >= 2")

        self.reversion_window = int(reversion_window)
        self.momentum_peak_threshold = float(momentum_peak_threshold)
        self.overbought_threshold = float(overbought_threshold)
        self.entry_amplifier = float(entry_amplifier)
        self.exit_amplifier = float(exit_amplifier)

        # Keep minimal but sufficient history for calculations
        # - momentum requires 10 bars of price/volume
        # - volume factor uses 20 bars of volume
        # - reversion uses `reversion_window` bars
        self._max_len = max(20, self.reversion_window)
        self._prices: List[float] = []
        self._volumes: List[float] = []

        self.initialized = False
        self.value = None

    def __repr__(self) -> str:
        return (
            "MomentumMeanReversionNautilusIndicator("
            f"reversion_window={self.reversion_window}, "
            f"momentum_peak_threshold={self.momentum_peak_threshold}, "
            f"overbought_threshold={self.overbought_threshold}, "
            f"entry_amplifier={self.entry_amplifier}, "
            f"exit_amplifier={self.exit_amplifier})"
        )

    # --- Indicator API -----------------------------------------------------

    def handle_quote_tick(self, tick) -> None:  # pragma: no cover - unsupported
        raise RuntimeError("MomentumMeanReversionNautilusIndicator does not support quote ticks")

    def handle_trade_tick(self, tick) -> None:  # pragma: no cover - unsupported
        raise RuntimeError("MomentumMeanReversionNautilusIndicator does not support trade ticks")

    def handle_bar(self, bar: Bar) -> None:
        # Ingest
        px = self._px_to_float(bar)
        vol = self._vol_to_float(bar)
        self._append(px, vol)

        # Mark initialized once we have enough observations
        required = max(20, 10, self.reversion_window)
        if not self.initialized and len(self._prices) >= required:
            self.initialized = True

        # Compute when initialized
        if not self.initialized:
            return

        # Priority: mean reversion signal (exits/profit taking) over momentum
        reversion_signal = self._calc_mean_reversion()
        if reversion_signal != 0.0:
            self.value = reversion_signal * self.exit_amplifier
            return

        # Otherwise momentum-biased entry signal
        momentum_signal = self._calc_momentum()
        self.value = momentum_signal * self.entry_amplifier

    def reset(self) -> None:
        self._prices.clear()
        self._volumes.clear()
        self.value = None
        self.initialized = False

    def _set_has_inputs(self, setting: bool) -> None:
        # Not used by this indicator (mirrors example in breakout.py)
        raise NotImplementedError()

    def _set_initialized(self, setting: bool) -> None:
        self.initialized = setting

    def _reset(self) -> None:
        self.reset()

    # --- Internals ---------------------------------------------------------

    def _append(self, price: float, volume: float) -> None:
        self._prices.append(price)
        self._volumes.append(volume)
        if len(self._prices) > self._max_len:
            self._prices.pop(0)
            self._volumes.pop(0)

    @staticmethod
    def _px_to_float(bar: Bar) -> float:
        try:
            return float(bar.close)
        except Exception:
            # Fallback for non-castable Price types
            return float(getattr(bar, "close", 0.0))

    @staticmethod
    def _vol_to_float(bar: Bar) -> float:
        vol = getattr(bar, "volume", 0.0)
        # Try common Quantity API first
        if hasattr(vol, "as_double") and callable(getattr(vol, "as_double")):
            try:
                return float(vol.as_double())
            except Exception:
                pass
        # Fallback conversion
        try:
            return float(vol)
        except Exception:
            return 0.0

    def _calc_momentum(self) -> float:
        # Need 10 bars for two 5-bar windows
        if len(self._prices) < 10:
            return 0.0

        recent_prices = self._prices[-5:]
        recent_volumes = self._volumes[-5:]
        older_prices = self._prices[-10:-5]
        older_volumes = self._volumes[-10:-5]

        sum_recent_vol = sum(recent_volumes)
        sum_older_vol = sum(older_volumes)

        vwap_recent = (
            sum(p * v for p, v in zip(recent_prices, recent_volumes)) / sum_recent_vol
            if sum_recent_vol > 0.0
            else sum(recent_prices) / 5.0
        )
        vwap_older = (
            sum(p * v for p, v in zip(older_prices, older_volumes)) / sum_older_vol
            if sum_older_vol > 0.0
            else sum(older_prices) / 5.0
        )

        if vwap_older == 0.0:
            return 0.0

        momentum = (vwap_recent - vwap_older) / vwap_older

        # Volume factor: last 3 relative to last 20 average
        tail = self._volumes[-20:] if len(self._volumes) >= 20 else self._volumes
        base = (sum(tail) / len(tail)) if tail else 1.0
        recent3 = self._volumes[-3:] if len(self._volumes) >= 3 else self._volumes
        vol_factor = (sum(recent3) / max(len(recent3), 1)) / max(base, 1.0)

        # Apply original scaling and cap
        return momentum * 1500.0 * min(vol_factor, 5.0)

    def _calc_mean_reversion(self) -> float:
        n = self.reversion_window
        if len(self._prices) < n:
            return 0.0

        # Exclude current price from mean baseline
        window = self._prices[-n:]
        mean_price = sum(window[:-1]) / max(len(window) - 1, 1)
        current = window[-1]

        if mean_price == 0.0:
            return 0.0

        deviation = (current - mean_price) / mean_price

        # Strong reversion when deviation exceeds threshold (bps -> decimal)
        threshold = self.overbought_threshold / 10000.0
        if deviation > threshold:
            return -2.0  # strong sell
        if deviation < -threshold:
            return 2.0   # strong buy

        # Moderate reversion when small but notable deviation
        if abs(deviation) > 0.001:  # 10 bps
            return -deviation * 10.0

        return 0.0

