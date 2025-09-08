#!/usr/bin/env python3
"""
Renko Trend Indicator (Nautilus)

Implements the `trend` output from the provided Pine script (Renko Chart):
- trend = 1 for up-column regime
- trend = -1 for down-column regime

Key parameters:
- method: 'ATR' or 'Traditional' (fixed brick size)
- atr_period: ATR lookback when method='ATR'
- brick_size: fixed box size when method='Traditional'
- source: 'close' or 'hl' (use close only, or use high/low depending on trend)
- reversal: number of bricks required for reversal (default 2)
- tick_size: optional price increment used to quantize ATR to ticks

Notes:
- This indicator updates on bars only.
- Initialization occurs once the first Renko trend is established.
- Value is an int in {-1, 1} once initialized; otherwise None.
"""

from __future__ import annotations

from typing import Optional, List

from nautilus_trader.indicators.base.indicator import Indicator
from nautilus_trader.model import Bar


class RenkoTrendNautilusIndicator(Indicator):
    """Nautilus-compatible Renko trend indicator (-1/1)."""

    initialized: bool = False
    value: int | None = None

    def __init__(
        self,
        method: str = "ATR",  # 'ATR' or 'Traditional'
        atr_period: int = 14,
        brick_size: float = 10.0,
        source: str = "hl",  # 'close' or 'hl'
        reversal: int = 2,
        tick_size: float | None = None,
    ) -> None:
        if method not in ("ATR", "Traditional"):
            raise ValueError("method must be 'ATR' or 'Traditional'")
        if atr_period <= 0:
            raise ValueError("atr_period must be >= 1")
        if brick_size <= 0:
            raise ValueError("brick_size must be > 0")
        if source not in ("close", "hl"):
            raise ValueError("source must be 'close' or 'hl'")
        if reversal <= 0:
            raise ValueError("reversal must be >= 1")

        self.method = method
        self.atr_period = int(atr_period)
        self.brick_size = float(brick_size)
        self.source = source
        self.reversal = int(reversal)
        self.tick_size = float(tick_size) if tick_size is not None else None

        # ATR state (Wilder smoothing)
        self._prev_close: Optional[float] = None
        self._tr_window: List[float] = []
        self._atr: Optional[float] = None

        # Renko state
        self._box: Optional[float] = None
        self._begin_price: Optional[float] = None
        self._trend: int = 0  # 0 unknown, -1 down, 1 up
        self._iopen: Optional[float] = None
        self._iclose: Optional[float] = None

        self.initialized = False
        self.value = None

    def __repr__(self) -> str:
        return (
            "RenkoTrendNautilusIndicator("
            f"method={self.method}, atr_period={self.atr_period}, "
            f"brick_size={self.brick_size}, source={self.source}, "
            f"reversal={self.reversal}, tick_size={self.tick_size})"
        )

    # --- Indicator API -----------------------------------------------------

    def handle_quote_tick(self, tick) -> None:  # pragma: no cover - unsupported
        raise RuntimeError("RenkoTrendNautilusIndicator does not support quote ticks")

    def handle_trade_tick(self, tick) -> None:  # pragma: no cover - unsupported
        raise RuntimeError("RenkoTrendNautilusIndicator does not support trade ticks")

    def handle_bar(self, bar: Bar) -> None:
        high = float(bar.high)
        low = float(bar.low)
        close = float(bar.close)
        open_ = float(bar.open)

        # Update ATR if needed
        self._update_atr(high, low, close)

        # Ensure box size
        if self._box is None:
            if self.method == "ATR":
                if self._atr is None:
                    # Not enough data yet to determine box size
                    return
                self._box = self._quantize(self._atr)
            else:
                self._box = max(self.brick_size, self._min_tick())

        box = float(self._box)
        if box <= 0:
            return

        # Initialize begin price on first bar with a box size
        if self._begin_price is None:
            self._begin_price = (open_ // box) * box

        # Determine price inputs based on source
        current_price = close if self.source == "close" else (high if self._trend == 1 else low)

        begin = float(self._begin_price)
        iclose_prev = self._iclose

        # Establish initial trend if unknown
        if self._trend == 0:
            if box * self.reversal <= abs(begin - current_price):
                if begin > current_price:
                    num = int(abs(begin - current_price) // box)
                    self._iopen = begin
                    self._iclose = begin - num * box
                    self._trend = -1
                elif begin < current_price:
                    num = int(abs(begin - current_price) // box)
                    self._iopen = begin
                    self._iclose = begin + num * box
                    self._trend = 1

        # Continue or reverse trend
        if self._trend == -1:
            nok = True
            # Continue down
            if begin > current_price and box <= abs(begin - current_price):
                num = int(abs(begin - current_price) // box)
                self._iclose = begin - num * box
                self._trend = -1
                self._begin_price = self._iclose
                nok = False
            # Attempt reversal to up
            temp_price = close if self.source == "close" else high
            if nok and (begin < temp_price) and (box * self.reversal <= abs(begin - temp_price)):
                num = int(abs(begin - temp_price) // box)
                self._iopen = begin + box
                self._iclose = begin + num * box
                self._trend = 1
                self._begin_price = self._iclose

        elif self._trend == 1:
            nok = True
            # Continue up
            if begin < current_price and box <= abs(begin - current_price):
                num = int(abs(begin - current_price) // box)
                self._iclose = begin + num * box
                self._trend = 1
                self._begin_price = self._iclose
                nok = False
            # Attempt reversal to down
            temp_price = close if self.source == "close" else low
            if nok and (begin > temp_price) and (box * self.reversal <= abs(begin - temp_price)):
                num = int(abs(begin - temp_price) // box)
                self._iopen = begin - box
                self._iclose = begin - num * box
                self._trend = -1
                self._begin_price = self._iclose

        # Recalculate box size on brick close change (ATR mode), mirroring Pine behavior
        if self.method == "ATR" and self._iclose is not None and iclose_prev is not None:
            if self._iclose != iclose_prev and self._atr is not None:
                self._box = self._quantize(self._atr)

        # Emit value once trend established
        if self._trend in (-1, 1):
            self.value = self._trend
            # Mark initialized on first valid trend
            if not self.initialized:
                self.initialized = True

    def reset(self) -> None:
        # ATR state
        self._prev_close = None
        self._tr_window = []
        self._atr = None

        # Renko state
        self._box = None
        self._begin_price = None
        self._trend = 0
        self._iopen = None
        self._iclose = None

        self.value = None
        self.initialized = False

    def _set_has_inputs(self, setting: bool) -> None:
        # Not used by this indicator
        raise NotImplementedError()

    def _set_initialized(self, setting: bool) -> None:
        self.initialized = setting

    def _reset(self) -> None:
        self.reset()

    # --- Internals ---------------------------------------------------------

    def _min_tick(self) -> float:
        return self.tick_size if self.tick_size is not None else 0.0

    def _quantize(self, value: float) -> float:
        ts = self._min_tick()
        if ts and ts > 0:
            # Round to nearest tick multiple
            steps = round(value / ts)
            q = max(steps * ts, ts)
            return q
        return max(value, 1e-12)  # avoid zero

    def _update_atr(self, high: float, low: float, close: float) -> None:
        tr: float
        if self._prev_close is None:
            tr = abs(high - low)
        else:
            tr = max(high - low, abs(high - self._prev_close), abs(low - self._prev_close))

        # Accumulate initial window for SMA-based seed
        if self._atr is None:
            self._tr_window.append(tr)
            if len(self._tr_window) > self.atr_period:
                self._tr_window.pop(0)
            if len(self._tr_window) == self.atr_period:
                self._atr = sum(self._tr_window) / float(self.atr_period)
        else:
            # Wilder smoothing
            self._atr = (self._atr * (self.atr_period - 1) + tr) / float(self.atr_period)

        self._prev_close = close

