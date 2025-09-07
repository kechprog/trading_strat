from typing import List
from nautilus_trader.model import Price
from nautilus_trader.indicators.base.indicator import Indicator
from nautilus_trader.model import Bar, QuoteTick, TradeTick

class HighLowDailyHistIndicator(Indicator):
    initialized: bool = False
    # (entry_high, entry_low, exit_high, exit_low)
    value: tuple[Price, Price, Price, Price] | None = None

    def __init__(
        self,
        enter_high_lookback: int,
        enter_low_lookback: int,
        exit_high_lookback: int,
        exit_low_lookback: int,
    ):
        # Validate lookbacks
        for name, lb in (
            ("enter_high_lookback", enter_high_lookback),
            ("enter_low_lookback", enter_low_lookback),
            ("exit_high_lookback", exit_high_lookback),
            ("exit_low_lookback", exit_low_lookback),
        ):
            if lb <= 0:
                raise ValueError(f"{name} must be >= 1")

        self.enter_high_lookback = enter_high_lookback
        self.enter_low_lookback = enter_low_lookback
        self.exit_high_lookback = exit_high_lookback
        self.exit_low_lookback = exit_low_lookback

        self._max_lookback = max(
            self.enter_high_lookback,
            self.enter_low_lookback,
            self.exit_high_lookback,
            self.exit_low_lookback,
        )

        # Two fixed-size circular buffers (no per-bar allocations)
        self._highs: List[Price | None] = [None] * self._max_lookback
        self._lows: List[Price | None] = [None] * self._max_lookback
        self._next_idx: int = 0

        self.initialized = False
        self.value = None

    def __repr__(self) -> str:
        return (
            "HighLowDailyHistIndicator("
            f"enter_high={self.enter_high_lookback}, "
            f"enter_low={self.enter_low_lookback}, "
            f"exit_high={self.exit_high_lookback}, "
            f"exit_low={self.exit_low_lookback})"
        )

    def handle_quote_tick(self, tick: QuoteTick) -> None:
        raise RuntimeError("HighLowDailyHistIndicator does not support quote ticks")

    def handle_trade_tick(self, tick: TradeTick) -> None:
        raise RuntimeError("HighLowDailyHistIndicator does not support trade ticks")

    def handle_bar(self, bar: Bar) -> None:
        # Compute levels from the previous complete days (exclude current day)
        # by calculating BEFORE inserting the current bar into the buffers.
        if self.initialized:
            def max_last(n: int) -> Price:
                idx = (self._next_idx - 1) % self._max_lookback
                best = self._highs[idx]
                # Scan previous n-1 elements
                for _ in range(1, n):
                    idx = (idx - 1) % self._max_lookback
                    v = self._highs[idx]
                    if v is not None and best is not None and v > best:
                        best = v
                # type: ignore
                return best  # Price

            def min_last(n: int) -> Price:
                idx = (self._next_idx - 1) % self._max_lookback
                best = self._lows[idx]
                for _ in range(1, n):
                    idx = (idx - 1) % self._max_lookback
                    v = self._lows[idx]
                    if v is not None and best is not None and v < best:
                        best = v
                # type: ignore
                return best  # Price

            entry_high = max_last(self.enter_high_lookback)
            entry_low = min_last(self.enter_low_lookback)
            exit_high = max_last(self.exit_high_lookback)
            exit_low = min_last(self.exit_low_lookback)
            self.value = (entry_high, entry_low, exit_high, exit_low)

        # Insert current day's bar into the buffers for use on the NEXT day
        self._highs[self._next_idx] = bar.high
        self._lows[self._next_idx] = bar.low

        # Initialize once we have filled the largest required window
        if not self.initialized and self._next_idx == self._max_lookback - 1:
            self.initialized = True
        # Advance ring buffer index
        self._next_idx = (self._next_idx + 1) % self._max_lookback

    def reset(self) -> None:
        self._highs = [None] * self._max_lookback
        self._lows = [None] * self._max_lookback
        self._next_idx = 0
        self.value = None
        self.initialized = False

    def _set_has_inputs(self, setting: bool) -> None:
        raise NotImplementedError()

    def _set_initialized(self, setting: bool) -> None:
        self.initialized = setting

    def _reset(self) -> None:
        self.reset()