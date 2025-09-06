from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.events.order import OrderAccepted, OrderCancelRejected, OrderCanceled, OrderDenied, OrderEmulated, OrderEvent, OrderExpired, OrderFilled, OrderInitialized, OrderModifyRejected, OrderPendingCancel, OrderPendingUpdate, OrderRejected, OrderReleased, OrderSubmitted, OrderTriggered, OrderUpdated
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.model import BarType, InstrumentId, BarSpecification, Bar, QuoteTick, TradeTick, Price, Quantity
from nautilus_trader.model.enums import BarAggregation, PriceType, OrderSide, AggregationSource
from nautilus_trader.config import StrategyConfig
from nautilus_trader.indicators.average.ema import ExponentialMovingAverage
from nautilus_trader.core.data import Data
from nautilus_trader.indicators.base.indicator import Indicator
from nautilus_trader.core.datetime import unix_nanos_to_dt
import math
# import pandas as pd

from typing import List

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

class BreakoutConfig(StrategyConfig, frozen=True):
    main_symbol: InstrumentId = InstrumentId.from_str("VOO.NASDAQ")
    reverse_symbol: InstrumentId = InstrumentId.from_str("SH.NASDAQ")
    long_entry: int = 1
    short_entry: int = 1
    long_exit: int = 7
    short_exit: int = 7

    ema_lookback_hours: int = 50


class Breakout(Strategy):
    def __init__(self, config: BreakoutConfig):
        super().__init__(config)

        self.main_symbol = config.main_symbol
        self.reverse_symbol = config.reverse_symbol
        self.long_entry = config.long_entry
        self.short_entry = config.short_entry
        self.long_exit = config.long_exit
        self.short_exit = config.short_exit

        self.ema_lookback_hours = config.ema_lookback_hours

        assert self.main_symbol.venue == self.reverse_symbol.venue, "Main and reverse symbols must be on the same venue(As of right now)"
        self.venue = config.main_symbol.venue

        self.bar_types = {
            "1min_main": BarType(
                self.main_symbol,
                BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST),
                aggregation_source=AggregationSource.EXTERNAL,
            ),
            "1min_reverse": BarType(
                self.reverse_symbol,
                BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST),
                aggregation_source=AggregationSource.EXTERNAL,
            ),

            "60min_main": BarType(
                self.main_symbol,
                BarSpecification(1, BarAggregation.HOUR, PriceType.LAST),
                aggregation_source=AggregationSource.EXTERNAL,
            ),
            "60min_reverse": BarType(
                self.reverse_symbol,
                BarSpecification(1, BarAggregation.HOUR, PriceType.LAST),
                aggregation_source=AggregationSource.EXTERNAL,
            ),

            "1day_main": BarType(
                self.main_symbol,
                BarSpecification(1, BarAggregation.DAY, PriceType.LAST),
                aggregation_source=AggregationSource.EXTERNAL,
            ),

            "1day_reverse": BarType(
                self.reverse_symbol,
                BarSpecification(1, BarAggregation.DAY, PriceType.LAST),
                aggregation_source=AggregationSource.EXTERNAL,
            )
        }

        self.hist_daily = None
        self.daily_live = None
        self._market_regime = 0  # -1 bearish, 0 neutral, 1 bullish based on hourly EMA

        self.ema = ExponentialMovingAverage(self.ema_lookback_hours, PriceType.LAST)
        # Four lookbacks: long/short entry and long/short exit
        # Use a single indicator instance which computes all levels at once.
        self.daily_levels = HighLowDailyHistIndicator(
            enter_high_lookback=self.long_entry,   # long entry uses high breakout
            enter_low_lookback=self.short_entry,   # short entry uses low breakdown
            exit_high_lookback=self.short_exit,    # short exit if price > recent high
            exit_low_lookback=self.long_exit,      # long exit if price < recent low
        )

        # No per-instrument minute price tracking in simple NETTING mode

    def on_start(self):
        self.log.info(f"Starting BreakoutV2 strategy with config: {self.__dict__}")

        main_instrument = self.cache.instrument(self.main_symbol)
        reverse_instrument = self.cache.instrument(self.reverse_symbol)
    
        if not main_instrument or not reverse_instrument:
            self.log.error(f"Instruments not found: main={main_instrument}, reverse={reverse_instrument}")
            self.stop()
            return
        
        # Subscribe to hourly + daily bars used by the strategy
        self.subscribe_bars(self.bar_types["1min_main"])       # for order fills / tracking
        self.subscribe_bars(self.bar_types["1min_reverse"])    # for order fills /
        self.subscribe_bars(self.bar_types["60min_main"])      # for on_hour_bar logic / EMA
        self.subscribe_bars(self.bar_types["60min_reverse"])   # reverse hourly for sizing/logic
        self.subscribe_bars(self.bar_types["1day_main"])       # for daily checks
        self.subscribe_bars(self.bar_types["1day_reverse"])    # for daily checks

        # indicators
        self.register_indicator_for_bars(self.bar_types["1day_main"], self.daily_levels)
        self.register_indicator_for_bars(self.bar_types["60min_main"], self.ema)

    def on_1minute_bar(self, bar: Bar):
        # Execute entries/exits on minute bars for better price reactivity (main symbol only)
        if bar.bar_type.instrument_id != self.main_symbol:
            return

        # Ensure signals are ready
        if not (self.ema.initialized and self.daily_levels.initialized):  # type: ignore
            return

        assert self.daily_levels.value is not None
        high_entry, low_entry, high_exit, low_exit = self.daily_levels.value

        # Positions
        main_pos: int = self.portfolio.net_position(self.main_symbol)  # type: ignore
        reverse_pos: int = self.portfolio.net_position(self.reverse_symbol)  # type: ignore

        # Exits first
        if main_pos > 0 and float(bar.low) < float(low_exit):
            order = self.order_factory.market(
                self.main_symbol,
                OrderSide.SELL,
                Quantity.from_int(int(main_pos)),
            )
            self.submit_order(order)

        if reverse_pos > 0 and float(bar.high) > float(high_exit):
            order = self.order_factory.market(
                self.reverse_symbol,
                OrderSide.SELL,
                Quantity.from_int(int(reverse_pos)),
            )
            self.submit_order(order)

        # Entries (only when flat and regime agrees)
        if main_pos == 0 and reverse_pos == 0:
            balance = self.portfolio.account(self.venue).balance_total().as_double()  # type: ignore
            if self._market_regime == 1 and float(bar.high) > float(high_entry):
                px = float(bar.close)
                qty = max(0, math.floor((balance * 0.95) / px))
                if qty > 0:
                    order = self.order_factory.market(self.main_symbol, OrderSide.BUY, Quantity.from_int(qty))
                    self.submit_order(order)

            if self._market_regime == -1 and float(bar.low) < float(low_entry):
                px = float(bar.close)
                qty = max(0, math.floor((balance * 0.95) / px))
                if qty > 0:
                    order = self.order_factory.market(self.reverse_symbol, OrderSide.BUY, Quantity.from_int(qty))
                    self.submit_order(order)


    def on_hour_bar(self, bar: Bar):
        if bar.bar_type.instrument_id != self.main_symbol:
            return
        if not self.ema.initialized:
            return
        self._market_regime = 1 if bar.close > self.ema.value else -1

    def on_daily_bar(self, bar: Bar):
        pass

    def on_stop(self):
        # Log final snapshot (may reflect pre-close state if fills are asynchronous)
        print(f"main pos: {self.portfolio.net_position(self.main_symbol)}, px: {self.cache.bar(self.bar_types['1min_main']).close}")
        print(f"reverse pos: {self.portfolio.net_position(self.reverse_symbol)}, px: {self.cache.bar(self.bar_types['1min_reverse']).close}")
        print(f"Final Balances: {self.portfolio.account(self.venue).balance_total().as_double()}")

    def on_order_filled(self, event: OrderFilled):
        """Print account balance when a position has been fully closed.

        We disallow more than one open position at a time (either main or reverse).
        After a fill, if both instruments are flat, this indicates a position-close
        trade has completed, so we print the current account balance (equity curve).
        """
        try:
            main_pos = int(self.portfolio.net_position(self.main_symbol))  # type: ignore
            reverse_pos = int(self.portfolio.net_position(self.reverse_symbol))  # type: ignore

            # Only print when we're flat across both instruments (i.e., just closed)
            if main_pos == 0 and reverse_pos == 0:
                balance = self.portfolio.account(self.venue).balance_total().as_double()  # type: ignore
                # print(f"Balance after close: {balance}")
        except Exception as e:
            # Don't let logging issues interrupt strategy flow
            self.log.warning(f"on_order_filled balance print failed: {e}")

    def on_bar(self, bar: Bar):
        bar_spec = bar.bar_type.spec
        match bar_spec.aggregation:
            case BarAggregation.MINUTE:
                if bar_spec.step == 1:
                    self.on_1minute_bar(bar)
            case BarAggregation.HOUR:
                if bar_spec.step == 1:
                    self.on_hour_bar(bar)
            case BarAggregation.DAY:
                if bar_spec.step == 1:
                    self.on_daily_bar(bar)
            case _:
                self.log.warning(f"Unhandled bar aggregation: {bar.bar_type.spec.aggregation}")
