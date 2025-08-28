from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.model import BarType, InstrumentId, BarSpecification, Bar, QuoteTick, TradeTick, Price
from nautilus_trader.model.enums import BarAggregation, PriceType
from nautilus_trader.config import StrategyConfig
from nautilus_trader.indicators.average.ema import ExponentialMovingAverage
from nautilus_trader.core.data import Data
from nautilus_trader.indicators.base.indicator import Indicator
# import pandas as pd

from typing import List

class HighLowDailyHistIndicator(Indicator):
    initialized: bool = False
    value: tuple[float, float] | None = None  # (high, low)

    _highs: List[Price | None]
    _lows: List[Price | None]

    def __init__(self, lookback: int = 7):
        if lookback <= 0:
            raise ValueError("lookback must be >= 1")

        self.lookback = lookback
        self._highs = [None] * self.lookback
        self._lows = [None] * self.lookback
        self._next_idx = 0

        self.initialized = False
        self.value = None

    def __repr__(self) -> str:
        return f"HighLowDailyHistIndicator(lookback={self.lookback})"

    def handle_quote_tick(self, tick: QuoteTick) -> None:
        raise RuntimeError("HighLowDailyHistIndicator does not support quote ticks")

    def handle_trade_tick(self, tick: TradeTick) -> None:
        raise RuntimeError("HighLowDailyHistIndicator does not support trade ticks")

    def handle_bar(self, bar: Bar) -> None:
        self._highs[self._next_idx] = bar.high
        self._lows[self._next_idx] = bar.low

        self.initialized = self.initialized or (self._next_idx == self.lookback - 1)
        
        self._next_idx = (self._next_idx + 1) % self.lookback


        if self.initialized:
            self.value = (max(self._highs), min(self._lows)) # type: ignore

    def reset(self) -> None:
        self._highs = [None] * self.lookback
        self._lows = [None] * self.lookback
        self._next_idx = 0
        self.value = None
        self.initialized = False

    def _set_has_inputs(self, setting: bool) -> None:
        raise NotImplementedError()

    def _set_initialized(self, setting: bool) -> None:
        self.initialized = setting

    def _reset(self) -> None:
        self.reset()

class BreakoutV2Config(StrategyConfig, frozen=True):
    main_symbol: InstrumentId = InstrumentId.from_str("VOO.NASDAQ")
    reverse_symbol: InstrumentId = InstrumentId.from_str("SH.NASDAQ")
    long_entry: int = 1
    short_entry: int = 1
    long_exit: int = 7
    short_exit: int = 7

    ema_lookback_hours: int = 50


class BreakoutV2(Strategy):
    def __init__(self, config: BreakoutV2Config):
        super().__init__(config)

        self.main_symbol = config.main_symbol
        self.reverse_symbol = config.reverse_symbol
        self.long_entry = config.long_entry
        self.short_entry = config.short_entry
        self.long_exit = config.long_exit
        self.short_exit = config.short_exit

        self.ema_lookback_hours = config.ema_lookback_hours

        self.hist_daily = None
        self.daily_live = None

        self.ema = ExponentialMovingAverage(self.ema_lookback_hours, PriceType.LAST)
        self.high_low_ind = HighLowDailyHistIndicator(lookback=max(self.long_entry, self.short_entry, self.long_entry, self.long_exit))

    def on_start(self):
        self.log.info(f"Starting BreakoutV2 strategy with config: {self.__dict__}")

        main_instrument = self.cache.instrument(self.main_symbol)
        reverse_instrument = self.cache.instrument(self.reverse_symbol)
    
        if not main_instrument or not reverse_instrument:
            self.log.error(f"Instruments not found: main={main_instrument}, reverse={reverse_instrument}")
            self.stop()
            return

        # 1 Minute Bars
        for symbol in [self.main_symbol, self.reverse_symbol]:
            spec = BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST)
            bar_type = BarType(symbol, spec)
            self.subscribe_bars(bar_type)

        # 30 Minute Bars
        for symbol in [self.main_symbol, self.reverse_symbol]:
            spec = BarSpecification(30, BarAggregation.MINUTE, PriceType.LAST)
            bar_type = BarType(symbol, spec)
            self.subscribe_bars(bar_type)

        # Daily Bars
        for symbol in [self.main_symbol, self.reverse_symbol]:
            spec = BarSpecification(1, BarAggregation.DAY, PriceType.LAST)
            bar_type = BarType(symbol, spec)
            self.subscribe_bars(bar_type)

        # Setup hist daily
        high_low_bar_type = BarType(
            self.main_symbol,
            BarSpecification(1, BarAggregation.DAY, PriceType.LAST)
        )
        self.register_indicator_for_bars(high_low_bar_type, self.high_low_ind)
        self.request_bars(high_low_bar_type)

        ema_bar_type = BarType(
            self.main_symbol,
            BarSpecification(30, BarAggregation.MINUTE, PriceType.LAST)
        )
        self.register_indicator_for_bars(ema_bar_type, self.ema)
        self.request_bars(ema_bar_type)

    def on_1minute_bar(self, bar: Bar):
        # print(self.ema.initialized)
        pass

    def on_30minute_bar(self, bar: Bar):
        pass

    def on_daily_bar(self, bar: Bar):
        print(self.high_low_ind.value)

    def on_historical_data(self, data: Data):
        pass
        

    def on_stop(self):
        self.log.info("Stopping BreakoutV2 strategy")

    def on_bar(self, bar: Bar):
        bar_spec = bar.bar_type.spec
        match bar_spec.aggregation:
            case BarAggregation.MINUTE:
                if bar_spec.step == 1:
                    self.on_1minute_bar(bar)
                elif bar_spec.step == 30:
                    self.on_30minute_bar(bar)
                else:
                    self.log.warning(f"Unhandled minute bar step: {bar_spec.step}")
            case BarAggregation.DAY:
                self.on_daily_bar(bar)
            case _:
                self.log.warning(f"Unhandled bar aggregation: {bar.bar_type.spec.aggregation}")
