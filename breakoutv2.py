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

        self.ema = ExponentialMovingAverage(self.ema_lookback_hours, PriceType.LAST)
        # TODO: 4 indicators perhaps?
        self.high_low_exit = HighLowDailyHistIndicator(lookback=self.long_entry)
        self.high_low_enter = HighLowDailyHistIndicator(lookback=self.short_exit)

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
        self.register_indicator_for_bars(self.bar_types["1day_main"], self.high_low_enter)
        self.register_indicator_for_bars(self.bar_types["1day_reverse"], self.high_low_exit)
        self.register_indicator_for_bars(self.bar_types["60min_main"], self.ema)

    def on_1minute_bar(self, bar: Bar):
        return

    def on_hour_bar(self, bar: Bar):
        if bar.bar_type.instrument_id != self.main_symbol:
            return

        if not self.ema.initialized or not self.high_low_enter.initialized or not self.high_low_exit.initialized: # type: ignore
            self.log.info("Indicators not initialized")
            return

        assert self.high_low_enter.value is not None and self.high_low_exit.value is not None
        high_entry, low_entry = self.high_low_enter.value
        high_exit, low_exit = self.high_low_exit.value
        indicator = 1 if bar.close > self.ema.value else -1

        main_pos: int = self.portfolio.net_position(self.main_symbol) #type: ignore
        reverse_pos: int = self.portfolio.net_position(self.reverse_symbol) #type: ignore

        main_last_px = self.cache.bar(
            BarType(self.main_symbol, 
                    BarSpecification(1, 
                                     BarAggregation.MINUTE, 
                                     PriceType.LAST))).close
        reverse_last_px = self.cache.bar(
            BarType(self.reverse_symbol, 
                    BarSpecification(1, 
                                     BarAggregation.MINUTE, 
                                     PriceType.LAST))).close

        if bar.close < low_exit and main_pos > 0:
            order = self.order_factory.market(
                self.main_symbol,
                OrderSide.SELL,
                Quantity.from_int(int(main_pos))
            )
            self.submit_order(order)

        if bar.close > high_exit and reverse_pos > 0:
            order = self.order_factory.market(
                self.reverse_symbol,
                OrderSide.SELL,
                Quantity.from_int(int(reverse_pos))
            )
            self.submit_order(order)

        if bar.close > high_entry and indicator == 1 and main_pos == 0 and reverse_pos == 0:
            balance = self.portfolio.account(self.venue).balance_total().as_double() # type: ignore
            quantity = math.floor((balance * 0.95) / main_last_px)
            order = self.order_factory.market(
                self.main_symbol,
                OrderSide.BUY,
                Quantity.from_int(quantity)
            )
            self.submit_order(order)

        if bar.close < low_entry and indicator == -1 and reverse_pos == 0 and main_pos == 0:
            balance = self.portfolio.account(self.venue).balance_total().as_double() # type: ignore
            quantity = math.floor((balance * 0.95) / reverse_last_px)
            order = self.order_factory.market(
                self.reverse_symbol,
                OrderSide.BUY,
                Quantity.from_int(quantity)
            )
            self.submit_order(order)

        print(unix_nanos_to_dt(bar.ts_event), unix_nanos_to_dt(bar.ts_init), bar)

    def on_order_pending_update(self, event: OrderPendingUpdate) -> None:
        print(event)

    def on_order_initialized(self, event: OrderInitialized) -> None:
        """ Called when order is intialized, not very important """
        pass

    def on_order_accepted(self, event: OrderAccepted) -> None:
        """ No idea what that does """
        pass
    
    def on_order_cancel_rejected(self, event: OrderCancelRejected) -> None:
        print(event)

    def on_order_canceled(self, event: OrderCanceled) -> None:
        print(event)

    def on_order_denied(self, event: OrderDenied) -> None:
        print(event)

    def on_order_emulated(self, event: OrderEmulated) -> None:
        print(event)

    def on_order_expired(self, event: OrderExpired) -> None:
        print(event)

    def on_order_filled(self, event: OrderFilled) -> None:
        """ When we get the order completed apperantly """
        # main_pos = self.portfolio.net_position(self.main_symbol)
        # reverse_pos = self.portfolio.net_position(self.reverse_symbol)

        # print(f"Order filled: {event}. Main pos: {main_pos}, Reverse pos: {reverse_pos}")
        # exit()
        pass

    def on_order_modify_rejected(self, event: OrderModifyRejected) -> None:
        print(event)

    def on_order_pending_cancel(self, event: OrderPendingCancel) -> None:
        print(event)

    def on_order_rejected(self, event: OrderRejected) -> None:
        print(event)

    def on_order_released(self, event: OrderReleased) -> None:
        print(event)

    def on_order_submitted(self, event: OrderSubmitted) -> None:
        """ When we get the order sent to the exchange """
        pass

    def on_order_triggered(self, event: OrderTriggered) -> None:
        print(event)

    def on_order_updated(self, event: OrderUpdated) -> None:
        print(event)


    def on_daily_bar(self, bar: Bar):
        pass

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
            case BarAggregation.HOUR:
                if bar_spec.step == 1:
                    self.on_hour_bar(bar)
            case BarAggregation.DAY:
                if bar_spec.step == 1:
                    self.on_daily_bar(bar)
            case _:
                self.log.warning(f"Unhandled bar aggregation: {bar.bar_type.spec.aggregation}")
                pass
        if bar_spec.aggregation == BarAggregation.MINUTE and bar_spec.step == 1:
            self.on_1minute_bar(bar)
        if bar_spec.aggregation == BarAggregation.DAY and bar_spec.step == 1:
            self.on_daily_bar(bar)
