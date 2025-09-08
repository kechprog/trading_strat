from nautilus_trader.model.events.order import OrderFilled
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.model import BarType, InstrumentId, BarSpecification, Bar, QuoteTick, TradeTick, Price, Quantity
from nautilus_trader.model.enums import BarAggregation, PriceType, OrderSide, AggregationSource
from nautilus_trader.config import StrategyConfig
from nautilus_trader.core.data import Data
from nautilus_trader.indicators.base.indicator import Indicator
from nautilus_trader.core.datetime import unix_nanos_to_dt
import math

from typing import List
from indicators.ema_indicator_nautilus import EMASignalIndicator
from indicators.momentum_mean_reversion_nautilus import MomentumMeanReversionNautilusIndicator
from indicators.high_low_hist import HighLowDailyHistIndicator
from indicators.renko_trend_nautilus import RenkoTrendNautilusIndicator


def _build_indicator(indicator_type: str, params: dict) -> Indicator:
    """Construct an Indicator instance from a type name and params.

    Supported indicator_type values:
    - "EMASignalIndicator" (alias: "ema", "EMA")
    - "MomentumMeanReversionNautilusIndicator" (alias: "mmr", "momentum_mean_reversion")
    """
    name = (indicator_type or "").strip()
    if name in ("EMASignalIndicator", "ema", "EMA"):
        return EMASignalIndicator(**(params or {}))
    if name in (
        "MomentumMeanReversionNautilusIndicator",
        "mmr",
        "momentum_mean_reversion",
    ):
        return MomentumMeanReversionNautilusIndicator(**(params or {}))
    if name in ("RenkoTrendNautilusIndicator", "renko", "Renko"):
        return RenkoTrendNautilusIndicator(**(params or {}))


class BreakoutConfig(StrategyConfig, frozen=True):
    main_symbol: InstrumentId = InstrumentId.from_str("VOO.NASDAQ")
    reverse_symbol: InstrumentId = InstrumentId.from_str("SH.NASDAQ")
    long_entry: int = 1
    short_entry: int = 1
    long_exit: int = 7
    short_exit: int = 7

    # Indicator-agnostic parameters
    indicator_bar: BarSpecification = BarSpecification(1, BarAggregation.HOUR, PriceType.LAST)  # which bar stream to feed the indicator
    indicator_type: str = "EMASignalIndicator"
    indicator_params: dict | None = None 


class Breakout(Strategy):
    def __init__(self, config: BreakoutConfig):
        super().__init__(config)

        self.main_symbol = config.main_symbol
        self.reverse_symbol = config.reverse_symbol
        self.long_entry = config.long_entry
        self.short_entry = config.short_entry
        self.long_exit = config.long_exit
        self.short_exit = config.short_exit
        assert self.main_symbol.venue == self.reverse_symbol.venue, "Main and reverse symbols must be on the same venue(As of right now)"
        self.venue = config.main_symbol.venue

        self.min_main = BarType(self.main_symbol, BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST), AggregationSource.EXTERNAL)
        self.min_reverse = BarType(self.reverse_symbol, BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST), AggregationSource.EXTERNAL)
        self.day_main = BarType(self.main_symbol, BarSpecification(1, BarAggregation.DAY, PriceType.LAST), AggregationSource.EXTERNAL)


        self.indicator_bar = config.indicator_bar
        self.indicator: Indicator = _build_indicator(
            config.indicator_type,
            config.indicator_params # type: ignore
        )

        self.daily_levels = HighLowDailyHistIndicator(
            enter_high_lookback=self.long_entry,
            enter_low_lookback=self.short_entry,
            exit_high_lookback=self.short_exit,
            exit_low_lookback=self.long_exit,
        )

    def on_start(self):
        self.log.info(f"Starting BreakoutV2 strategy with config: {self.__dict__}")

        main_instrument = self.cache.instrument(self.main_symbol)
        reverse_instrument = self.cache.instrument(self.reverse_symbol)
    
        if not main_instrument or not reverse_instrument:
            self.log.error(f"Instruments not found: main={main_instrument}, reverse={reverse_instrument}")
            self.stop()
            return
        
        # Subscribe to minute + daily bars used by the strategy
        self.subscribe_bars(self.min_main)
        self.subscribe_bars(self.min_reverse)
        self.subscribe_bars(BarType(self.main_symbol, self.indicator_bar))
        self.subscribe_bars(self.day_main)

        # Register indicators for the relevant bar streams
        self.register_indicator_for_bars(self.day_main, self.daily_levels)
        self.register_indicator_for_bars(BarType(self.main_symbol, self.indicator_bar), self.indicator)

    def on_1minute_bar(self, bar: Bar):
        if bar.bar_type.instrument_id != self.main_symbol:
            return

        # Ensure signals are ready. Note that the daily levels indicator sets
        # `initialized` once its lookback is filled, but `value` is only
        # available from the NEXT daily bar. Guard on both.
        if not (self.indicator.initialized and self.daily_levels.initialized):  # type: ignore
            return
        if self.daily_levels.value is None:
            return
        high_entry, low_entry, high_exit, low_exit = self.daily_levels.value

        # Normalize indicator signal to a signed float so we can accept
        # both discrete {-1, 1} and continuous signals.
        signal = float(self.indicator.value)

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
            if signal > 0 and float(bar.high) > float(high_entry):
                px = float(bar.close)
                qty = max(0, math.floor((balance * 0.95) / px))
                if qty > 0:
                    order = self.order_factory.market(self.main_symbol, OrderSide.BUY, Quantity.from_int(qty))
                    self.submit_order(order)

            if signal < 0 and float(bar.low) < float(low_entry):
                px = float(bar.close)
                qty = max(0, math.floor((balance * 0.95) / px))
                if qty > 0:
                    order = self.order_factory.market(self.reverse_symbol, OrderSide.BUY, Quantity.from_int(qty))
                    self.submit_order(order)

    def on_stop(self):
        # Log final snapshot (may reflect pre-close state if fills are asynchronous)
        print(f"main pos: {self.portfolio.net_position(self.main_symbol)}, px: {self.cache.bar(self.min_main).close}")
        print(f"reverse pos: {self.portfolio.net_position(self.reverse_symbol)}, px: {self.cache.bar(self.min_reverse).close}")
        print(f"Final Balances: {self.portfolio.account(self.venue).balance_total().as_double()}") # type: ignore

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
