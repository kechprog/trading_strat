import shutil
from decimal import Decimal
from pathlib import Path

import pandas as pd

from nautilus_trader.backtest.node import BacktestDataConfig
from nautilus_trader.backtest.node import BacktestEngineConfig
from nautilus_trader.backtest.node import BacktestNode
from nautilus_trader.backtest.node import BacktestRunConfig
from nautilus_trader.backtest.node import BacktestVenueConfig
from nautilus_trader.config import ImportableStrategyConfig
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.model import BarType, InstrumentId, instruments, BarSpecification, Venue
from nautilus_trader.model.enums import BarAggregation, PriceType, AssetClass, OmsType, AccountType
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.wranglers import BarDataWrangler
from nautilus_trader.test_kit.providers import CSVBarDataLoader
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.model import Symbol
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model import Bar
from nautilus_trader.persistence.config import DataCatalogConfig

catalog = ParquetDataCatalog("./catalog")
nasdaq_venue = Venue("NASDAQ")

start_time = "2018-04-01"
end_time = "2023-07-01"


data_configs = [
    BacktestDataConfig(
        catalog_path="./catalog",
        data_cls=Bar,
        instrument_ids=[InstrumentId.from_str("VOO.NASDAQ"), InstrumentId.from_str("SH.NASDAQ")],
        bar_spec=BarSpecification(
            1, BarAggregation.MINUTE, PriceType.LAST
        ),
        start_time=start_time,
        end_time=end_time
    )
]

venue_conf = BacktestVenueConfig(
    "NASDAQ",
    oms_type="NETTING",
    account_type="MARGIN",
    base_currency="USD",
    starting_balances=["100_000 USD"],
    bar_adaptive_high_low_ordering=False,
)

bt_config = BacktestEngineConfig(
    trader_id="Simple-001",
    logging=LoggingConfig("WARNING"),
    strategies=[
        ImportableStrategyConfig(
            strategy_path="breakout:Breakout",
            config_path="breakout:BreakoutConfig",
            config={
                "main_symbol": InstrumentId.from_str("VOO.NASDAQ"),
                "reverse_symbol": InstrumentId.from_str("SH.NASDAQ"),
                "long_entry": 2,
                "short_entry": 2,
                "long_exit": 4,
                "short_exit": 5,
                # Indicator settings
                "indicator_bar": BarSpecification(1, BarAggregation.HOUR, PriceType.LAST),
                "indicator_type": "MomentumMeanReversionNautilusIndicator",
                "indicator_params": {
                    "reversion_window": 30,
                    "momentum_peak_threshold": 1.0,
                    "overbought_threshold": 2.5,
                    "entry_amplifier": 2.0,
                    "exit_amplifier": 1.5,
                },
                # "indicator_type": "EMA",
                # "indicator_params": {
                #     "period": 25,
                # }
                # "indicator_type": "Renko",
                # "indicator_params": {
                #     "method": "ATR",  # 'ATR' or 'Traditional'
                #     "atr_period": 14,
                #     "brick_size": 4.0,
                #     "source": "close",  # 'close' or 'hl'
                #     "reversal": 4,
                #     "tick_size": None,
                # },
            }
        ),

    ],
    catalogs=[DataCatalogConfig(path="./catalog")],
)

config = BacktestRunConfig(
    engine=bt_config,
    data=data_configs,
    venues=[venue_conf],
)

node = BacktestNode(configs=[config])

results = node.run()

print(results)