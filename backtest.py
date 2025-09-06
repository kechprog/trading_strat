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

start_time = "2014-04-01"
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
    oms_type="HEDGING",
    account_type="MARGIN",
    base_currency="USD",
    starting_balances=["1_000_000 USD"],
    # fill_model=FillModel(),
    bar_adaptive_high_low_ordering=False,
)

bt_config = BacktestEngineConfig(
    trader_id="Simple-001",
    logging=LoggingConfig("WARNING"),
    strategies=[
        ImportableStrategyConfig(
            strategy_path="breakoutv2:BreakoutV2",
            config_path="breakoutv2:BreakoutV2Config",
            config={
                "main_symbol": InstrumentId.from_str("VOO.NASDAQ"),
                "reverse_symbol": InstrumentId.from_str("SH.NASDAQ"),
                "long_entry": 1,
                "short_entry": 1,
                "long_exit": 7,
                "short_exit": 7,
                "ema_lookback_hours": 50,
            },
        ),
        # ImportableStrategyConfig(
        #     strategy_path="breakout:BreakoutStrategy",
        #     config_path="breakout:BreakoutConfig",
        #     config={},
        # )
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
