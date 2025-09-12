from nautilus_trader.model import BarType, InstrumentId, BarSpecification
from nautilus_trader.model.enums import BarAggregation, PriceType, AggregationSource
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.wranglers import BarDataWrangler
from nautilus_trader.test_kit.providers import CSVBarDataLoader # type: ignore
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.core.datetime import unix_nanos_to_dt
import shutil, os
import pandas as pd

if os.path.isdir("./catalog"):
    shutil.rmtree("./catalog")

catalog = ParquetDataCatalog("./catalog")
voo = TestInstrumentProvider.equity("VOO", "NASDAQ")
sh = TestInstrumentProvider.equity("SH", "NASDAQ")
catalog.write_data([voo, sh])

def write_data(path: str, instrument, bar_spec: BarSpecification):
    instrument_id = InstrumentId.from_str(f"{instrument.symbol}.{instrument.venue}")
    match bar_spec.aggregation:
        case BarAggregation.MINUTE:
            init_delta = pd.Timedelta(minutes=1)
        case BarAggregation.HOUR:
            init_delta = pd.Timedelta(hours=1)
        case BarAggregation.DAY:
            init_delta = pd.Timedelta(days=1)
        case _:
            raise ValueError(f"Unsupported timeframe: {bar_spec.aggregation}")
    df = CSVBarDataLoader.load(path).sort_values("timestamp")
    df.index = df.index + init_delta
    wr = BarDataWrangler(
        BarType(instrument_id, bar_spec, aggregation_source=AggregationSource.EXTERNAL),
        instrument
    )
    catalog.write_data(wr.process(df))


### 1min
write_data(
    "./raw_data/VOO_1min_splits_only_standard.csv",
    voo,
    BarSpecification(
        step=1,
        aggregation=BarAggregation.MINUTE,
        price_type=PriceType.LAST
    )
)

write_data(
    "./raw_data/SH_1min_splits_only_standard.csv",
    sh,
    BarSpecification(
        step=1,
        aggregation=BarAggregation.MINUTE,
        price_type=PriceType.LAST
    )
)

### 60min
write_data(
    "./raw_data/VOO_60min_splits_only_standard.csv",
    voo,
    BarSpecification(
        step=1,
        aggregation=BarAggregation.HOUR,
        price_type=PriceType.LAST
    )
)

write_data(
    "./raw_data/SH_60min_splits_only_standard.csv",
    sh,
    BarSpecification(
        step=1,
        aggregation=BarAggregation.HOUR,
        price_type=PriceType.LAST
    )
)

### Daily
write_data(
    "./raw_data/VOO_daily_splits_only.csv",
    voo,
    BarSpecification(
        step=1,
        aggregation=BarAggregation.DAY,
        price_type=PriceType.LAST
    )
)

write_data(
    "./raw_data/SH_daily_splits_only.csv",
    sh,
    BarSpecification(
        step=1,
        aggregation=BarAggregation.DAY,
        price_type=PriceType.LAST
    )
)