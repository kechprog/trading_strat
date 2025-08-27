from nautilus_trader.model import BarType, InstrumentId, BarSpecification
from nautilus_trader.model.enums import BarAggregation, PriceType, AggregationSource
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.wranglers import BarDataWrangler
from nautilus_trader.test_kit.providers import CSVBarDataLoader # type: ignore
from nautilus_trader.test_kit.providers import TestInstrumentProvider
import shutil, os

if os.path.isdir("./catalog"):
    shutil.rmtree("./catalog")

catalog = ParquetDataCatalog("./catalog")
voo = TestInstrumentProvider.equity("VOO", "NASDAQ")
sh = TestInstrumentProvider.equity("SH", "NASDAQ")
catalog.write_data([voo, sh])

df = CSVBarDataLoader.load(
    "./raw_data/SH_1min_splits_only_standard.csv"
).sort_values("timestamp")

for f in [1, 5, 15, 30]:
    wr = BarDataWrangler(
        BarType(InstrumentId.from_str("SH.NASDAQ"), BarSpecification(
            step=f,
            aggregation=BarAggregation.MINUTE,
            price_type=PriceType.LAST
        ),
        aggregation_source=AggregationSource.EXTERNAL),
        sh
    )
    catalog.write_data(wr.process(df))


df = CSVBarDataLoader.load(
    "./raw_data/VOO_1min_splits_only_standard.csv"
).sort_values("timestamp")

for f in [1, 5, 15, 30]:
    wr = BarDataWrangler(
        BarType(InstrumentId.from_str("VOO.NASDAQ"), BarSpecification(
            step=f,
            aggregation=BarAggregation.MINUTE,
            price_type=PriceType.LAST
        ),
        aggregation_source=AggregationSource.EXTERNAL),
        voo
    )
    catalog.write_data(wr.process(df))


df = CSVBarDataLoader.load(
    "./raw_data/SH_daily_splits_only.csv"
).sort_values("timestamp")

wr = BarDataWrangler(
    BarType(InstrumentId.from_str("SH.NASDAQ"), BarSpecification(
        step=1,
        aggregation=BarAggregation.DAY,
        price_type=PriceType.LAST
    ),
    aggregation_source=AggregationSource.EXTERNAL),
    sh
)
catalog.write_data(wr.process(df))


df = CSVBarDataLoader.load(
    "./raw_data/VOO_daily_splits_only.csv"
).sort_values("timestamp")

wr = BarDataWrangler(
    BarType(InstrumentId.from_str("VOO.NASDAQ"), BarSpecification(
        step=1,
        aggregation=BarAggregation.DAY,
        price_type=PriceType.LAST
    ),
    aggregation_source=AggregationSource.EXTERNAL),
    voo
)
catalog.write_data(wr.process(df))