#!/usr/bin/env python3
"""
Nautilus Historical Data Converter (raw_data version)

Converts CSV files in ./raw_data (or a provided directory) to Nautilus parquet format
for backtesting. This version supports files named as:

  {instrument}_{interval}_{adjustment}_[trading_hours].csv

Where:
  - interval: one of 1min, 30min, 60min, daily
  - adjustment: one of splits_only, none, all
  - trading_hours: one of standard, extended (intraday only; not present for daily)

Examples:
  - SPY_1min_splits_only_standard.csv
  - SH_1min_splits_only_extended.csv
  - SPY_daily_splits_only.csv

CSV format is expected to be: timestamp,open,high,low,close,volume

Usage:
    python convert_hist_raw.py [input_directory] [-o OUTPUT] [-v]
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional, Dict, Tuple
import pandas as pd
from datetime import datetime
import pytz
import requests
import os
from dotenv import load_dotenv

from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.model.data import Bar, BarType, BarSpecification
from nautilus_trader.model.enums import AggregationSource, BarAggregation, PriceType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('convert_hist_raw.log')
    ]
)
logger = logging.getLogger(__name__)


ALLOWED_INTERVALS = {"1min", "30min", "60min", "daily"}
ALLOWED_ADJUSTMENTS = {"splits_only", "none", "all"}
ALLOWED_HOURS = {"standard", "extended"}


class RawDataHistoricalConverter:
    """Converts CSV files under raw_data naming to Nautilus parquet format."""

    def __init__(self, input_directory: Path, output_directory: Optional[Path] = None, offline: bool = False):
        self.input_directory = Path(input_directory)
        self.output_directory = output_directory or Path("./nautilus_catalog")
        self.catalog = ParquetDataCatalog(self.output_directory)

        # Load environment variables for Alpha Vantage API (optional; used only for exchange mapping)
        load_dotenv("alpha_data_getter/.env")  # support legacy location
        load_dotenv(".env")
        self.alpha_vantage_api_key = os.getenv("ALPHA_KEY") if not offline else None
        if not self.alpha_vantage_api_key:
            logger.info("Running in offline mode for exchange detection (using fallback mapping).")

        # Cache for symbol-to-exchange mapping
        self.symbol_exchange_cache: Dict[str, str] = {}

        logger.info("Initialized raw_data converter:")
        logger.info(f"  Input directory: {self.input_directory}")
        logger.info(f"  Output directory: {self.output_directory}")
        logger.info(f"  Offline exchange detection: {self.alpha_vantage_api_key is None}")

    def discover_csv_files(self) -> List[Path]:
        """Discover all CSV files in the input directory with supported patterns."""
        csv_files = list(self.input_directory.glob("*.csv"))
        logger.info(f"Found {len(csv_files)} CSV files")

        valid_files: List[Path] = []
        for file_path in csv_files:
            if self.parse_filename(file_path) is not None:
                valid_files.append(file_path)
            else:
                logger.warning(f"Skipping file with invalid pattern: {file_path.name}")

        logger.info(f"Found {len(valid_files)} valid CSV files matching raw_data pattern")
        return valid_files

    def parse_filename(self, file_path: Path) -> Optional[Dict[str, Optional[str]]]:
        """
        Parse filename into its components.

        Returns dict with keys: symbol, interval, adjustment, hours (hours is None for daily).
        """
        name = file_path.name
        if not name.endswith(".csv"):
            return None
        stem = name[:-4]
        parts = stem.split("_")
        if len(parts) < 3:
            return None

        # Case 1: intraday -> ..._{interval}_{adjustment}_{hours}.csv
        # Handle adjustment being either 'none'/'all' or 'splits_only'
        if len(parts) >= 4 and parts[-1] in ALLOWED_HOURS:
            # Option A: adjustment is single token (none|all)
            if parts[-2] in {"none", "all"} and parts[-3] in (ALLOWED_INTERVALS - {"daily"}):
                symbol = "_".join(parts[:-3])
                return {
                    "symbol": symbol,
                    "interval": parts[-3],
                    "adjustment": parts[-2],
                    "hours": parts[-1],
                }
            # Option B: adjustment is 'splits_only' split across two tokens
            if len(parts) >= 5 and parts[-2] == "only" and parts[-3] == "splits" and parts[-4] in (ALLOWED_INTERVALS - {"daily"}):
                symbol = "_".join(parts[:-4])
                return {
                    "symbol": symbol,
                    "interval": parts[-4],
                    "adjustment": "splits_only",
                    "hours": parts[-1],
                }

        # Case 2: daily -> ..._daily_{adjustment}.csv
        # Option A: adjustment is single token (none|all)
        if len(parts) >= 3 and parts[-2] == "daily" and parts[-1] in {"none", "all"}:
            symbol = "_".join(parts[:-2])
            if symbol:
                return {"symbol": symbol, "interval": "daily", "adjustment": parts[-1], "hours": None}
        # Option B: adjustment is 'splits_only' split across two tokens
        if len(parts) >= 4 and parts[-3] == "daily" and parts[-2] == "splits" and parts[-1] == "only":
            symbol = "_".join(parts[:-3])
            if symbol:
                return {"symbol": symbol, "interval": "daily", "adjustment": "splits_only", "hours": None}

        return None

    def get_symbol_exchange(self, symbol: str) -> str:
        """Get the exchange for a symbol using Alpha Vantage API or fallback mapping."""
        if symbol in self.symbol_exchange_cache:
            return self.symbol_exchange_cache[symbol]

        # Offline or restricted-network mode -> fallback
        if not self.alpha_vantage_api_key:
            venue = self._get_fallback_exchange(symbol)
            self.symbol_exchange_cache[symbol] = venue
            return venue

        # Try Alpha Vantage OVERVIEW (will fallback on any error)
        try:
            url = "https://www.alphavantage.co/query"
            params = {
                "function": "OVERVIEW",
                "symbol": symbol,
                "apikey": self.alpha_vantage_api_key,
            }
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            exchange = data.get("Exchange")
            if not exchange:
                raise RuntimeError("No exchange in response")
            venue = self._map_exchange_to_venue(exchange)
            self.symbol_exchange_cache[symbol] = venue
            return venue
        except Exception as e:
            logger.warning(f"Exchange lookup failed for {symbol}: {e}. Using fallback mapping.")
            venue = self._get_fallback_exchange(symbol)
            self.symbol_exchange_cache[symbol] = venue
            return venue

    def _map_exchange_to_venue(self, exchange: str) -> str:
        mapping = {
            "NASDAQ": "NASDAQ",
            "NYSE": "NYSE",
            "NYSE American": "NYSEAMERICAN",
            "NYSE ARCA": "NYSEARCA",
            "BATS": "BATS",
            "New York Stock Exchange": "NYSE",
            "NASDAQ Global Market": "NASDAQ",
            "NASDAQ Global Select": "NASDAQ",
            "NASDAQ Capital Market": "NASDAQ",
        }
        if exchange in mapping:
            return mapping[exchange]
        up = exchange.upper()
        if "NASDAQ" in up:
            return "NASDAQ"
        if "NYSE" in up:
            return "NYSE"
        logger.warning(f"Unknown exchange '{exchange}', defaulting to NYSE")
        return "NYSE"

    def _get_fallback_exchange(self, symbol: str) -> str:
        if symbol in {"SPY", "QQQ", "IWM", "VTI", "VOO", "VEA", "VWO", "SH"}:
            return "NASDAQ"
        return "NYSE"

    def create_instrument_id(self, symbol: str) -> InstrumentId:
        venue = self.get_symbol_exchange(symbol)
        return InstrumentId.from_str(f"{symbol}.{venue}")

    def create_bar_type(self, symbol: str, interval: str) -> BarType:
        instrument_id = self.create_instrument_id(symbol)

        if interval == "1min":
            spec = BarSpecification(step=1, aggregation=BarAggregation.MINUTE, price_type=PriceType.LAST)
        elif interval == "30min":
            spec = BarSpecification(step=30, aggregation=BarAggregation.MINUTE, price_type=PriceType.LAST)
        elif interval == "60min":
            spec = BarSpecification(step=1, aggregation=BarAggregation.HOUR, price_type=PriceType.LAST)
        elif interval == "daily":
            spec = BarSpecification(step=1, aggregation=BarAggregation.DAY, price_type=PriceType.LAST)
        else:
            logger.warning(f"Unknown interval '{interval}', defaulting to 1-minute")
            spec = BarSpecification(step=1, aggregation=BarAggregation.MINUTE, price_type=PriceType.LAST)

        return BarType(
            instrument_id=instrument_id,
            bar_spec=spec,
            aggregation_source=AggregationSource.EXTERNAL,
        )

    def read_csv_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        try:
            df = pd.read_csv(file_path)

            # AlphaVantage sometimes returns a single-column message CSV
            if len(df.columns) == 1 and 'Information' in df.columns:
                logger.warning(f"File {file_path.name} contains API message, not data. Skipping.")
                return None

            expected = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            if not all(col in df.columns for col in expected):
                logger.error(f"File {file_path.name} missing required columns. Expected {expected}; found {list(df.columns)}")
                return None
            if df.empty:
                logger.warning(f"File {file_path.name} is empty")
                return None
            logger.info(f"Read {len(df):,} rows from {file_path.name}")
            return df
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
            return None

    def convert_to_bars(self, df: pd.DataFrame, bar_type: BarType) -> List[Bar]:
        bars: List[Bar] = []
        for _, row in df.iterrows():
            try:
                t = str(row['timestamp'])
                if 'T' in t:
                    dt = datetime.fromisoformat(t.replace('Z', '+00:00'))
                elif ' ' in t:
                    dt = datetime.strptime(t, '%Y-%m-%d %H:%M:%S')
                else:
                    dt = datetime.strptime(t, '%Y-%m-%d')
                    dt = dt.replace(hour=20, minute=0, second=0, microsecond=0)  # 4pm ET ~ 20:00 UTC
                if dt.tzinfo is None:
                    dt = pytz.UTC.localize(dt)
                ts = dt_to_unix_nanos(pd.Timestamp(dt))

                open_p = float(row['open'])
                high_p = float(row['high'])
                low_p = float(row['low'])
                close_p = float(row['close'])
                vol_q = float(row['volume'])

                bars.append(
                    Bar(
                        bar_type=bar_type,
                        open=Price.from_str(f"{open_p:.5f}"),
                        high=Price.from_str(f"{high_p:.5f}"),
                        low=Price.from_str(f"{low_p:.5f}"),
                        close=Price.from_str(f"{close_p:.5f}"),
                        volume=Quantity.from_str(f"{vol_q:.0f}"),
                        ts_event=ts,
                        ts_init=ts,
                    )
                )
            except Exception as e:
                logger.error(f"Error converting row to Bar: {e}; row={row}")
                continue
        logger.info(f"Converted {len(bars):,} bars for {bar_type.instrument_id}")
        return bars

    def process_file(self, file_path: Path) -> bool:
        info = self.parse_filename(file_path)
        if not info:
            return False

        symbol = info['symbol']  # type: ignore
        interval = info['interval']  # type: ignore
        adjustment = info['adjustment']  # type: ignore
        hours = info['hours']  # type: ignore

        if interval == 'daily':
            logger.info(f"Processing {file_path.name} -> symbol={symbol}, interval={interval}, adjustment={adjustment}")
        else:
            logger.info(f"Processing {file_path.name} -> symbol={symbol}, interval={interval}, adjustment={adjustment}, hours={hours}")

        df = self.read_csv_file(file_path)
        if df is None:
            return False

        bar_type = self.create_bar_type(symbol, interval)  # uses exchange mapping
        bars = self.convert_to_bars(df, bar_type)
        if not bars:
            logger.warning(f"No valid bars created from {file_path.name}")
            return False

        bars.sort(key=lambda b: b.ts_init)
        try:
            self.catalog.write_data(bars)
            logger.info(f"Wrote {len(bars):,} bars to catalog")
            return True
        except Exception as e:
            logger.error(f"Error writing bars to catalog: {e}")
            return False

    def convert_all(self) -> Dict[str, int]:
        logger.info("Starting conversion of raw_data files")
        files = self.discover_csv_files()
        results = {"processed": 0, "successful": 0, "failed": 0}
        for p in files:
            results["processed"] += 1
            if self.process_file(p):
                results["successful"] += 1
            else:
                results["failed"] += 1
        logger.info(f"Conversion complete. Results: {results}")
        return results


def main():
    parser = argparse.ArgumentParser(
        description="Convert raw_data CSV files to Nautilus parquet format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python convert_hist_raw.py                 # uses ./raw_data -> ./nautilus_catalog
  python convert_hist_raw.py raw_data -o nautilus_catalog
  python convert_hist_raw.py /path/to/dir --offline
        """
    )

    parser.add_argument(
        "input_directory",
        type=str,
        nargs="?",
        default="./raw_data",
        help="Directory containing CSV files to convert (default: ./raw_data)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default="./nautilus_catalog",
        help="Output directory for parquet catalog (default: ./nautilus_catalog)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Disable network exchange lookup and use fallback mapping",
    )

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")

    input_dir = Path(args.input_directory)
    if not input_dir.exists() or not input_dir.is_dir():
        logger.error(f"Input directory invalid: {input_dir}")
        sys.exit(1)

    try:
        converter = RawDataHistoricalConverter(
            input_directory=input_dir,
            output_directory=Path(args.output),
            offline=bool(args.offline),
        )
        res = converter.convert_all()
        if res.get("successful", 0) == 0:
            logger.error("No files were successfully converted")
            sys.exit(1)
        logger.info("Conversion completed successfully")
    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
