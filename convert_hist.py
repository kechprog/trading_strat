#!/usr/bin/env python3
"""
Nautilus Historical Data Converter

Converts CSV files from Alpha Vantage format to Nautilus parquet format for backtesting.
Expects CSV files with columns: timestamp,open,high,low,close,volume

Usage:
    python convert_hist.py /path/to/csv/directory
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional, Dict, Tuple
import pandas as pd
import re
from datetime import datetime
import pytz
import requests
import os
from dotenv import load_dotenv

from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.model.data import Bar, BarType, BarSpecification
from nautilus_trader.model.enums import AggregationSource, BarAggregation, PriceType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import CurrencyPair, Equity
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('convert_hist.log')
    ]
)
logger = logging.getLogger(__name__)


class HistoricalDataConverter:
    """Converts Alpha Vantage CSV files to Nautilus parquet format."""
    
    def __init__(self, input_directory: Path, output_directory: Optional[Path] = None):
        self.input_directory = Path(input_directory)
        self.output_directory = output_directory or Path("./nautilus_catalog")
        self.catalog = ParquetDataCatalog(self.output_directory)
        
        # Pattern to match CSV filenames: SYMBOL_YYYY_MM_interval.csv or SYMBOL_daily.csv
        self.filename_pattern = re.compile(r'^([A-Z]+)_(\d{4})_(\d{2})_(.+)\.csv$')
        self.daily_filename_pattern = re.compile(r'^([A-Z]+)_(daily)\.csv$')
        
        # Load environment variables for Alpha Vantage API
        load_dotenv("alpha_data_getter/.env")  # Check alpha_data_getter first
        load_dotenv(".env")  # Also check root directory
        self.alpha_vantage_api_key = os.getenv("ALPHA_KEY")
        if not self.alpha_vantage_api_key:
            logger.warning("ALPHA_KEY not found in environment variables. Exchange detection will use fallback logic.")
        
        # Cache for symbol-to-exchange mapping to avoid repeated API calls
        self.symbol_exchange_cache = {}
        
        logger.info(f"Initialized converter:")
        logger.info(f"  Input directory: {self.input_directory}")
        logger.info(f"  Output directory: {self.output_directory}")
        logger.info(f"  Alpha Vantage API available: {bool(self.alpha_vantage_api_key)}")
    
    def discover_csv_files(self) -> List[Path]:
        """Discover all CSV files in the input directory."""
        csv_files = list(self.input_directory.glob("*.csv"))
        logger.info(f"Found {len(csv_files)} CSV files")
        
        # Filter files that match our expected patterns
        valid_files = []
        for file_path in csv_files:
            if self.filename_pattern.match(file_path.name) or self.daily_filename_pattern.match(file_path.name):
                valid_files.append(file_path)
            else:
                logger.warning(f"Skipping file with invalid pattern: {file_path.name}")
        
        logger.info(f"Found {len(valid_files)} valid CSV files matching pattern")
        return valid_files
    
    def parse_filename(self, file_path: Path) -> Optional[Tuple[str, int, int, str]]:
        """Parse filename to extract symbol, year, month, and interval."""
        # Try intraday pattern first: SYMBOL_YYYY_MM_interval.csv
        match = self.filename_pattern.match(file_path.name)
        if match:
            symbol, year_str, month_str, interval = match.groups()
            try:
                year = int(year_str)
                month = int(month_str)
                return symbol, year, month, interval
            except ValueError as e:
                logger.error(f"Error parsing year/month from filename {file_path.name}: {e}")
                return None
        
        # Try daily pattern: SYMBOL_daily.csv
        daily_match = self.daily_filename_pattern.match(file_path.name)
        if daily_match:
            symbol, interval = daily_match.groups()
            # For daily files, year and month are not relevant since we get all historical data
            return symbol, 0, 0, interval
        
        logger.error(f"Could not parse filename: {file_path.name}")
        return None
    
    def get_symbol_exchange(self, symbol: str) -> str:
        """Get the exchange for a symbol using Alpha Vantage API."""
        # Check cache first
        if symbol in self.symbol_exchange_cache:
            return self.symbol_exchange_cache[symbol]
        
        # Fallback logic if no API key
        if not self.alpha_vantage_api_key:
            return self._get_fallback_exchange(symbol)
        
        try:
            # Use Alpha Vantage OVERVIEW function to get company information
            url = "https://www.alphavantage.co/query"
            params = {
                "function": "OVERVIEW",
                "symbol": symbol,
                "apikey": self.alpha_vantage_api_key
            }
            
            logger.debug(f"Fetching exchange info for symbol: {symbol}")
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Check if we got valid data
            if "Exchange" in data and data["Exchange"]:
                exchange = data["Exchange"]
                logger.debug(f"Found exchange for {symbol}: {exchange}")
                
                # Map Alpha Vantage exchange names to common venue codes
                venue = self._map_exchange_to_venue(exchange)
                self.symbol_exchange_cache[symbol] = venue
                return venue
            else:
                logger.warning(f"No exchange data found for symbol {symbol} in Alpha Vantage response")
                venue = self._get_fallback_exchange(symbol)
                self.symbol_exchange_cache[symbol] = venue
                return venue
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error fetching exchange for {symbol}: {e}")
            venue = self._get_fallback_exchange(symbol)
            self.symbol_exchange_cache[symbol] = venue
            return venue
        except Exception as e:
            logger.error(f"Error fetching exchange for {symbol}: {e}")
            venue = self._get_fallback_exchange(symbol)
            self.symbol_exchange_cache[symbol] = venue
            return venue
    
    def _map_exchange_to_venue(self, exchange: str) -> str:
        """Map Alpha Vantage exchange names to venue codes."""
        exchange_mapping = {
            "NASDAQ": "NASDAQ",
            "NYSE": "NYSE", 
            "NYSE American": "NYSEAMERICAN",
            "NYSE ARCA": "NYSEARCA",
            "BATS": "BATS",
            "New York Stock Exchange": "NYSE",
            "NASDAQ Global Market": "NASDAQ",
            "NASDAQ Global Select": "NASDAQ",
            "NASDAQ Capital Market": "NASDAQ"
        }
        
        # Try exact match first
        if exchange in exchange_mapping:
            return exchange_mapping[exchange]
        
        # Try partial matches
        exchange_upper = exchange.upper()
        if "NASDAQ" in exchange_upper:
            return "NASDAQ"
        elif "NYSE" in exchange_upper:
            return "NYSE"
        else:
            logger.warning(f"Unknown exchange '{exchange}', defaulting to NYSE")
            return "NYSE"
    
    def _get_fallback_exchange(self, symbol: str) -> str:
        """Fallback logic for determining exchange when API is not available."""
        if symbol in ['SPY', 'QQQ', 'IWM', 'VTI', 'VOO', 'VEA', 'VWO', 'SH']:  # Common ETFs
            return "NASDAQ" 
        elif symbol.startswith('GOF'):  # Futures
            return "CME"
        else:
            return "NYSE"  # Default for stocks

    def create_instrument_id(self, symbol: str) -> InstrumentId:
        """Create a Nautilus InstrumentId from a symbol using Alpha Vantage to determine exchange."""
        venue = self.get_symbol_exchange(symbol)
        return InstrumentId.from_str(f"{symbol}.{venue}")
    
    def create_bar_type(self, symbol: str, interval: str) -> BarType:
        """Create a BarType for the given symbol and interval."""
        instrument_id = self.create_instrument_id(symbol)
        
        # Map interval string to BarSpecification
        if interval == "1min":
            step = 1
            aggregation = BarAggregation.MINUTE
        elif interval == "5min":
            step = 5
            aggregation = BarAggregation.MINUTE
        elif interval == "15min":
            step = 15
            aggregation = BarAggregation.MINUTE
        elif interval == "30min":
            step = 30
            aggregation = BarAggregation.MINUTE
        elif interval == "60min" or interval == "1hour":
            step = 1
            aggregation = BarAggregation.HOUR
        elif interval == "daily":
            step = 1
            aggregation = BarAggregation.DAY
        else:
            logger.warning(f"Unknown interval '{interval}', defaulting to 1-minute")
            step = 1
            aggregation = BarAggregation.MINUTE
        
        bar_spec = BarSpecification(
            step=step,
            aggregation=aggregation,
            price_type=PriceType.LAST
        )
        
        return BarType(
            instrument_id=instrument_id,
            bar_spec=bar_spec,
            aggregation_source=AggregationSource.EXTERNAL
        )
    
    def read_csv_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """Read and validate CSV file."""
        try:
            logger.debug(f"Reading CSV file: {file_path}")
            
            # Read CSV file
            df = pd.read_csv(file_path)
            
            # Check if file contains valid data or just API message
            if len(df.columns) == 1 and 'Information' in df.columns:
                logger.warning(f"File {file_path.name} contains API message, not data. Skipping.")
                return None
            
            # Validate expected columns
            expected_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            if not all(col in df.columns for col in expected_columns):
                logger.error(f"File {file_path.name} missing required columns. Expected: {expected_columns}, Found: {list(df.columns)}")
                return None
            
            # Basic data validation
            if len(df) == 0:
                logger.warning(f"File {file_path.name} is empty")
                return None
            
            logger.info(f"Successfully read {len(df)} rows from {file_path.name}")
            return df
            
        except Exception as e:
            logger.error(f"Error reading CSV file {file_path}: {e}")
            return None
    
    def convert_to_bars(self, df: pd.DataFrame, bar_type: BarType) -> List[Bar]:
        """Convert DataFrame to Nautilus Bar objects."""
        bars = []
        
        for _, row in df.iterrows():
            try:
                # Parse timestamp
                timestamp_str = str(row['timestamp'])
                
                # Handle different timestamp formats
                if 'T' in timestamp_str:
                    # ISO format: 2023-01-01T09:30:00
                    dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                elif ' ' in timestamp_str:
                    # Simple format: 2023-01-01 09:30:00
                    dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                else:
                    # Daily format: 2023-01-01 (date only)
                    dt = datetime.strptime(timestamp_str, '%Y-%m-%d')
                    # Set time to market close (4:00 PM ET = 20:00 UTC)
                    dt = dt.replace(hour=20, minute=0, second=0, microsecond=0)
                
                # Ensure timezone aware (assume UTC if not specified)
                if dt.tzinfo is None:
                    dt = pytz.UTC.localize(dt)
                
                # Convert to UNIX nanoseconds
                ts_event = dt_to_unix_nanos(pd.Timestamp(dt))
                ts_init = ts_event  # For historical data, these are the same
                
                # Create Bar object - convert pandas Series to float explicitly
                open_price = float(row['open'])
                high_price = float(row['high'])
                low_price = float(row['low'])
                close_price = float(row['close'])
                volume_qty = float(row['volume'])
                
                bar = Bar(
                    bar_type=bar_type,
                    open=Price.from_str(f"{open_price:.5f}"),
                    high=Price.from_str(f"{high_price:.5f}"),
                    low=Price.from_str(f"{low_price:.5f}"),
                    close=Price.from_str(f"{close_price:.5f}"),
                    volume=Quantity.from_str(f"{volume_qty:.0f}"),
                    ts_event=ts_event,
                    ts_init=ts_init
                )
                
                bars.append(bar)
                
            except Exception as e:
                logger.error(f"Error converting row to Bar: {e}, Row: {row}")
                continue
        
        logger.info(f"Converted {len(bars)} bars for {bar_type.instrument_id}")
        return bars
    
    def process_file(self, file_path: Path) -> bool:
        """Process a single CSV file."""
        logger.info(f"Processing file: {file_path.name}")
        
        # Parse filename
        parsed = self.parse_filename(file_path)
        if not parsed:
            return False
        
        symbol, year, month, interval = parsed
        if interval == "daily":
            logger.info(f"  Symbol: {symbol}, Interval: {interval} (all historical data)")
        else:
            logger.info(f"  Symbol: {symbol}, Year: {year}, Month: {month}, Interval: {interval}")
        
        # Read CSV file
        df = self.read_csv_file(file_path)
        if df is None:
            return False
        
        # Create bar type
        bar_type = self.create_bar_type(symbol, interval)
        logger.info(f"  Created bar type: {bar_type}")
        
        # Convert to bars
        bars = self.convert_to_bars(df, bar_type)
        if not bars:
            logger.warning(f"No valid bars created from {file_path.name}")
            return False
        
        # Sort bars by timestamp (ts_init) before writing to catalog
        bars.sort(key=lambda x: x.ts_init)
        
        # Write to catalog
        try:
            self.catalog.write_data(bars)
            logger.info(f"Successfully wrote {len(bars)} bars to catalog")
            return True
        except Exception as e:
            logger.error(f"Error writing bars to catalog: {e}")
            return False
    
    def convert_all(self) -> Dict[str, int]:
        """Convert all CSV files in the input directory."""
        logger.info("Starting conversion of all CSV files")
        
        # Discover files
        csv_files = self.discover_csv_files()
        if not csv_files:
            logger.warning("No valid CSV files found")
            return {"processed": 0, "successful": 0, "failed": 0}
        
        # Process files
        results = {"processed": 0, "successful": 0, "failed": 0}
        
        for file_path in csv_files:
            results["processed"] += 1
            if self.process_file(file_path):
                results["successful"] += 1
            else:
                results["failed"] += 1
        
        logger.info(f"Conversion complete. Results: {results}")
        return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Convert Alpha Vantage CSV files to Nautilus parquet format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python convert_hist.py /path/to/csv/files
  python convert_hist.py ./alpha_data_getter/1MIN
        """
    )
    
    parser.add_argument(
        "input_directory",
        type=str,
        help="Directory containing CSV files to convert"
    )
    
    parser.add_argument(
        "-o", "--output",
        type=str,
        default="./nautilus_catalog",
        help="Output directory for parquet catalog (default: ./nautilus_catalog)"
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")
    
    # Validate input directory
    input_dir = Path(args.input_directory)
    if not input_dir.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        sys.exit(1)
    
    if not input_dir.is_dir():
        logger.error(f"Input path is not a directory: {input_dir}")
        sys.exit(1)
    
    # Create converter and run
    try:
        converter = HistoricalDataConverter(
            input_directory=input_dir,
            output_directory=Path(args.output)
        )
        
        results = converter.convert_all()
        
        if results["successful"] > 0:
            logger.info(f"Conversion completed successfully!")
            logger.info(f"  Files processed: {results['processed']}")
            logger.info(f"  Files successful: {results['successful']}")
            logger.info(f"  Files failed: {results['failed']}")
            logger.info(f"  Output catalog: {converter.output_directory}")
        else:
            logger.error("No files were successfully converted")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
