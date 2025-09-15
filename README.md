# Trading Strategy Backtesting Framework

This project implements a trading strategy backtesting system using the Nautilus Trader platform. Backtest simulates a full venue with order book and slippage. The strategy uses trend divergence analysis with volume confirmation to identify trading opportunities.

## Strategy Overview

The strategy is based on the concept of **Trend Divergence with Volume Confirmation**:

1. **Dual Trend Analysis**:
   - Long-term trend: Linear regression on EMA(100 hours)
   - Short-term trend: Recent price momentum (6-12 days period)

2. **Volume as Confirmation Signal**:
   - Big volume jumps indicate genuine trend changes
   - Moderate volume increases suggest temporary deviations with continuation

3. **Trading Logic**:
   - When short-term and long-term trends diverge:
     - Big volume jump → Follow short-term trend (new trend forming)
     - Moderate/low volume → Expect reversion to long-term trend

## Project Structure

```
├── backtest.py              # Main backtest configuration and execution
├── breakout.py              # Core strategy implementation
├── indicators/              # Custom trading indicators
│   ├── trend_div_vol_spike.py  # Main indicator logic
│   └── high_low_hist.py     # High/low level tracking
├── visualize.py             # Visualization tools for backtest results
└── requirements.txt         # Project dependencies
```

## Implementation Details

### Core Components

1. **Breakout Strategy** (`breakout.py`):
   - Uses VOO (S&P 500 ETF) as the main instrument and SH (Short S&P 500 ETF) as the reverse instrument
   - Entry/exit rules based on daily high/low levels and indicator signals
   - Risk management with position sizing based on account balance

2. **Trend Divergence Indicator** (`indicators/trend_div_vol_spike.py`):
   - Calculates long-term and short-term trends
   - Analyzes volume patterns to confirm trend changes
   - Generates continuous signals for the strategy

3. **High/Low Historical Indicator** (`indicators/high_low_hist.py`):
   - Tracks historical high/low levels for entry and exit points
   - Provides dynamic support/resistance levels

### Backtest Configuration

The backtest runs from January 1, 2018 to January 1, 2020 with:
- Initial capital: $100,000
- Instruments: VOO.NASDAQ (long) and SH.NASDAQ (short)
- Timeframe: 1-minute bars
- Indicator: TrendDivVolSpikeIndicator with volume spike filter

## Results

Based on the backtest results:
- **Total Profit**: $20,849.04 (20.85% return)
- **Sharpe Ratio**: 1.37
- **Sortino Ratio**: 3.12
- **Win Rate**: 45.7%
- **Profit Factor**: 1.70

The strategy generated 350 trades with 798 total events over the test period.

## Usage

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Run the backtest:
   ```bash
   python backtest.py
   ```

3. Visualize results:
   ```bash
   python visualize.py
   ```

## Order Book Simulation

The system uses Nautilus Trader's advanced market simulator which provides:
- Full order book reconstruction
- Realistic market dynamics
- Accurate transaction cost modeling
- Proper handling of market orders, limit orders, and partial fills

This ensures that backtest results closely reflect how the strategy would perform in real market conditions.

## Data

The strategy uses minute-level data for VOO and SH ETFs. The data is stored in parquet format in the `catalog` directory.

## Visualization

The backtest generates two CSV files:
- `backtest_1m_log.csv`: Per-minute snapshots with price, indicator values, and equity
- `backtest_events.csv`: Entry/exit events with details

These can be visualized using the `visualize.py` script which creates interactive plots showing:
- Price action with entry/exit markers
- Cumulative indicator values
- Equity curve