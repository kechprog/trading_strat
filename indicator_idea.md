# Trend Divergence with Volume Confirmation Indicator

## Core Concept

This indicator aims to distinguish between temporary market fluctuations and genuine trend reversals by analyzing the relationship between short-term price movements, long-term trends, and volume patterns.

## Key Principles

1. **Dual Trend Analysis**
   - Long-term trend: Represents the major market direction (using linear regression on EMA with 100-hour period)
   - Short-term trend: Represents recent price momentum (using shorter period analysis)

2. **Volume as Confirmation Signal**
   - **Big volume jump**: Indicates a genuine trend change or regime shift
   - **Moderate volume increase**: Indicates temporary deviation or quick correction with continuation of the major trend
   - When short-term trend diverges from long-term trend:
     - Big volume jump → New trend forming, follow short-term trend
     - Moderate volume → Temporary fluctuation, expect reversion to long-term trend

3. **Divergence Markers**
   - Beginning of deviation: Marked by a volume jump
   - End of correction: Marked by another volume jump
   - General end of deviation: Not clearly marked by volume, may be followed by another deviation

4. **Trading Logic**
   - Short-term down while long-term up:
     - Big volume jump → New trend → SHORT signal
     - Moderate/low volume → Temporary deviation → LONG signal (reversion)
   - Short-term up while long-term down:
     - Big volume jump → New trend → LONG signal
     - Moderate/low volume → Temporary deviation → SHORT signal (reversion)

## Implementation Details

### Volume Transformation
- Apply log transformation to volume: log(volume)
- Calculate μ (mean) and σ (standard deviation) of log(volume) over the lookback window
- Calculate z-score: z = (log(current_volume) - μ) / σ

### Significance Parameters
- **Big volume jump**: Z-score above a high threshold (e.g., > 1.5-2.0)
- **Moderate volume increase**: Z-score above a lower threshold but below the big jump threshold (e.g., 0.5-1.5)
- **Low volume**: Z-score below the moderate threshold

### Timeframes
- Long-term trend: Linear regression on EMA(100 hours)
- Short-term trend: 6-12 days period

## Signal Generation

1. Calculate long-term trend direction
2. Calculate short-term trend direction
3. Measure divergence between trends
4. Analyze volume pattern during divergence:
   - Big volume jump → Trend change signal
   - Moderate volume → Reversion signal
   - Low volume → Continue monitoring
5. Generate signal based on divergence and volume confirmation:
   - Divergence + big volume → Follow short-term trend
   - Divergence + moderate/low volume → Expect reversion to long-term trend

## Expected Advantages

- Better discrimination between temporary pullbacks and trend reversals
- Volume confirmation reduces false signals
- Adapts to different market conditions (trending vs. mean-reverting)
- Distinguishes between different types of market moves based on volume signatures