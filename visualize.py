"""Visualization utilities for backtest outputs.

Creates a two-row figure:
- Top: price (close), indicator cumulative sum, and entry/exit markers
- Bottom: equity curve (total portfolio balance)

Reads the CSVs written by the Breakout strategy on stop:
- backtest_1m_log.csv: per-minute snapshots with price, indicator, equity
- backtest_events.csv: entry/exit events with side, qty, price

Usage:
    python visualize.py \
        --log-csv backtest_1m_log.csv \
        --events-csv backtest_events.csv \
        --output-html backtest_plot.html \
        --show

Both --events-csv and --output-html are optional. If --show is passed,
the plot opens in a browser. If --output-html is provided, it also writes
an HTML file you can share.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import pandas as pd
import pytz
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def _load_log_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
    # Normalize expected columns
    expected_cols = {
        "open", "high", "low", "close", "balance_total",
        "position_main", "position_reverse", "indicator_value",
    }
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        # Not fatal; we plot what we can
        pass
    return df


def _load_events_csv(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    df = pd.read_csv(path)
    if df.empty:
        return None
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
    return df


def plot_equity_and_signals(
    log_csv: Path | str = Path("backtest_1m_log.csv"),
    events_csv: Path | str | None = Path("backtest_events.csv"),
    output_html: Path | str | None = None,
    show: bool = True,
    price_col: str = "close",
    indicator_col: str = "indicator_value",
    compress_time: bool = True,
) -> go.Figure:
    """Build and optionally render the 2-row figure.

    - Row 1: price (close) + indicator + entry/exit markers
    - Row 2: equity (balance_total)
    """
    log_csv = Path(log_csv)
    df = _load_log_csv(log_csv)
    if df.empty:
        raise ValueError(f"Log CSV has no data: {log_csv}")

    events: Optional[pd.DataFrame] = None
    if events_csv is not None:
        events = _load_events_csv(Path(events_csv))

    # Ensure time ordering for correct cumulative calculations and plotting
    if "time" in df.columns:
        # Convert to market timezone for plotting and rangebreaks
        df["time_plot"] = df["time"]
        df = df.sort_values("time_plot").reset_index(drop=True)
    else:
        raise KeyError("Expected 'time' column in log CSV")

    if events is not None and "time" in events.columns:
        events["time_plot"] = events["time"]

    # Prepare base figure with secondary y on the top row (for indicator if scales differ)
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.07,
        row_heights=[0.65, 0.35],
        specs=[[{"secondary_y": True}], [{}]],
    )

    # Row 1: Price
    if price_col not in df.columns:
        raise KeyError(f"Column '{price_col}' not found in {log_csv}")
    fig.add_trace(
        go.Scatter(
            x=df["time_plot"],
            y=df[price_col],
            mode="lines",
            name="Price (close)",
            line=dict(color="#1f77b4"),
            hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Close=%{y:.2f}<extra></extra>",
        ),
        row=1,
        col=1,
        secondary_y=False,
    )

    # Row 1: Indicator cumulative sum (if available)
    if indicator_col in df.columns and df[indicator_col].notna().any():
        ind_series = pd.to_numeric(df[indicator_col], errors="coerce").fillna(0.0)
        df["indicator_cumsum"] = ind_series.cumsum()
        fig.add_trace(
            go.Scatter(
                x=df["time_plot"],
                y=df["indicator_cumsum"],
                mode="lines",
                name="Indicator (cumsum)",
                line=dict(color="#ff7f0e", dash="dot"),
                hovertemplate="%{x|%Y-%m-%d %H:%M}<br>CumIndicator=%{y:.4f}<extra></extra>",
            ),
            row=1,
            col=1,
            secondary_y=True,
        )

    # Row 1: Entry/Exit markers (if events present)
    if events is not None and not events.empty:
        # Join main close for y-positioning of markers to align with price chart
        ev = events.merge(
            df[["time_plot", price_col]].rename(columns={price_col: "main_close_at_event"}),
            left_on="time_plot",
            right_on="time_plot",
            how="left",
        )

        # Map event type to marker style/color
        style_map = {
            "entry_long": dict(symbol="triangle-up", color="#2ca02c"),
            "exit_long": dict(symbol="x", color="#2ca02c"),
            "entry_short": dict(symbol="triangle-down", color="#d62728"),
            "exit_short": dict(symbol="x", color="#d62728"),
        }

        for evt_type, style in style_map.items():
            sub = ev[ev["event_type"] == evt_type]
            if sub.empty:
                continue
            fig.add_trace(
                go.Scatter(
                    x=sub["time_plot"],
                    y=sub["main_close_at_event"],
                    mode="markers",
                    name=f"{evt_type}",
                    marker=dict(
                        symbol=style["symbol"],
                        size=10,
                        color=style["color"],
                        line=dict(width=1, color="#000000"),
                        opacity=0.9,
                    ),
                    hovertemplate=(
                        "%{x|%Y-%m-%d %H:%M}"  # time
                        "<br>%{customdata[0]}"     # symbol
                        "<br>%{customdata[1]}"     # side
                        "<br>Qty=%{customdata[2]}"  # qty
                        "<br>TradePx=%{customdata[3]:.2f}"  # trade price
                        "<extra>" + evt_type + "</extra>"
                    ),
                    customdata=sub[["symbol", "side", "qty", "price"]].values,
                ),
                row=1,
                col=1,
                secondary_y=False,
            )

    # Row 2: Equity curve
    if "balance_total" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["time_plot"],
                y=df["balance_total"],
                mode="lines",
                name="Equity (balance_total)",
                line=dict(color="#9467bd"),
                hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Equity=%{y:.2f}<extra></extra>",
            ),
            row=2,
            col=1,
        )
    else:
        # Fallback: show not available placeholder
        fig.add_annotation(
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.1,
            showarrow=False,
            text="No equity column 'balance_total' in log CSV",
        )

    # Layout
    fig.update_layout(
        title="Backtest Price, Signals, and Equity",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=20, t=50, b=40),
        hovermode="x unified",
        template="plotly_white",
    )
    # Apply rangebreaks to compress non-trading time
    if compress_time:
        rangebreaks = [
            dict(bounds=[16, 9.5], pattern="hour"),  # Hide 16:00 -> 09:30
            # dict(bounds=["sat", "sun"]),            # Hide weekends
        ]
        fig.update_xaxes(rangebreaks=rangebreaks)

    fig.update_xaxes(title_text="Time", row=2, col=1)
    fig.update_yaxes(title_text="Price", row=1, col=1, secondary_y=False)
    fig.update_yaxes(title_text="Indicator (cumsum)", row=1, col=1, secondary_y=True)
    fig.update_yaxes(title_text="Equity", row=2, col=1)

    # Output
    if output_html is not None:
        out = Path(output_html)
        out.parent.mkdir(parents=True, exist_ok=True)
        # Embed Plotly JS to keep file self-contained/offline-friendly
        fig.write_html(out, include_plotlyjs=True)

    if show:
        # Opens in default browser
        fig.show()

    return fig


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Plot price and cumulative indicator with entries/exits and equity curve.")
    p.add_argument("--log-csv", type=Path, default=Path("backtest_1m_log.csv"), help="Path to per-minute log CSV.")
    p.add_argument("--events-csv", type=Path, default=Path("backtest_events.csv"), help="Path to events CSV (optional).", nargs="?")
    p.add_argument("--output-html", type=Path, default=None, help="Write interactive HTML to this path (optional).")
    p.add_argument("--no-show", action="store_true", help="Do not open the figure in a browser.")
    p.add_argument("--price-col", type=str, default="close", help="Price column to plot (default: close).")
    p.add_argument("--indicator-col", type=str, default="indicator_value", help="Indicator column to plot (default: indicator_value).")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    plot_equity_and_signals(
        log_csv=args.log_csv,
        events_csv=args.events_csv,
        output_html=args.output_html,
        show=not args.no_show,
        price_col=args.price_col,
        indicator_col=args.indicator_col,
    )


if __name__ == "__main__":
    main()
