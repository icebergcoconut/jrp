"""
Backtesting Engine — Event-Driven Backtest for Trading Signals

Entry rule: BUY signal from XGBoost model
Exit rule: After 20 trading days OR when HOLD signal is generated
Metrics: Cumulative return, Sharpe ratio, max drawdown, win rate
"""

import pandas as pd
import numpy as np
import json
import logging
from typing import Optional
from dataclasses import dataclass, field, asdict
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class Trade:
    """Represents a single trade."""
    ticker: str
    entry_date: str
    entry_price: float
    exit_date: Optional[str] = None
    exit_price: Optional[float] = None
    holding_days: int = 0
    pnl: float = 0.0
    pnl_pct: float = 0.0
    exit_reason: str = ""


@dataclass
class BacktestResult:
    """Complete backtest result for a single ticker."""
    ticker: str
    start_date: str
    end_date: str
    initial_capital: float
    final_capital: float
    total_return_pct: float
    annualized_return_pct: float
    sharpe_ratio: float
    max_drawdown_pct: float
    max_drawdown_duration_days: int
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate_pct: float
    avg_win_pct: float
    avg_loss_pct: float
    profit_factor: float
    avg_holding_days: float
    trades: list = field(default_factory=list)
    daily_portfolio_values: list = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["trades"] = [asdict(t) if isinstance(t, Trade) else t for t in self.trades]
        return d


class BacktestEngine:
    """
    Event-driven backtesting engine.

    Simulates trading based on BUY/HOLD signals:
    - BUY signal → Enter position (if not already in one)
    - Exit after HOLDING_PERIOD days or on HOLD signal
    - Calculates comprehensive performance metrics
    """

    def __init__(
        self,
        initial_capital: float = 1_000_000,  # ¥1,000,000
        holding_period: int = 20,
        commission_rate: float = 0.001,  # 0.1% per trade
    ):
        self.initial_capital = initial_capital
        self.holding_period = holding_period
        self.commission_rate = commission_rate

    def run(self, df: pd.DataFrame, ticker: str) -> BacktestResult:
        """
        Run backtest on a single ticker's data.

        Args:
            df: DataFrame with columns: Date, Close, signal, confidence
                signal: 'BUY' or 'HOLD'
                confidence: float 0-1 (from ML model)
            ticker: Stock ticker symbol

        Returns:
            BacktestResult with all metrics and trade history
        """
        df = df.sort_values("Date").reset_index(drop=True)

        capital = self.initial_capital
        position = None  # Current active trade
        trades = []
        daily_values = []

        for i, row in df.iterrows():
            date = str(row["Date"])[:10]
            close = float(row["Close"])
            signal = row.get("signal", "HOLD")
            confidence = float(row.get("confidence", 0.5))

            # Check exit conditions for open position
            if position is not None:
                position.holding_days += 1

                # Exit conditions:
                # 1. Holding period exceeded
                # 2. Signal changed to HOLD (model says no longer a buy)
                should_exit = (
                    position.holding_days >= self.holding_period
                    or (signal == "HOLD" and position.holding_days >= 3)  # Min 3 days hold
                )

                if should_exit:
                    # Close position
                    position.exit_date = date
                    position.exit_price = close
                    position.pnl = (close - position.entry_price) * (capital / position.entry_price)
                    position.pnl_pct = ((close - position.entry_price) / position.entry_price) * 100
                    position.exit_reason = (
                        "holding_period" if position.holding_days >= self.holding_period
                        else "signal_change"
                    )

                    # Apply commission
                    commission = capital * self.commission_rate
                    capital = capital * (close / position.entry_price) - commission

                    trades.append(position)
                    logger.debug(
                        f"  EXIT {ticker} @ ¥{close:,.0f} "
                        f"({position.pnl_pct:+.2f}%, {position.holding_days}d, {position.exit_reason})"
                    )
                    position = None

            # Check entry conditions
            if position is None and signal == "BUY":
                position = Trade(
                    ticker=ticker,
                    entry_date=date,
                    entry_price=close,
                )
                logger.debug(f"  ENTRY {ticker} @ ¥{close:,.0f} (confidence: {confidence:.2f})")

            # Record daily portfolio value
            if position is not None:
                current_value = capital * (close / position.entry_price)
            else:
                current_value = capital

            daily_values.append({
                "date": date,
                "portfolio_value": round(current_value, 2),
                "close": close,
                "signal": signal,
                "in_position": position is not None,
            })

        # Close any remaining position at the end
        if position is not None:
            last_row = df.iloc[-1]
            position.exit_date = str(last_row["Date"])[:10]
            position.exit_price = float(last_row["Close"])
            position.pnl_pct = ((position.exit_price - position.entry_price) / position.entry_price) * 100
            position.exit_reason = "end_of_data"
            capital = capital * (position.exit_price / position.entry_price)
            trades.append(position)

        # Calculate metrics
        return self._calculate_metrics(ticker, df, trades, daily_values, capital)

    def _calculate_metrics(
        self,
        ticker: str,
        df: pd.DataFrame,
        trades: list[Trade],
        daily_values: list[dict],
        final_capital: float,
    ) -> BacktestResult:
        """Calculate comprehensive backtest metrics."""

        portfolio_series = pd.Series([d["portfolio_value"] for d in daily_values])

        # Basic returns
        total_return_pct = ((final_capital - self.initial_capital) / self.initial_capital) * 100

        # Annualized return
        n_days = len(daily_values)
        n_years = max(n_days / 252, 0.01)  # Trading days per year
        annualized_return = ((final_capital / self.initial_capital) ** (1 / n_years) - 1) * 100

        # Daily returns for Sharpe calculation
        daily_returns = portfolio_series.pct_change().dropna()

        # Sharpe ratio (annualized, risk-free rate = 0)
        if len(daily_returns) > 1 and daily_returns.std() > 0:
            sharpe = (daily_returns.mean() / daily_returns.std()) * np.sqrt(252)
        else:
            sharpe = 0.0

        # Max drawdown
        peak = portfolio_series.expanding(min_periods=1).max()
        drawdown = (portfolio_series - peak) / peak
        max_drawdown_pct = abs(float(drawdown.min())) * 100

        # Max drawdown duration
        is_in_drawdown = drawdown < 0
        dd_groups = (~is_in_drawdown).cumsum()
        if is_in_drawdown.any():
            dd_durations = is_in_drawdown.groupby(dd_groups).sum()
            max_dd_duration = int(dd_durations.max())
        else:
            max_dd_duration = 0

        # Trade statistics
        total_trades = len(trades)
        winning_trades = [t for t in trades if t.pnl_pct > 0]
        losing_trades = [t for t in trades if t.pnl_pct <= 0]
        n_wins = len(winning_trades)
        n_losses = len(losing_trades)
        win_rate = (n_wins / max(total_trades, 1)) * 100

        avg_win = np.mean([t.pnl_pct for t in winning_trades]) if winning_trades else 0.0
        avg_loss = np.mean([t.pnl_pct for t in losing_trades]) if losing_trades else 0.0

        # Profit factor
        gross_profit = sum(t.pnl_pct for t in winning_trades) if winning_trades else 0.0
        gross_loss = abs(sum(t.pnl_pct for t in losing_trades)) if losing_trades else 0.001
        profit_factor = gross_profit / gross_loss

        # Average holding days
        avg_holding = np.mean([t.holding_days for t in trades]) if trades else 0.0

        start_date = str(df["Date"].min())[:10]
        end_date = str(df["Date"].max())[:10]

        return BacktestResult(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
            initial_capital=self.initial_capital,
            final_capital=round(final_capital, 2),
            total_return_pct=round(total_return_pct, 2),
            annualized_return_pct=round(annualized_return, 2),
            sharpe_ratio=round(sharpe, 3),
            max_drawdown_pct=round(max_drawdown_pct, 2),
            max_drawdown_duration_days=max_dd_duration,
            total_trades=total_trades,
            winning_trades=n_wins,
            losing_trades=n_losses,
            win_rate_pct=round(win_rate, 1),
            avg_win_pct=round(avg_win, 2),
            avg_loss_pct=round(avg_loss, 2),
            profit_factor=round(profit_factor, 2),
            avg_holding_days=round(avg_holding, 1),
            trades=[asdict(t) for t in trades],
            daily_portfolio_values=daily_values,
        )


def run_backtest_for_ticker(
    ticker: str,
    data_path: str = "data/features",
    initial_capital: float = 1_000_000,
) -> BacktestResult:
    """
    Convenience function to run backtest for a single ticker.
    Loads data from feature files and runs the engine.
    """
    import os

    # Try to load processed features
    safe_name = ticker.replace(".", "_")
    feature_file = os.path.join(data_path, f"{safe_name}_features.csv")

    if os.path.exists(feature_file):
        df = pd.read_csv(feature_file)
    else:
        # Fallback: load from validated data
        validated_file = os.path.join("data", "validated", "stock_data.csv")
        if os.path.exists(validated_file):
            full_df = pd.read_csv(validated_file)
            df = full_df[full_df["ticker"] == ticker].copy()
        else:
            raise FileNotFoundError(f"No data found for {ticker}")

    # Ensure required columns
    if "signal" not in df.columns:
        # Generate simple signal based on SMA crossover
        df["signal"] = "HOLD"
        if "sma_25" in df.columns and "sma_75" in df.columns:
            df.loc[
                (df["sma_25"] > df["sma_75"]) &
                (df["sma_25"].shift(1) <= df["sma_75"].shift(1)),
                "signal"
            ] = "BUY"

    if "confidence" not in df.columns:
        df["confidence"] = 0.5

    engine = BacktestEngine(initial_capital=initial_capital)
    return engine.run(df, ticker)


if __name__ == "__main__":
    # Demo with synthetic data
    import sys
    sys.path.insert(0, ".")
    from data_pipeline.config import TICKER_LIST

    logger.info("=" * 60)
    logger.info("  📊 Backtesting Engine — Demo Run")
    logger.info("=" * 60)

    # Generate synthetic data for demo
    np.random.seed(42)
    dates = pd.date_range("2024-01-01", "2026-02-01", freq="B")
    price = 3000 + np.cumsum(np.random.randn(len(dates)) * 30)
    price = np.maximum(price, 100)  # Ensure positive prices

    demo_df = pd.DataFrame({
        "Date": dates,
        "Close": price,
        "signal": np.random.choice(["BUY", "HOLD", "HOLD", "HOLD", "HOLD"], size=len(dates)),
        "confidence": np.random.uniform(0.3, 0.9, size=len(dates)),
    })

    engine = BacktestEngine()
    result = engine.run(demo_df, "7203.T")

    print(f"\n{'='*50}")
    print(f"  Ticker: {result.ticker}")
    print(f"  Period: {result.start_date} → {result.end_date}")
    print(f"  Total Return: {result.total_return_pct:+.2f}%")
    print(f"  Annualized: {result.annualized_return_pct:+.2f}%")
    print(f"  Sharpe Ratio: {result.sharpe_ratio:.3f}")
    print(f"  Max Drawdown: {result.max_drawdown_pct:.2f}%")
    print(f"  Win Rate: {result.win_rate_pct:.1f}%")
    print(f"  Trades: {result.total_trades}")
    print(f"  Profit Factor: {result.profit_factor:.2f}")
    print(f"{'='*50}")
