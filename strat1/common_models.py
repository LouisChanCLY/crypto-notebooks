from dataclasses import dataclass
from typing import List
import pandas as pd
import matplotlib.pyplot as plt


@dataclass
class Trade:
    date: str
    btc_close_price: float
    trade_type: bool  # True for enter, False for exit
    amount: float
    unit: float

    def __repr__(self):
        str_type = "Enter" if self.trade_type else "Exit"
        lines = (
            f"=== {str_type} Position ===",
            f"Date: {self.date}",
            f"BTC Close Price: {self.btc_close_price}",
            f"{str_type} Amount: $ {self.amount:,.2f}",
            f"{str_type} Unit: ₿ {self.unit:,.8f}",
            "=== End ===",
        )
        return "\n".join(lines)


@dataclass
class StrategySummary:
    start_date: str
    end_date: str
    starting_capital: float
    trades: List[Trade]
    total_pnl: float
    total_pnl_pct: float
    sharpe_ratio: float
    days_in_market: int
    days_in_market_pct: float
    portfolio_value_timeline: pd.Series
    max_drawdown_pct: float
    max_drawdown_period: float
    drawdown_period_timeline: pd.Series
    watermark_timeline: pd.Series

    def __repr__(self):
        lines = (
            "=== Strategy End ===",
            f"Date Range: {self.start_date} till {self.end_date}",
            f"No. of Trades: {len(self.trades)}",
            f"Total PnL: {self.total_pnl:,.2f} ({self.total_pnl_pct:.2%})",
            f"Sharpe Ratio: {self.sharpe_ratio:.2f}",
            f"Days in Market: {self.days_in_market:,g} ({self.days_in_market_pct:.2%})",
        )
        return "\n".join(lines)

    def plot_equity_curve(self):
        fig, ax = plt.subplots(figsize=(25, 5))
        self.portfolio_value_timeline.plot(ax=ax)
        return ax
