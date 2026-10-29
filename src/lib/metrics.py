"""Pure metric functions – no Spark dependency, unit-tested in CI."""

import pandas as pd


def daily_return_pct(prices: pd.Series) -> pd.Series:
    """Day-over-day change in percent."""
    return prices.pct_change() * 100


def drawdown_from_high_pct(prices: pd.Series) -> pd.Series:
    """Distance from the running maximum in percent (0 or negative)."""
    running_max = prices.cummax()
    return (prices / running_max - 1) * 100


def is_drop_alert(change_pct: float | None, threshold_pct: float | None) -> bool:
    """True when change breaches a negative threshold, e.g. -12 vs -10."""
    if change_pct is None or threshold_pct is None:
        return False
    return change_pct <= threshold_pct
