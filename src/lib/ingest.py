"""Pure helpers for price ingestion – no Spark / yfinance dependency, unit-tested in CI."""

from datetime import date, timedelta

import pandas as pd

TRUTHY = {"true", "1", "yes", "y", "ano", "x"}

PRICE_COLUMNS = [
    "ticker", "date", "open", "high", "low", "close", "adj_close", "volume", "currency",
]

_RENAME = {
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Adj Close": "adj_close",
    "Volume": "volume",
}


def is_truthy(value) -> bool:
    """Sheet flag → bool. Accepts TRUE/1/yes/ano/x (case-insensitive)."""
    return str(value).strip().lower() in TRUTHY


def fetch_start(last_date: date | None, overlap_days: int = 5) -> date | None:
    """Start date for incremental fetch; None means full history.

    Re-fetches a few days back so late corrections from Yahoo get merged in.
    """
    if last_date is None:
        return None
    return last_date - timedelta(days=overlap_days)


def fx_ticker(currency: str) -> str:
    """Yahoo FX pair converting `currency` to USD, e.g. 'EUR' → 'EURUSD=X'."""
    return f"{currency.upper()}USD=X"


def normalize_history(raw: pd.DataFrame, ticker: str, currency: str | None) -> pd.DataFrame:
    """yfinance Ticker.history(auto_adjust=False) output → bronze_prices schema."""
    if raw is None or raw.empty:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    df = raw.rename(columns=_RENAME).copy()
    if "adj_close" not in df.columns:
        df["adj_close"] = df["close"]
    if "volume" not in df.columns:
        df["volume"] = 0

    # index is tz-aware in exchange time → .date gives the trading day
    df["date"] = pd.DatetimeIndex(df.index).date
    df["ticker"] = ticker
    df["currency"] = currency
    df = df.dropna(subset=["close"])
    df["volume"] = df["volume"].fillna(0).astype("int64")
    for col in ["open", "high", "low", "close", "adj_close"]:
        df[col] = df[col].astype("float64")

    df = df.drop_duplicates(subset=["ticker", "date"], keep="last")
    return df[PRICE_COLUMNS].reset_index(drop=True)
