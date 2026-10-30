from datetime import date

import pandas as pd
import pytest

from lib.ingest import PRICE_COLUMNS, fetch_start, fx_ticker, is_truthy, normalize_history


@pytest.mark.parametrize(
    "value, expected",
    [("TRUE", True), ("true", True), ("1", True), (" ano ", True), ("x", True),
     ("FALSE", False), ("0", False), ("", False), (None, False)],
)
def test_is_truthy(value, expected):
    assert is_truthy(value) is expected


def test_fetch_start():
    assert fetch_start(None) is None
    assert fetch_start(date(2026, 10, 29)) == date(2026, 10, 24)
    assert fetch_start(date(2026, 10, 29), overlap_days=0) == date(2026, 10, 29)


def test_fx_ticker():
    assert fx_ticker("eur") == "EURUSD=X"


def _raw(index):
    return pd.DataFrame(
        {
            "Open": [1.0, 2.0, 3.0],
            "High": [1.5, 2.5, 3.5],
            "Low": [0.5, 1.5, None],
            "Close": [1.2, None, 3.2],
            "Adj Close": [1.1, None, 3.1],
            "Volume": [100, 200, None],
        },
        index=index,
    )


def test_normalize_history():
    idx = pd.DatetimeIndex(
        ["2026-10-27 00:00", "2026-10-28 00:00", "2026-10-29 00:00"], tz="America/New_York"
    )
    out = normalize_history(_raw(idx), "AAPL", "USD")

    assert list(out.columns) == PRICE_COLUMNS
    assert len(out) == 2                       # row with null close dropped
    assert out["date"].tolist() == [date(2026, 10, 27), date(2026, 10, 29)]
    assert out["volume"].tolist() == [100, 0]
    assert (out["ticker"] == "AAPL").all()
    assert out["volume"].dtype == "int64"


def test_normalize_history_without_adj_close():
    idx = pd.DatetimeIndex(["2026-10-27", "2026-10-28", "2026-10-29"])
    raw = _raw(idx).drop(columns=["Adj Close"])
    out = normalize_history(raw, "EURUSD=X", "USD")
    assert out["adj_close"].tolist() == out["close"].tolist()


def test_normalize_history_empty():
    out = normalize_history(pd.DataFrame(), "XXX", None)
    assert out.empty
    assert list(out.columns) == PRICE_COLUMNS
