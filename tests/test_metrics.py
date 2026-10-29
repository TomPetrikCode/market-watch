import pandas as pd
import pytest

from lib.metrics import daily_return_pct, drawdown_from_high_pct, is_drop_alert


def test_daily_return_pct():
    result = daily_return_pct(pd.Series([100.0, 110.0, 99.0]))
    assert pd.isna(result.iloc[0])
    assert result.iloc[1] == pytest.approx(10.0)
    assert result.iloc[2] == pytest.approx(-10.0)


def test_drawdown_from_high_pct():
    result = drawdown_from_high_pct(pd.Series([100.0, 120.0, 90.0, 130.0]))
    assert list(result.round(2)) == [0.0, 0.0, -25.0, 0.0]


@pytest.mark.parametrize(
    "change, threshold, expected",
    [(-12.0, -10.0, True), (-10.0, -10.0, True), (-5.0, -10.0, False),
     (None, -10.0, False), (-12.0, None, False)],
)
def test_is_drop_alert(change, threshold, expected):
    assert is_drop_alert(change, threshold) is expected
