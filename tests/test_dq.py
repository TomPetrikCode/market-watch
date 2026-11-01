import pytest

from lib.dq import ERROR, WARN, blocking_failures, make_result, max_stale_days


def test_make_result_pass():
    r = make_result("freshness", ERROR, [])
    assert r["status"] == "PASS"
    assert r["failed_count"] == 0
    assert r["sample"] is None


def test_make_result_fail_dedup_and_sample():
    r = make_result("freshness", ERROR, ["MSFT", "AAPL", "MSFT", "X", "Y"], sample_size=2)
    assert r["status"] == "FAIL"
    assert r["failed_count"] == 4
    assert r["sample"] == "AAPL, MSFT"


def test_make_result_bad_severity():
    with pytest.raises(ValueError):
        make_result("x", "INFO", [])


def test_blocking_failures():
    results = [
        make_result("a", ERROR, ["MSFT"]),
        make_result("b", ERROR, []),
        make_result("c", WARN, ["AAPL"]),
    ]
    assert [r["check_name"] for r in blocking_failures(results)] == ["a"]


def test_max_stale_days():
    limits = {"crypto": 2, "default": 4}
    assert max_stale_days("crypto", limits) == 2
    assert max_stale_days("stock", limits) == 4
    assert max_stale_days(None, limits) == 4
