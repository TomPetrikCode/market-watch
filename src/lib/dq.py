"""Pure helpers for data quality results – no Spark dependency, unit-tested in CI."""

ERROR = "ERROR"
WARN = "WARN"


def make_result(name: str, severity: str, failed: list, sample_size: int = 10) -> dict:
    """One check result. `failed` = identifiers of failing rows, e.g. ['MSFT', 'AAPL']."""
    if severity not in (ERROR, WARN):
        raise ValueError(f"Unknown severity: {severity}")
    failed = sorted({str(f) for f in failed})
    return {
        "check_name": name,
        "severity": severity,
        "status": "PASS" if not failed else "FAIL",
        "failed_count": len(failed),
        "sample": ", ".join(failed[:sample_size]) or None,
    }


def blocking_failures(results: list[dict]) -> list[dict]:
    """Failed ERROR checks – these must stop the pipeline before alerts."""
    return [r for r in results if r["severity"] == ERROR and r["status"] == "FAIL"]


def max_stale_days(asset_type: str | None, limits: dict) -> int:
    """Allowed age of the last price in days for an asset type."""
    return limits.get(asset_type or "", limits["default"])
