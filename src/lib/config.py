"""Central configuration: catalog, schema and table names."""

CATALOG = "workspace"
SCHEMA = "market_watch"


def table(name: str) -> str:
    """Return fully qualified table name, e.g. table('bronze_prices')."""
    return f"{CATALOG}.{SCHEMA}.{name}"


BRONZE_WATCHLIST = table("bronze_watchlist")
BRONZE_PRICES = table("bronze_prices")
BRONZE_CRYPTO = table("bronze_crypto")
BRONZE_MACRO = table("bronze_macro")
SILVER_PRICES = table("silver_prices")
GOLD_METRICS = table("gold_metrics")
GOLD_ALERTS = table("gold_alerts")
