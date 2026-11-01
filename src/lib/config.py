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
SILVER_WATCHLIST = table("silver_watchlist")
SILVER_FX = table("silver_fx")
GOLD_METRICS = table("gold_metrics")
GOLD_CORRELATIONS = table("gold_correlations")
GOLD_ALERTS = table("gold_alerts")

# Benchmark for relative strength and pairs for rolling correlation
BENCHMARK = "SPY"
CORRELATION_PAIRS = [("BTC-USD", "ETH-USD"), ("ETH-USD", "BMNR")]

# is_volatile: 20d volatility above this multiple of its own 1y average
VOL_SPIKE_MULTIPLIER = 1.5


# Data quality (30_dq_checks)
DQ_RESULTS = table("dq_results")
DQ_MAX_STALE_DAYS = {"crypto": 2, "default": 4}    # weekend + holiday for stocks
DQ_MAX_DAILY_MOVE_PCT = 50                         # bigger 1d move on a non-crypto = suspicious
DQ_RECENT_DAYS = 30                                # price validity checked on recent data only
ALLOWED_ASSET_TYPES = ["stock", "etf", "crypto", "benchmark"]
