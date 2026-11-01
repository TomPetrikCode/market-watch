# Databricks notebook source
# MAGIC %md
# MAGIC # 30_dq_checks
# MAGIC Kontroly kvality dat před alerty. Popis kontrol: `docs/data_quality.md`
# MAGIC
# MAGIC - **ERROR** = data jsou špatně → notebook spadne → denní job se zastaví a alerty se nepošlou
# MAGIC - **WARN**  = podezřelé → jen se zapíše, job pokračuje
# MAGIC
# MAGIC Výsledky každého běhu se přidají do `dq_results` (historie kontrol).

# COMMAND ----------

import os
import sys

from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.append(os.path.abspath(".."))
from lib.config import (
    ALLOWED_ASSET_TYPES,
    BRONZE_PRICES,
    DQ_MAX_DAILY_MOVE_PCT,
    DQ_MAX_STALE_DAYS,
    DQ_RECENT_DAYS,
    DQ_RESULTS,
    GOLD_METRICS,
    SILVER_FX,
    SILVER_PRICES,
    SILVER_WATCHLIST,
)
from lib.dq import ERROR, WARN, blocking_failures, make_result

watchlist = spark.table(SILVER_WATCHLIST)
active = watchlist.filter("is_active").select("ticker", "asset_type")
silver = spark.table(SILVER_PRICES)
gold = spark.table(GOLD_METRICS)

results = []


def check(name: str, severity: str, bad_df, key_expr):
    """Spustí kontrolu: bad_df = problémové řádky, key_expr = co vypsat jako vzorek."""
    keys = [r[0] for r in bad_df.select(key_expr).distinct().collect()]
    result = make_result(name, severity, keys)
    results.append(result)
    icon = "✓" if result["status"] == "PASS" else ("✗" if severity == ERROR else "⚠")
    print(f"{icon} {severity:5} {name}: {result['failed_count']} {result['sample'] or ''}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Čerstvost – poslední data nejsou starší než limit

# COMMAND ----------

stale_limit = F.when(
    F.col("asset_type") == "crypto", F.lit(DQ_MAX_STALE_DAYS["crypto"])
).otherwise(F.lit(DQ_MAX_STALE_DAYS["default"]))

freshness = (
    gold.join(active, on=["ticker", "asset_type"], how="inner")
    .groupBy("ticker", "asset_type").agg(F.max("date").alias("last_date"))
    .withColumn("age_days", F.datediff(F.current_date(), F.col("last_date")))
)
check(
    "freshness", ERROR,
    freshness.filter(F.col("age_days") > stale_limit),
    F.concat_ws(" ", "ticker", F.concat(F.lit("("), F.col("age_days"), F.lit("d)"))),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Úplnost – každý aktivní ticker má data v silver i gold

# COMMAND ----------

for layer, df in [("silver", silver), ("gold", gold)]:
    check(
        f"completeness_{layer}", ERROR,
        active.join(df.select("ticker").distinct(), on="ticker", how="left_anti"),
        F.col("ticker"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Unikátnost – ticker + date jen jednou

# COMMAND ----------

for layer, df in [("silver", silver), ("gold", gold)]:
    check(
        f"unique_{layer}", ERROR,
        df.groupBy("ticker", "date").count().filter("count > 1"),
        F.concat_ws(" ", "ticker", F.col("date").cast("string")),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Kurzy – každá cena má kurz do USD

# COMMAND ----------

check(
    "fx_rate_missing", ERROR,
    silver.filter(F.col("usd_rate").isNull() | F.isnan("usd_rate")),
    F.col("ticker"),
)

fx_currencies = spark.table(SILVER_FX).select("currency").distinct()
check(
    "fx_pair_missing", ERROR,
    silver.filter(F.col("currency") != "USD").select("currency").distinct()
    .join(fx_currencies, on="currency", how="left_anti"),
    F.col("currency"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Validní ceny (posledních N dní)
# MAGIC Jen nová data – stará historie z Yahoo občas obsahuje drobné nesrovnalosti, které už nespravíme
# MAGIC a nechceme kvůli nim zastavovat job každý den.

# COMMAND ----------

recent_bronze = (
    spark.table(BRONZE_PRICES)
    .join(active.select("ticker"), on="ticker", how="inner")
    .filter(F.col("date") >= F.date_sub(F.current_date(), DQ_RECENT_DAYS))
)
check(
    "price_validity", ERROR,
    recent_bronze.filter(
        (F.col("close") <= 0) | (F.col("high") < F.col("low")) | (F.col("volume") < 0)
    ),
    F.concat_ws(" ", "ticker", F.col("date").cast("string")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6) Podezřelý denní skok (WARN)

# COMMAND ----------

check(
    "suspicious_daily_move", WARN,
    gold.filter(
        (F.col("asset_type") != "crypto")
        & (F.col("date") >= F.date_sub(F.current_date(), DQ_RECENT_DAYS))
        & (F.abs("ret_1d_pct") > DQ_MAX_DAILY_MOVE_PCT)
    ),
    F.concat_ws(" ", "ticker", F.col("date").cast("string"),
                F.concat(F.round("ret_1d_pct", 1).cast("string"), F.lit("%"))),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7) Watchlist (WARN)

# COMMAND ----------

check(
    "watchlist_duplicate_ticker", WARN,
    watchlist.groupBy("ticker").count().filter("count > 1"),
    F.col("ticker"),
)
check(
    "watchlist_unknown_asset_type", WARN,
    watchlist.filter(~F.col("asset_type").isin(ALLOWED_ASSET_TYPES) | F.col("asset_type").isNull()),
    F.concat_ws(" ", "ticker", F.coalesce("asset_type", F.lit("NULL"))),
)
check(
    "watchlist_positive_threshold", WARN,
    watchlist.filter((F.col("alert_1d_pct") > 0) | (F.col("alert_from_high_pct") > 0)),
    F.col("ticker"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Uložení výsledků + zastavení při ERROR

# COMMAND ----------

schema = T.StructType([
    T.StructField("check_name", T.StringType()),
    T.StructField("severity", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("failed_count", T.LongType()),
    T.StructField("sample", T.StringType()),
])

run = (
    spark.createDataFrame(results, schema=schema)
    .withColumn("run_at", F.current_timestamp())
    .select("run_at", "check_name", "severity", "status", "failed_count", "sample")
)
run.write.mode("append").saveAsTable(DQ_RESULTS)
display(run)

blocking = blocking_failures(results)
if blocking:
    names = ", ".join(f"{r['check_name']} ({r['sample']})" for r in blocking)
    raise RuntimeError(f"DQ ERROR – pipeline zastavena, alerty se nepošlou: {names}")

print(f"DQ OK – {len(results)} kontrol, varování: "
      f"{sum(r['status'] == 'FAIL' for r in results)}")
