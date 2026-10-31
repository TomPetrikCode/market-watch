# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 10_silver
# MAGIC bronze → silver: typovaná, vyčištěná data v USD
# MAGIC
# MAGIC 1. `silver_watchlist` – poslední snapshot watchlistu s typy
# MAGIC 2. `silver_fx` – denní kurzy měna → USD
# MAGIC 3. `silver_prices` – ceny akcií/krypta, přepočtené do USD
# MAGIC
# MAGIC Silver se pokaždé přepočítá celý (overwrite) – dat je málo, je to jednodušší než MERGE.

# COMMAND ----------

import os
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

sys.path.append(os.path.abspath(".."))
from lib.config import BRONZE_PRICES, BRONZE_WATCHLIST, SILVER_FX, SILVER_PRICES, SILVER_WATCHLIST

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) silver_watchlist

# COMMAND ----------

wl = spark.table(BRONZE_WATCHLIST)
latest = wl.agg(F.max("_ingested_at")).first()[0]
if latest is None:
    raise ValueError("bronze_watchlist je prázdná – spusť nejdřív 01_ingest_watchlist")

wl_latest = wl.filter(F.col("_ingested_at") == latest)

silver_wl = wl_latest.select(
    F.upper(F.trim("ticker")).alias("ticker"),
    F.lower(F.trim("asset_type")).alias("asset_type"),
    F.lower(F.trim("is_active")).isin("true", "1", "yes", "y", "ano", "x").alias("is_active"),
    F.expr("try_cast(trim(alert_1d_pct) AS double)").alias("alert_1d_pct"),
    F.expr("try_cast(trim(alert_from_high_pct) AS double)").alias("alert_from_high_pct"),
    F.trim("note").alias("note"),
)

silver_wl.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_WATCHLIST)
display(silver_wl)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) silver_fx

# COMMAND ----------

prices = spark.table(BRONZE_PRICES)

silver_fx = (
    prices.filter(F.col("ticker").endswith("USD=X"))
    .select(
        F.substring("ticker", 1, 3).alias("currency"),   # EURUSD=X → EUR
        F.col("date"),
        F.col("close").alias("usd_rate"),                # 1 EUR = x USD
    )
)

silver_fx.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_FX)
display(silver_fx.groupBy("currency").agg(
    F.count("*").alias("rows"), F.min("date").alias("first"), F.max("date").alias("last")
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) silver_prices

# COMMAND ----------

# 3a) jen akcie/ETF/krypto (bez FX párů)
#     krypto (podle asset_type z watchlistu) se obchoduje 24/7 → dnešní svíčka je nedokončená
crypto_tickers = [
    r["ticker"] for r in silver_wl.filter(F.col("asset_type") == "crypto").select("ticker").collect()
]
is_crypto = F.col("ticker").isin(crypto_tickers)

assets = (
    prices.filter(~F.col("ticker").endswith("USD=X"))
    .filter(~(is_crypto & (F.col("date") >= F.current_date())))
)

# 3b) připojení kurzu podle měny a dne
joined = assets.join(silver_fx, on=["currency", "date"], how="left")

# 3c) forward fill: chybí-li kurz (svátek), vezmi poslední známý
w = Window.partitionBy("ticker").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)

silver_prices = (
    joined
    .withColumn("usd_rate", F.last("usd_rate", ignorenulls=True).over(w))
    # 3d) USD tickery mají kurz 1
    .withColumn(
        "usd_rate",
        F.when(F.col("currency") == "USD", F.lit(1.0)).otherwise(F.col("usd_rate")),
    )
    # 3e) přepočet do USD
    .withColumn("close_usd", F.col("close") * F.col("usd_rate"))
    .withColumn("adj_close_usd", F.col("adj_close") * F.col("usd_rate"))
    .select(
        "ticker", "date", "currency", "close", "adj_close", "volume",
        "usd_rate", "close_usd", "adj_close_usd",
    )
)

silver_prices.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_PRICES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kontroly

# COMMAND ----------

sp = spark.table(SILVER_PRICES)

dupes = sp.groupBy("ticker", "date").count().filter("count > 1").count()
missing_fx = sp.filter(F.col("usd_rate").isNull()).count()

print(f"Duplicity: {dupes}")
print(f"Řádky bez kurzu: {missing_fx}")
assert dupes == 0, "Duplicitní ticker+date v silver_prices"

display(sp.groupBy("ticker", "currency").agg(
    F.count("*").alias("rows"),
    F.min("date").alias("first"),
    F.max("date").alias("last"),
    F.round(F.avg("usd_rate"), 4).alias("avg_usd_rate"),
).orderBy("ticker"))