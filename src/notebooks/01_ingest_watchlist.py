# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01_ingest_watchlist
# MAGIC Google Sheets watchlist → bronze

# COMMAND ----------

import io, os, sys
import pandas as pd
import requests
from pyspark.sql import functions as F

sys.path.append(os.path.abspath(".."))          # zpřístupní src/lib
from lib.config import table, BRONZE_WATCHLIST

EXPECTED = ["ticker", "asset_type", "is_active", "alert_1d_pct", "alert_from_high_pct", "note"]

row = spark.table(table("app_config")).filter("key = 'watchlist_csv_url'").first()
if row is None:
    raise ValueError("Chybí watchlist_csv_url v app_config")

resp = requests.get(row["value"], timeout=30)
resp.raise_for_status()

pdf = pd.read_csv(io.StringIO(resp.content.decode("utf-8")), dtype=str, keep_default_na=False)
pdf.columns = [c.strip().lower() for c in pdf.columns]

missing = set(EXPECTED) - set(pdf.columns)
if missing:
    raise ValueError(f"Ve sheetu chybí sloupce: {missing}")

pdf = pdf[EXPECTED]
pdf = pdf[pdf["ticker"].str.strip() != ""]
if pdf.empty:
    raise ValueError("Watchlist je prázdný")

print(f"Načteno {len(pdf)} tickerů")

# COMMAND ----------

sdf = (spark.createDataFrame(pdf)
       .withColumn("_source", F.lit("google_sheets"))
       .withColumn("_ingested_at", F.current_timestamp()))

sdf.write.mode("append").saveAsTable(BRONZE_WATCHLIST)
display(sdf)