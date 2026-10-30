# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02_ingest_prices
# MAGIC Yahoo Finance daily prices → bronze_prices
# MAGIC
# MAGIC - tickery = aktivní řádky z posledního snapshotu `bronze_watchlist`
# MAGIC - + FX páry pro měny ≠ USD (např. `EURUSD=X`) kvůli převodu v silveru
# MAGIC - nový ticker → celá historie, existující → od posledního data − 5 dní
# MAGIC - zápis přes `MERGE` na (ticker, date) → idempotentní, bez duplicit

# COMMAND ----------

# MAGIC %pip install yfinance --quiet

# COMMAND ----------

import os
import sys
import time

import pandas as pd
import yfinance as yf
from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.append(os.path.abspath(".."))          # zpřístupní src/lib
from lib.config import BRONZE_PRICES, BRONZE_WATCHLIST
from lib.ingest import fetch_start, fx_ticker, is_truthy, normalize_history

SLEEP_S = 0.3                                    # šetrnost vůči Yahoo rate-limitu

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tickery z posledního snapshotu watchlistu

# COMMAND ----------

wl = spark.table(BRONZE_WATCHLIST)
latest = wl.agg(F.max("_ingested_at")).first()[0]
if latest is None:
    raise ValueError("bronze_watchlist je prázdná – spusť nejdřív 01_ingest_watchlist")

rows = wl.filter(F.col("_ingested_at") == latest).select("ticker", "is_active").collect()
tickers = sorted({r["ticker"].strip().upper() for r in rows if is_truthy(r["is_active"])})
if not tickers:
    raise ValueError("Ve watchlistu není žádný aktivní ticker")

print(f"Snapshot {latest}: {len(rows)} řádků, aktivních {len(tickers)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inkrementální stav

# COMMAND ----------

last_dates = {
    r["ticker"]: r["last_date"]
    for r in spark.table(BRONZE_PRICES)
    .groupBy("ticker").agg(F.max("date").alias("last_date")).collect()
}


def fetch(ticker: str) -> pd.DataFrame:
    start = fetch_start(last_dates.get(ticker))
    kwargs = {"period": "max"} if start is None else {"start": start.isoformat()}
    t = yf.Ticker(ticker)
    raw = t.history(interval="1d", auto_adjust=False, actions=False, **kwargs)
    try:
        currency = t.fast_info["currency"]
    except Exception:
        currency = None
    return normalize_history(raw, ticker, currency)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Stažení cen + FX

# COMMAND ----------

frames, failed = [], []


def run(batch):
    for tk in batch:
        try:
            df = fetch(tk)
        except Exception as e:                   # jeden vadný ticker neshodí celý běh
            print(f"  ✗ {tk}: {e}")
            failed.append(tk)
            continue
        if df.empty:
            print(f"  – {tk}: žádná nová data")
            if tk not in last_dates:             # nový ticker bez dat = asi špatný symbol
                failed.append(tk)
        else:
            mode = "full" if tk not in last_dates else "incr"
            print(f"  ✓ {tk}: {len(df)} řádků ({mode}, {df['currency'].iloc[0]})")
            frames.append(df)
        time.sleep(SLEEP_S)


run(tickers)

currencies = {c for f in frames for c in f["currency"].dropna().unique()} - {"USD"}
fx = sorted(fx_ticker(c) for c in currencies)
print(f"FX páry: {fx}")
run(fx)

if not frames:
    raise RuntimeError(f"Nestáhlo se nic. Selhaly: {failed}")
if failed:
    print(f"⚠ Selhaly tickery: {failed}")    # řeší 30_dq_checks

pdf = pd.concat(frames, ignore_index=True)
print(f"Celkem {len(pdf)} řádků pro {pdf['ticker'].nunique()} tickerů")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE do bronze_prices

# COMMAND ----------

schema = T.StructType([
    T.StructField("ticker", T.StringType()),
    T.StructField("date", T.DateType()),
    T.StructField("open", T.DoubleType()),
    T.StructField("high", T.DoubleType()),
    T.StructField("low", T.DoubleType()),
    T.StructField("close", T.DoubleType()),
    T.StructField("adj_close", T.DoubleType()),
    T.StructField("volume", T.LongType()),
    T.StructField("currency", T.StringType()),
])

src = (spark.createDataFrame(pdf, schema=schema)
       .withColumn("_source", F.lit("yahoo_finance"))
       .withColumn("_ingested_at", F.current_timestamp()))
src.createOrReplaceTempView("src_prices")

result = spark.sql(f"""
    MERGE INTO {BRONZE_PRICES} t
    USING src_prices s
    ON t.ticker = s.ticker AND t.date = s.date
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
display(result)                                  # num_inserted / num_updated

# COMMAND ----------

display(spark.sql(f"""
    SELECT ticker, currency, COUNT(*) AS rows, MIN(date) AS first_date, MAX(date) AS last_date
    FROM {BRONZE_PRICES}
    GROUP BY ticker, currency
    ORDER BY ticker
"""))
