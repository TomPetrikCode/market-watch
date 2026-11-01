# Databricks notebook source
# MAGIC %md
# MAGIC # 20_gold_metrics
# MAGIC silver → gold: metriky pro dashboard a alerty. Popis všech metrik: `docs/metrics.md`
# MAGIC
# MAGIC 1. `gold_metrics` – denně pro každý aktivní ticker: výnosy, drawdown, trend, volatilita,
# MAGIC    objem, výkon vs SPY
# MAGIC 2. `gold_correlations` – klouzavá 90denní korelace denních výnosů pro páry z configu
# MAGIC
# MAGIC Cenové metriky se počítají v **lokální měně** (chování samotného aktiva, bez vlivu kurzu),
# MAGIC srovnání se SPY v **USD** (aby bylo srovnatelné).

# COMMAND ----------

import os
import sys
from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F

sys.path.append(os.path.abspath(".."))
from lib.config import (
    BENCHMARK,
    CORRELATION_PAIRS,
    GOLD_CORRELATIONS,
    GOLD_METRICS,
    SILVER_PRICES,
    SILVER_WATCHLIST,
    VOL_SPIKE_MULTIPLIER,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Vstup: jen aktivní tickery z watchlistu

# COMMAND ----------

watchlist = spark.table(SILVER_WATCHLIST).filter("is_active").select("ticker", "asset_type")

prices = (
    spark.table(SILVER_PRICES)
    .join(watchlist, on="ticker", how="inner")
    .withColumn("day", F.unix_date("date"))       # datum jako číslo dne → okna podle kalendáře
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Okna
# MAGIC - `rows`  = počet **obchodních dní** (řádků) – klouzavé průměry, volatilita, objem
# MAGIC - `range` = počet **kalendářních dní** – týdenní/měsíční výnos, 52W maximum
# MAGIC   (akcie i krypto pak měří stejné období, i když krypto obchoduje i o víkendu)

# COMMAND ----------

w = Window.partitionBy("ticker").orderBy("date")
w_day = Window.partitionBy("ticker").orderBy("day")


def rows(n: int):
    """Posledních n obchodních dní včetně dneška."""
    return w.rowsBetween(-(n - 1), Window.currentRow)


def full_avg(col: str, n: int, window):
    """Průměr přes okno – jen když je k dispozici celých n hodnot, jinak NULL."""
    return F.when(F.count(col).over(window) == n, F.avg(col).over(window))


def price_days_ago(col: str, days: int):
    """Poslední známá cena k datu před `days` kalendářními dny (víkend → pátek)."""
    return F.last(col, ignorenulls=True).over(w_day.rangeBetween(Window.unboundedPreceding, -days))


def pct_change(new, old):
    """(new / old − 1) × 100; NULL, když stará hodnota chybí nebo je 0."""
    return (F.try_divide(new, old) - 1) * 100


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) gold_metrics

# COMMAND ----------

# --- pohyb ceny ----------------------------------------------------------------------------
m = (
    prices
    .withColumn("ret_1d_pct", pct_change(F.col("adj_close"), F.lag("adj_close").over(w)))
    .withColumn("ret_1w_pct", pct_change(F.col("adj_close"), price_days_ago("adj_close", 7)))
    .withColumn("ret_1m_pct", pct_change(F.col("adj_close"), price_days_ago("adj_close", 30)))
)

# --- pokles od maxima ----------------------------------------------------------------------
m = (
    m
    .withColumn("high_52w", F.max("adj_close").over(w_day.rangeBetween(-364, Window.currentRow)))
    .withColumn("high_ath", F.max("adj_close").over(w.rowsBetween(Window.unboundedPreceding, 0)))
    .withColumn("drawdown_52w_pct", pct_change(F.col("adj_close"), F.col("high_52w")))
    .withColumn("drawdown_ath_pct", pct_change(F.col("adj_close"), F.col("high_ath")))
)

# --- trend ---------------------------------------------------------------------------------
m = (
    m
    .withColumn("ma50", full_avg("adj_close", 50, rows(50)))
    .withColumn("ma200", full_avg("adj_close", 200, rows(200)))
    .withColumn("pct_vs_ma50", pct_change(F.col("adj_close"), F.col("ma50")))
    .withColumn("pct_vs_ma200", pct_change(F.col("adj_close"), F.col("ma200")))
    .withColumn("above_ma200", F.col("adj_close") > F.col("ma200"))
)

# --- volatilita ----------------------------------------------------------------------------
# denní směrodatná odchylka výnosů × √(obchodních dní v roce) → roční volatilita v %
trading_days = F.when(F.col("asset_type") == "crypto", F.lit(365)).otherwise(F.lit(252))

m = (
    m
    .withColumn(
        "vol_20d_pct",
        F.when(
            F.count("ret_1d_pct").over(rows(20)) == 20,
            F.stddev("ret_1d_pct").over(rows(20)) * F.sqrt(trading_days),
        ),
    )
    .withColumn("vol_baseline_pct", full_avg("vol_20d_pct", 252, rows(252)))  # normál za rok
    .withColumn(
        "is_volatile",
        F.col("vol_20d_pct") > F.col("vol_baseline_pct") * F.lit(VOL_SPIKE_MULTIPLIER),
    )
)

# --- objem ---------------------------------------------------------------------------------
avg_volume_20d = full_avg("volume", 20, w.rowsBetween(-20, -1))   # 20 předchozích dní, bez dneška
m = m.withColumn("volume_ratio", F.try_divide(F.col("volume"), avg_volume_20d))

# --- výkon vs SPY za 3 měsíce (v USD) -----------------------------------------------------
m = m.withColumn("ret_3m_usd_pct", pct_change(F.col("adj_close_usd"), price_days_ago("adj_close_usd", 91)))

spy_3m = (
    m.filter(F.col("ticker") == BENCHMARK)
    .select("date", F.col("ret_3m_usd_pct").alias("spy_ret_3m_pct"))
)

m = (
    m.join(spy_3m, on="date", how="left")
    # krypto o víkendu: SPY neobchoduje → vezmi poslední známou hodnotu
    .withColumn("spy_ret_3m_pct", F.last("spy_ret_3m_pct", ignorenulls=True).over(
        w.rowsBetween(Window.unboundedPreceding, 0)))
    .withColumn("rel_3m_vs_spy_pct", F.col("ret_3m_usd_pct") - F.col("spy_ret_3m_pct"))
)

gold_metrics = m.select(
    "ticker", "date", "asset_type", "currency", "adj_close", "adj_close_usd", "volume",
    "ret_1d_pct", "ret_1w_pct", "ret_1m_pct",
    "drawdown_52w_pct", "drawdown_ath_pct",
    "ma50", "ma200", "pct_vs_ma50", "pct_vs_ma200", "above_ma200",
    "vol_20d_pct", "vol_baseline_pct", "is_volatile",
    "volume_ratio",
    "rel_3m_vs_spy_pct",
)

gold_metrics.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_METRICS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) gold_correlations

# COMMAND ----------

returns = spark.table(GOLD_METRICS).select("ticker", "date", "ret_1d_pct")


def pair_returns(a: str, b: str):
    """Denní výnosy dvou tickerů vedle sebe – jen dny, kdy obchodovaly oba."""
    ret_a = returns.filter(F.col("ticker") == a).select("date", F.col("ret_1d_pct").alias("ret_a"))
    ret_b = returns.filter(F.col("ticker") == b).select("date", F.col("ret_1d_pct").alias("ret_b"))
    return (
        ret_a.join(ret_b, on="date", how="inner")
        .withColumn("ticker_a", F.lit(a))
        .withColumn("ticker_b", F.lit(b))
    )


pairs = reduce(lambda x, y: x.unionByName(y), [pair_returns(a, b) for a, b in CORRELATION_PAIRS])

wc = (
    Window.partitionBy("ticker_a", "ticker_b").orderBy("date")
    .rowsBetween(-89, Window.currentRow)
)

gold_correlations = (
    pairs
    .withColumn(
        "corr_90d",
        F.when(F.count("ret_a").over(wc) == 90, F.corr("ret_a", "ret_b").over(wc)),
    )
    .select("ticker_a", "ticker_b", "date", "corr_90d")
)

gold_correlations.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    GOLD_CORRELATIONS
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kontroly

# COMMAND ----------

gm = spark.table(GOLD_METRICS)
dupes = gm.groupBy("ticker", "date").count().filter("count > 1").count()
print(f"Duplicity: {dupes}")
assert dupes == 0, "Duplicitní ticker+date v gold_metrics"

# poslední den pro každý ticker – tohle uvidí dashboard
last_day = gm.groupBy("ticker").agg(F.max("date").alias("date"))
display(
    gm.join(last_day, on=["ticker", "date"])
    .select(
        "ticker", "date",
        *[F.round(c, 2).alias(c) for c in [
            "ret_1d_pct", "ret_1w_pct", "ret_1m_pct", "drawdown_52w_pct", "drawdown_ath_pct",
            "pct_vs_ma50", "pct_vs_ma200", "vol_20d_pct", "vol_baseline_pct", "volume_ratio",
            "rel_3m_vs_spy_pct",
        ]],
        "above_ma200", "is_volatile",
    )
    .orderBy("drawdown_52w_pct")
)

# COMMAND ----------

gc = spark.table(GOLD_CORRELATIONS)
last_corr = gc.groupBy("ticker_a", "ticker_b").agg(F.max("date").alias("date"))
display(gc.join(last_corr, on=["ticker_a", "ticker_b", "date"]))
