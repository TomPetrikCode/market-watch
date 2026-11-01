# Databricks notebook source
# MAGIC %md
# MAGIC # 20_gold_metrics
# MAGIC silver → gold: metriky pro dashboard a alerty
# MAGIC
# MAGIC 1. `gold_metrics` – denně pro každý aktivní ticker: výnos, drawdown, MA50/200, relativní síla vs SPY
# MAGIC 2. `gold_correlations` – klouzavá 90denní korelace denních výnosů pro páry z configu
# MAGIC
# MAGIC Cenové metriky se počítají v **lokální měně** (chování samotného aktiva, bez vlivu kurzu),
# MAGIC relativní síla vs SPY v **USD** (aby byla srovnatelná).

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
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Vstup: jen aktivní tickery z watchlistu

# COMMAND ----------

watchlist = spark.table(SILVER_WATCHLIST).filter("is_active").select("ticker", "asset_type")
prices = spark.table(SILVER_PRICES).join(watchlist, on="ticker", how="inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) gold_metrics

# COMMAND ----------

w = Window.partitionBy("ticker").orderBy("date")                       # pořadí dní v tickeru
w_all = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)  # od začátku po dnešek
w50 = w.rowsBetween(-49, Window.currentRow)                          # posledních 50 řádků
w200 = w.rowsBetween(-199, Window.currentRow)                        # posledních 200 řádků


def moving_avg(col: str, window, n: int):
    """Klouzavý průměr – jen když je k dispozici celých n dní, jinak NULL."""
    return F.when(F.count(col).over(window) == n, F.avg(col).over(window))


# benchmark (SPY) v USD po dnech; krypto má i víkendy → doplníme poslední známou hodnotu
spy = (
    spark.table(SILVER_PRICES)
    .filter(F.col("ticker") == BENCHMARK)
    .select("date", F.col("adj_close_usd").alias("spy_usd"))
)

gold_metrics = (
    prices
    # denní výnos v %
    .withColumn("ret_1d_pct", (F.col("adj_close") / F.lag("adj_close").over(w) - 1) * 100)
    # vzdálenost od dosavadního maxima v % (0 nebo záporné)
    .withColumn("drawdown_pct", (F.col("adj_close") / F.max("adj_close").over(w_all) - 1) * 100)
    # klouzavé průměry a trend
    .withColumn("ma50", moving_avg("adj_close", w50, 50))
    .withColumn("ma200", moving_avg("adj_close", w200, 200))
    .withColumn("above_ma200", F.col("adj_close") > F.col("ma200"))
    # relativní síla: cena v USD / SPY v USD (roste = ticker je lepší než trh)
    .join(spy, on="date", how="left")
    .withColumn("spy_usd", F.last("spy_usd", ignorenulls=True).over(w_all))
    .withColumn("rs_vs_spy", F.col("adj_close_usd") / F.col("spy_usd"))
    .select(
        "ticker", "date", "asset_type", "currency",
        "adj_close", "adj_close_usd",
        "ret_1d_pct", "drawdown_pct", "ma50", "ma200", "above_ma200", "rs_vs_spy",
    )
)

gold_metrics.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_METRICS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) gold_correlations

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
        F.round("ret_1d_pct", 2).alias("ret_1d_pct"),
        F.round("drawdown_pct", 2).alias("drawdown_pct"),
        "above_ma200",
        F.round("rs_vs_spy", 4).alias("rs_vs_spy"),
    )
    .orderBy("drawdown_pct")
)

# COMMAND ----------

gc = spark.table(GOLD_CORRELATIONS)
last_corr = gc.groupBy("ticker_a", "ticker_b").agg(F.max("date").alias("date"))
display(gc.join(last_corr, on=["ticker_a", "ticker_b", "date"]))
