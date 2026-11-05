# Databricks notebook source
# MAGIC %md
# MAGIC # 40_alerts
# MAGIC gold_metrics + prahy z watchlistu → `gold_alerts`. E-mail posílá Databricks SQL Alert nad touto tabulkou.
# MAGIC
# MAGIC | alert_type | Kdy vznikne |
# MAGIC |---|---|
# MAGIC | `drop_1d` | denní pokles ≤ `alert_1d_pct` – pokaždé (každý den je samostatná událost) |
# MAGIC | `drawdown_52w` | pokles od 52W maxima **překročí** `alert_from_high_pct` (včera nad prahem, dnes pod) |
# MAGIC | `volatility_spike` | `is_volatile` se **nově** zapne (včera false, dnes true) |
# MAGIC
# MAGIC Vyhodnocuje se posledních `ALERT_LOOKBACK_DAYS` dní a zapisuje přes `MERGE` na (ticker, date, alert_type):
# MAGIC když job jeden den neproběhne, alert se neztratí, a opakovaný běh nevytvoří duplicitu.

# COMMAND ----------

import os
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

sys.path.append(os.path.abspath(".."))
from lib.config import ALERT_LOOKBACK_DAYS, GOLD_ALERTS, GOLD_METRICS, SILVER_WATCHLIST

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Metriky + prahy + hodnota z předchozího obchodního dne

# COMMAND ----------

thresholds = (
    spark.table(SILVER_WATCHLIST)
    .filter("is_active")
    .select("ticker", "alert_1d_pct", "alert_from_high_pct")
)

w = Window.partitionBy("ticker").orderBy("date")

m = (
    spark.table(GOLD_METRICS)
    .join(thresholds, on="ticker", how="inner")
    # předchozí obchodní den – kvůli "překročení" prahu
    .withColumn("prev_drawdown_52w_pct", F.lag("drawdown_52w_pct").over(w))
    .withColumn("prev_is_volatile", F.lag("is_volatile").over(w))
    .filter(F.col("date") >= F.date_sub(F.current_date(), ALERT_LOOKBACK_DAYS))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Pravidla

# COMMAND ----------

drop_1d = (
    m.filter(F.col("ret_1d_pct") <= F.col("alert_1d_pct"))
    .select(
        "ticker", "date",
        F.lit("drop_1d").alias("alert_type"),
        F.col("ret_1d_pct").alias("value_pct"),
        F.col("alert_1d_pct").alias("threshold_pct"),
        F.format_string("%s: %.1f %% za den (práh %.1f %%)",
                        "ticker", "ret_1d_pct", "alert_1d_pct").alias("message"),
    )
)

# překročení: dnes pod prahem A (včera nad prahem NEBO včera bez hodnoty = začátek historie)
drawdown_52w = (
    m.filter(
        (F.col("drawdown_52w_pct") <= F.col("alert_from_high_pct"))
        & (F.col("prev_drawdown_52w_pct").isNull()
           | (F.col("prev_drawdown_52w_pct") > F.col("alert_from_high_pct")))
    )
    .select(
        "ticker", "date",
        F.lit("drawdown_52w").alias("alert_type"),
        F.col("drawdown_52w_pct").alias("value_pct"),
        F.col("alert_from_high_pct").alias("threshold_pct"),
        F.format_string("%s: %.1f %% od 52W maxima (práh %.1f %%)",
                        "ticker", "drawdown_52w_pct", "alert_from_high_pct").alias("message"),
    )
)

volatility_spike = (
    m.filter(F.col("is_volatile") & ~F.coalesce(F.col("prev_is_volatile"), F.lit(False)))
    .select(
        "ticker", "date",
        F.lit("volatility_spike").alias("alert_type"),
        F.col("vol_20d_pct").alias("value_pct"),
        F.col("vol_baseline_pct").alias("threshold_pct"),
        F.format_string("%s: volatilita %.1f %% vs normál %.1f %%",
                        "ticker", "vol_20d_pct", "vol_baseline_pct").alias("message"),
    )
)

alerts = drop_1d.unionByName(drawdown_52w).unionByName(volatility_spike)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) MERGE do gold_alerts – jen nové alerty, existující se nemění

# COMMAND ----------

alerts.withColumn("detected_at", F.current_timestamp()).createOrReplaceTempView("new_alerts")

result = spark.sql(f"""
    MERGE INTO {GOLD_ALERTS} t
    USING new_alerts s
    ON t.ticker = s.ticker AND t.date = s.date AND t.alert_type = s.alert_type
    WHEN NOT MATCHED THEN INSERT *
""")
display(result)                                  # num_inserted_rows = nové alerty v tomto běhu

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nové alerty dnes
# MAGIC Stejný dotaz použije SQL Alert pro e-mail.

# COMMAND ----------

display(spark.sql(f"""
    SELECT date, ticker, alert_type, ROUND(value_pct, 1) AS value_pct, message
    FROM {GOLD_ALERTS}
    WHERE detected_at >= current_date()
    ORDER BY alert_type, value_pct
"""))