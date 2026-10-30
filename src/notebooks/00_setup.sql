-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00_setup
-- MAGIC Creates schema and tables (DDL). Idempotent – safe to re-run.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.market_watch;

CREATE TABLE IF NOT EXISTS workspace.market_watch.app_config (
  key STRING, value STRING, updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS workspace.market_watch.bronze_watchlist (
  ticker STRING, asset_type STRING, is_active STRING,
  alert_1d_pct STRING, alert_from_high_pct STRING, note STRING,
  _source STRING, _ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS workspace.market_watch.bronze_prices (
  ticker STRING, date DATE,
  open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, adj_close DOUBLE,
  volume BIGINT, currency STRING,
  _source STRING, _ingested_at TIMESTAMP
);
