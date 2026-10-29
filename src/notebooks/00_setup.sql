-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00_setup
-- MAGIC Creates schema and tables (DDL). Idempotent – safe to re-run.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.market_watch;
