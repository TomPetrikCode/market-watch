# Market Watch

![CI](https://github.com/TomPetrikCode/market-watch/actions/workflows/ci.yml/badge.svg)

Personal market monitoring platform built on **Databricks**: daily ingestion of stocks, ETFs, crypto and macro data from multiple sources, medallion architecture (bronze → silver → gold), data quality checks, dashboards and price-drop alerts.

## Why
_TODO: what problem it solves, how I use it._

## Architecture
_TODO: docs/architecture.png_

| Layer  | Content |
|--------|---------|
| Bronze | Raw data: watchlist (Google Sheets), prices (Yahoo Finance), crypto (CoinGecko), macro (FRED) |
| Silver | Cleaned, deduplicated, unified schema |
| Gold   | Returns, drawdown, moving averages, relative strength, rolling correlations |

## Data sources
| Source | Data | Notebook |
|--------|------|----------|
| Google Sheets | Watchlist & alert thresholds | `01_ingest_watchlist` |
| Yahoo Finance | Daily OHLCV for stocks/ETFs/crypto | `02_ingest_prices` |
| CoinGecko | Crypto market data | `03_ingest_crypto` |
| FRED | Macro indicators (VIX, rates, USD) | `04_ingest_macro` |

## Repository structure
```
.github/workflows/   CI pipeline (lint + tests)
resources/           Databricks job definitions (Asset Bundles)
src/notebooks/       Pipeline notebooks (ingest → silver → gold → DQ → alerts)
src/lib/             Reusable, unit-tested Python logic
tests/               pytest unit tests
dashboards/          AI/BI dashboard definitions
docs/                Architecture diagram, screenshots
```

## Dashboards
_TODO: screenshots_

## Alerts
_TODO: how drop alerts work_

## Tech stack
Databricks (Free Edition), Delta Lake, Unity Catalog, PySpark, SQL, Lakeflow Jobs, AI/BI Dashboards, GitHub Actions, ruff, pytest

## Roadmap
- [ ] Phase 1 – MVP: watchlist + prices + medallion layers + dashboard
- [ ] Phase 2 – Daily job + e-mail alerts + Asset Bundles + CD
- [ ] Phase 3 – Macro, correlations, fundamentals, sentiment
