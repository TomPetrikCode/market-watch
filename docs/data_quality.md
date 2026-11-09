# Data quality checks

`30_dq_checks` runs after the gold layer and **before alerts**. Its job: make sure an alert is never
sent from bad data. Code: [`src/notebooks/30_dq_checks.py`](../src/notebooks/30_dq_checks.py),
helpers in [`src/lib/dq.py`](../src/lib/dq.py) (unit-tested in CI).

## Severity

| Severity | Meaning | What happens |
|---|---|---|
| **ERROR** | the data is wrong | the notebook fails → the daily job stops → **no alerts are sent**, you get a job-failure e-mail |
| **WARN** | suspicious, but may be fine | logged, the job continues |

Why stop instead of continuing: a missing alert is an inconvenience, a false alert built on a stale or
broken price is worse – you might act on it. "No alert" is the safe failure mode.

## Checks

| Check | Severity | What it catches | Example |
|---|---|---|---|
| `freshness` | ERROR | last price older than 4 days (stocks, covers weekend + holiday) or 2 days (crypto) | Yahoo stopped returning MSFT → `MSFT (5d)` |
| `completeness_silver` / `_gold` | ERROR | an active watchlist ticker has no data at all | typo in the sheet: `MSTF` instead of `MSFT` |
| `unique_silver` / `_gold` | ERROR | the same ticker + date twice | a bug in a join duplicating rows |
| `fx_rate_missing` | ERROR | a price without a USD exchange rate | EUR rate missing → SXR8.DE cannot be converted |
| `fx_pair_missing` | ERROR | a currency with no FX pair at all | new ticker in GBP, `GBPUSD=X` not downloaded |
| `price_validity` | ERROR | close ≤ 0, high < low, negative volume (last 30 days) | corrupted row from the source |
| `suspicious_daily_move` | WARN | a non-crypto asset moved more than ±50 % in a day (last 30 days) | real crash – or an unadjusted stock split |
| `watchlist_duplicate_ticker` | WARN | the same ticker twice in the sheet | copy-paste mistake |
| `watchlist_unknown_asset_type` | WARN | `asset_type` not in `stock`, `etf`, `crypto`, `benchmark` | `stok` – crypto detection and volatility would treat it wrong |
| `watchlist_positive_threshold` | WARN | an alert threshold above 0 | `10` instead of `-10` – the drop alert would never fire as intended |

## Design decisions

- **Recent data only for price validity and jumps.** Decades of Yahoo history contain small
  inconsistencies we cannot fix. Checking them would fail the job every day for something that is
  not new. The checks focus on what changed recently.
- **Freshness is measured on gold**, because that is what alerts read.
- **Every run is stored** in `dq_results` (`run_at`, `check_name`, `severity`, `status`,
  `failed_count`, `sample`) – a history of data health that can be charted on the dashboard.
- **`sample`** lists up to 10 failing items so the cause is visible without querying anything.

## Settings (`src/lib/config.py`)

| Setting | Default |
|---|---|
| `DQ_MAX_STALE_DAYS` | crypto 2, others 4 |
| `DQ_MAX_DAILY_MOVE_PCT` | 50 |
| `DQ_RECENT_DAYS` | 30 |
| `ALLOWED_ASSET_TYPES` | stock, etf, crypto, benchmark |
