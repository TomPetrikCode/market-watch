# Alerts

`40_alerts` turns gold metrics and the per-ticker thresholds from the watchlist into rows in
`gold_alerts`. A **Databricks SQL Alert** on top of that table sends one summary e-mail per day.
Code: [`src/notebooks/40_alerts.py`](../src/notebooks/40_alerts.py).

## Alert types

| `alert_type` | Fires when | How often |
|---|---|---|
| `drop_1d` | `ret_1d_pct ≤ alert_1d_pct` (e.g. −10 %) | every time – each day's drop is its own event |
| `drawdown_52w` | `drawdown_52w_pct` **crosses** `alert_from_high_pct` (yesterday above, today at/below) | once per crossing |
| `volatility_spike` | `is_volatile` **turns on** (yesterday false, today true) | once per spike |

Tickers without thresholds (e.g. the SPY benchmark) get no price alerts.

## Design decisions

- **Crossing, not state.** A stock sitting 30 % below its high for two weeks should not send the same
  e-mail every day. The alert says "look at this now"; after that you follow it yourself.
  A new alert comes only after the price recovers above the threshold and crosses it again.
  A deeper fall (−25 → −35 %) does not trigger a new alert – a deliberate choice to avoid noise.
- **One summary e-mail**, not one per ticker – on a market-wide sell-off that would be ten e-mails at once.
- **No e-mail on a quiet day.** Whether the pipeline is running is covered by the job-failure notification,
  so a daily "nothing happened" e-mail would only add noise.
- **Look-back window + MERGE.** The last 7 days (`ALERT_LOOKBACK_DAYS` in config) are re-evaluated on every
  run and written with `MERGE` on `(ticker, date, alert_type)`, inserting only new rows.
  If the job misses a day, the alert is caught on the next run; re-running the job never creates a duplicate.
- **Alerts only after data quality.** `40_alerts` runs after `30_dq_checks`; a failed ERROR check stops the
  job, so alerts are never built from stale or broken data (see [data_quality.md](data_quality.md)).
- **E-mail via SQL Alert, not Python.** No SMTP credentials in code; the notebook only decides *what* is an
  alert, Databricks handles *delivery*.

## `gold_alerts` columns

| Column | Meaning |
|---|---|
| `ticker`, `date` | which asset, which trading day |
| `alert_type` | see above |
| `value_pct` | the metric value (return, drawdown or volatility in %) |
| `threshold_pct` | the threshold it was compared with (for volatility: its normal level) |
| `message` | human-readable line for the e-mail, e.g. `APP: -10.4 % za den (práh -10.0 %)` |
| `detected_at` | when the pipeline created the alert – the e-mail query selects today's rows |