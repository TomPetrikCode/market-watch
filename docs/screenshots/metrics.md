# Gold metrics – business logic

This document explains every metric in the gold layer: **what it tells you, how it is calculated,
how to read it, and why it is designed this way.**

Tables: `gold_metrics` (one row per ticker per day) and `gold_correlations` (one row per pair per day).
Code: [`src/notebooks/20_gold_metrics.py`](../src/notebooks/20_gold_metrics.py).
Settings: [`src/lib/config.py`](../src/lib/config.py).

---

## Questions the metrics answer

| Question | Metrics |
|---|---|
| Did something happen today? | `ret_1d_pct`, `volume_ratio` |
| How has it moved recently? | `ret_1w_pct`, `ret_1m_pct` |
| Is it cheap compared to its recent past? | `drawdown_52w_pct`, `pct_vs_ma200` |
| What is the trend? | `pct_vs_ma50`, `pct_vs_ma200`, `above_ma200` |
| Is it moving more than usual? | `vol_20d_pct`, `is_volatile` |
| Is it beating the market? | `rel_3m_vs_spy_pct` |
| Do two assets move together? | `corr_90d` |

---

## General rules

**Adjusted close price.** All metrics use `adj_close` – the closing price adjusted for stock splits and
dividends. Without it, a 2-for-1 split would look like a 50 % crash.

**Local currency vs USD.** Price metrics use the asset's own currency (CSU.TO in CAD, IPCO.ST in SEK),
so they describe the asset itself and are not distorted by exchange rate moves.
Only the comparison with SPY (`rel_3m_vs_spy_pct`) uses USD, because comparing two assets requires
the same currency.

**Trading days vs calendar days.**
- *Trading days* (rows) for moving averages, volatility and volume – the market convention
  ("50-day moving average" = last 50 trading sessions).
- *Calendar days* for 1-week / 1-month / 3-month returns and the 52-week high. Crypto trades 7 days
  a week, stocks 5 – counting calendar days makes "one week" mean the same period for both.
  If the date a week ago was a weekend, the last available price before it is used (Friday).

**Not enough history → NULL.** A metric is empty until there is enough data for it
(e.g. `ma200` needs 200 trading days). A 200-day average computed from 3 days would be misleading.

---

## Price movement

### `ret_1d_pct` – daily return
- **What:** % change of today's close vs the previous trading day.
- **Formula:** `(close_today / close_previous_day − 1) × 100`
- **Read it:** `-6.8` = the price fell 6.8 % since the last close.
- **Used for:** the daily drop alert – compared with `alert_1d_pct` from the watchlist
  (e.g. alert when `ret_1d_pct ≤ -10`).

### `ret_1w_pct`, `ret_1m_pct` – weekly / monthly return
- **What:** % change vs the price 7 / 30 calendar days ago.
- **Read it:** `ret_1d = -3`, `ret_1w = +6` → bad day, but still up on the week. Context for daily moves.
- **Why calendar days:** see general rules – same period for stocks and crypto.

---

## Distance from the high

### `drawdown_52w_pct` – drop from the 52-week high
- **What:** how far the price is below its highest close in the last 365 days. Always 0 or negative.
- **Formula:** `(close_today / max_close_last_365_days − 1) × 100`
- **Read it:** `-25` = the price is 25 % below its 1-year high. `0` = it is at a new 1-year high.
- **Used for:** the "buy the dip" alert – compared with `alert_from_high_pct` from the watchlist.
- **Why 52 weeks and not all-time:** see below.

### `drawdown_ath_pct` – drop from the all-time high
- **What:** same, but against the highest price in the whole history.
- **Read it:** RIG shows about `-94` – it is 94 % below its 2008 peak.
- **Why only informational:** a stock that crashed 15 years ago would trigger a drawdown alert
  *every single day*. The 52-week version answers the useful question: "is it cheap compared
  to the recent past?"

---

## Trend

### `ma50`, `ma200` – moving averages
- **What:** average closing price of the last 50 / 200 trading days.
- **Read it:** MA50 = short/medium-term trend, MA200 = long-term trend (roughly 10 months).

### `pct_vs_ma50`, `pct_vs_ma200` – price vs its average
- **What:** how many % the current price is above or below the moving average.
- **Formula:** `(close_today / ma − 1) × 100`
- **Read it:** `pct_vs_ma200 = -12` = the price is 12 % below its 200-day average – cheap relative to
  its long-term norm. `+30` = far above the average, possibly stretched.
- **Why:** the raw MA value (e.g. `187.34`) means nothing without comparing it to the price.
  The % difference is readable immediately and comparable across tickers.

### `above_ma200` – long-term uptrend flag
- **What:** `true` when the price is above its 200-day average.
- **Read it:** a classic simple trend filter – above = long-term uptrend, below = downtrend.

---

## Volatility

### `vol_20d_pct` – 20-day volatility (annualised)
- **What:** how much the price has been jumping up and down over the last 20 trading days.
- **Formula:** standard deviation of the last 20 daily returns × √252 (stocks) or × √365 (crypto).
  Multiplying by √(trading days per year) converts it to a yearly figure – the industry standard.
- **Read it:** `30` = typical for a large stable stock, `60–100` = a wild small cap or crypto.
- **Limitation:** uses closing prices only (close-to-close), so it does not see moves *within* a day
  (a stock that drops 8 % intraday and recovers by the close). Good enough for daily monitoring.

### `vol_baseline_pct` – normal volatility
- **What:** average of `vol_20d_pct` over the last 252 trading days (~1 year) – the asset's "normal".

### `is_volatile` – moving more than usual
- **What:** `true` when `vol_20d_pct > vol_baseline_pct × 1.5` (multiplier `VOL_SPIKE_MULTIPLIER` in config).
- **Read it:** BTC with 60 % volatility is normal for BTC → `false`. Microsoft jumping from 25 % to 45 % → `true`.
- **Why compare the asset with itself and not a fixed threshold:** with a fixed threshold
  (e.g. 60 %) crypto would be flagged every day and the flag would carry no information.
  "More volatile than *its own* normal" is what signals that something is going on.
- **Why the close-only limitation does not break it:** today's volatility and the baseline are
  measured the same way, so any underestimation affects both equally.

---

## Volume

### `volume_ratio` – today's volume vs normal
- **What:** today's traded volume divided by the average volume of the previous 20 trading days
  (today excluded, so a spike does not inflate its own baseline).
- **Read it:** `1.0` = normal day, `3.0` = three times the usual volume.
  `ret_1d = -8` with `volume_ratio = 3` → heavy selling, a strong signal.
  `ret_1d = -8` with `volume_ratio = 0.8` → a drop on thin trading, less convincing.
- **NULL** when there is no volume data (e.g. FX) or not enough history.

---

## Market comparison

### `rel_3m_vs_spy_pct` – 3-month performance vs S&P 500
- **What:** the ticker's 3-month return minus SPY's 3-month return, both in USD
  (benchmark `BENCHMARK` in config).
- **Formula:** `ret_3m_usd(ticker) − ret_3m_usd(SPY)`, in percentage points.
- **Read it:** ticker `+20 %`, SPY `+5 %` → `+15` = beat the market by 15 points.
  `-10` = underperformed. SPY itself always shows `0`.
- **Why USD:** a Canadian stock in CAD vs SPY in USD is not a fair comparison; converting both to USD
  shows the result a USD-based investor would actually see.
- **Why this replaced the price ratio:** the previous `rs_vs_spy` (price / SPY price) produced numbers
  like `102` or `0.009` that only made sense as a trend on a chart. A difference in returns is readable
  as a single number.
- **Crypto on weekends:** SPY does not trade on weekends, so the last known SPY value (Friday) is used.

---

## Correlation

### `corr_90d` – rolling 90-day correlation (`gold_correlations`)
- **What:** how closely the daily returns of two assets moved together over the last 90 common trading days.
  Pairs are defined in `CORRELATION_PAIRS` in config (now BTC×ETH and ETH×BMNR).
- **Read it:** `1` = always move together, `0` = independent, `-1` = always opposite.
  BTC × ETH around `0.9` = they move almost the same – holding both is not much diversification.
  ETH × BMNR around `0.6` = BMNR (a company holding ETH) follows ETH, but with its own noise.
- **Why returns and not prices:** any two assets that both went up over time would show a high price
  correlation even if unrelated. Daily returns show whether they move together *day to day*.
- **Common days only:** for a crypto × stock pair, weekends are skipped because the stock has no price.

---

## Changing the settings

| Setting | Where | Default |
|---|---|---|
| Daily drop alert threshold | watchlist sheet, `alert_1d_pct` (per ticker) | e.g. `-10` |
| Drawdown alert threshold | watchlist sheet, `alert_from_high_pct` (per ticker) | e.g. `-25` |
| Benchmark | `config.py`, `BENCHMARK` | `SPY` |
| Correlation pairs | `config.py`, `CORRELATION_PAIRS` | BTC×ETH, ETH×BMNR |
| Volatility spike multiplier | `config.py`, `VOL_SPIKE_MULTIPLIER` | `1.5` |
