# Purpose of This Data Dictionary

This document explains **what the data means** in plain language and **which API to call** for common goals. It is aimed at API consumers, analysts, and others who are not deep in the codebase.

For exact request/response schemas, query parameters, and status codes, use the interactive API reference at **`/scalar`** on a running server (e.g. `http://localhost:8000/scalar`). The OpenAPI JSON is at **`/openapi.json`**.

---

# Main Business Entities

## Listed instrument (asset)

**What it is:** A row in the instrument register (cadastro) for a tradable symbol on B3 (e.g. a stock ticker).

**Why it exists:** Gives stable metadata (name, ISIN, segment) tied to the ticker used everywhere else in the API and database.

**Where it comes from:** Normalized cadastro CSV produced by the daily scraper → ETL load into `dim_assets`.

**Typical use:** Resolve ticker to name/ISIN; search listed names.

---

## Daily consolidated trade (negócios)

**What it is:** One summary row per ticker per **trading day**: OHLC-style prices, variation, volume, trade count from B3 consolidated trades.

**Why it exists:** Represents the official daily trading summary from negócios consolidados.

**Where it comes from:** Normalized negócios CSV → `fact_daily_trades`.

**Typical use:** Screen or chart daily volume and OHLC from the database (not tick-level trades).

---

## Daily quote (database)

**What it is:** One row per ticker per **trading day** with last/min/max/avg prices, variation, volume, and trade count, stored in `fact_daily_quotes`.

**Why it exists:** The ORM calls this a legacy/compatibility table name; several **database-backed** quote endpoints read from it (distinct from COTAHIST and from live B3 snapshots).

**Where it comes from:** ETL daily quotes pipeline (from negócios-derived data; see [`app/db/models.py`](../app/db/models.py) `FactDailyQuote`) — see [architecture.md](architecture.md) for pipeline flow.

**Typical use:** Latest closing-style fields per ticker, historical daily series from **Postgres**.

---

## Intraday quote point (database)

**What it is:** A single timestamped price point for a ticker, stored in `fact_quotes` (Timescale hypertable when the DB extension is enabled).

**Why it exists:** Persist minute-level (or finer) paths produced from B3 Daily Fluctuation History batch jobs.

**Where it comes from:** JSONL from `run_b3_quote_batch.py` → `run_intraday_quotes_pipeline`.

**Typical use:** Build intraday charts from **your** database, not from live B3 on every request.

---

## COTAHIST history row

**What it is:** One record from B3’s annual COTAHIST file at **full B3 grain** (many columns in DB; API returns a reduced projection). **Multiple rows for the same calendar date and ticker are possible** (different B3 natural keys).

**Why it exists:** Long historical backfill separate from the daily bulletin pipeline.

**Where it comes from:** Annual ZIP / fixed-width TXT → `fact_cotahist_daily`.

**Typical use:** Historical research where you need COTAHIST fields; understand overlap with daily quote tables (see limitations below).

---

## ETL run (audit)

**What it is:** A row in `etl_runs` describing one pipeline execution: name, status, timestamps, optional source file/date, row counts, error message on failure.

**Why it exists:** Observability and debugging of loads triggered by CLI, Prefect handoffs, or API (where applicable).

**Where it comes from:** Written by pipeline functions in `app/etl/orchestration/pipeline.py` (unless a caller passes `record_audit=False`).

**Typical use:** Check whether last load succeeded; **GET `/etl/runs`**.

---

## Market overview (aggregated, not a stored table)

**What it is:** A single JSON payload for one `trade_date`: top gainers/losers (from daily quotes), most traded lists (from daily trades), and counts.

**Why it exists:** Convenience for dashboards without multiple client-side joins.

**Where it comes from:** Computed at request time from `fact_daily_quotes` and `fact_daily_trades`.

---

## Asset coverage and asset overview (composite API payloads)

**What it is:** **Coverage** summarizes which date ranges exist in DB tables for a ticker. **Overview** bundles master data, latest DB quotes/trades/intraday, optional live delayed snapshot, and section flags.

**Why it exists:** One call to understand “what do we have for this ticker?”

**Where it comes from:** Aggregated from several repositories + optional B3 HTTP.

---

## Scraper run audit (internal)

**What it is:** Rows in `scraper_run_audit` for scraper executions (e.g. Prefect tasks, COTAHIST fetch in Docker worker).

**Why it exists:** Operational auditing separate from `etl_runs`.

**Not exposed** by a dedicated HTTP route in this repository — inspect via SQL if needed.

---

# Important Fields

## Asset (`AssetRead` / `GET /assets`, `GET /assets/{ticker}`)

| Field | Meaning | Type / format | Example | Notes |
|-------|---------|---------------|---------|--------|
| `ticker` | Exchange symbol | string | `PETR4` | Stored uppercase |
| `asset_name` | Company / instrument name | string or null | `PETROBRAS PN N2` | |
| `isin` | ISIN code | string or null | `BRPETRACNPR6` | |
| `segment` | Listing segment | string or null | `Novo Mercado` | |
| `source_file_date` | Date from source cadastro file | date or null | `2024-06-14` | |
| `id` | Internal surrogate key | integer | `1` | |
| `created_at` / `updated_at` | Row timestamps | datetime (TZ) | ISO 8601 | |

`dim_assets` also has `instrument_type` and `is_active` in the database; they are **not** on `AssetRead` — use DB or extend API if needed.

---

## Daily quote (`DailyQuoteRead` / DB quote routes)

| Field | Meaning | Type / format | Example | Notes |
|-------|---------|---------------|---------|--------|
| `ticker` | Symbol | string | `PETR4` | |
| `trade_date` | Session date | date | `2024-06-14` | |
| `last_price` | Reference last price for the day | decimal or null | `38.45` | |
| `min_price` / `max_price` / `avg_price` | Day range / average | decimal or null | | |
| `variation_pct` | Day variation | decimal or null | `1.25` | Percent as stored |
| `financial_volume` | Financial volume | decimal or null | | Currency per B3 source |
| `trade_count` | Number of trades | integer or null | | |
| `source_file_name` | Source file | string or null | | Traceability |
| `id` | Row id | integer | | |
| `ingested_at` | When loaded | datetime | | |

---

## Daily trade (`TradeRead`)

| Field | Meaning | Type / format | Example | Notes |
|-------|---------|---------------|---------|--------|
| `ticker` | Symbol | string | `PETR4` | |
| `trade_date` | Session date | date | `2024-06-14` | |
| `open_price` / `close_price` / `min_price` / `max_price` / `avg_price` | OHLC-style | decimal or null | | |
| `variation_pct` | Day variation | decimal or null | | |
| `financial_volume` | Turnover | decimal or null | | |
| `trade_count` | Trade count | integer or null | | |
| `source_file_name` | Source file | string or null | | |
| `id` / `ingested_at` | Row id / load time | int / datetime | | |

---

## Intraday fact quote (`FactQuoteRead` / `GET /fact-quotes/...`)

| Field | Meaning | Type / format | Example | Notes |
|-------|---------|---------------|---------|--------|
| `ticker` | Symbol | string | `PETR4` | |
| `quoted_at` | Point in time | datetime (TZ) | | Hypertable partition key |
| `trade_date` | Session date | date | | |
| `close_price` | Price at that time | decimal or null | | From JSONL path |
| `price_fluctuation_pct` | Fluctuation vs reference | decimal or null | | |

---

## COTAHIST row (`CotahistDailyRead`)

| Field | Meaning | Type / format | Example | Notes |
|-------|---------|---------------|---------|--------|
| `ticker` | Symbol | string | `PETR4` | |
| `trade_date` | Session date | date | | |
| `open_price` / `min_price` / `max_price` / `avg_price` | OHLC from COTAHIST | decimal or null | | |
| `close_price` | Last / settlement in file | decimal or null | | Maps from DB `last_price` |
| `best_bid` / `best_ask` | Quote sides | decimal or null | | |
| `trade_count` | Trades | integer or null | | |
| `quantity_total` / `volume_financial` | Size metrics | decimal or null | | |
| `isin` | ISIN | string | | |
| `especi` / `tp_merc` | B3 specifiers | string | | |
| `expiration_date` / `strike_price` | Derivatives fields | date / decimal or null | | |

---

## ETL run list item (`ETLRunListItem` / `GET /etl/runs`)

| Field | Meaning | Type / format | Example | Notes |
|-------|---------|---------------|---------|--------|
| `run_id` | Primary key | integer | | Same as `etl_runs.id` |
| `pipeline_name` | Which pipeline ran | string | | |
| `status` | Outcome | string | `success` / `failed` / … | |
| `started_at` / `finished_at` | Bounds | datetime | | |
| `duration_seconds` | Length | float or null | | When both times set |
| `source_date` / `source_file` | Provenance | date / string or null | | |
| `message` | Error or note | string or null | | Often error text on failure |
| `rows_inserted` / `rows_failed` | Counts | integer or null | | |

---

## Candle (`CandleRead`)

| Field | Meaning | Type / format | Example | Notes |
|-------|---------|---------------|---------|--------|
| `interval` | Bucket size | string | `1d`, `15m` | See route docs for allowed values |
| `start_time` / `end_time` | Bucket bounds | datetime | | |
| `open` / `high` / `low` / `close` | OHLC | decimal or null | | |
| `volume` | Financial volume | decimal or null | | Mainly daily candles |
| `point_count` | Raw points aggregated | integer or null | | Fact-quote candle route |

---

## Indicator series (`IndicatorSeriesRead`)

| Field | Meaning | Type / format | Example | Notes |
|-------|---------|---------------|---------|--------|
| `ticker` | Symbol | string | | |
| `indicator` | Which indicator | string | `SMA`, `EMA`, `RSI` | |
| `period` | Window / RSI parameter | integer | `14` | Bounded (see API) |
| `source_range` | Input close span | object | `start` / `end` dates | From daily quotes |
| `values` | Points | array | `as_of` + `value` | `value` null until enough history |

---

# API Consumer Guide

Use **`GET /health`** for liveness/version.

| If you want… | Use |
|--------------|-----|
| Paginated list of instruments; search by name/ticker | `GET /assets?q=...` |
| One instrument by ticker | `GET /assets/{ticker}` |
| Date ranges available per table for a ticker | `GET /assets/{ticker}/coverage` |
| One combined snapshot (master + DB + optional live) | `GET /assets/{ticker}/overview` |
| Latest **database** daily quote per ticker (many tickers) | `GET /quotes/latest` |
| Historical **database** daily quotes for one ticker | `GET /quotes/{ticker}/history` |
| OHLC candles (daily from DB quotes; intraday from DB fact_quotes) | `GET /quotes/{ticker}/candles` |
| SMA / EMA / RSI from **database** daily closes | `GET /quotes/{ticker}/indicators` |
| Latest **live delayed** intraday snapshot (B3 HTTP) | `GET /quotes/{ticker}` |
| Legacy delayed snapshot shape (B3 HTTP) | `GET /quotes/{ticker}/snapshot` |
| Full **live delayed** intraday series (B3 HTTP) | `GET /quotes/{ticker}/intraday` |
| Paginated **database** daily trades | `GET /trades` |
| Trade history for one ticker | `GET /trades/{ticker}/history` |
| One trade row for ticker + date | `GET /trades/{ticker}?trade_date=...` |
| **Persisted** intraday series from DB | `GET /fact-quotes/{ticker}/series` |
| **Persisted** intraday candles (fact_quotes only) | `GET /fact-quotes/{ticker}/candles` |
| All **persisted** intraday points for one session date | `GET /fact-quotes/{ticker}/days/{trade_date}` |
| COTAHIST rows with filters | `GET /cotahist/` |
| COTAHIST history for one ticker | `GET /cotahist/{ticker}/history` |
| Market movers / volume ranks for a date | `GET /market/overview?trade_date=...` |
| ETL audit history | `GET /etl/runs` |
| Run instruments+trades ETL for latest local CSVs | `POST /etl/run-latest` (body optional; local only) |
| Backfill ETL for a date range (local CSVs) | `POST /etl/backfill` |

**Remote scraping via API:** `POST /etl/run-latest` and `POST /etl/backfill` return **501** if `source_mode` is `remote` — use the scheduler or scripts instead.

---

# Data Freshness and Limitations

## Scheduled loads

- **Daily registry** (cadastro + negócios + DB handoff) runs on a **cron** schedule in the scheduler container (default **08:00 America/Sao_Paulo**; override `PREFECT_DAILY_REGISTRY_CRON`).
- **Intraday quotes** flow runs on an **interval** (default **30 minutes**) but **no-ops outside** the configured B3 quote window (session open/close with padding — see `B3_EQUITIES_SESSION_OPEN` / `CLOSE` and `app/etl/orchestration/market_hours.py`).

**To be confirmed for your deployment:** Actual wall-clock freshness depends on when B3 publishes files, scraper duration, and whether bootstrap/cron overlap.

## Live B3 routes

- Data from `GET /quotes/{ticker}`, `/snapshot`, `/intraday` is **delayed**, not real-time.
- Responses may be **cached** in memory per ticker (default TTL **300 seconds** from `b3_quote_cache_ttl` in settings).
- B3 may **rate-limit** or block; APIs return documented error shapes (e.g. 503) when that happens.

## COTAHIST vs daily quotes

- **`fact_cotahist_daily`** and **`fact_daily_quotes`** can both have rows for the same calendar date and ticker but **different semantics and grain**. Nothing automatically reconciles them. Choose the source that matches your use case ([architecture.md](architecture.md) data sources).

## ETL audit completeness

- Normal pipeline runs record **`etl_runs`**. Some COTAHIST or batch modes may skip or consolidate audit rows — see `scripts/run_etl.py` and pipeline docstrings for edge cases.

## Weekends and holidays

- The intraday window check does **not** model the exchange calendar; scheduled ticks may still run and return “skipped” or empty upstream data.

---

# Related documentation

- [architecture.md](architecture.md) — containers, data flow, operations.
- [README.md](../README.md) — quick start and Docker.
