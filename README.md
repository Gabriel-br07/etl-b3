# etl-b3

ETL pipeline and REST API for B3 public daily market data (Boletim Diário do Mercado).

Short, focused documentation for getting the project running locally or with Docker.

---

## Overview

- Scrapes B3 daily files (instruments + consolidated trades) using Playwright.
- Normalizes files and runs a 25-minute quote ingestion loop that fetches per-ticker daily fluctuation histories and writes JSONL + report CSV outputs.
- Stores results in **PostgreSQL + TimescaleDB** and exposes a FastAPI HTTP API (Scalar docs at `/scalar`).

---

## Architecture

```
scheduler (Docker container)
  |
  +--> daily Playwright scrapers
  |      run_b3_scraper.py          -> cadastro_instrumentos_YYYYMMDD.normalized.csv
  |      run_b3_scraper_negocios.py -> negocios_consolidados_YYYYMMDD.normalized.csv
  |
  +--> DB load: instruments + trades   (pipeline.py: run_instruments_and_trades_pipeline)
  |      dim_assets        — upsert ON CONFLICT (ticker)
  |      fact_daily_trades — upsert ON CONFLICT (ticker, trade_date)
  |
  +--> 25-min quote loop
         run_b3_quote_batch.py -> daily_fluctuation_YYYYMMDDTHHMMSS.jsonl
         |
         +--> DB load: intraday quotes  (pipeline.py: run_intraday_quotes_pipeline)
                fact_quotes (TimescaleDB hypertable, INSERT ON CONFLICT DO NOTHING)

ETL audit trail: etl_runs table records every pipeline execution (RUNNING → SUCCESS/FAILED).
API layer (FastAPI): /assets, /quotes, /trades, /fact-quotes, /health, /etl
```

---

## How the load flow works

### Three-transaction audit pattern

Every pipeline run uses **three separate database transactions** to ensure the audit log is always recorded, even when the data load fails:

```
Transaction 1 (audit start)
  └─ INSERT etl_runs (status=RUNNING) → commit → store run_id

Transaction 2 (data load)  ← atomic batch
  ├─ load_assets(db, rows)   — upsert dim_assets
  ├─ load_trades(db, rows)   — upsert fact_daily_trades
  └─ commit  (or rollback on ANY exception — no partial commits)

Transaction 3 (audit finish) ← independent of transaction 2
  └─ UPDATE etl_runs SET status=SUCCESS/FAILED → commit
```

Key properties:
- **Atomicity**: if transaction 2 raises any exception, the whole batch rolls back. No partial rows are committed.
- **Audit isolation**: transaction 3 always runs in its own session, so a data failure does not prevent recording `status=FAILED` in `etl_runs`.
- **Idempotency**: upsert semantics (ON CONFLICT DO UPDATE / DO NOTHING) mean reprocessing the same file is safe — it updates existing rows rather than duplicating them.

### Entry points

| Function | Module | Writes to |
|---|---|---|
| `run_instruments_and_trades_pipeline(csv, trades, date)` | `app/etl/orchestration/pipeline.py` | `dim_assets`, `fact_daily_trades`, `etl_runs` |
| `run_intraday_quotes_pipeline(jsonl_path)` | `app/etl/orchestration/pipeline.py` | `fact_quotes`, `etl_runs` |
| `run_daily_quotes_pipeline(...)` | `app/etl/orchestration/pipeline.py` | `fact_daily_quotes`, `etl_runs` |

### Loaders and repositories

```
pipeline.py
  └─ db_loader.py
       ├─ load_assets(db, rows)           → AssetRepository.upsert_many()
       ├─ load_trades(db, rows)           → TradeRepository.upsert_many()
       ├─ load_daily_quotes(db, rows)     → QuoteRepository.upsert_many()   (legacy)
       └─ load_intraday_quotes(db, rows)  → FactQuoteRepository.insert_many()
```

Repositories **never call `db.commit()`** — the caller (`managed_session`) commits once at the end of the block to provide atomicity across all repository calls.

---

## Database schema

| Table | Description |
|---|---|
| `dim_assets` | Instrument master data (one row per ticker) |
| `fact_daily_trades` | Consolidated trading data by asset/date (NegociosConsolidados) |
| `fact_daily_quotes` | Daily consolidated quotes (legacy, kept for API compatibility) |
| `fact_quotes` | Intraday time-series quotes — **TimescaleDB hypertable** partitioned on `quoted_at` |
| `etl_runs` | ETL observability: pipeline name, status, rows inserted/failed, source file/date |

---

## REST API

The API exposes B3 market data stored in the database plus optional live delayed data from B3. All list endpoints return paginated responses with `total`, `limit`, `offset`, and `items`.

- **Base URL (local):** `http://localhost:8000`
- **Documentation:** [http://localhost:8000/scalar](http://localhost:8000/scalar) — interactive API reference (Scalar).

### Main routes

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check (version, environment). |
| GET | `/assets` | List assets (paginated; optional search `q` by ticker or name). |
| GET | `/assets/{ticker}` | Get asset by ticker. |
| GET | `/quotes/latest` | Latest daily quote per ticker (DB). |
| GET | `/quotes/{ticker}/history` | Historical daily quotes for a ticker (DB). |
| GET | `/quotes/{ticker}` | Latest intraday snapshot (live B3, delayed). |
| GET | `/quotes/{ticker}/intraday` | Full intraday series (live B3, delayed). |
| GET | `/quotes/{ticker}/snapshot` | Legacy delayed quote snapshot (live B3). |
| GET | `/trades` | List daily trades (filter by `trade_date`, `ticker`, `start_date`/`end_date`). |
| GET | `/trades/{ticker}/history` | Trade history for a ticker. |
| GET | `/trades/{ticker}` | Single daily trade (query param `trade_date` required). |
| GET | `/fact-quotes/{ticker}/series` | Intraday series from DB (query `start`, `end` datetime, `limit`). |
| GET | `/fact-quotes/{ticker}/days/{trade_date}` | Intraday points for one trade date (DB). |
| POST | `/etl/run-latest` | Trigger ETL for latest date (local CSV). |
| POST | `/etl/backfill` | Trigger historical ETL backfill (body: `date_from`, `date_to`). |

### Endpoint reference — data provided

This section describes **what type of data** each endpoint returns so you can choose the right one for your use case. **DB** = data persisted in the application database (from the ETL pipeline). **Live B3** = data fetched on demand from B3’s public API (delayed, not real-time).

#### Health

| Endpoint | Data source | Response shape |
|----------|-------------|----------------|
| `GET /health` | Application | `status` (e.g. `"ok"`), `version`, `environment`. Use for liveness/readiness probes. |

#### Assets (B3 listed instruments)

| Endpoint | Data source | Response shape |
|----------|-------------|----------------|
| `GET /assets` | DB (`dim_assets`) | Paginated list. Each item: `ticker`, `asset_name`, `isin`, `segment`, `source_file_date`, `id`, `created_at`, `updated_at`. Optional search by ticker or name (`q`). |
| `GET /assets/{ticker}` | DB | Single asset: same fields as above. 404 if ticker not found. |

#### Quotes (daily and live)

| Endpoint | Data source | Response shape |
|----------|-------------|----------------|
| `GET /quotes/latest` | DB (`fact_daily_quotes`) | Paginated list of **latest daily quote per ticker** (one row per ticker, most recent `trade_date`). Each item: `ticker`, `trade_date`, `last_price`, `min_price`, `max_price`, `avg_price`, `variation_pct`, `financial_volume`, `trade_count`, `source_file_name`, `id`, `ingested_at`. Optional filter by comma-separated `tickers`. |
| `GET /quotes/{ticker}/history` | DB | List of **historical daily quotes** for one ticker. Same fields per item. Optional `start_date`, `end_date`, `limit`. Ordered by `trade_date` descending. |
| `GET /quotes/{ticker}` | Live B3 (delayed) | **Latest intraday snapshot** for the ticker: `ticker`, `trade_date`, `message_datetime`, last quote fields (e.g. `close_price`, `price_fluctuation_pct`), `delayed`, `fetched_at`. Cached in-memory (configurable TTL). |
| `GET /quotes/{ticker}/intraday` | Live B3 (delayed) | **Full minute-level intraday series**: `ticker`, `trade_date`, `message_datetime`, `points` (array of `time`, `close_price`, etc.), `delayed`, `fetched_at`. |
| `GET /quotes/{ticker}/snapshot` | Live B3 (delayed) | Legacy **delayed quote snapshot**: same idea as `/quotes/{ticker}` with a slightly different field layout. |

#### Trades (daily consolidated — Negocios Consolidados)

| Endpoint | Data source | Response shape |
|----------|-------------|----------------|
| `GET /trades` | DB (`fact_daily_trades`) | Paginated list of **daily consolidated trades**. Each item: `id`, `ticker`, `trade_date`, `open_price`, `close_price`, `min_price`, `max_price`, `avg_price`, `variation_pct`, `financial_volume`, `trade_count`, `source_file_name`, `ingested_at`. Filter by `trade_date`, `ticker`, `start_date`/`end_date`. |
| `GET /trades/{ticker}/history` | DB | List of **daily trades for one ticker**. Same fields. Optional `start_date`, `end_date`, `limit`. |
| `GET /trades/{ticker}` | DB | **Single daily trade** for ticker + `trade_date` (query param required). Same fields. 404 if not found. |

#### Fact quotes (intraday time-series from DB)

| Endpoint | Data source | Response shape |
|----------|-------------|----------------|
| `GET /fact-quotes/{ticker}/series` | DB (`fact_quotes` hypertable) | List of **intraday quote points** in a datetime range. Each item: `ticker`, `quoted_at`, `trade_date`, `close_price`, `price_fluctuation_pct`. Query params: `start`, `end` (ISO 8601), `limit`. Use for persisted intraday data; for live data use `/quotes/{ticker}/intraday`. |
| `GET /fact-quotes/{ticker}/days/{trade_date}` | DB | All **intraday points for one trade date**. Same fields per item. Returns empty list if no data for that day. |

#### ETL (pipeline triggers)

| Endpoint | Data source | Response shape |
|----------|-------------|----------------|
| `POST /etl/run-latest` | Local CSV (B3_DATA_DIR) | Runs ETL for the latest available date. Returns `status`, `result` (pipeline summary: target date, assets/trades upserted, status). Only supports local source; 501 if `source_mode=remote`. |
| `POST /etl/backfill` | Local CSV | Runs ETL for a date range (body: `date_from`, `date_to`, optional `source_mode`). Returns `total_dates`, `results` (array of per-date summaries). Only supports local source. |

### Example requests

```http
GET /assets?q=PETR&limit=10
GET /trades?trade_date=2024-06-14&limit=20
GET /trades/PETR4?trade_date=2024-06-14
GET /quotes/PETR4/history?start_date=2024-06-01&end_date=2024-06-14
GET /fact-quotes/PETR4/days/2024-06-14
```

---

## Project structure (high level)

```
.
├── app/
│   ├── api/             # FastAPI: app/api/routes/ (assets, quotes, trades, fact_quotes, health, etl)
│   ├── core/            # config (pydantic-settings), logging, constants
│   ├── db/              # SQLAlchemy models, engine, session factory
│   ├── etl/
│   │   ├── ingestion/   # file adapters (local, remote), ticker filter
│   │   ├── loaders/     # db_loader.py: load_assets, load_trades, load_intraday_quotes
│   │   ├── orchestration/
│   │   │   ├── pipeline.py     # ← PRIMARY: standalone ETL pipeline (used by scheduler + API)
│   │   │   ├── flow.py         # Prefect-based flow (optional, for Prefect deployments)
│   │   │   └── csv_resolver.py # CSV discovery with retry/fallback
│   │   ├── parsers/     # instruments_parser, trades_parser, jsonl_quotes_parser
│   │   └── transforms/  # b3_transforms (pure Polars functions)
│   ├── repositories/    # AssetRepository, TradeRepository, QuoteRepository,
│   │                    # FactQuoteRepository, ETLRunRepository
│   ├── schemas/         # Pydantic API schemas
│   └── use_cases/       # batch_ingestion use case
├── alembic/             # Alembic migration scripts
│   └── versions/
│       ├── 0001_initial_schema.py   # dim_assets, fact_daily_quotes, etl_runs
│       └── 0002_schema_v2.py        # fact_daily_trades, fact_quotes (hypertable),
│                                    # enrich etl_runs, add asset_id FKs
├── docker/
│   ├── entrypoint.sh        # root bootstrap: fix permissions, drop to scraper
│   ├── run_daily_batch.sh   # runs both daily Playwright scrapers
│   ├── scheduler.py         # main orchestrator loop
│   └── initdb/
│       └── 01_timescaledb.sql  # CREATE EXTENSION timescaledb (auto-run by Postgres)
├── scripts/             # CLI scripts: run_etl.py, run_b3_scraper.py, run_b3_quote_batch.py
├── tests/               # pytest: unit/, integration/, e2e/, fixtures/, conftest.py
├── .github/workflows/   # GitHub Actions: ci.yml (ruff, ty, pytest)
├── .pre-commit-config.yaml  # ruff + ty on git commit (after pre-commit install)
├── CONTRIBUTING.md      # hooks, manual lint/typecheck, CI overview
├── TESTING-STRATEGY.md  # short pyramid summary; details in README
├── .env.example         # environment variable reference — copy to .env for local dev
├── compose.yaml         # Docker Compose base: db, scheduler, api (profile)
├── compose.override.yaml # Dev overrides: api hot reload + app bind mount (merged automatically)
├── Dockerfile           # unified scraper/scheduler image
├── Dockerfile.api       # slim API-only image (no Playwright)
└── pyproject.toml
```

---

## Environment variables

Copy `.env.example` to `.env` and adjust for your environment.

| Variable | Local default | Docker value | Description |
|---|---|---|---|
| `DATABASE_URL` | `postgresql://etlb3:etlb3pass@localhost:5432/etlb3` | `postgresql://etlb3:etlb3pass@db:5432/etlb3` | PostgreSQL connection string |
| `APP_ENV` | `development` | `production` | Controls SQL echo and log verbosity |
| `DB_POOL_SIZE` | `5` | `5` | SQLAlchemy connection pool size |
| `DB_MAX_OVERFLOW` | `10` | `10` | Additional connections beyond pool_size |
| `DB_POOL_RECYCLE` | `1800` | `1800` | Recycle connections after N seconds (prevents stale-connection errors) |
| `RUN_MIGRATIONS_ON_STARTUP` | `true` | `true` | Auto-run `alembic upgrade head` on scheduler start |
| `DAILY_RUN_HOUR` | `20` | `20` | Hour to trigger daily scrapers |
| `DAILY_RUN_MINUTE` | `0` | `0` | Minute to trigger daily scrapers |
| `SCRAPER_INTERVAL_SECONDS` | `1500` | `1500` | Quote loop cycle length (25 min) |
| `B3_DATA_DIR` | `data/sample` | `/app/data/raw` | Root raw-data directory |
| `LOG_LEVEL` | `DEBUG` | `INFO` | Python logging level |
| `PLAYWRIGHT_HEADLESS` | `false` | `true` | Run browser headless |

> **Local vs Docker key difference**: `DATABASE_URL` uses `localhost` locally and the Compose service name `db` inside Docker. Everything else uses the same application code — only env vars differ.

---

## Local development setup

### 1. Install dependencies

```powershell
uv venv
uv sync --locked
```

### 2. Configure environment

```powershell
cp .env.example .env
# Edit .env if needed — defaults work with the Docker db service below
```

### 3. Start PostgreSQL + TimescaleDB locally

```powershell
# Starts only the db service (not the scheduler)
docker compose up -d db
```

This starts `timescale/timescaledb:2.17.2-pg16` on `localhost:5432`.
The `01_timescaledb.sql` init script enables the TimescaleDB extension automatically on first boot.

### 4. Run Alembic migrations

```powershell
alembic upgrade head
```

### 5. Run the API

```powershell
uv run uvicorn app.main:app --reload
```

Use `uv run` so that uvicorn runs with the project virtualenv (where FastAPI, scalar_fastapi, etc. are installed). If you run `uvicorn` from a global or conda Python, you may get `ModuleNotFoundError: No module named 'scalar_fastapi'`.

- API base: `http://localhost:8000`
- Interactive docs: [http://localhost:8000/scalar](http://localhost:8000/scalar)

See [REST API](#rest-api) above for main routes and examples.

### 6. Run the ETL pipeline manually (local)

**Option A — CLI script (recommended)**

```powershell
# Auto-discovers CSVs from B3_DATA_DIR (data/sample by default)
python scripts/run_etl.py

# Explicit paths + date
python scripts/run_etl.py `
    --instruments data/raw/b3/boletim_diario/2024-06-14/cadastro_instrumentos_20240614.normalized.csv `
    --trades      data/raw/b3/boletim_diario/2024-06-14/negocios_consolidados_20240614.normalized.csv `
    --date        2024-06-14
```

**Option B — Python REPL / notebook**

```python
from pathlib import Path
from datetime import date
from app.etl.orchestration.pipeline import (
    run_instruments_and_trades_pipeline,
    run_intraday_quotes_pipeline,
)

# Load instruments + trades
result = run_instruments_and_trades_pipeline(
    instruments_csv=Path("data/raw/b3/boletim_diario/2024-06-14/cadastro_instrumentos_20240614.normalized.csv"),
    trades_file=Path("data/raw/b3/boletim_diario/2024-06-14/negocios_consolidados_20240614.normalized.csv"),
    target_date=date(2024, 6, 14),
)
print(result)
# {'target_date': '2024-06-14', 'assets_upserted': 512, 'trades_upserted': 389, 'status': 'success'}

# Load JSONL intraday quotes
result = run_intraday_quotes_pipeline(
    jsonl_path=Path("data/raw/b3/daily_fluctuation_history/2024-06-14/daily_fluctuation_20240614T100000.jsonl"),
)
print(result)
# {'source_file': 'daily_fluctuation_20240614T100000.jsonl', 'rows_inserted': 7800, 'status': 'success'}
```

**Option C — HTTP API**

```http
POST http://localhost:8000/etl/run-latest

POST http://localhost:8000/etl/backfill
Content-Type: application/json

{"date_from": "2024-06-01", "date_to": "2024-06-14"}
```

### 7. Run scrapers locally (requires Playwright + Chromium)

```powershell
playwright install chromium
python scripts/run_b3_scraper.py
python scripts/run_b3_scraper_negocios.py
python scripts/run_b3_quote_batch.py
```

---

## Running with Docker (full pipeline)

Compose uses two files:

- **compose.yaml** — base definition for `db`, `scheduler`, and `api` (API is under profile `api`). Single source of truth.
- **compose.override.yaml** — development overrides only (hot reload and bind mount of `./app` for the `api` service). It is merged automatically only when you run `docker compose up` with no `-f` flag: Compose then loads `compose.yaml` and merges `compose.override.yaml` if present. If you run `docker compose -f compose.yaml up`, only the base file is loaded; to include the override, use `docker compose -f compose.yaml -f compose.override.yaml up`. To run without overrides (e.g. production-like), use `docker compose -f compose.yaml up` so that only the base file is used.

The API service is built from `Dockerfile.api` (slim image, no Playwright).

```powershell
# Build and start db + scheduler
docker compose build
docker compose up -d

# Follow logs
docker compose logs -f scheduler
docker compose logs -f db
```

On first start the scheduler will:
1. Wait for the `db` service healthcheck to pass (`pg_isready`)
2. Run `alembic upgrade head` automatically
3. Wait until the scheduled daily run time (`DAILY_RUN_HOUR:DAILY_RUN_MINUTE`)
4. Run the Playwright scrapers → produce CSVs
5. Load instruments + trades into the DB (`dim_assets`, `fact_daily_trades`)
6. Start the 25-minute quote loop → produce JSONL → load into `fact_quotes`

### Start the optional API service

```powershell
# Build API image (slim, from Dockerfile.api) and start db + scheduler + API
docker compose build api
docker compose --profile api up -d

# API: http://localhost:8000 — docs: http://localhost:8000/scalar
docker compose logs -f api
```

With `compose.override.yaml` present, the API runs with `--reload` and `./app` mounted, so code changes take effect without restarting the container.

### Rebuild, stop, logs, shell

```powershell
# Rebuild API after dependency or Dockerfile.api changes
docker compose build api
docker compose --profile api up -d

# Stop all services
docker compose down

# Logs
docker compose logs -f api
docker compose logs -f scheduler
docker compose logs -f db

# Shell inside the API container
docker compose exec api /bin/bash
```

### Run the API without Docker

Same as today: from the project root, with a venv and `.env` configured:

```powershell
uv run uvicorn app.main:app --reload
```

API base: `http://localhost:8000`, docs: `http://localhost:8000/scalar`.

### Manual operations inside the running container

```powershell
# Apply migrations manually
docker compose exec scheduler /app/.venv/bin/alembic upgrade head

# Run ETL pipeline manually
docker compose exec scheduler /app/.venv/bin/python /app/scripts/run_etl.py

# With explicit paths
docker compose exec scheduler /app/.venv/bin/python /app/scripts/run_etl.py \
    --instruments /app/data/raw/b3/boletim_diario/2026-03-08/cadastro_instrumentos_20260308.normalized.csv \
    --date 2026-03-08

# Trigger daily scrapers immediately
docker compose exec scheduler /app/docker/run_daily_batch.sh

# Run quote batch manually
docker compose exec scheduler /app/.venv/bin/python /app/scripts/run_b3_quote_batch.py

# Open psql
docker compose exec db psql -U etlb3 -d etlb3

# Check ETL audit log
docker compose exec db psql -U etlb3 -d etlb3 \
    -c "SELECT pipeline_name, status, rows_inserted, started_at FROM etl_runs ORDER BY started_at DESC LIMIT 10;"

# Check hypertable info
docker compose exec db psql -U etlb3 -d etlb3 \
    -c "SELECT * FROM timescaledb_information.hypertables;"
```

### Persistent volumes

| Volume | Contents |
|---|---|
| `db_data` | PostgreSQL data files (TimescaleDB) |
| `scraper_data` | Scraped raw files, JSONL, screenshots, traces |

To reset the database (destructive!):

```powershell
docker compose down -v
docker compose up -d
```

---

## Code quality

Lint and static typing run on **every commit** if you install [pre-commit](https://pre-commit.com/) hooks ([Ruff](https://docs.astral.sh/ruff/), [ty](https://docs.astral.sh/ty/)). See [CONTRIBUTING.md](CONTRIBUTING.md) for the full workflow.

**One-time setup:**

```powershell
uv sync --extra dev
uv run pre-commit install
```

**Manual checks (same as hooks / CI):**

```powershell
uv run ruff check .
uv run ty check
```

Configuration lives in `pyproject.toml` (`[tool.ruff]`, `[tool.ty]`). By default **ty** type-checks `app`, `scripts`, and `docker` only (not `tests/`), to keep the gate stable alongside dynamic test patterns.

---

## Running tests

Layout follows a **testing pyramid** (see [TESTING-STRATEGY.md](TESTING-STRATEGY.md)): fast **unit** tests, **integration** tests (TestClient, `respx`, entrypoints), and optional **E2E** black-box HTTP when `E2E_BASE_URL` is set.

**Default CI / local suite** — no real browser, no live HTTP, no optional DB smoke tests:

```powershell
uv run pytest tests/ -m "not e2e and not live and not db" --tb=short
```

**Fast unit-only loop:**

```powershell
uv run pytest tests/unit -q
```

**With coverage** (`pytest-cov`; config in `pyproject.toml`):

```powershell
uv run pytest tests/ -m "not e2e and not live and not db" --cov --cov-report=term-missing
```

**Optional PostgreSQL smoke** (`@pytest.mark.db`) — requires a reachable `DATABASE_URL` (e.g. `docker compose up -d db` + `alembic upgrade head`):

```powershell
uv run pytest tests/ -m db -v
```

**Optional E2E** — start the API, then set `E2E_BASE_URL` (e.g. `http://127.0.0.1:8000`):

```powershell
$env:E2E_BASE_URL = "http://127.0.0.1:8000"
uv run pytest tests/e2e -m e2e -v
```

### Test layout

| Path | Focus |
|------|--------|
| `tests/unit/` | Pure logic: column mapping, CSV resolver, ticker filter, asset sanitization / upsert SQL shape (mocked session). |
| `tests/integration/test_api.py` | FastAPI `TestClient`: health, Scalar/OpenAPI, route smoke; ETL routes with mocked pipeline; `/quotes/latest` ticker parsing. |
| `tests/integration/test_b3_quotes.py` | B3 client (`respx`), parsers, cache, use cases, quote routes, `read_tickers`, batch ingestion (JSONL + report). |
| `tests/integration/test_scheduler.py` | `docker/scheduler.py` orchestration (mocked subprocess/sleep/DB). |
| `tests/integration/test_cli_entrypoints.py` | CLI `main()` dispatch (mocked pipelines/scrapers). |
| `tests/integration/test_run_daily_batch.py` | `docker/run_daily_batch.sh` contract (bash `-n`, expected invocations). |
| `tests/integration/test_db_smoke.py` | `SELECT 1` against real Postgres when available (skipped if DB down). |
| `tests/e2e/test_api_blackbox.py` | `httpx` against running API (`E2E_BASE_URL`). |
| `tests/conftest.py` | Shared `client` fixture (`TestClient`). |
| `tests/fixtures/` | Static CSV samples and Polars builders. |

Coverage sources: `app`, `docker`, `scripts` (see `[tool.coverage.run]` in `pyproject.toml`).

### CI and repo layout notes

- **GitHub Actions:** [`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs on push and pull requests targeting `main` or `master` (Ruff, ty, pytest with the same marker filter as above). If you remove or rename this workflow, update this README and [CONTRIBUTING.md](CONTRIBUTING.md).
- **Tests** were reorganized from a flat `tests/test_*.py` layout into `tests/unit/`, `tests/integration/`, and `tests/e2e/` (shared `tests/conftest.py`). Older module names such as `test_b3_quote_integration.py` live on as `tests/integration/test_b3_quotes.py`.
- **Legacy DB-heavy integration tests** that lived at paths like `tests/test_load_integration.py` / `tests/test_pipeline.py` are not in the current tree; coverage for loaders/pipelines is intentionally slimmer and documented in the table above.

---

## Alembic migration commands

```powershell
alembic upgrade head        # apply all pending migrations
alembic downgrade -1        # roll back one step
alembic current             # show current revision
alembic history             # show full history
alembic revision --autogenerate -m "describe_change"  # create migration from model
```

---

## Notes

- `alembic upgrade head` is safe to run multiple times (idempotent).
- The TimescaleDB extension is enabled by `docker/initdb/01_timescaledb.sql` which runs automatically on first DB container start.
- The `fact_quotes` hypertable is created in migration `0002_schema_v2` via `SELECT create_hypertable(...)`. The migration works on plain PostgreSQL too — without TimescaleDB, `fact_quotes` is a regular table.
- `flow.py` (Prefect-based flow) is preserved for teams using a Prefect server. For all other cases — local, Docker, scripts, API — use `pipeline.py` directly or `scripts/run_etl.py`.
- `DATABASE_URL` is the single source of truth for the database connection. Individual `DB_HOST`/`DB_PORT`/`DB_USER`/`DB_PASSWORD`/`DB_NAME` components are documented in `.env.example` as reference.

---

License: see PKG-INFO / project metadata
