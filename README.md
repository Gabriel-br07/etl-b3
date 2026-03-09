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
  +--> DB load: instruments + trades
  |      dim_assets (upsert)
  |      fact_daily_trades (upsert)
  |
  +--> 25-min quote loop
         run_b3_quote_batch.py -> daily_fluctuation_YYYYMMDDTHHMMSS.jsonl
         |
         +--> DB load: intraday quotes
                fact_quotes (TimescaleDB hypertable, INSERT ON CONFLICT DO NOTHING)

ETL audit trail: etl_runs table records every pipeline execution.
API layer (FastAPI): /assets, /quotes, /etl endpoints
```

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

## Project structure (high level)

```
.
├── app/
│   ├── api/             # FastAPI routers: assets, quotes, etl, health
│   ├── core/            # config (pydantic-settings), logging, constants
│   ├── db/              # SQLAlchemy models, engine, session factory
│   ├── etl/
│   │   ├── ingestion/   # file adapters (local, remote), ticker filter
│   │   ├── loaders/     # db_loader.py: load_assets, load_trades, load_intraday_quotes
│   │   ├── orchestration/
│   │   │   ├── pipeline.py     # standalone ETL pipeline (used by scheduler)
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
├── scripts/             # CLI scripts: run_b3_scraper.py, run_b3_quote_batch.py, ...
├── tests/               # pytest tests (127 unit tests, no real DB required)
├── compose.yaml         # Docker Compose: db (TimescaleDB) + scheduler
├── Dockerfile           # scheduler image
└── pyproject.toml
```

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql://etlb3:etlb3pass@localhost:5432/etlb3` | PostgreSQL connection string |
| `RUN_MIGRATIONS_ON_STARTUP` | `true` | Run `alembic upgrade head` on scheduler start |
| `DAILY_RUN_HOUR` | `20` | Hour to trigger daily scrapers |
| `DAILY_RUN_MINUTE` | `0` | Minute to trigger daily scrapers |
| `SCRAPER_INTERVAL_SECONDS` | `1500` | Quote loop cycle length (25 min) |
| `B3_DATA_DIR` | `/app/data/raw` | Root raw-data directory |
| `LOG_LEVEL` | `INFO` | Python logging level |
| `PLAYWRIGHT_HEADLESS` | `true` | Run browser headless |
| `DB_POOL_SIZE` | `5` | SQLAlchemy engine pool size: number of persistent DB connections to keep in the pool. Tune upward for higher concurrency/workers. |
| `DB_MAX_OVERFLOW` | `10` | SQLAlchemy engine max overflow: number of additional connections to open beyond `DB_POOL_SIZE` when demand spikes. Set to 0 to prevent overflow. |

Copy `.env.example` to `.env` for local development (see below).

---

## Local development setup

### 1. Install dependencies

```powershell
# Windows (uv recommended)
uv venv
uv sync --locked
```

### 2. Start PostgreSQL + TimescaleDB locally

```powershell
# Starts only the db service (not the scheduler)
docker compose up -d db
```

This starts `timescale/timescaledb:2.17.2-pg16` on `localhost:5432`.
The `01_timescaledb.sql` init script enables the TimescaleDB extension automatically on first boot.

### 3. Create a `.env` file

```ini
DATABASE_URL=postgresql://etlb3:etlb3pass@localhost:5432/etlb3
APP_ENV=development
LOG_LEVEL=DEBUG
```

### 4. Run Alembic migrations

```powershell
# Applies 0001_initial_schema + 0002_schema_v2 (includes hypertable creation)
alembic upgrade head
```

Check current migration state:

```powershell
alembic current
alembic history
```

### 5. Run the API

```powershell
uvicorn app.main:app --reload
# Scalar docs: http://localhost:8000/scalar
# OpenAPI JSON: http://localhost:8000/openapi.json
```

### 6. Run scrapers and load data locally

```powershell
# Run daily scrapers (Playwright - needs chromium)
playwright install chromium
python scripts/run_b3_scraper.py
python scripts/run_b3_scraper_negocios.py

# Run quote batch (auto-discovers instruments CSV)
python scripts/run_b3_quote_batch.py
```

After scrapers run, trigger the ETL load manually via the API:

```http
POST http://localhost:8000/etl/run-latest
```

Or run the pipeline directly in Python:

```python
from pathlib import Path
from datetime import date
from app.etl.orchestration.pipeline import (
    run_instruments_and_trades_pipeline,
    run_intraday_quotes_pipeline,
)

# Load instruments + trades
run_instruments_and_trades_pipeline(
    instruments_csv=Path("data/raw/b3/boletim_diario/2024-06-14/cadastro_instrumentos_20240614.normalized.csv"),
    trades_file=Path("data/raw/b3/boletim_diario/2024-06-14/negocios_consolidados_20240614.normalized.csv"),
    target_date=date(2024, 6, 14),
)

# Load JSONL intraday quotes
run_intraday_quotes_pipeline(
    jsonl_path=Path("data/raw/b3/daily_fluctuation_history/2024-06-14/daily_fluctuation_20240614T100000.jsonl"),
)
```

---

## Running with Docker (full pipeline)

```powershell
# Build and start both services (db + scheduler)
docker compose build
docker compose up -d

# Follow logs
docker compose logs -f scheduler
docker compose logs -f db
```

On first start the scheduler will:
1. Wait for the `db` service healthcheck to pass
2. Run `alembic upgrade head` automatically (controlled by `RUN_MIGRATIONS_ON_STARTUP=true`)
3. Wait until the scheduled daily run time, then run the Playwright scrapers
4. Load instruments + trades into the DB
5. Start the 25-minute quote loop, loading each JSONL batch into the hypertable

### Manual operations inside the running container

```powershell
# Apply migrations manually
docker compose exec scheduler /app/.venv/bin/alembic upgrade head

# Trigger daily scrapers immediately
docker compose exec scheduler /app/docker/run_daily_batch.sh --date 2024-06-14

# Run quote batch manually
docker compose exec scheduler /app/.venv/bin/python /app/scripts/run_b3_quote_batch.py

# Open psql against the DB
docker compose exec db psql -U etlb3 -d etlb3

# Check hypertable info
docker compose exec db psql -U etlb3 -d etlb3 -c "SELECT * FROM timescaledb_information.hypertables;"
```

### Persistent volumes

| Volume | Contents |
|---|---|
| `db_data` | PostgreSQL data files (TimescaleDB) |
| `scraper_data` | Scraped raw files, JSONL, screenshots, traces |

To reset the database (destructive!):

```powershell
docker compose down -v   # removes named volumes
docker compose up -d
```

---

## Running tests

```powershell
# All unit tests (no DB or browser required)
python -m pytest tests/ -m "not e2e and not live" --tb=short

# Only the new DB integration tests
python -m pytest tests/test_db_loader.py tests/test_etl_run_repository.py tests/test_pipeline.py tests/test_jsonl_quotes_parser.py -v
```

---

## Alembic migration commands

```powershell
# Apply all pending migrations
alembic upgrade head

# Roll back one step
alembic downgrade -1

# Show current revision
alembic current

# Show full history
alembic history

# Auto-generate a new migration from model changes
alembic revision --autogenerate -m "describe_change"
```

---

## Notes

- `alembic upgrade head` is safe to run multiple times (idempotent).
- The TimescaleDB extension is enabled by `docker/initdb/01_timescaledb.sql` which runs automatically on first DB container start.
- The `fact_quotes` hypertable is created in migration `0002_schema_v2` via `SELECT create_hypertable(...)`, and that migration also runs `CREATE EXTENSION timescaledb` when the extension is available.
- For local development without the Docker DB, set `DATABASE_URL` to point to a PostgreSQL 14+ instance. TimescaleDB is **recommended** (to get `fact_quotes` as a hypertable), but the migrations are written to run on plain PostgreSQL as well; without TimescaleDB, `fact_quotes` remains a regular Postgres table.

---

License: see PKG-INFO / project metadata
