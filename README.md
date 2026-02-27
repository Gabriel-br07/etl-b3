# etl-b3

ETL pipeline and REST API for **B3 public daily market data** (Brazil stock exchange – Boletim Diário do Mercado).

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack Rationale](#tech-stack-rationale)
- [Project Structure](#project-structure)
- [Quick Start (Docker)](#quick-start-docker)
- [Local Setup (without Docker)](#local-setup-without-docker)
- [Environment Variables](#environment-variables)
- [Running Migrations](#running-migrations)
- [Running the ETL](#running-the-etl)
- [API Reference](#api-reference)
- [Scalar API Docs](#scalar-api-docs)
- [Limitations](#limitations)
- [B3 Source Discovery Notes](#b3-source-discovery-notes)
- [Next Steps](#next-steps)

---

## Overview

This MVP ingests B3 end-of-day public files, transforms the data with Polars, loads it into PostgreSQL, and exposes it via a FastAPI REST API documented with Scalar (not Swagger/ReDoc).

**Data sources:**
- *Cadastro de Instrumentos (Listado)* – listed instruments master data
- *Negócios Consolidados do Pregão (Listado)* – daily consolidated trades

Official entrypoint: [B3 Boletim Diário do Mercado](https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/consultas/boletim-diario/boletim-diario-do-mercado/)

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        FastAPI (app/main.py)                      │
│   /health  /assets  /quotes/latest  /quotes/{ticker}/history      │
│   /etl/run-latest  /etl/backfill  /scalar (Scalar docs)           │
└────────────────────────────┬─────────────────────────────────────┘
                             │
                    ┌────────▼────────┐
                    │   Services /    │
                    │  Repositories   │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │   PostgreSQL    │
                    │  dim_assets     │
                    │  fact_daily_    │
                    │  quotes         │
                    │  etl_runs       │
                    └─────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│                     ETL Pipeline (Prefect)                        │
│                                                                    │
│  [Source Adapter]  →  [Parser]  →  [Transform]  →  [Loader]      │
│  LocalFileAdapter      instruments_parser   b3_transforms  db_loader │
│  RemoteAdapter  (TODO) trades_parser                              │
└──────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack Rationale

| Tool | Why |
|------|-----|
| **FastAPI** | Modern async Python web framework, excellent OpenAPI support |
| **Polars** | Significantly faster than Pandas for file transformations, lazy evaluation, type-safe |
| **PostgreSQL** | Reliable RDBMS, excellent for time-series queries, supports upserts natively |
| **SQLAlchemy 2.x** | Pythonic ORM with full async support and Pydantic compatibility |
| **Alembic** | Schema migration management |
| **Prefect** | Simple, observable orchestration with retries and logging built-in |
| **Scalar** | Beautiful, modern API documentation as an alternative to Swagger/ReDoc |
| **Pydantic v2** | Fast validation, settings management |
| **httpx** | Async-capable HTTP client for B3 downloads |
| **Docker** | Reproducible local environment |

---

## Project Structure

```
etl-b3/
├── app/
│   ├── api/              # FastAPI routers
│   ├── core/             # Config, logging, constants
│   ├── db/               # SQLAlchemy engine, session, ORM models
│   ├── schemas/          # Pydantic request/response schemas
│   ├── repositories/     # DB CRUD / upsert logic
│   ├── etl/
│   │   ├── ingestion/    # Source adapters (local + remote)
│   │   ├── parsers/      # CSV/ZIP file parsers
│   │   ├── transforms/   # Polars transformation functions
│   │   ├── loaders/      # DB loading (upsert wrappers)
│   │   └── orchestration/ # Prefect flows
│   ├── scraping/         # Playwright browser-based scrapers
│   │   ├── common/       # BaseScraper, browser factory, storage, exceptions
│   │   ├── b3/           # B3 Boletim Diário scraper (selectors, downloader)
│   │   └── site2/        # TODO: second site (placeholder)
│   └── main.py
├── alembic/              # DB migration scripts
├── tests/
│   ├── e2e/              # Playwright E2E tests (pytest-playwright)
│   └── fixtures/         # Sample B3-like CSV/ZIP files
├── scripts/
│   ├── run_etl.py        # ETL pipeline CLI
│   └── run_b3_scraper.py # Playwright scraper CLI
├── data/
│   ├── sample/           # Local B3 source files (fallback mode)
│   ├── raw/              # Scraper downloads land here
│   ├── screenshots/      # Step / failure screenshots
│   └── traces/           # Playwright trace recordings
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── alembic.ini
├── .env.example
└── README.md
```

---

## Quick Start (Docker)

```bash
# 1. Clone and configure
git clone https://github.com/Gabriel-br07/etl-b3.git
cd etl-b3
cp .env.example .env
```

# 2. Place B3 files in data/sample/ (see naming convention below)
#    Or use the included sample fixtures.

# 3. Start services
```bash
docker-compose up --build
```

# 4. Open Scalar docs
```bash
open http://localhost:8000/scalar
```

### Start only the PostgreSQL database (Docker)

If you only want to run the database (useful to run migrations or start the API locally against a DB), you can:

Option A — using `docker-compose` (recommended when working with this repo):

```powershell
# Start only the 'db' service defined in docker-compose.yml (detached)
docker-compose up -d db

# Follow logs (optional)
docker-compose logs -f db --tail 50

# Check containers and health status
docker-compose ps
```

Option B — using `docker run` (standalone container):

```powershell
# Run Postgres with the same defaults used by the project
docker run --name etl-b3-postgres \
  -e POSTGRES_USER=etlb3 \
  -e POSTGRES_PASSWORD=etlb3pass \
  -e POSTGRES_DB=etlb3 \
  -p 5432:5432 \
  -v etl-b3-postgres-data:/var/lib/postgresql/data \
  -d postgres:16-alpine

# View logs
docker logs -f etl-b3-postgres --tail 50

# Stop and remove when done
docker rm -f etl-b3-postgres
```

Notes:
- The `docker-compose.yml` in this repo exposes Postgres on the host at port `5432` and sets the default credentials to:

```
POSTGRES_USER=etlb3
POSTGRES_PASSWORD=etlb3pass
POSTGRES_DB=etlb3
```

- When connecting from your host (for example from `alembic` or `psql`), use:

```
DATABASE_URL=postgresql://etlb3:etlb3pass@localhost:5432/etlb3
```

- To stop and remove all compose services and volumes (clean state):

```powershell
docker-compose down -v
```

---

## Local Setup (without Docker)

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install -e ".[dev]"

# Configure environment
cp .env.example .env
# Edit DATABASE_URL to point to your PostgreSQL instance

# Run migrations
alembic upgrade head

# Start API
uvicorn app.main:app --reload
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `APP_ENV` | `development` | Application environment |
| `DATABASE_URL` | `postgresql://etlb3:etlb3pass@localhost:5432/etlb3` | PostgreSQL connection URL |
| `B3_DATA_DIR` | `data/sample` | Directory for local B3 source files |
| `B3_BULLETIN_ENTRYPOINT_URL` | B3 Boletim Diário URL | Official B3 page for file discovery |
| `B3_INSTRUMENTS_URL_TEMPLATE` | *(unset)* | Direct download URL template for instruments file |
| `B3_TRADES_URL_TEMPLATE` | *(unset)* | Direct download URL template for trades file |
| `LOG_LEVEL` | `INFO` | Logging level |
| `API_TITLE` | `ETL B3 API` | API documentation title |
| `API_VERSION` | `0.1.0` | API version string |

---

## Running Migrations

```bash
# Apply all migrations
alembic upgrade head

# Create a new migration (after changing models)
alembic revision --autogenerate -m "description"

# Downgrade one step
alembic downgrade -1
```

---

## Running the ETL

### Via API endpoint
```bash
# Trigger ETL for today (local file mode)
curl -X POST http://localhost:8000/etl/run-latest \
  -H "Content-Type: application/json" \
  -d '{"source_mode": "local"}'
```

### Via CLI script
```bash
# Run for today (local mode)
python scripts/run_etl.py

# Run for a specific date
python scripts/run_etl.py --date 2024-06-14

# Run with remote mode (requires URL templates configured)
python scripts/run_etl.py --date 2024-06-14 --mode remote
```

### Local file naming convention
Place B3 files in `data/sample/` (or `B3_DATA_DIR`) with these patterns:
- `CadInstrumento_YYYY-MM-DD.csv` or `CadInstrumento.csv`
- `NegociosConsolidados_YYYY-MM-DD.zip` (or `.csv`) or `NegociosConsolidados.zip`

---

## API Reference

### Health
```bash
curl http://localhost:8000/health
```

### Assets
```bash
# List all assets
curl http://localhost:8000/assets

# Search by ticker or name
curl "http://localhost:8000/assets?q=PETR&limit=10"

# Get specific asset
curl http://localhost:8000/assets/PETR4
```

### Quotes
```bash
# Latest quotes for all tickers
curl http://localhost:8000/quotes/latest

# Latest quotes for specific tickers
curl "http://localhost:8000/quotes/latest?tickers=PETR4,VALE3"

# Historical quotes for a ticker
curl "http://localhost:8000/quotes/PETR4/history?start_date=2024-01-01&end_date=2024-06-14"
```

### ETL
```bash
# Run ETL for today
curl -X POST http://localhost:8000/etl/run-latest \
  -H "Content-Type: application/json" \
  -d '{"source_mode": "local"}'

# Backfill a date range
curl -X POST http://localhost:8000/etl/backfill \
  -H "Content-Type: application/json" \
  -d '{"date_from": "2024-06-01", "date_to": "2024-06-14", "source_mode": "local"}'
```

---

## Scalar API Docs

Open [http://localhost:8000/scalar](http://localhost:8000/scalar) in your browser.

- Swagger UI is **disabled** (`/docs` returns 404)
- ReDoc is **disabled** (`/redoc` returns 404)
- OpenAPI schema is available at `/openapi.json`

---

## Limitations

- **End-of-day data only** – B3 public files are daily closing/consolidated data; this is NOT real-time or intraday data.
- **Local file fallback** – Remote discovery of B3 Boletim Diário files requires manual URL configuration (B3 does not expose a stable machine-readable API). See `B3_INSTRUMENTS_URL_TEMPLATE` and `B3_TRADES_URL_TEMPLATE` env vars.
- **Synchronous ETL** – The `/etl/run-latest` endpoint runs synchronously. For production, move to async task execution (Celery/Prefect server).
- **No authentication** – MVP has no auth. Add OAuth2/API keys before exposing publicly.

---

## B3 Source Discovery Notes

The official B3 Boletim Diário do Mercado page is:
> https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/consultas/boletim-diario/boletim-diario-do-mercado/

**MVP approach:**
1. Files are expected locally in `data/sample/` (or `B3_DATA_DIR`).
2. Configure `B3_INSTRUMENTS_URL_TEMPLATE` and `B3_TRADES_URL_TEMPLATE` for direct download.
3. Full HTML scraping of the B3 page is marked as **TODO** in `app/etl/ingestion/remote_adapter.py`.

**Why not scrape automatically?**
B3 does not provide a stable REST API for public file discovery. The HTML page structure changes. The MVP uses a local fallback with clear extension points for remote adapters.

---

## Playwright Scraper (Browser-Based Download)

The `app/scraping/` module automates the B3 Boletim Diário web page using Playwright
to click through the UI and download the CSV files directly from the browser.

### Architecture

```
app/scraping/
├── __init__.py
├── common/
│   ├── base.py           # BaseScraper ABC + ScrapeResult dataclass
│   ├── browser.py        # Playwright browser/context factory (reads Settings)
│   ├── exceptions.py     # ElementNotFoundError, DownloadError, NavigationError
│   └── storage.py        # Deterministic output-path helpers
├── b3/
│   ├── __init__.py
│   ├── scraper.py        # BoletimDiarioScraper (full browser flow)
│   ├── selectors.py      # B3Selectors – all DOM locators in one place
│   └── downloader.py     # Playwright download capture + ScrapeResult
└── site2/                # TODO: second site (placeholder)
    ├── __init__.py
    └── scraper.py
```

Downloaded files land in:
```
data/raw/b3/boletim_diario/<YYYY-MM-DD>/cadastro_instrumentos_YYYYMMDD.csv
```

### First-time setup

```bash
# Install Playwright browser (once per machine / Docker build)
python -m playwright install chromium
```

### Running the scraper

**Normal run** (reads `PLAYWRIGHT_HEADLESS` from `.env`, default = headed/visible):

```bash
python scripts/run_b3_scraper.py
```

**Run for a specific date:**

```bash
python scripts/run_b3_scraper.py --date 2024-06-14
```

**Visual / debug run** – browser visible, slowed down, screenshots after each step:

```bash
python scripts/run_b3_scraper.py --no-headless --slow-mo 500 --screenshots
```

**Full debug** – headed + slow + screenshots + trace recording:

```bash
python scripts/run_b3_scraper.py --no-headless --slow-mo 800 --screenshots --traces
```

**Playwright Inspector** (step through each action interactively):

```powershell
# Windows PowerShell
$env:PWDEBUG="1"; python scripts/run_b3_scraper.py --no-headless

# Linux / macOS
PWDEBUG=1 python scripts/run_b3_scraper.py --no-headless
```

**View a saved trace:**

```bash
playwright show-trace data/traces/b3/trace_2024-06-14_success.zip
```

### Scraper environment variables

| Variable | Default | Description |
|---|---|---|
| `PLAYWRIGHT_HEADLESS` | `false` | `true` = headless (CI), `false` = visible browser (dev) |
| `PLAYWRIGHT_SLOW_MO` | `0` | ms delay between actions; `300–800` is good for watching |
| `PLAYWRIGHT_TIMEOUT_MS` | `30000` | Default wait timeout for elements and navigation |
| `PLAYWRIGHT_DOWNLOADS_DIR` | `data/raw` | Playwright temp downloads directory |
| `B3_OUTPUT_DIR` | `data/raw` | Root dir for saved scraper output |
| `B3_SCREENSHOTS_DIR` | `data/screenshots` | Where step/failure screenshots are saved |
| `B3_TRACE_DIR` | `data/traces` | Where Playwright traces are saved |

### Running E2E tests

**All scraper tests (requires internet + Playwright Chromium):**

```bash
pytest tests/e2e/
```

**Selector resilience tests only** (no network, fast – great for CI):

```bash
pytest tests/e2e/test_b3_scraper.py::TestB3SelectorsResilience -v
```

**Skip all live/e2e tests** (existing unit tests only):

```bash
pytest -m "not e2e and not live"
```

**Headed browser with slow-mo** (watch the test run):

```bash
pytest tests/e2e/ --headed --slowmo 500
```

**Playwright Inspector in tests** (step through test actions):

```powershell
# Windows PowerShell
$env:PWDEBUG="1"; pytest tests/e2e/ --headed -s
```

**Always-on trace recording** (saves zip to `data/traces/`):

```bash
pytest tests/e2e/ --tracing=on --output=data/traces
```

**Screenshots on failure** land automatically in `data/screenshots/e2e/` via the conftest hook.

---

## Next Steps

- [x] Implement HTML scraping of B3 Boletim Diário page for automatic file discovery (Playwright scraper in `app/scraping/b3/`)
- [ ] Implement site 2 scraper (provide URL + interaction steps — placeholder is ready in `app/scraping/site2/`)
- [ ] Add Prefect server / scheduling (e.g., daily cron at 20:00 BRT)
- [ ] Add async background tasks for ETL execution (FastAPI BackgroundTasks or Celery)
- [ ] Intraday data provider integration (B3 FTP or paid data vendor)
- [ ] dbt layer for analytical models on top of fact tables
- [ ] Observability: OpenTelemetry, Prometheus metrics, Grafana dashboards
- [ ] Authentication: API keys / OAuth2
- [ ] CI/CD pipeline (GitHub Actions)
- [ ] Data quality checks (Great Expectations or dbt tests)
- [ ] Corporate actions / splits handling

