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
- [Batch Quote Ingestion (JSONL + Report)](#batch-quote-ingestion-jsonl--report)
- [Dual-Source Ticker Filter](#dual-source-ticker-filter)
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
│   │   ├── ingestion/
│   │   │   ├── ticker_reader.py    # Read tickers from normalized instruments CSV
│   │   │   ├── ticker_filter.py    # ★ Dual-source ticker filter (master + negocios)
│   │   │   ├── local_adapter.py
│   │   │   └── remote_adapter.py
│   │   ├── parsers/      # CSV/ZIP file parsers (instruments + trades)
│   │   ├── transforms/   # Polars transformation functions
│   │   ├── loaders/      # DB loading (upsert wrappers)
│   │   └── orchestration/ # Prefect flows
│   ├── integrations/b3/  # B3 live-quote HTTP client, parser, models, service
│   ├── use_cases/quotes/
│   │   ├── batch_ingestion.py  # ★ Batch quote ingestion → JSONL + report CSV
│   │   └── ...
│   ├── scraping/         # Playwright browser-based scrapers
│   │   ├── common/       # BaseScraper, browser factory, storage, exceptions
│   │   ├── b3/           # B3 Boletim Diário scraper (selectors, downloader)
│   │   └── site2/        # TODO: second site (placeholder)
│   └── main.py
├── alembic/              # DB migration scripts
├── tests/
│   ├── fixtures/         # Sample B3-like CSV/ZIP files + filter test fixtures
│   ├── test_ticker_filter.py       # ★ 55 tests for dual-source filter
│   ├── test_b3_quote_integration.py
│   └── ...
├── scripts/
│   ├── run_b3_quote_batch.py  # ★ Batch quote CLI (auto-filter by default)
│   ├── run_b3_scraper.py      # Playwright scraper CLI (cadastro_instrumentos)
│   ├── run_b3_scraper_negocios.py  # Playwright scraper CLI (negocios_consolidados)
│   └── run_etl.py
├── data/
│   ├── sample/           # Local B3 source files (fallback mode)
│   ├── raw/
│   │   └── b3/
│   │       ├── boletim_diario/<YYYY-MM-DD>/   # Scraper downloads
│   │       └── daily_fluctuation_history/<YYYY-MM-DD>/  # ★ JSONL + report output
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

## Batch Quote Ingestion (JSONL + Report)

The script `scripts/run_b3_quote_batch.py` fetches live delayed quotes from the B3
`DailyFluctuationHistory` endpoint for every ticker resolved by the filter pipeline
and writes two output files per run.

### Output files

| File | Description |
|---|---|
| `daily_fluctuation_{YYYYMMDDTHHMMSS}.jsonl` | One JSON line per ticker — metadata + full intraday price history array |
| `report_{YYYYMMDDTHHMMSS}.csv` | One row per attempted ticker — HTTP status, success flag, data-point count |

Both files land in:
```
data/raw/b3/daily_fluctuation_history/<reference-date>/
```

### JSONL line schema

Each line in the `.jsonl` file is a self-contained JSON object:

```json
{
  "ticker_requested": "PETR4",
  "ticker_returned":  "PETR4",
  "business_status_code": "OK",
  "message_datetime": "2026-02-26T15:30:00",
  "trade_date":  "2026-02-26",
  "collected_at": "2026-02-26T18:00:00+00:00",
  "points_count": 120,
  "price_history": [
    {
      "quote_time": "2026-02-26T10:01:00",
      "close_price": "37.90",
      "price_fluctuation_percentage": "-0.10"
    },
    { "...": "..." }
  ]
}
```

### Report CSV columns

| Column | Description |
|---|---|
| `ticker_requested` | Ticker sent to the API |
| `ticker_returned` | Ticker symbol in the response |
| `trade_date` | Trade date from the response |
| `http_status` | HTTP status code (`200`, `404`, `429`, etc.) or `ERROR` |
| `request_succeeded` | `true` / `false` |
| `points_count` | Number of intraday price points returned |
| `error_message` | Empty on success; exception message on failure |

### Running the batch

**Default run** — the dual-source filter is applied automatically:

```bash
uv run python scripts/run_b3_quote_batch.py
```

> By default, the script looks for the companion `negocios_consolidados_*.normalized.csv`
> **in the same folder** as the instruments CSV and activates the ticker filter.
> If the file is not found it falls back gracefully to the full instruments list.

**Explicit instruments file + date label:**

```bash
uv run python scripts/run_b3_quote_batch.py \
  --instruments data/raw/b3/boletim_diario/2026-02-26/cadastro_instrumentos_20260226.normalized.csv \
  --date 2026-02-26
```

**Explicit trades file (custom path or different date):**

```bash
uv run python scripts/run_b3_quote_batch.py \
  --instruments data/raw/b3/boletim_diario/2026-02-26/cadastro_instrumentos_20260226.normalized.csv \
  --trades      data/raw/b3/boletim_diario/2026-02-25/negocios_consolidados_20260225.normalized.csv \
  --date 2026-02-26
```

**Use full master list (skip negocios filter):**

```bash
uv run python scripts/run_b3_quote_batch.py --filter-mode fallback
```

**Disable auto-discovery entirely (no filter at all):**

```bash
uv run python scripts/run_b3_quote_batch.py --no-auto-trades
```

**Custom output directory:**

```bash
uv run python scripts/run_b3_quote_batch.py --output-dir data/output/quotes
```

### CLI reference

| Flag | Default | Description |
|---|---|---|
| `--instruments PATH` | `data/raw/b3/boletim_diario/2026-02-26/cadastro_instrumentos_20260226.normalized.csv` | Path to normalized instruments CSV |
| `--trades PATH` | *(auto)* | Explicit path to normalized negocios CSV — overrides auto-discovery |
| `--no-auto-trades` | `false` | Disable auto-discovery; query all instruments tickers unfiltered |
| `--filter-mode` | `strict` | `strict` = master ∩ negocios · `fallback` = full master list |
| `--date YYYY-MM-DD` | today | Reference date for the output folder |
| `--output-dir DIR` | `data/raw/b3/daily_fluctuation_history` | Root output directory |

---

## Dual-Source Ticker Filter

The filter pipeline at `app/etl/ingestion/ticker_filter.py` combines two B3 data
sources to build the best possible list of tickers before hitting the quote endpoint,
avoiding useless HTTP requests and rate-limit pressure.

### Why two sources?

| Source alone | Problem |
|---|---|
| Only `cadastro_instrumentos` | Contains many listed instruments that had zero trades (options, warrants, inactive equities) — generates many 404 / empty responses |
| Only `negocios_consolidados` | Only reflects the **previous trading day** — valid listed equities that haven't traded recently would be silently dropped |

The filter uses **both together**: `cadastro_instrumentos` as the authoritative master
universe, `negocios_consolidados` as a recent-activity signal.

### Filter rules

**Master structural filter** (`cadastro_instrumentos`):

| Column | Rule |
|---|---|
| `Segmento` | must equal `CASH` |
| `Mercado` | must equal `EQUITY-CASH` |
| `Categoria` | must equal `SHARES` |
| `Data inicio negocio` | ≤ reference date (or absent → accept) |
| `Data fim negocio` | null / empty **or** ≥ reference date |

**Operational filter** (`negocios_consolidados`):

| Column | Rule |
|---|---|
| `Quantidade de negocios` | > 0 |
| `Preco de fechamento` | non-null and non-empty |

> Column matching is **accent- and case-insensitive**: `"Data início negócio"`,
> `"DATA INICIO NEGOCIO"` and `"data inicio negocio"` all resolve correctly.

### Output

```python
from app.etl.ingestion.ticker_filter import build_ticker_filter

result = build_ticker_filter(
    instruments_csv="data/raw/b3/.../cadastro_instrumentos_20260226.normalized.csv",
    trades_csv="data/raw/b3/.../negocios_consolidados_20260226.normalized.csv",
    reference_date=date(2026, 2, 26),
)

result.strict_filtered_tickers   # master ∩ negocios — preferred (fewer, higher-quality)
result.fallback_master_tickers    # all master tickers — safety net
result.master_only_tickers        # master − negocios — for diagnostics / logging
```

### Module responsibilities

| File | Responsibility |
|---|---|
| `app/etl/ingestion/ticker_filter.py` | `build_ticker_filter()` — public API; `_apply_master_filter()`, `_apply_negocios_filter()` — rules; `_normalise_key()` — accent/case normalisation |
| `app/use_cases/quotes/batch_ingestion.py` | Calls `build_ticker_filter` when `trades_csv` is provided; selects `strict` or `fallback` list based on `filter_mode` |
| `scripts/run_b3_quote_batch.py` | CLI entry-point; auto-discovers the negocios sibling in the same folder |
| `tests/test_ticker_filter.py` | 55 unit + integration tests |

### Important: the endpoint is the final truth

The filter only **reduces** the request pool.  
If a ticker passes the filter but the B3 endpoint returns no `lstQtn` data (empty
price history), that is handled downstream in the ingestion stage — the ticker still
gets a row in the report CSV with `points_count = 0`.

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
|---|---:|---|
| `PLAYWRIGHT_HEADLESS` | `false` | `true` = headless (CI), `false` = visible browser (dev) |
| `PLAYWRIGHT_SLOW_MO` | `0` | ms delay between actions; `300–800` is good for watching |
| `PLAYWRIGHT_TIMEOUT_MS` | `30000` | Default wait timeout for elements and navigation |
| `PLAYWRIGHT_DOWNLOADS_DIR` | `data/raw` | Playwright temporary downloads directory (used by the browser during capture) |
| `PLAYWRIGHT_PAUSE_AFTER_OPEN_MS` | `2000` | Pause (ms) right after opening the entrypoint page to allow dynamic content to render |
| `PLAYWRIGHT_PAUSE_BETWEEN_ACTIONS_MS` | `800` | Small pause (ms) between interactive actions (clicks/selects) to improve stability/observability |
| `B3_OUTPUT_DIR` | `data/raw` | Root directory for saved scraper output |
| `B3_SCREENSHOTS_DIR` | `data/screenshots` | Where step/failure screenshots are saved |
| `B3_TRACE_DIR` | `data/traces` | Where Playwright traces are saved |
| `PWDEBUG` | `1` | Playwright debug inspector flag — set to `1` to enable the interactive inspector (PWDEBUG) |

---

## B3 Live Quote Snapshot Integration

### What it does

The integration at `app/integrations/b3/` provides access to **public delayed quote snapshots** directly from a B3 internal market-data endpoint, without Playwright or browser automation:

```
GET /mds/api/v1/DailyFluctuationHistory/{ticker}
```

This is a public, unauthenticated endpoint discovered in B3's front-end traffic.  
Data is **always delayed** (not real-time) and is clearly labelled as such in every response (`"delayed": true`, `"source": "b3_public_internal_endpoint"`).

### Important notes

- **The endpoint may change over time.** B3 has not publicly documented this path.  If it stops working, check B3's public quote pages in DevTools and update `B3_QUOTE_BASE_URL` in your `.env` or `app/integrations/b3/constants.py`.
- **Cookies are obtained naturally.** `warm_session()` visits the public B3 quote page first, letting the server set any required cookies without hardcoding them. No DevTools cookie capture is needed.
- **No Playwright dependency at runtime.** The entire flow uses `httpx` only.

### Folder responsibilities

| Path | Responsibility |
|---|---|
| `app/integrations/b3/constants.py` | All URLs, header defaults, and string literals — one place to update when the API changes |
| `app/integrations/b3/exceptions.py` | Domain exceptions (`B3TickerNotFoundError`, `B3TemporaryBlockError`, `B3UnexpectedResponseError`) |
| `app/integrations/b3/models.py` | `RawDailyFluctuation` (mirrors API JSON) and `NormalizedQuote` (clean domain output) |
| `app/integrations/b3/client.py` | `B3QuoteClient` — all HTTP transport, session warm-up, retry logic |
| `app/integrations/b3/parser.py` | Converts raw JSON `dict` → `NormalizedQuote`; raises on unexpected shape |
| `app/integrations/b3/service.py` | `B3QuoteService` — orchestrates client + parser + in-memory TTL cache |
| `app/use_cases/quotes/get_daily_fluctuation.py` | Thin use-case function; the only symbol imported by the API route |
| `app/api/quotes.py` | `GET /quotes/{ticker}/snapshot` endpoint |

### API endpoint

```
GET /quotes/{ticker}/snapshot
```

**Success (200):**
```json
{
  "ticker": "PETR4",
  "trade_date": "2024-06-14",
  "last_price": "38.45",
  "min_price": "37.90",
  "max_price": "38.90",
  "average_price": "38.22",
  "oscillation_pct": "1.25",
  "source": "b3_public_internal_endpoint",
  "delayed": true,
  "fetched_at": "2024-06-14T15:00:00+00:00"
}
```

**Error codes:**
| Code | Meaning |
|---|---|
| `404` | Ticker not found on B3 |
| `503` | B3 is temporarily rate-limiting / blocking (403 or 429 upstream) |
| `502` | Unexpected upstream response (bad JSON, unknown HTTP status) |

### Configuration

All values have sensible defaults and can be overridden via environment variables or `.env`:

| Variable | Default | Description |
|---|---|---|
| `B3_QUOTE_BASE_URL` | `https://cotacao.b3.com.br/mds/api/v1` | Base URL for the market-data proxy |
| `B3_QUOTE_WARM_SESSION_URL` | B3 public quote page URL | Page visited to obtain session cookies |
| `B3_QUOTE_TIMEOUT` | `15.0` | HTTP request timeout in seconds |
| `B3_QUOTE_HTTP2` | `false` | Enable HTTP/2 for the quote client |
| `B3_QUOTE_CACHE_TTL` | `300` | Per-ticker in-memory cache TTL in seconds (0 = disabled) |

### Running the tests

```bash
# All B3 integration tests (no network calls — fully mocked via respx)
uv run pytest tests/test_b3_quote_integration.py -v

# Full suite
uv run pytest tests/ -m "not e2e and not live" -v
```

---

## Next Steps

- [x] Implement HTML scraping of B3 Boletim Diário page for automatic file discovery (Playwright scraper in `app/scraping/b3/`)
- [x] Batch quote ingestion CLI — single JSONL output + report CSV (`scripts/run_b3_quote_batch.py`)
- [x] Dual-source ticker filter — `cadastro_instrumentos` × `negocios_consolidados` (`app/etl/ingestion/ticker_filter.py`)
- [x] Auto-discovery of negocios sibling file — filter active by default with zero config
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

