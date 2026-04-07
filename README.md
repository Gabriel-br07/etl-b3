# etl-b3

ETL pipeline and REST API for B3 public daily market data (*Boletim Diário do Mercado*): scrape and load instruments, consolidated trades, daily and intraday quotes, and optional annual COTAHIST history into PostgreSQL/TimescaleDB, with a FastAPI surface and Scalar docs.

## What This Project Does

- **Ingests** B3 bulletin files (Playwright), intraday quote batches (HTTP), and optional COTAHIST annual files.
- **Stores** results in PostgreSQL; intraday series use a TimescaleDB hypertable when the extension is enabled.
- **Serves** JSON over HTTP: assets, trades, quotes (DB and live delayed B3), fact-quotes, COTAHIST, market overview, ETL triggers, and health checks.

## Main Capabilities

- Scheduled **Prefect** flows in Docker: daily cadastro + negócios + registry loads; intraday quote batch inside a configurable B3 session window.
- **CLI scripts** for scrapers, full ETL, quote batch, COTAHIST annual fetch, and manual Prefect-style runs.
- **Optional full (heavy) stack:** COTAHIST one-shot via Compose profile `full` / `cotahist` — not part of the default **light** `docker compose up`.

## Quick Start

### Prerequisites

- **Docker** (Docker Desktop or compatible) — use this for the **recommended** path: the **light** stack (`db` + `scheduler` + `api`) via `docker compose up -d`. That is **not** the same as the optional **COTAHIST** job: heavy annual backfill runs as a **separate one-shot** with Compose profile `full` / `cotahist` (see below), never as part of the default `up`.
- **Python 3.13+** and **[uv](https://docs.astral.sh/uv/)** — required to run the API or scripts **on your machine** (local mode, tests, Alembic on the host, `uv run …`).

### Environment

```powershell
cp .env.example .env
```

The **full variable list and comments** live in [`.env.example`](.env.example) (that file is the source of truth for env names and defaults). Defaults assume Postgres user/db `etlb3` (Compose uses the `db` service; local mode often uses `localhost:5432`).

### Install dependencies (uv)

```powershell
uv venv
uv sync --locked
```

You need this for **local mode** and for any **host-side** commands (`uv run alembic`, `uv run uvicorn`, tests). The Docker images use their own virtualenv inside the container; you do not need `uv` *inside* the container.

### 1) Docker (recommended)

Compose supports two levels of work. **Light** is what most people want day to day; **full** is heavier and optional.

| Mode | What you get | Weight |
|------|----------------|--------|
| **Light (default)** | `db` + `scheduler` (Prefect + Playwright) + `api` — daily bulletin ETL, intraday quote loop, HTTP API | **Lower** — normal CPU, disk, and network for daily use. |
| **Full (optional)** | Everything in **light**, plus the ability to run the **COTAHIST** one-shot service (`compose` profiles `full` / `cotahist`) | **Higher** — large annual downloads/loads, more disk and time; run only when you need historical COTAHIST backfill. |

`docker compose up -d` starts **only the light stack**. The COTAHIST worker is **not** started by default; you run it explicitly when you want the full/heavy path (see below).

**Light stack — build and run:**

```powershell
docker compose build
docker compose up -d
```

- **API:** `http://localhost:8000` — docs: `http://localhost:8000/scalar`, OpenAPI: `http://localhost:8000/openapi.json`
- **Scheduler** runs Prefect `serve` (`daily-registry`, `intraday-quotes`). Details: [docs/architecture.md](docs/architecture.md).

`compose.override.yaml` (if present) adds API hot reload and a bind mount of `./app`. Production-like run without those overrides:

```powershell
docker compose -f compose.yaml up -d
```

**Full stack — COTAHIST only when needed:**

```powershell
docker compose --profile full run --rm cotahist
```

Marker file, env vars, and behavior: [docs/architecture.md](docs/architecture.md#operational-notes).

### 2) Local mode (API on the host)

Use this when you prefer **only Postgres in Docker** (or another Postgres you manage) and run FastAPI **locally** with `uv`.

```powershell
docker compose up -d db
uv run alembic upgrade head
uv run uvicorn app.main:app --reload
```

- Same URLs as above: `http://localhost:8000`, `/scalar`, `/openapi.json`
- Use `uv run` so dependencies come from the project virtualenv.

This path does **not** start the **scheduler** (no automatic scrapes/Prefect). For scraping and scheduled ETL, use **Docker light** or run scripts manually (see [Running the Project](#running-the-project)).

## Running the Project

The [Quick Start](#quick-start) above covers **Docker (light vs full)** and **local API + Docker DB**. Below is a concise reference and extra commands.

### Docker: light stack (logs and rebuild)

```powershell
docker compose up -d
docker compose logs -f scheduler
docker compose logs -f api
```

After changing code under `app/`, **rebuild the scheduler image** if you are not bind-mounting it (default `compose.yaml` does not mount `./app` into `scheduler`):

```powershell
docker compose build scheduler
docker compose up -d --force-recreate scheduler
```

### Local: API on the host

```powershell
uv run uvicorn app.main:app --reload
```

### Individual scripts (from repo root)

| Script | Purpose |
|--------|---------|
| [`scripts/run_etl.py`](scripts/run_etl.py) | Orchestrate pipeline steps (instruments, trades, daily quotes, intraday JSONL, COTAHIST) |
| [`scripts/run_b3_scraper.py`](scripts/run_b3_scraper.py) | Cadastro (instruments) scraper |
| [`scripts/run_b3_scraper_negocios.py`](scripts/run_b3_scraper_negocios.py) | Negócios consolidados scraper |
| [`scripts/run_b3_quote_batch.py`](scripts/run_b3_quote_batch.py) | Intraday quote batch → JSONL |
| [`scripts/run_prefect_daily_flow.py`](scripts/run_prefect_daily_flow.py) | Manual Prefect-style daily chain (options for registry-only, combined, COTAHIST) |
| [`scripts/run_b3_cotahist_annual.py`](scripts/run_b3_cotahist_annual.py) | Download/extract/validate annual COTAHIST (no DB) |

Examples:

```powershell
uv run python scripts/run_etl.py
uv run python scripts/run_b3_scraper.py
uv run python scripts/run_b3_quote_batch.py
```

Inside a running scheduler container:

```powershell
docker compose exec scheduler /app/.venv/bin/python /app/scripts/run_etl.py
docker compose exec scheduler /app/docker/run_daily_batch.sh
```

### COTAHIST worker (optional, heavy)

```powershell
docker compose --profile full run --rm cotahist
```

Details, marker file, and env vars: [docs/architecture.md](docs/architecture.md#operational-notes).

## Common Commands

| Goal | Command |
|------|---------|
| Lint | `uv run ruff check .` |
| Typecheck | `uv run ty check` |
| Tests (default CI filter) | `uv run pytest tests/ -m "not e2e and not live and not db" --tb=short` |
| Migrations | `uv run alembic upgrade head` |
| Compose DB only | `docker compose up -d db` |
| Compose logs | `docker compose logs -f api` |

Dev hooks: `uv sync --locked --extra dev` then `uv run pre-commit install` — see [CONTRIBUTING.md](CONTRIBUTING.md).

## API Access

| Item | URL / note |
|------|------------|
| Base URL (local) | `http://localhost:8000` |
| Health | `GET /health` |
| Human-friendly reference | `GET /scalar` |
| Machine-readable schema | `GET /openapi.json` |

Which endpoint to use for each goal: [docs/data-dictionary.md](docs/data-dictionary.md#api-consumer-guide).

**Note:** `POST /etl/run-latest` and `POST /etl/backfill` only support **local** CSVs under `B3_DATA_DIR` on the API host; `source_mode=remote` returns **501** (use scheduler or scripts for Playwright).

## Documentation Map

| Document | Audience |
|----------|----------|
| [docs/architecture.md](docs/architecture.md) | System context, containers, data flow, ops |
| [docs/data-dictionary.md](docs/data-dictionary.md) | Data meanings and API consumer guide |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Hooks, lint, typecheck, CI |
| [.env.example](.env.example) | **Source of truth** for environment variables (names, defaults, notes) |

**Source-of-truth split (intentional):** This README stays short. Material that used to live in a long README table—**every env var, schedules, bootstrap, COTAHIST flags, compose edge cases**—now belongs in **[`.env.example`](.env.example)** (variables) and **[docs/architecture.md](docs/architecture.md)** (runtime, containers, data flow, operational notes). If something is missing, check those two before opening an issue.

## Intended Audience

This README targets **operators**, **new contributors**, and **API users** who need to run the stack quickly. Deep architecture, data sources, and ETL internals live in **`docs/architecture.md`**; field-level API orientation is in **`docs/data-dictionary.md`**.

## Running Tests

Tests follow a pyramid: fast **unit** tests, **integration** tests (TestClient, `respx`, scripts), optional **E2E** when `E2E_BASE_URL` is set, and optional **db**-marked tests when Postgres is up.

```powershell
uv run pytest tests/ -m "not e2e and not live and not db" --tb=short
uv run pytest tests/unit -q
```

With database (`docker compose up -d db` + migrations):

```powershell
uv run pytest tests/ -m db -v
```

E2E (API must be running):

```powershell
$env:E2E_BASE_URL = "http://127.0.0.1:8000"
uv run pytest tests/e2e -m e2e -v
```

More detail: [CONTRIBUTING.md](CONTRIBUTING.md) and layout under `tests/`.

## Alembic

```powershell
uv run alembic upgrade head
uv run alembic current
uv run alembic history
```

See [Alembic](https://alembic.sqlalchemy.org/) for revision workflows.

## License

See project metadata / PKG-INFO.
