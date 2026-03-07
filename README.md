# etl-b3

ETL pipeline and REST API for B3 public daily market data (Boletim Diário do Mercado).

Short, focused documentation for getting the project running locally or with Docker.

---

## Overview

- Scrapes B3 daily files (instruments + consolidated trades) using Playwright.
- Normalizes files and runs a 25-minute quote ingestion loop that fetches per-ticker daily fluctuation histories and writes JSONL + report CSV outputs.
- Stores results in PostgreSQL and exposes a FastAPI HTTP API (Scalar docs at /scalar).

---

## Architecture (brief)

scheduler (container or local script)
  ↓
daily scrapers (Playwright): `run_b3_scraper.py`, `run_b3_scraper_negocios.py`
  ↓
normalized CSV (data/raw/b3/boletim_diario/YYYY-MM-DD/)
  ↓
25-minute quote ingestion: `run_b3_quote_batch.py` → JSONL + report CSV
  ↓
loader → PostgreSQL + API

Key entrypoints:
- API app factory: `app/main.py`
- Scheduler/orchestrator (container runtime): `docker/scheduler.py`
- Daily batch runner: `docker/run_daily_batch.sh`
- Quote batch script: `scripts/run_b3_quote_batch.py`

---

## Project structure (high level)

.
├── app/                 # application code: API, ETL, scrapers, config
├── docker/              # entrypoint and helper scripts for container
├── scripts/             # CLI scripts to run scrapers and batches
├── data/                # sample and runtime data (raw, screenshots, traces)
├── tests/               # pytest tests
├── compose.yaml         # docker-compose for the scheduler service
├── Dockerfile           # image used by the `scheduler` service
├── pyproject.toml       # project metadata and dependencies
└── README.md            # this file

See `app/` for routers (`app/api/`), ETL pipeline code (`app/etl/`), and configuration (`app/core/config.py`).

---

## Setup using uv (recommended)

Install uv (official):

- Linux / macOS

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

- Windows PowerShell

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Verify:

```bash
uv --version
```

Project setup (recommended, uses the lockfile present in the repo):

```bash
uv venv
uv sync --locked
```

If you prefer pip and a requirements file, you can use:

```bash
uv pip install -r requirements.txt  # only if you have a requirements file
```

Note: this repository includes `pyproject.toml` and a lockfile approach — `uv sync --locked` reproduces the same environment used by the Docker image.

---

## Running locally (without Docker)

1) Create the virtual environment and install deps

```bash
uv venv
uv sync --locked
```

2) (If you will run Playwright scrapers) install the browser binaries

```bash
# inside the activated uv venv
playwright install chromium
```

3) Run the daily scrapers (Playwright)

```bash
python scripts/run_b3_scraper.py           # cadastro de instrumentos
python scripts/run_b3_scraper_negocios.py  # negocios consolidados
```

4) Run the quote batch (uses normalized CSV auto-discovery by default)

```bash
python scripts/run_b3_quote_batch.py
```

5) Run the API (optional)

```bash
# apply migrations if you use the DB
alembic upgrade head
# start the API with auto-reload
uvicorn app.main:app --reload
# Scalar docs available at: http://localhost:8000/scalar
```

---

## Running with Docker (recommended for full pipeline)

Build and start the scheduler container (it contains Playwright + uv environment):

```powershell
# from repository root (PowerShell / Windows)
docker compose build
docker compose up -d
```

What the `scheduler` container does:
- Ensures `/app/data` directories and permissions (via `docker/entrypoint.sh`).
- Runs the daily Playwright scrapers (`docker/run_daily_batch.sh`).
- Auto-discovers the normalized instruments CSV and then starts the 25-minute quote ingestion loop (`scripts/run_b3_quote_batch.py`).

Useful docker commands:

```powershell
# follow logs
docker compose logs -f scheduler

# run daily batch manually inside the running container
docker compose exec scheduler /app/docker/run_daily_batch.sh --date 2024-06-14

# run quote batch inside the running container
docker compose exec scheduler /app/.venv/bin/python /app/scripts/run_b3_quote_batch.py
```

---

## Quick commands (summary)

- Run scrapers locally:

```bash
python scripts/run_b3_scraper.py
python scripts/run_b3_scraper_negocios.py
```

- Run quote batch locally (auto-discover CSV):

```bash
python scripts/run_b3_quote_batch.py
```

- Start Docker scheduler (build + run):

```bash
docker compose build
docker compose up -d
```

- Start API locally:

```bash
alembic upgrade head
uvicorn app.main:app --reload
```

- Run tests:

```bash
pytest
```

---

## Where outputs go

- Normalized instruments CSVs and raw downloads: `data/raw/b3/boletim_diario/YYYY-MM-DD/`
- Daily quote JSONL and report CSV: `data/raw/b3/daily_fluctuation_history/YYYY-MM-DD/`
- Screenshots and traces: `data/screenshots/`, `data/traces/`

---

## Notes & next steps

- The repository uses `uv` and a lockfile for deterministic installs (`uv sync --locked`).
- The scheduler is implemented in `docker/scheduler.py`; Docker Compose runs a single `scheduler` service that performs daily scrapes and an intraday quote loop.

Suggestions for docs improvements (not required to run):
- Add a small `CONTRIBUTING.md` with dev workflow and how to add Playwright tests.
- Provide a lightweight `requirements.txt` for users who don't use `uv`.
- Document required environment variables (DB URL, PLAYWRIGHT_HEADLESS) in a short table or `.env.example` reference.

---

License: see PKG-INFO / project metadata
