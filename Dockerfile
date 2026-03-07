# =============================================================================
# Dockerfile — etl-b3 unified scraper/scheduler image
# =============================================================================
#
# Single image for the `scheduler` service.  Replaces:
#   docker/Dockerfile.base
#   docker/Dockerfile.scheduler
#   docker/Dockerfile.scraper_daily
#   docker/Dockerfile.scraper_25m
#   docker/Dockerfile.init
#
# Provides:
#   - Python 3.13 + uv
#   - Playwright / Chromium  (for the daily B3 scrapers)
#   - All project Python dependencies
#   - Non-root `scraper` user (uid 1001) for runtime
#   - Pre-created data directory tree (avoids permission errors on volume mount)
#
# Build & run:
#   docker compose build
#   docker compose up -d
# =============================================================================

FROM python:3.13-slim

# ---------------------------------------------------------------------------
# System packages
#   tzdata            — timezone database (America/Sao_Paulo)
#   gcc / build-essential / python3-dev — C extensions for some deps
#   libpq-dev         — PostgreSQL client headers (psycopg2)
#   util-linux        — flock, used in run_daily_batch.sh
# ---------------------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
    gcc \
    build-essential \
    python3-dev \
    libpq-dev \
    util-linux \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Timezone
# ---------------------------------------------------------------------------
ENV TZ=America/Sao_Paulo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# ---------------------------------------------------------------------------
# uv — fast Python package manager
# ---------------------------------------------------------------------------
COPY --from=ghcr.io/astral-sh/uv:0.7.2 /uv /uvx /bin/

WORKDIR /app

# ---------------------------------------------------------------------------
# Python dependencies — cache-friendly: lockfiles first, source second
# ---------------------------------------------------------------------------
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-install-project

COPY . .
RUN uv sync --locked

ENV PATH="/app/.venv/bin:$PATH"

# ---------------------------------------------------------------------------
# Playwright runtime environment
# ---------------------------------------------------------------------------
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV XDG_CONFIG_HOME=/tmp/.chromium
ENV XDG_CACHE_HOME=/tmp/.chromium
ENV PLAYWRIGHT_HEADLESS=true

# Install OS-level browser dependencies as root (apt-get only, no binary)
RUN /app/.venv/bin/python -m playwright install-deps chromium

# ---------------------------------------------------------------------------
# Non-root runtime user + directory structure
#
# All directories are created here as root, then ownership is transferred to
# uid 1001 in ONE chown call — avoids repeated chown layers.
# The volume mount (/app/data) will overlay these dirs at runtime; the pre-
# created tree ensures the first run never hits "Permission denied" even
# before the volume is populated, because Docker merges image layers with the
# volume at mount time and the scraper user already owns the paths.
# ---------------------------------------------------------------------------
RUN groupadd -r scraper && useradd -r -g scraper -u 1001 -d /home/scraper -s /bin/sh scraper \
 && mkdir -p \
    /home/scraper/.prefect \
    "${PLAYWRIGHT_BROWSERS_PATH}" \
    /tmp/.chromium \
    /app/data/raw/b3/boletim_diario \
    /app/data/raw/b3/daily_fluctuation_history \
    /app/data/screenshots/b3 \
    /app/data/traces/e2e \
 && chmod 1777 /tmp/.chromium \
 && chmod +x /app/docker/run_daily_batch.sh /app/docker/entrypoint.sh \
 && chown -R scraper:scraper /app /home/scraper "${PLAYWRIGHT_BROWSERS_PATH}" /tmp/.chromium

# ---------------------------------------------------------------------------
# Prefect and HOME — must be set before any Python process runs.
# useradd -r (system account) defaults HOME=/root; override explicitly so
# Prefect never attempts to read/write /root/.prefect in any code path.
# ---------------------------------------------------------------------------
ENV HOME=/home/scraper
ENV PREFECT_HOME=/home/scraper/.prefect
ENV PREFECT_PROFILES_PATH=/home/scraper/.prefect/profiles.toml

# Install Chromium browser binary as scraper (avoids a 300 MB chown layer)
# NOTE: entrypoint.sh runs as root and drops to scraper; the Playwright install
# below must still run as scraper so the binary is owned by scraper from the start.
USER scraper
RUN /app/.venv/bin/python -m playwright install chromium

# ---------------------------------------------------------------------------
# Entrypoint: runs as root → fixes volume ownership → exec as scraper
# CMD: the scheduler (passed to entrypoint as "$@")
# ---------------------------------------------------------------------------
USER root
ENTRYPOINT ["/app/docker/entrypoint.sh"]
CMD ["/app/.venv/bin/python", "/app/docker/scheduler.py"]
