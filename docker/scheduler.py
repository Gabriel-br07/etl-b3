#!/usr/bin/env python3
"""
docker/scheduler.py — unified ETL pipeline orchestrator
=========================================================

Drives the full daily pipeline inside a single container:

    [scheduler] ensuring data dirs
    [scheduler] running daily scrapers          ← run_b3_scraper.py + run_b3_scraper_negocios.py
    [scheduler] csv generated successfully      ← cadastro_instrumentos_YYYYMMDD.normalized.csv
    [scheduler] starting 25m loop               ← run_b3_quote_batch.py, every SCRAPER_INTERVAL_SECONDS

CSV resolution strategy
-----------------------
1. Try today's CSV
2. If not found → try yesterday's CSV (fallback)
3. Retry up to RETRY_COUNT times (RETRY_DELAY_SECONDS apart)
4. If still missing after all retries → log fatal, sleep and retry next cycle

Environment variables
---------------------
DAILY_RUN_HOUR            Hour to trigger the daily batch (default: 20)
DAILY_RUN_MINUTE          Minute to trigger (default: 0)
B3_DATA_DIR               Root raw-data directory (default: /app/data/raw)
RETRY_COUNT               CSV-lookup retries (default: 3)
RETRY_DELAY_SECONDS       Seconds between retries (default: 60)
SCRAPER_INTERVAL_SECONDS  Quote-scraper cycle length in seconds (default: 1500 = 25 min)
LOG_LEVEL                 Python logging level (default: INFO)
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
    force=True,
)
log = logging.getLogger("scheduler")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DAILY_RUN_HOUR: int   = int(os.environ.get("DAILY_RUN_HOUR", "20"))
DAILY_RUN_MINUTE: int = int(os.environ.get("DAILY_RUN_MINUTE", "0"))
B3_DATA_DIR: Path     = Path(os.environ.get("B3_DATA_DIR", "/app/data/raw"))
RETRY_COUNT: int      = int(os.environ.get("RETRY_COUNT", "3"))
RETRY_DELAY: int      = int(os.environ.get("RETRY_DELAY_SECONDS", "60"))
SCRAPER_INTERVAL: int = int(os.environ.get("SCRAPER_INTERVAL_SECONDS", "1500"))

BOLETIM_DIR: Path = B3_DATA_DIR / "b3" / "boletim_diario"
PYTHON:       str = "/app/.venv/bin/python"
DAILY_BATCH:  str = "/app/docker/run_daily_batch.sh"
QUOTE_SCRIPT: str = "/app/scripts/run_b3_quote_batch.py"


# ---------------------------------------------------------------------------
# CSV discovery
# ---------------------------------------------------------------------------

from app.etl.orchestration.csv_resolver import (
    resolve_instruments_csv,
    ensure_data_dirs as resolver_ensure_data_dirs,
    CSVNotFoundError,
)


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def run_daily_scrapers() -> bool:
    """Run run_daily_batch.sh and return True on success."""
    batch = Path(DAILY_BATCH)
    if not batch.exists():
        log.error("[scheduler] daily batch script not found: %s", DAILY_BATCH)
        return False
    if not os.access(batch, os.X_OK):
        log.error(
            "[scheduler] daily batch script is not executable: %s  "
            "(fix: chmod +x %s)",
            DAILY_BATCH, DAILY_BATCH,
        )
        return False

    log.info("[scheduler] running daily scrapers  script=%s", DAILY_BATCH)
    start  = time.monotonic()
    result = subprocess.run([DAILY_BATCH], capture_output=False, text=True)
    elapsed = int(time.monotonic() - start)
    if result.returncode == 0:
        log.info("[scheduler] daily scrapers done  elapsed=%ds", elapsed)
        return True
    log.error(
        "[scheduler] daily scrapers failed  exit=%d  elapsed=%ds",
        result.returncode, elapsed,
    )
    return False


def run_quote_batch(instruments_csv: Path) -> bool:
    """Run run_b3_quote_batch.py for one cycle and return True on success."""
    log.info("[scheduler] quote batch start  instruments=%s", instruments_csv)
    start  = time.monotonic()
    result = subprocess.run(
        [PYTHON, QUOTE_SCRIPT, "--instruments", str(instruments_csv)],
        capture_output=False,
        text=True,
    )
    elapsed = int(time.monotonic() - start)
    if result.returncode == 0:
        log.info("[scheduler] quote batch ok  elapsed=%ds", elapsed)
        return True
    log.error(
        "[scheduler] quote batch failed, retrying next cycle  exit=%d  elapsed=%ds",
        result.returncode, elapsed,
    )
    return False


# ---------------------------------------------------------------------------
# Scheduler helpers
# ---------------------------------------------------------------------------


def seconds_until(hour: int, minute: int) -> int:
    """Seconds until the next occurrence of hour:minute (today or tomorrow)."""
    now    = datetime.now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return int((target - now).total_seconds())


def marker_path(d: date) -> Path:
    """Marker file written after a successful daily batch run.

    Written inside B3_DATA_DIR (e.g. /app/data/raw) which is always owned by
    scraper after entrypoint.sh runs.  Using the volume root (parent) would
    risk a PermissionError if chown hasn't propagated yet.
    """
    return B3_DATA_DIR / f".daily_batch_ran_{d.isoformat()}"


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("=" * 60)
    log.info("[scheduler] starting")
    log.info("  daily run    : %02d:%02d local", DAILY_RUN_HOUR, DAILY_RUN_MINUTE)
    log.info("  data dir     : %s", B3_DATA_DIR)
    log.info("  retry count  : %d", RETRY_COUNT)
    log.info("  retry delay  : %ds", RETRY_DELAY)
    log.info("  quote interval: %ds (%.0fmin)", SCRAPER_INTERVAL, SCRAPER_INTERVAL / 60)
    log.info("=" * 60)

    # Ensure directories via the shared resolver helper to avoid drift
    try:
        resolver_ensure_data_dirs(B3_DATA_DIR)
    except PermissionError:
        # Match previous behaviour: fatal and exit if we cannot create dirs
        log.critical("[scheduler] fatal: cannot create data directories (PermissionError)")
        sys.exit(1)

    while True:
        today  = date.today()
        marker = marker_path(today)

        # ------------------------------------------------------------------
        # Phase 1 — Daily scrapers (once per calendar day)
        # ------------------------------------------------------------------
        if not marker.exists():
            # New scheduling policy: we will run the daily scrapers at the configured
            # DAILY_RUN_HOUR:DAILY_RUN_MINUTE. If the scheduler starts after that
            # time and today's marker is missing, we consider the scheduled window
            # missed and will run the scrapers immediately ("run-missed" policy).
            # If the scheduler starts before the configured time, we wait until the
            # scheduled time.
            secs_to_run = seconds_until(DAILY_RUN_HOUR, DAILY_RUN_MINUTE)
            now = datetime.now()
            target_time = now.replace(hour=DAILY_RUN_HOUR, minute=DAILY_RUN_MINUTE, second=0, microsecond=0)

            if target_time <= now:
                # We're past the scheduled time for today — run immediately (missed window)
                log.info("[scheduler] marker missing and scheduled time already passed — running missed daily scrapers now")
                ok = run_daily_scrapers()
            else:
                # We're before the scheduled time — sleep until then
                log.info("[scheduler] marker missing and scheduled time in %ds — sleeping until scheduled run", int(secs_to_run))
                time.sleep(secs_to_run)
                ok = run_daily_scrapers()

            if ok:
                marker.parent.mkdir(parents=True, exist_ok=True)
                marker.touch()
                log.info("[scheduler] marker written: %s", marker)
            else:
                log.error(
                    "[scheduler] daily scrapers failed — will retry in %ds", RETRY_DELAY
                )
                time.sleep(RETRY_DELAY)
                continue
        else:
            log.info("[scheduler] daily scrapers already ran today (%s)", today)

        # ------------------------------------------------------------------
        # Phase 2 — Validate CSV (use shared resolver)
        # ------------------------------------------------------------------
        try:
            csv_path = resolve_instruments_csv(
                data_dir=B3_DATA_DIR,
                retry_count=RETRY_COUNT,
                retry_delay_seconds=RETRY_DELAY,
            )
        except CSVNotFoundError:
            secs = min(RETRY_DELAY, 120)
            log.warning(
                "[scheduler] no instruments csv available — sleeping %ds before retry", secs
            )
            time.sleep(secs)
            continue

        log.info("[scheduler] csv generated successfully: %s", csv_path)

        # ------------------------------------------------------------------
        # Phase 3 — Intraday quote scraper loop
        # ------------------------------------------------------------------
        log.info("[scheduler] starting 25m loop")
        while True:
            # Refresh date in case we crossed midnight
            current_today = date.today()

            # Break out of quote loop if a new day has started (daily run needed)
            if current_today != today:
                log.info(
                    "[scheduler] new day detected (%s) — returning to daily phase",
                    current_today,
                )
                break

            # Break out if the next daily run window is approaching
            secs_to_run = seconds_until(DAILY_RUN_HOUR, DAILY_RUN_MINUTE)
            if secs_to_run < SCRAPER_INTERVAL:
                log.info(
                    "[scheduler] daily run in %ds — pausing quote loop", secs_to_run
                )
                time.sleep(secs_to_run)
                break

            # Refresh CSV path at the top of each cycle (protects against
            # yesterday-fallback becoming stale when a new day's CSV arrives)
            try:
                fresh_csv = resolve_instruments_csv(
                    data_dir=B3_DATA_DIR,
                    retry_count=0,
                    retry_delay_seconds=0,
                )
            except CSVNotFoundError:
                fresh_csv = None

            if fresh_csv:
                csv_path = fresh_csv

            cycle_start = time.monotonic()
            run_quote_batch(csv_path)
            elapsed = int(time.monotonic() - cycle_start)

            sleep_for = max(SCRAPER_INTERVAL - elapsed, 60)
            log.info("[scheduler] sleeping %ds until next quote cycle", sleep_for)
            time.sleep(sleep_for)


if __name__ == "__main__":
    main()

