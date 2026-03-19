#!/usr/bin/env python3
"""
docker/scheduler.py — unified ETL pipeline orchestrator
=========================================================

Reworked scheduling logic (2026-03):
- Run daily scrapers once immediately on startup
- Run intraday quotes immediately on startup
- Schedule next daily run at the next occurrence of DAILY_RUN_HOUR:DAILY_RUN_MINUTE
- Schedule intraday runs every SCRAPER_INTERVAL seconds (next = now + SCRAPER_INTERVAL)
- Do not use marker to block the startup-run
- Failures are logged and scheduler remains alive; tasks are rescheduled per rules

Other responsibilities (unchanged): wait for DB, run migrations, resolve CSVs, load DB.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from app.core.constants import ETLStatus
from app.etl.orchestration.csv_resolver import (
    CSVNotFoundError,
    ensure_data_dirs as resolver_ensure_data_dirs,
    resolve_instruments_csv,
)

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
# Backwards-compatible env variables: prefer SCRAPER_RETRY_* names but fall back to existing ones
SCRAPER_RETRY_ATTEMPTS: int = int(os.environ.get("SCRAPER_RETRY_ATTEMPTS", os.environ.get("RETRY_COUNT", "3")))
SCRAPER_RETRY_DELAY_SECONDS: int = int(os.environ.get("SCRAPER_RETRY_DELAY_SECONDS", os.environ.get("RETRY_DELAY_SECONDS", "15")))
# Default intraday interval changed to 1800s (30 minutes) per new requirement
SCRAPER_INTERVAL: int = int(os.environ.get("SCRAPER_INTERVAL_SECONDS", "1800"))
RUN_MIGRATIONS: bool  = os.environ.get("RUN_MIGRATIONS_ON_STARTUP", "true").lower() not in ("false", "0", "no")

# Prefer tools from PATH (Dockerfile ensures /app/.venv/bin is on PATH)
PYTHON:       str = os.environ.get("PYTHON", "python")
ALEMBIC:      str = os.environ.get("ALEMBIC", "alembic")
DAILY_BATCH:  str = os.environ.get("DAILY_BATCH", "/app/docker/run_daily_batch.sh")
QUOTE_SCRIPT: str = os.environ.get("QUOTE_SCRIPT", "/app/scripts/run_b3_quote_batch.py")

# ---------------------------------------------------------------------------
# DB startup helpers
# ---------------------------------------------------------------------------

def run_migrations() -> bool:
    """Run Alembic migrations and return True on success.

    Captures stdout/stderr and logs them on failure to aid diagnosis.
    """
    log.info("[scheduler] running Alembic migrations (alembic upgrade head)")

    # If alembic is not installed or not in PATH, treat this as a fatal error
    # when RUN_MIGRATIONS is enabled, since migrations cannot be applied.
    try:
        which = subprocess.run(["which", ALEMBIC], capture_output=True, text=True)
        if which.returncode != 0:
            log.error(
                "[scheduler] alembic binary '%s' not found in PATH; "
                "cannot run required migrations and will abort startup",
                ALEMBIC,
            )
            return False
    except Exception:
        # Best-effort: continue and let subprocess.show error
        pass

    try:
        result = subprocess.run(
            [ALEMBIC, "upgrade", "head"],
            capture_output=True,
            text=True,
            cwd="/app",
        )
    except FileNotFoundError:
        log.error("[scheduler] alembic binary not found: %s", ALEMBIC)
        return False
    except Exception:
        log.exception("[scheduler] unexpected error when invoking alembic")
        return False

    if result.returncode == 0:
        log.info("[scheduler] migrations applied successfully")
        _out = getattr(result, "stdout", None)
        if _out:
            log.debug("[scheduler] alembic stdout: %s", _out)
        return True

    # Log failure details (stderr/stdout) for triage
    log.error("[scheduler] migrations FAILED  exit=%d", result.returncode)
    _out = getattr(result, "stdout", None)
    _err = getattr(result, "stderr", None)
    if _out:
        log.error("[scheduler] alembic stdout:\n%s", _out)
    if _err:
        log.error("[scheduler] alembic stderr:\n%s", _err)
    return False


def wait_for_db(retries: int = 15, delay: float = 4.0) -> bool:
    """Wait until the DB is reachable. Returns True when connected."""
    try:
        from app.db.engine import wait_for_db as _wait
        _wait(retries=retries, delay=delay)
        return True
    except RuntimeError as exc:
        log.critical("[scheduler] DB unavailable: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def run_daily_scrapers() -> bool:
    """Run run_daily_batch.sh and return True on success.

    Executes the shell script directly so its shebang is honored, but also
    captures stdout/stderr for improved logging in case of failures.
    """
    batch = Path(DAILY_BATCH)
    if not batch.exists():
        log.error("[scheduler] daily batch script not found: %s", DAILY_BATCH)
        try:
            log.error("[scheduler] /app/docker contents: %s", list(Path('/app/docker').iterdir()))
        except Exception:
            log.exception("[scheduler] failed to list /app/docker")
        return False
    if not os.access(batch, os.X_OK):
        try:
            st = batch.stat()
            log.error(
                "[scheduler] daily batch script is not executable: %s  owner=%s  mode=%o",
                DAILY_BATCH, f"{st.st_uid}:{st.st_gid}", st.st_mode & 0o777,
            )
        except Exception:
            log.exception("[scheduler] failed to stat %s", DAILY_BATCH)
        log.error(
            "[scheduler] daily batch script is not executable: %s  (fix: chmod +x %s)",
            DAILY_BATCH, DAILY_BATCH,
        )
        return False

    log.info("[scheduler] running daily scrapers  script=%s", DAILY_BATCH)
    start  = time.monotonic()
    try:
        # run via PATH so the shebang or interpreter is used as intended
        result = subprocess.run([str(batch)], capture_output=True, text=True)
    except Exception:
        log.exception("[scheduler] failed to execute daily batch script")
        return False
    elapsed = int(time.monotonic() - start)
    if result.returncode == 0:
        log.info("[scheduler] daily scrapers done  elapsed=%ds", elapsed)
        _out = getattr(result, "stdout", None)
        if _out:
            log.debug("[scheduler] daily batch stdout:\n%s", _out)
        return True

    log.error(
        "[scheduler] daily scrapers failed  exit=%d  elapsed=%ds",
        result.returncode, elapsed,
    )
    _out = getattr(result, "stdout", None)
    _err = getattr(result, "stderr", None)
    if _out:
        log.error("[scheduler] daily batch stdout:\n%s", _out)
    if _err:
        log.error("[scheduler] daily batch stderr:\n%s", _err)
    return False


def _warn_if_stale(csv_path: Path, max_age_hours: int = 48) -> None:
    """Warn when *csv_path* is older than *max_age_hours*.

    A stale CSV in the Docker volume (from a previous run) can cause the ETL
    to load old/wrong data or fail header detection if the B3 format changed.
    """
    try:
        import time as _t
        age_s = _t.time() - csv_path.stat().st_mtime
        age_h = age_s / 3600
        if age_h > max_age_hours:
            log.warning(
                "[scheduler] STALE FILE WARNING: %s is %.1f hours old "
                "(threshold=%dh). This may be a leftover from a previous Docker "
                "volume. Consider clearing the volume or re-running the scraper.",
                csv_path, age_h, max_age_hours,
            )
        else:
            log.debug("[scheduler] CSV age OK: %.1f hours (%s)", age_h, csv_path.name)
    except Exception as exc:
        log.debug("[scheduler] could not check staleness of %s: %s", csv_path, exc)


def _log_csv_diagnostics(csv_path: Path) -> None:
    """Log file metadata and first few rows of *csv_path* for environment diagnostics.

    This makes it easy to spot Docker-vs-local differences:
    - file age (could be a stale file from a previous run)
    - file ownership/permissions
    - actual first CSV line (catches preamble-as-header issues)
    """
    try:
        stat = csv_path.stat()
        import time as _time
        age_s = int(_time.time() - stat.st_mtime)
        log.info(
            "[scheduler] CSV diagnostics: path=%s  size=%d bytes  age=%ds  "
            "uid=%d  gid=%d  mode=%o",
            csv_path, stat.st_size, age_s,
            stat.st_uid, stat.st_gid, stat.st_mode & 0o777,
        )
    except Exception as exc:
        log.warning("[scheduler] CSV diagnostics stat failed for %s: %s", csv_path, exc)

    try:
        import csv as _csv
        with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
            reader = _csv.reader(fh, delimiter=";")
            rows = [next(reader, None) for _ in range(3)]
        log.info(
            "[scheduler] CSV first 3 rows of %s: %s",
            csv_path.name, [r for r in rows if r is not None],
        )
    except Exception as exc:
        log.warning("[scheduler] CSV preview failed for %s: %s", csv_path, exc)


def load_instruments_and_trades(csv_path: Path) -> bool:
    """Load instruments CSV (and sibling trades file) into the database.

    Discovers the negocios_consolidados sibling automatically using the same
    strategy as run_b3_quote_batch.py.  Returns True on success.
    """
    import re

    folder = csv_path.parent
    trades_path: Path | None = None

    m = re.search(r"(\d{8})", csv_path.stem)
    if m:
        date_tag = m.group(1)
        candidate = folder / f"negocios_consolidados_{date_tag}.normalized.csv"
        if candidate.exists():
            trades_path = candidate

    if trades_path is None:
        siblings = sorted(folder.glob("negocios_consolidados_*.normalized.csv"))
        if siblings:
            trades_path = siblings[-1]

    # Derive the target date from the CSV filename (YYYYMMDD)
    target_date = date.today()
    if m:
        try:
            from datetime import datetime as dt
            target_date = dt.strptime(m.group(1), "%Y%m%d").date()
        except ValueError:
            pass

    log.info(
        "[scheduler] loading instruments+trades  csv=%s  trades=%s",
        csv_path, trades_path,
    )
    _log_csv_diagnostics(csv_path)

    try:
        from app.etl.orchestration.pipeline import run_instruments_and_trades_pipeline
        result = run_instruments_and_trades_pipeline(csv_path, trades_path, target_date)
        status = result.get("status")
        assets = result.get("assets_upserted", 0)
        trades = result.get("trades_upserted", 0)

        if status == ETLStatus.SUCCESS and assets > 0:
            log.info(
                "[scheduler] DB load ok  assets=%d  trades=%d",
                assets, trades,
            )

            # --- NEW: also attempt to run daily_quotes pipeline (loads fact_daily_quotes)
            # Use the discovered trades_path (negocios_consolidados normalized CSV) when available.
            if trades_path:
                try:
                    from app.etl.orchestration.pipeline import run_daily_quotes_pipeline
                    dq_result = run_daily_quotes_pipeline(trades_path, target_date)
                    dq_status = dq_result.get("status")
                    dq_rows = dq_result.get("quotes_upserted", 0)
                    if dq_status == ETLStatus.SUCCESS:
                        log.info("[scheduler] daily quotes loaded  rows=%d  file=%s", dq_rows, trades_path)
                    else:
                        log.warning("[scheduler] daily quotes pipeline returned status=%s for %s", dq_status, trades_path)
                except Exception:
                    log.exception("[scheduler] run_daily_quotes_pipeline raised an exception for %s", trades_path)
            else:
                log.debug("[scheduler] no negocios_consolidados CSV found — skipping daily quotes pipeline")

            return True

        if status == ETLStatus.SUCCESS and assets == 0:
            # Normalization succeeded but the instruments CSV produced zero assets —
            # this is the core Docker bug: preamble was chosen as header.
            log.error(
                "[scheduler] DB load returned SUCCESS but assets_upserted=0 for %s. "
                "This strongly indicates the normalized CSV header was not detected "
                "correctly (preamble/descriptive line chosen as header). "
                "trades_upserted=%d  status=%s",
                csv_path, trades, status,
            )
            return False

        log.error(
            "[scheduler] DB load finished with status=%s  assets=%d  trades=%d  error=%s",
            status, assets, trades, result.get("error", ""),
        )
        return False
    except Exception as exc:
        log.exception("[scheduler] DB load raised an exception: %s", exc)
        return False


def load_latest_jsonl(reference_date: date) -> bool:
    """Find the most recent JSONL file for *reference_date* and load it.

    Returns True if at least one file was found and loaded without errors.
    """
    jsonl_dir = B3_DATA_DIR / "b3" / "daily_fluctuation_history" / reference_date.isoformat()
    if not jsonl_dir.exists():
        log.debug("[scheduler] no JSONL dir for %s – skipping DB load", reference_date)
        return False

    candidates = sorted(jsonl_dir.glob("daily_fluctuation_*.jsonl"))
    if not candidates:
        log.debug("[scheduler] no JSONL files in %s – skipping", jsonl_dir)
        return False

    jsonl_path = candidates[-1]  # most recent by name (timestamp suffix)
    log.info("[scheduler] loading intraday quotes from %s", jsonl_path)

    try:
        from app.etl.orchestration.pipeline import run_intraday_quotes_pipeline
        result = run_intraday_quotes_pipeline(jsonl_path)
        if result.get("status") == ETLStatus.SUCCESS:
            log.info(
                "[scheduler] intraday quotes loaded  rows=%d  file=%s",
                result.get("rows_inserted", 0), result.get("source_file"),
            )
            return True
        log.error(
            "[scheduler] intraday quotes load finished with status=%s",
            result.get("status"),
        )
        return False
    except Exception as exc:
        log.exception("[scheduler] intraday quotes load raised an exception: %s", exc)
        return False


def run_with_retry(func: Callable[[], Any], *, name: str, attempts: int = SCRAPER_RETRY_ATTEMPTS, delay_seconds: int = SCRAPER_RETRY_DELAY_SECONDS) -> bool:
    """Run ``func`` up to ``attempts`` times with configurable delay and exponential backoff.

    func should either return True on success or False on failure, or raise an
    exception on failure. Exceptions are treated as retryable unless they are
    obviously configuration errors (not detected here).

    Returns True on success, False on final failure. Logs each attempt with
    clear information about attempt numbers, reason, stdout/stderr when
    available and elapsed time.
    """
    total = attempts
    base_delay = delay_seconds

    for attempt in range(1, total + 1):
        log.info("[scheduler] scraper=%s attempt=%d/%d starting", name, attempt, total)
        start = time.monotonic()
        try:
            ok = func()
        except Exception as exc:
            elapsed = int(time.monotonic() - start)
            log.error("[scheduler] scraper=%s attempt=%d/%d failed (exception) elapsed=%ds: %s", name, attempt, total, elapsed, exc)
            log.debug("[scheduler] scraper=%s exception details", name, exc_info=True)
            ok = False
        else:
            elapsed = int(time.monotonic() - start)
            if ok:
                log.info("[scheduler] scraper=%s attempt=%d/%d succeeded elapsed=%ds", name, attempt, total, elapsed)
                return True
            else:
                log.warning("[scheduler] scraper=%s attempt=%d/%d returned failure elapsed=%ds", name, attempt, total, elapsed)

        # If we reach here, we need to retry (unless it was the last attempt)
        if attempt < total:
            backoff = base_delay * (2 ** (attempt - 1))
            log.info("[scheduler] scraper=%s retrying in %ds", name, backoff)
            time.sleep(backoff)
        else:
            log.error("[scheduler] scraper=%s exhausted attempts (%d/%d) — giving up", name, attempt, total)
            return False

    return False


def run_quote_batch(instruments_csv: Path) -> bool:
    """Run run_b3_quote_batch.py for one cycle and return True on success.

    This function uses `run_with_retry` to ensure the quote batch is retried
    individually up to SCRAPER_RETRY_ATTEMPTS times with SCRAPER_RETRY_DELAY_SECONDS
    as base delay.
    """
    def _invoke() -> bool:
        log.info("[scheduler] quote batch start  instruments=%s", instruments_csv)
        start  = time.monotonic()
        try:
            result = subprocess.run(
                [PYTHON, QUOTE_SCRIPT, "--instruments", str(instruments_csv)],
                capture_output=True,
                text=True,
            )
        except Exception as exc:
            log.exception("[scheduler] failed to start quote batch subprocess: %s", exc)
            return False
        elapsed = int(time.monotonic() - start)
        _out = getattr(result, "stdout", None)
        _err = getattr(result, "stderr", None)
        if result.returncode == 0:
            log.info("[scheduler] quote batch ok  elapsed=%ds", elapsed)
            if _out:
                log.debug("[scheduler] quote batch stdout:\n%s", _out)
            return True
        # Non-zero return code
        log.error("[scheduler] quote batch failed  exit=%d  elapsed=%ds", result.returncode, elapsed)
        if _out:
            log.error("[scheduler] quote batch stdout:\n%s", _out)
        if _err:
            log.error("[scheduler] quote batch stderr:\n%s", _err)
        return False

    return run_with_retry(_invoke, name="b3_quote_batch")


# ---------------------------------------------------------------------------
# Scheduler helpers
# ---------------------------------------------------------------------------


def next_daily_run(now: datetime | None = None) -> datetime:
    """Return the next datetime for DAILY_RUN_HOUR:DAILY_RUN_MINUTE in local time.

    If the target time today is after `now`, return today at that time, else
    return the same time tomorrow.
    """
    now = now or datetime.now()
    target = now.replace(hour=DAILY_RUN_HOUR, minute=DAILY_RUN_MINUTE, second=0, microsecond=0)
    if target <= now:
        target = target + timedelta(days=1)
    return target


def seconds_until(dt: datetime) -> float:
    """Seconds until the given datetime from now."""
    now = datetime.now()
    return max(0.0, (dt - now).total_seconds())


# ---------------------------------------------------------------------------
# New wrapper functions required by user
# ---------------------------------------------------------------------------

def run_daily_scrapers_once() -> None:
    """Run the daily scrapers once (startup or scheduled) and log outcome.

    Does not raise; failures are logged and the caller will decide rescheduling.
    """
    log.info("[scheduler] executing daily scrapers (startup/scheduled)")
    try:
        ok = run_daily_scrapers()
        if ok:
            log.info("[scheduler] daily scrapers executed successfully on startup/scheduled run")
        else:
            log.error("[scheduler] daily scrapers failed on startup/scheduled run")
    except Exception:
        log.exception("[scheduler] unexpected error while running daily scrapers")


def run_intraday_quotes_once(instruments_csv: Path | None) -> None:
    """Run one intraday cycle: quote batch + load JSONL.

    If instruments_csv is None attempt to resolve it. Failures are logged; no
    exception is raised here so the scheduler keeps running.
    """
    log.info("[scheduler] executing intraday cycle (startup/scheduled)")
    try:
        csv_path = instruments_csv
        if csv_path is None:
            try:
                csv_path = resolve_instruments_csv(
                    data_dir=B3_DATA_DIR,
                    retry_count=0,
                    retry_delay_seconds=0,
                )
            except CSVNotFoundError:
                csv_path = None

        if csv_path is None:
            log.warning("[scheduler] intraday skipped: no instruments CSV available")
            return

        run_quote_batch(csv_path)
        load_latest_jsonl(date.today())
    except Exception:
        log.exception("[scheduler] unexpected error during intraday cycle")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("=" * 60)
    log.info("[scheduler] starting")
    log.info("  daily run    : %02d:%02d local", DAILY_RUN_HOUR, DAILY_RUN_MINUTE)
    log.info("  data dir     : %s", B3_DATA_DIR)
    log.info("  retry count  : %d", SCRAPER_RETRY_ATTEMPTS)
    log.info("  retry delay  : %ds", SCRAPER_RETRY_DELAY_SECONDS)
    log.info("  quote interval: %ds (%.0fmin)", SCRAPER_INTERVAL, SCRAPER_INTERVAL / 60)
    log.info("  run migrations: %s", RUN_MIGRATIONS)
    log.info("=" * 60)

    # Ensure directories via the shared resolver helper to avoid drift
    try:
        resolver_ensure_data_dirs(B3_DATA_DIR)
    except PermissionError:
        log.critical("[scheduler] fatal: cannot create data directories (PermissionError)")
        sys.exit(1)

    # ------------------------------------------------------------------
    # DB startup: wait + migrate
    # ------------------------------------------------------------------
    if not wait_for_db():
        log.critical("[scheduler] cannot connect to DB — exiting")
        sys.exit(1)

    if RUN_MIGRATIONS:
        if not run_migrations():
            log.critical("[scheduler] migrations failed — exiting to prevent data corruption")
            sys.exit(2)

    # ------------------------------------------------------------------
    # Immediate startup runs (always run once on startup)
    #  - daily scrapers (cadastro + negocios)
    #  - intraday cycle (quote batch + load JSONL)
    # ------------------------------------------------------------------
    # 1) Run daily scrapers once at startup
    run_daily_scrapers_once()

    # 2) Attempt to resolve instruments CSV (may have been produced by scrapers)
    csv_path = None
    try:
        csv_path = resolve_instruments_csv(
            data_dir=B3_DATA_DIR,
            retry_count=SCRAPER_RETRY_ATTEMPTS,
            retry_delay_seconds=SCRAPER_RETRY_DELAY_SECONDS,
        )
        log.info("[scheduler] csv resolved after startup: %s", csv_path)
        _warn_if_stale(csv_path)
    except CSVNotFoundError:
        log.warning("[scheduler] no instruments CSV found after startup run — ETL load will be retried on schedule")
        csv_path = None

    # 3) Load instruments+trades if available (best-effort)
    if csv_path:
        try:
            load_instruments_and_trades(csv_path)
        except Exception:
            log.exception("[scheduler] unexpected error while loading instruments/trades at startup")

    # 4) Run intraday immediately on startup
    run_intraday_quotes_once(csv_path)

    # Compute next schedule points
    next_daily = next_daily_run()
    next_intraday = datetime.now() + timedelta(seconds=SCRAPER_INTERVAL)

    log.info("[scheduler] next daily run scheduled at %s", next_daily.isoformat())
    log.info("[scheduler] next intraday run scheduled at %s", next_intraday.isoformat())

    # Main scheduling loop — wake on the nearest scheduled job
    while True:
        now = datetime.now()
        # determine which job is next
        to_daily = (now >= next_daily)
        to_intraday = (now >= next_intraday)

        if not to_daily and not to_intraday:
            # Sleep until the next event
            sleep_secs = min((next_daily - now).total_seconds(), (next_intraday - now).total_seconds())
            sleep_for = max(1, int(sleep_secs))
            log.debug("[scheduler] sleeping %ds until next event (daily in %ds, intraday in %ds)", sleep_for, int((next_daily - now).total_seconds()), int((next_intraday - now).total_seconds()))
            time.sleep(sleep_for)
            continue

        # If both are due, prefer running daily first (it produces CSVs used by intraday)
        if to_daily:
            log.info("[scheduler] scheduled daily run triggered (scheduled for %s)", next_daily.isoformat())
            try:
                run_daily_scrapers_once()
            except Exception:
                log.exception("[scheduler] unexpected error during scheduled daily run")
            # After scheduled daily run, attempt to resolve CSV and load into DB
            try:
                try:
                    csv_path = resolve_instruments_csv(
                        data_dir=B3_DATA_DIR,
                        retry_count=SCRAPER_RETRY_ATTEMPTS,
                        retry_delay_seconds=SCRAPER_RETRY_DELAY_SECONDS,
                    )
                    log.info("[scheduler] csv resolved after daily run: %s", csv_path)
                    _warn_if_stale(csv_path)
                except CSVNotFoundError:
                    log.warning("[scheduler] no instruments CSV found after daily run — will retry later")
                    csv_path = None

                if csv_path:
                    load_instruments_and_trades(csv_path)
            except Exception:
                log.exception("[scheduler] unexpected error handling CSV after daily run")

            # schedule next daily run
            next_daily = next_daily_run(now=datetime.now())
            log.info("[scheduler] next daily run scheduled at %s", next_daily.isoformat())

            # If intraday was also due now, we'll let the loop handle it in the next iteration

        if to_intraday:
            log.info("[scheduler] scheduled intraday run triggered (scheduled for %s)", next_intraday.isoformat())
            try:
                # Refresh CSV path non-blocking
                try:
                    fresh = resolve_instruments_csv(
                        data_dir=B3_DATA_DIR,
                        retry_count=0,
                        retry_delay_seconds=0,
                    )
                    if fresh:
                        csv_path = fresh
                except CSVNotFoundError:
                    # keep previous csv_path (may be None)
                    pass

                run_intraday_quotes_once(csv_path)
            except Exception:
                log.exception("[scheduler] unexpected error during scheduled intraday run")

            # schedule next intraday run
            next_intraday = datetime.now() + timedelta(seconds=SCRAPER_INTERVAL)
            log.info("[scheduler] next intraday run scheduled at %s", next_intraday.isoformat())


if __name__ == "__main__":
    main()

