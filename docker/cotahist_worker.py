#!/usr/bin/env python3
"""Optional Docker worker: fetch B3 annual COTAHIST ZIPs and load ``fact_cotahist_daily``.

Runs as a separate Compose service (profile ``cotahist``). Does **not** run Alembic;
waits until ``fact_cotahist_daily`` exists (scheduler or another process must apply migrations).

Environment (optional unless noted):
    COTAHIST_SKIP_DOWNLOAD   If true/1/yes: skip ``run_b3_cotahist_annual.py`` (load only).
    COTAHIST_YEAR            Single year (mutually exclusive with START/END).
    COTAHIST_YEAR_START      Inclusive range start (requires END).
    COTAHIST_YEAR_END        Inclusive range end (requires START).
    COTAHIST_TABLE_WAIT_RETRIES   Max polls for ``fact_cotahist_daily`` (default 120).
    COTAHIST_TABLE_WAIT_DELAY_S   Seconds between polls (default 5).
    COTAHIST_DB_WAIT_RETRIES      Passed to ``wait_for_db`` first arg (default 30).
    COTAHIST_DB_WAIT_DELAY_S      Passed to ``wait_for_db`` delay (default 4).
    COTAHIST_FAIL_FAST       If true: pass ``--fail-fast`` to the fetch script.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.exc import OperationalError, ProgrammingError

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
    force=True,
)
log = logging.getLogger("cotahist_worker")

APP_ROOT = Path("/app")
FETCH_SCRIPT = APP_ROOT / "scripts" / "run_b3_cotahist_annual.py"
ETL_SCRIPT = APP_ROOT / "scripts" / "run_etl.py"


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    if v is None or not v.strip():
        return default
    return int(v)


def _env_float(name: str, default: float) -> float:
    v = os.environ.get(name)
    if v is None or not v.strip():
        return default
    return float(v)


def wait_for_cotahist_table(retries: int, delay_s: float) -> None:
    """Block until ``fact_cotahist_daily`` exists and is queryable."""
    from app.db.engine import engine

    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1 FROM fact_cotahist_daily LIMIT 0"))
            log.info(
                "[cotahist_worker] fact_cotahist_daily ready (attempt %d/%d)",
                attempt,
                retries,
            )
            return
        except (OperationalError, ProgrammingError) as exc:
            log.warning(
                "[cotahist_worker] COTAHIST table not ready (%d/%d): %s — retry in %.0fs",
                attempt,
                retries,
                exc,
                delay_s,
            )
            if attempt >= retries:
                raise RuntimeError(
                    "fact_cotahist_daily is not available after waiting. "
                    "Ensure the scheduler (or alembic upgrade head) has run migrations."
                ) from exc
            time.sleep(delay_s)


def _resolve_year_args() -> tuple[list[str], list[str]]:
    """Return (fetch_cli_args, etl_cli_args) for year selection only."""
    from app.core.config import settings

    single = os.environ.get("COTAHIST_YEAR")
    start = os.environ.get("COTAHIST_YEAR_START")
    end = os.environ.get("COTAHIST_YEAR_END")

    single_norm = single.strip() if single is not None else ""
    start_norm = start.strip() if start is not None else ""
    end_norm = end.strip() if end is not None else ""

    if single_norm:
        if start_norm or end_norm:
            raise SystemExit(
                "Use either COTAHIST_YEAR or COTAHIST_YEAR_START/COTAHIST_YEAR_END, not both"
            )
        y = int(single_norm)
        return ["--year", str(y)], ["--cotahist-year", str(y)]

    if start_norm or end_norm:
        if not (start_norm and end_norm):
            raise SystemExit(
                "COTAHIST_YEAR_START and COTAHIST_YEAR_END must both be set when using a range"
            )
        lo, hi = int(start_norm), int(end_norm)
        if lo > hi:
            lo, hi = hi, lo
        fy = ["--from-year", str(lo), "--to-year", str(hi)]
        ey = ["--cotahist-from-year", str(lo), "--cotahist-to-year", str(hi)]
        return fy, ey

    lo, hi = settings.b3_cotahist_year_start, settings.b3_cotahist_year_end
    if lo > hi:
        lo, hi = hi, lo
    return (
        ["--from-year", str(lo), "--to-year", str(hi)],
        ["--cotahist-from-year", str(lo), "--cotahist-to-year", str(hi)],
    )


def main() -> None:
    log.info("[cotahist_worker] starting")

    from app.core.config import settings
    from app.db.engine import wait_for_db

    python = sys.executable
    cotahist_root = Path(settings.b3_cotahist_annual_dir).resolve()
    log.info("[cotahist_worker] data root: %s", cotahist_root)

    wait_for_db(
        retries=_env_int("COTAHIST_DB_WAIT_RETRIES", 30),
        delay=_env_float("COTAHIST_DB_WAIT_DELAY_S", 4.0),
    )

    wait_for_cotahist_table(
        retries=_env_int("COTAHIST_TABLE_WAIT_RETRIES", 120),
        delay_s=_env_float("COTAHIST_TABLE_WAIT_DELAY_S", 5.0),
    )

    fetch_year_args, etl_year_args = _resolve_year_args()

    if not _env_bool("COTAHIST_SKIP_DOWNLOAD", False):
        fetch_cmd: list[str] = [
            python,
            str(FETCH_SCRIPT),
            "--data-dir",
            str(cotahist_root),
            *fetch_year_args,
        ]
        if _env_bool("COTAHIST_FAIL_FAST", False):
            fetch_cmd.append("--fail-fast")
        log.info("[cotahist_worker] running fetch: %s", " ".join(fetch_cmd))
        r = subprocess.run(fetch_cmd, cwd=str(APP_ROOT), check=False)
        if r.returncode != 0:
            log.error("[cotahist_worker] fetch script exited %s", r.returncode)
            sys.exit(r.returncode)
    else:
        log.info("[cotahist_worker] skipping download (COTAHIST_SKIP_DOWNLOAD set)")

    etl_cmd: list[str] = [
        python,
        str(ETL_SCRIPT),
        "--run-cotahist-annual",
        "--cotahist-data-dir",
        str(cotahist_root),
        *etl_year_args,
    ]
    log.info("[cotahist_worker] running ETL load: %s", " ".join(etl_cmd))
    r2 = subprocess.run(etl_cmd, cwd=str(APP_ROOT), check=False)
    if r2.returncode != 0:
        log.error("[cotahist_worker] run_etl exited %s", r2.returncode)
        sys.exit(r2.returncode)

    log.info("[cotahist_worker] finished successfully")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        log.exception("[cotahist_worker] fatal error")
        sys.exit(1)
