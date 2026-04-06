#!/usr/bin/env python3
"""Optional Docker worker: fetch B3 annual COTAHIST ZIPs and load ``fact_cotahist_daily``.

Runs as a separate Compose service (profiles ``full`` or ``cotahist``). Does **not** run Alembic;
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
    COTAHIST_LOCK_KEY_1      Postgres advisory lock key 1 (default 4242).
    COTAHIST_LOCK_KEY_2      Postgres advisory lock key 2 (default 1).

Fetch/extract is recorded in ``scraper_run_audit`` (``scraper_name=cotahist``) when the DB is
reachable; ETL load remains audited only in ``etl_runs`` via ``run_etl.py``.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import InterfaceError, OperationalError, ProgrammingError, SQLAlchemyError

from app.etl.orchestration.prefect.tasks.audit_tasks import (
    finish_scraper_audit,
    start_scraper_audit,
)

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


def _resolve_lock_keys() -> tuple[int, int]:
    return (
        _env_int("COTAHIST_LOCK_KEY_1", 4242),
        _env_int("COTAHIST_LOCK_KEY_2", 1),
    )


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


def _year_range_inclusive_from_fetch_args(fetch_year_args: list[str]) -> tuple[int, int]:
    if "--year" in fetch_year_args:
        i = fetch_year_args.index("--year")
        y = int(fetch_year_args[i + 1])
        return (y, y)
    i = fetch_year_args.index("--from-year")
    j = fetch_year_args.index("--to-year")
    lo, hi = int(fetch_year_args[i + 1]), int(fetch_year_args[j + 1])
    if lo > hi:
        lo, hi = hi, lo
    return (lo, hi)


def _is_audit_db_unavailable(exc: BaseException) -> bool:
    return isinstance(exc, (OperationalError, InterfaceError))


def _tail_subprocess_output(stdout: str | None, stderr: str | None, max_chars: int = 4000) -> str:
    combined = ((stdout or "") + (stderr or "")).strip()
    if len(combined) <= max_chars:
        return combined
    return "... [truncated]\n" + combined[-max_chars:]


def _safe_finish_scraper_audit(audit_id: int, **kwargs: Any) -> None:
    status = kwargs.get("status")
    try:
        finish_scraper_audit(audit_id=audit_id, **kwargs)
    except Exception:
        log.exception(
            "[cotahist_worker] finish_scraper_audit failed audit_id=%s status=%s",
            audit_id,
            status,
        )


def _scraper_audit_base_metadata(cotahist_root: Path, fetch_year_args: list[str]) -> dict:
    lo, hi = _year_range_inclusive_from_fetch_args(fetch_year_args)
    return {
        "stage": "docker_annual_fetch",
        "data_dir": str(cotahist_root),
        "year_range": [lo, hi],
    }


def _record_extract_skipped_audit(cotahist_root: Path, fetch_year_args: list[str]) -> None:
    base = _scraper_audit_base_metadata(cotahist_root, fetch_year_args)
    try:
        audit_id = start_scraper_audit(
            scraper_name="cotahist",
            target_date=None,
            retry_count=0,
            status="running",
            metadata_json={**base, "reason": "COTAHIST_SKIP_DOWNLOAD"},
        )
        log.info(
            "[cotahist_worker] scraper extract audit started (skip path) audit_id=%s",
            audit_id,
        )
        _safe_finish_scraper_audit(
            audit_id,
            status="skipped",
            retry_count=0,
            metadata_json={**base, "reason": "COTAHIST_SKIP_DOWNLOAD"},
        )
        log.info("[cotahist_worker] scraper extract audit skipped audit_id=%s", audit_id)
    except Exception as audit_exc:
        if _is_audit_db_unavailable(audit_exc):
            log.warning(
                "[cotahist_worker] scraper audit unavailable (DB); skip-download path without audit row: %s",
                audit_exc,
            )
            return
        raise


def _run_fetch_subprocess_with_scraper_audit(
    *,
    fetch_cmd: list[str],
    cotahist_root: Path,
    fetch_year_args: list[str],
) -> None:
    base = _scraper_audit_base_metadata(cotahist_root, fetch_year_args)
    lo, hi = _year_range_inclusive_from_fetch_args(fetch_year_args)
    audit_id: int | None = None
    audit_finished = False
    try:
        try:
            audit_id = start_scraper_audit(
                scraper_name="cotahist",
                target_date=None,
                retry_count=0,
                status="running",
                metadata_json={**base, "fetch_cmd": " ".join(fetch_cmd)},
            )
            log.info(
                "[cotahist_worker] scraper extract audit started audit_id=%s",
                audit_id,
            )
        except Exception as audit_exc:
            if _is_audit_db_unavailable(audit_exc):
                log.warning(
                    "[cotahist_worker] scraper audit unavailable (DB); running fetch without audit: %s",
                    audit_exc,
                )
            else:
                raise

        log.info("[cotahist_worker] running fetch: %s", " ".join(fetch_cmd))
        r = subprocess.run(
            fetch_cmd,
            cwd=str(APP_ROOT),
            check=False,
            capture_output=True,
            text=True,
        )

        if audit_id is not None:
            if r.returncode != 0:
                tail = _tail_subprocess_output(r.stdout, r.stderr)
                err_msg = f"fetch script exited {r.returncode}"
                if tail:
                    err_msg = f"{err_msg}; output_tail=\n{tail}"
                _safe_finish_scraper_audit(
                    audit_id,
                    status="failed",
                    retry_count=0,
                    error_type="SubprocessError",
                    error_message=err_msg,
                    metadata_json={
                        **base,
                        "returncode": r.returncode,
                        "subprocess_output_tail": tail,
                    },
                )
                audit_finished = True
                log.error(
                    "[cotahist_worker] fetch script exited %s audit_id=%s",
                    r.returncode,
                    audit_id,
                )
                sys.exit(r.returncode)

            paths = [
                cotahist_root / str(y) / f"COTAHIST_A{y}.TXT" for y in range(lo, hi + 1)
            ]
            last_path = paths[-1]
            _safe_finish_scraper_audit(
                audit_id,
                status="success",
                retry_count=0,
                output_path=str(last_path),
                output_file_name=last_path.name,
                metadata_json={
                    **base,
                    "paths": [str(p) for p in paths],
                    "years_ok": list(range(lo, hi + 1)),
                    "years_skipped_404": [],
                },
            )
            audit_finished = True
            log.info("[cotahist_worker] fetch completed audit_id=%s", audit_id)
        elif r.returncode != 0:
            log.error("[cotahist_worker] fetch script exited %s", r.returncode)
            sys.exit(r.returncode)
    finally:
        if audit_id is not None and not audit_finished:
            _safe_finish_scraper_audit(
                audit_id,
                status="failed",
                retry_count=0,
                error_type="RuntimeError",
                error_message="fetch stage aborted before audit could be finalized",
                metadata_json=base,
            )


def main() -> None:
    log.info("[cotahist_worker] starting")

    from app.core.config import settings
    from app.db.engine import engine, wait_for_db

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

    lock_key_1, lock_key_2 = _resolve_lock_keys()
    lock_params = {"k1": lock_key_1, "k2": lock_key_2}
    with engine.connect() as lock_conn:
        acquired = bool(
            lock_conn.execute(
                text("SELECT pg_try_advisory_lock(:k1, :k2)"),
                lock_params,
            ).scalar()
        )
        if not acquired:
            log.error(
                "[cotahist_worker] another COTAHIST run is already active "
                "(advisory lock %d:%d). Exiting without running.",
                lock_key_1,
                lock_key_2,
            )
            sys.exit(75)
        log.info(
            "[cotahist_worker] advisory lock acquired (%d:%d)",
            lock_key_1,
            lock_key_2,
        )
        # SQLAlchemy 2.x starts an implicit transaction on execute(); end it now
        # so this connection does not remain idle-in-transaction while long
        # subprocess work runs. The advisory lock is session-level and survives COMMIT.
        lock_conn.commit()

        try:
            fetch_year_args, etl_year_args = _resolve_year_args()

            if not _env_bool("COTAHIST_SKIP_DOWNLOAD", False):
                fetch_cmd = [
                    python,
                    str(FETCH_SCRIPT),
                    "--data-dir",
                    str(cotahist_root),
                    *fetch_year_args,
                ]
                if _env_bool("COTAHIST_FAIL_FAST", False):
                    fetch_cmd.append("--fail-fast")
                _run_fetch_subprocess_with_scraper_audit(
                    fetch_cmd=fetch_cmd,
                    cotahist_root=cotahist_root,
                    fetch_year_args=fetch_year_args,
                )
            else:
                log.info("[cotahist_worker] skipping download (COTAHIST_SKIP_DOWNLOAD set)")
                _record_extract_skipped_audit(cotahist_root, fetch_year_args)

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
        finally:
            try:
                unlocked = bool(
                    lock_conn.execute(
                        text("SELECT pg_advisory_unlock(:k1, :k2)"),
                        lock_params,
                    ).scalar()
                )
                if unlocked:
                    log.info(
                        "[cotahist_worker] advisory lock released (%d:%d)",
                        lock_key_1,
                        lock_key_2,
                    )
                else:
                    log.warning(
                        "[cotahist_worker] advisory lock was not held during release (%d:%d)",
                        lock_key_1,
                        lock_key_2,
                    )
            except SQLAlchemyError:
                log.exception(
                    "[cotahist_worker] failed to release advisory lock (%d:%d)",
                    lock_key_1,
                    lock_key_2,
                )

    log.info("[cotahist_worker] finished successfully")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        log.exception("[cotahist_worker] fatal error")
        sys.exit(1)
