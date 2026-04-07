"""One-shot stack bootstrap before ``prefect serve`` (Docker scheduler entrypoint).

Runs :func:`lightweight_bootstrap_flow` once per persistent data volume unless a
marker file exists or bootstrap is disabled via environment variables.

If ``SKIP_STACK_BOOTSTRAP_IF_FRESH`` is true (default in Compose) and today's
cadastro normalized CSV already exists under ``B3_DATA_DIR``, the flow is skipped
but the marker is written so restarts do not repeat the check unnecessarily.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from app.etl.orchestration.csv_resolver import find_csv_for_date
from app.etl.orchestration.prefect.flows.daily_scraping_flow import lightweight_bootstrap_flow

log = logging.getLogger("prefect_bootstrap")
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    force=True,
)


def _env_enabled(name: str, default: bool = True) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def default_marker_path() -> Path:
    raw = os.environ.get("B3_DATA_DIR", "/app/data/raw")
    # Marker lives next to volume root (parent of .../raw)
    return Path(raw).resolve().parent / ".bootstrap_light_v1.done"


def _skip_bootstrap_due_to_fresh_cadastro() -> bool:
    if not _env_enabled("SKIP_STACK_BOOTSTRAP_IF_FRESH", default=True):
        return False
    raw = os.environ.get("B3_DATA_DIR", "/app/data/raw")
    data_dir = Path(raw).resolve()
    return find_csv_for_date(data_dir, date.today()) is not None


def _write_marker(marker: Path) -> bool:
    try:
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text(
            datetime.now(timezone.utc).isoformat(),
            encoding="utf-8",
        )
    except OSError:
        log.exception("could not write bootstrap marker at %s", marker)
        return False
    return True


def main() -> int:
    if not _env_enabled("RUN_STACK_BOOTSTRAP", default=True):
        log.info("stack bootstrap disabled (RUN_STACK_BOOTSTRAP=false)")
        return 0

    marker = Path(os.environ.get("STACK_BOOTSTRAP_MARKER", str(default_marker_path())))
    force = _env_enabled("FORCE_STACK_BOOTSTRAP", default=False)
    if marker.exists() and not force:
        log.info("stack bootstrap skipped: marker exists at %s", marker)
        return 0

    if _skip_bootstrap_due_to_fresh_cadastro():
        log.info(
            "stack bootstrap skipped: fresh cadastro CSV for today under %s "
            "(SKIP_STACK_BOOTSTRAP_IF_FRESH)",
            os.environ.get("B3_DATA_DIR", "/app/data/raw"),
        )
        if not _write_marker(marker):
            return 1
        log.info("bootstrap marker written after fresh-data skip: %s", marker)
        return 0

    log.info("starting lightweight stack bootstrap")
    try:
        lightweight_bootstrap_flow()
    except Exception:
        log.exception("stack bootstrap failed")
        return 1

    if not _write_marker(marker):
        return 1

    log.info("stack bootstrap finished; marker written to %s", marker)
    return 0


if __name__ == "__main__":
    sys.exit(main())
