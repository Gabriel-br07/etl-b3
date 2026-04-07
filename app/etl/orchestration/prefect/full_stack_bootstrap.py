"""Marker-gated wrapper for the heavy full-stack worker (COTAHIST today).

Compose service ``cotahist`` runs this module so ``docker compose --profile full up -d``
does not re-execute multi-hour ingestion on every restart. The real work stays in
``docker/cotahist_worker.py`` (subprocess — no duplicated business logic).

Environment:
    RUN_FULL_STACK_BOOTSTRAP   If false/0/no: exit 0 without running the worker (default true).
    STACK_FULL_BOOTSTRAP_MARKER  Path to marker file (default: sibling of ``.../raw`` → ``.bootstrap_full_v1.done``).
    FORCE_FULL_STACK_BOOTSTRAP   If true: run worker even when marker exists.
    COTAHIST_WORKER_SCRIPT       Override path to ``cotahist_worker.py`` (default ``/app/docker/cotahist_worker.py``).
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("full_stack_bootstrap")
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


def default_full_marker_path() -> Path:
    raw = os.environ.get("B3_DATA_DIR", "/app/data/raw")
    return Path(raw).resolve().parent / ".bootstrap_full_v1.done"


def main() -> int:
    if not _env_enabled("RUN_FULL_STACK_BOOTSTRAP", default=True):
        log.info("full-stack bootstrap disabled (RUN_FULL_STACK_BOOTSTRAP=false)")
        return 0

    marker = Path(os.environ.get("STACK_FULL_BOOTSTRAP_MARKER", str(default_full_marker_path())))
    force = _env_enabled("FORCE_FULL_STACK_BOOTSTRAP", default=False)
    if marker.exists() and not force:
        log.info("full-stack bootstrap skipped: marker exists at %s", marker)
        return 0

    worker = Path(os.environ.get("COTAHIST_WORKER_SCRIPT", "/app/docker/cotahist_worker.py"))
    if not worker.is_file():
        log.error("cotahist worker script not found: %s", worker)
        return 1

    python = os.environ.get("PYTHON", sys.executable)
    app_root = Path(os.environ.get("APP_ROOT", "/app"))
    log.info("starting full-stack worker: %s %s (cwd=%s)", python, worker, app_root)
    proc = subprocess.run([python, str(worker)], cwd=str(app_root))
    if proc.returncode != 0:
        log.error("full-stack worker exited %s", proc.returncode)
        return proc.returncode

    try:
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text(datetime.now(timezone.utc).isoformat(), encoding="utf-8")
    except OSError:
        log.exception("could not write full-stack bootstrap marker at %s", marker)
        return 1

    log.info("full-stack bootstrap finished; marker written to %s", marker)
    return 0


if __name__ == "__main__":
    sys.exit(main())
