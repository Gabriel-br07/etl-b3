"""
app/etl/orchestration/csv_resolver.py
======================================
Instruments CSV resolver with retry and yesterday-fallback logic.

Used by:
  - docker/scheduler.py (standalone container)
  - scripts/run_b3_quote_batch.py (can import for auto-discovery)

Resolution order (per attempt):
  1. Today's cadastro_instrumentos_YYYYMMDD.normalized.csv
  2. Yesterday's cadastro_instrumentos_YYYYMMDD.normalized.csv
  3. Retry up to *retry_count* times (sleeping *retry_delay_seconds* between)
  4. Raise CSVNotFoundError after all attempts exhausted

Usage
-----
    from app.etl.orchestration.csv_resolver import resolve_instruments_csv

    csv_path = resolve_instruments_csv()          # defaults: 3 retries, 60s delay
    csv_path = resolve_instruments_csv(           # custom
        data_dir=Path("/app/data/raw"),
        retry_count=5,
        retry_delay_seconds=30,
    )
"""

from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from pathlib import Path

log = logging.getLogger(__name__)


class CSVNotFoundError(RuntimeError):
    """Raised when the instruments CSV cannot be resolved after all retries."""


def find_csv_for_date(data_dir: Path, target: date) -> Path | None:
    """
    Look for ``cadastro_instrumentos_YYYYMMDD.normalized.csv`` for *target*.

    Search strategy:
      1. Exact path: ``<data_dir>/b3/boletim_diario/<YYYY-MM-DD>/
                       cadastro_instrumentos_<YYYYMMDD>.normalized.csv``
      2. Glob fallback: any ``cadastro_instrumentos_*.normalized.csv`` in the
         dated folder (picks alphabetically last = most recent).

    Returns the resolved :class:`~pathlib.Path`, or ``None`` if not found.
    """
    date_str = target.isoformat()            # YYYY-MM-DD
    date_compact = date_str.replace("-", "") # YYYYMMDD
    folder = data_dir / "b3" / "boletim_diario" / date_str

    exact = folder / f"cadastro_instrumentos_{date_compact}.normalized.csv"
    if exact.exists():
        log.debug("Exact match: %s", exact)
        return exact

    if folder.exists():
        candidates = sorted(folder.glob("cadastro_instrumentos_*.normalized.csv"))
        if candidates:
            log.debug("Glob match: %s", candidates[-1])
            return candidates[-1]

    return None


def ensure_data_dirs(data_dir: Path) -> None:
    """
    Create all required sub-directories under *data_dir*.

    Directories created:
      - ``<data_dir>/b3/boletim_diario``
      - ``<data_dir>/b3/daily_fluctuation_history``
      - ``<data_dir>/../screenshots/b3``
      - ``<data_dir>/../traces/e2e``
    """
    dirs = [
        data_dir / "b3" / "boletim_diario",
        data_dir / "b3" / "daily_fluctuation_history",
        data_dir.parent / "screenshots" / "b3",
        data_dir.parent / "traces" / "e2e",
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
        log.debug("ensured dir: %s", d)
    log.info("[etl_pipeline] Data directories verified under: %s", data_dir)


def resolve_instruments_csv(
    data_dir: Path | str | None = None,
    retry_count: int = 3,
    retry_delay_seconds: int = 60,
) -> Path:
    """
    Resolve the most recent instruments CSV with retry and yesterday fallback.

    Parameters
    ----------
    data_dir:
        Root raw-data directory (default: ``/app/data/raw``).
    retry_count:
        Number of retry cycles (default: 3).
    retry_delay_seconds:
        Seconds to sleep between retries (default: 60).

    Returns
    -------
    Path
        Resolved path to the instruments CSV.

    Raises
    ------
    CSVNotFoundError
        If the CSV cannot be found after all retries.
    """
    if data_dir is None:
        import os
        data_dir = Path(os.environ.get("B3_DATA_DIR", "/app/data/raw"))
    data_dir = Path(data_dir)

    today = date.today()
    yesterday = today - timedelta(days=1)

    log.info(
        "[etl_pipeline] CSV discovery — today=%s  yesterday=%s  retries=%d  delay=%ds",
        today, yesterday, retry_count, retry_delay_seconds,
    )

    for attempt in range(retry_count + 1):
        if attempt > 0:
            log.info(
                "[etl_pipeline] Attempt %d/%d — waiting %ds before retry…",
                attempt, retry_count, retry_delay_seconds,
            )
            time.sleep(retry_delay_seconds)

        # 1. Try today
        csv = find_csv_for_date(data_dir, today)
        if csv:
            log.info("[etl_pipeline] Found today's CSV: %s", csv)
            return csv
        log.info("[etl_pipeline] Today's CSV not found (%s)", today)

        # 2. Try yesterday
        csv = find_csv_for_date(data_dir, yesterday)
        if csv:
            log.warning(
                "[etl_pipeline] CSV today not found → using yesterday fallback: %s",
                csv,
            )
            return csv
        log.info("[etl_pipeline] Yesterday's CSV not found either (%s)", yesterday)

    raise CSVNotFoundError(
        f"Instruments CSV not found after {retry_count} retries. "
        f"Checked: {data_dir}/b3/boletim_diario/{today}/ "
        f"and {data_dir}/b3/boletim_diario/{yesterday}/. "
        "Ensure scraper_daily completed successfully."
    )

