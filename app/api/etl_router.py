"""ETL trigger router.

Triggers the standalone pipeline (no Prefect server required).  The router
resolves the most recent instruments + trades CSV from B3_DATA_DIR for
'local' mode; 'remote' mode downloads from B3 (requires Playwright).
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Body
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.constants import SourceMode
from app.db.engine import get_db
from app.etl.orchestration.pipeline import run_instruments_and_trades_pipeline
from app.schemas import ETLBackfillRequest, ETLRunRequest

router = APIRouter(prefix="/etl", tags=["ETL"])


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_local_paths(target_date: date) -> tuple[Path, Path | None]:
    """Resolve instruments CSV + sibling trades file for *target_date*.

    Resolution order:
    1. Exact dated CSV:  <B3_DATA_DIR>/b3/boletim_diario/<YYYY-MM-DD>/
                          cadastro_instrumentos_<YYYYMMDD>.normalized.csv
    2. Yesterday fallback.
    3. Any glob match in the dated directory.

    Returns (instruments_csv, trades_file_or_None).
    Raises HTTPException 404 if no CSV found.
    """
    from app.etl.orchestration.csv_resolver import CSVNotFoundError, resolve_instruments_csv

    data_dir = Path(settings.b3_data_dir)
    try:
        instruments_csv = resolve_instruments_csv(
            data_dir=data_dir,
            retry_count=0,  # API routes don't block — fail fast
            retry_delay_seconds=0,
        )
    except CSVNotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail=f"Instruments CSV not found in {data_dir}: {exc}",
        ) from exc

    # Sibling trades file discovery
    trades_file: Path | None = None
    folder = instruments_csv.parent
    m = re.search(r"(\d{8})", instruments_csv.stem)
    if m:
        candidate = folder / f"negocios_consolidados_{m.group(1)}.normalized.csv"
        if candidate.exists():
            trades_file = candidate
    if trades_file is None:
        siblings = sorted(folder.glob("negocios_consolidados_*.normalized.csv"))
        if siblings:
            trades_file = siblings[-1]

    return instruments_csv, trades_file


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/run-latest",
    summary="Trigger ETL for the latest available date",
    description=(
        "Runs the B3 daily ETL synchronously for today's date (or the latest available "
        "file in local mode). Returns the pipeline summary including row counts and status."
    ),
)
def run_latest(
    body: ETLRunRequest = Body(default_factory=ETLRunRequest),
    db: Session = Depends(get_db),  # noqa: ARG001 – kept for future middleware
) -> dict:
    target_date = date.today()

    if body.source_mode == SourceMode.REMOTE:
        raise HTTPException(
            status_code=501,
            detail=(
                "source_mode='remote' is not supported via the HTTP API "
                "(Playwright scraping must run as a background process). "
                "Use the scheduler container or run scripts/run_b3_scraper.py manually."
            ),
        )

    instruments_csv, trades_file = _resolve_local_paths(target_date)
    result = run_instruments_and_trades_pipeline(instruments_csv, trades_file, target_date)
    return {"status": "ok", "result": result}


@router.post(
    "/backfill",
    summary="Trigger historical ETL backfill",
    description=(
        "Runs the B3 ETL for a date range using local CSV files. "
        "Iterates day by day synchronously. "
        "Only processes dates where a CSV file is available locally."
    ),
)
def run_backfill(body: ETLBackfillRequest) -> dict:
    if body.source_mode == SourceMode.REMOTE:
        raise HTTPException(
            status_code=501,
            detail="source_mode='remote' is not supported via the HTTP API.",
        )

    data_dir = Path(settings.b3_data_dir)
    results = []
    current = body.date_from

    while current <= body.date_to:
        from app.etl.orchestration.csv_resolver import find_csv_for_date

        csv_path = find_csv_for_date(data_dir, current)
        if csv_path is None:
            results.append({
                "date": str(current),
                "status": "skipped",
                "reason": "no CSV found",
            })
            current += timedelta(days=1)
            continue

        # Sibling trades file
        trades_file: Path | None = None
        folder = csv_path.parent
        m = re.search(r"(\d{8})", csv_path.stem)
        if m:
            candidate = folder / f"negocios_consolidados_{m.group(1)}.normalized.csv"
            if candidate.exists():
                trades_file = candidate
        if trades_file is None:
            siblings = sorted(folder.glob("negocios_consolidados_*.normalized.csv"))
            if siblings:
                trades_file = siblings[-1]

        try:
            result = run_instruments_and_trades_pipeline(csv_path, trades_file, current)
            results.append({"date": str(current), "status": "ok", "result": result})
        except Exception as exc:  # noqa: BLE001
            results.append({"date": str(current), "status": "error", "error": str(exc)})

        current += timedelta(days=1)

    return {"total_dates": len(results), "results": results}

