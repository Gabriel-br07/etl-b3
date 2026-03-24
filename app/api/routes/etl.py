"""ETL trigger router.

Triggers the standalone pipeline (no Prefect server required).  The router
resolves the most recent instruments + trades CSV from B3_DATA_DIR for
'local' mode; 'remote' mode downloads from B3 (requires Playwright).
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.constants import SourceMode
from app.db.engine import get_db
from app.etl.orchestration.pipeline import run_instruments_and_trades_pipeline
from app.repositories.etl_run_repository import ETLRunRepository
from app.schemas import ETLBackfillRequest, ETLRunListItem, ETLRunRequest, PaginatedResponse
from app.db.models import ETLRun

router = APIRouter(prefix="/etl", tags=["ETL"])


def _parse_started_filter(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _etl_run_to_item(run: ETLRun) -> ETLRunListItem:
    duration: float | None = None
    if run.finished_at is not None:
        duration = (run.finished_at - run.started_at).total_seconds()
    return ETLRunListItem(
        run_id=run.id,
        pipeline_name=run.pipeline_name,
        status=run.status,
        started_at=run.started_at,
        finished_at=run.finished_at,
        duration_seconds=duration,
        source_date=run.source_date,
        source_file=run.source_file,
        message=run.message,
        rows_inserted=run.rows_inserted,
        rows_failed=run.rows_failed,
    )


@router.get(
    "/runs",
    response_model=PaginatedResponse[ETLRunListItem],
    summary="List ETL run history",
    description=(
        "Paginated audit log from ``etl_runs``, newest first. "
        "Filter by pipeline, status, source date, or started_at window. "
        "``duration_seconds`` is set when ``finished_at`` is present."
    ),
    responses={
        200: {"description": "Paginated ETL runs."},
        400: {"description": "Invalid datetime filter."},
    },
)
def list_etl_runs(
    pipeline_name: str | None = Query(None, description="Exact pipeline_name match."),
    status: str | None = Query(None, description="Run status (e.g. success, failed, running)."),
    source_date: date | None = Query(None),
    started_after: str | None = Query(None, description="ISO 8601 lower bound on started_at."),
    started_before: str | None = Query(None, description="ISO 8601 upper bound on started_at."),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
) -> PaginatedResponse[ETLRunListItem]:
    sa = _parse_started_filter(started_after)
    sb = _parse_started_filter(started_before)
    if started_after is not None and sa is None:
        raise HTTPException(status_code=400, detail="Invalid started_after; use ISO 8601.")
    if started_before is not None and sb is None:
        raise HTTPException(status_code=400, detail="Invalid started_before; use ISO 8601.")
    if sa is not None and sb is not None and sb < sa:
        raise HTTPException(status_code=400, detail="started_before must be >= started_after.")
    repo = ETLRunRepository(db)
    rows, total = repo.list_runs(
        pipeline_name=pipeline_name,
        status=status,
        source_date=source_date,
        started_after=sa,
        started_before=sb,
        limit=limit,
        offset=offset,
    )
    return PaginatedResponse(
        total=total,
        limit=limit,
        offset=offset,
        items=[_etl_run_to_item(r) for r in rows],
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_local_paths(target_date: date) -> tuple[Path, Path | None]:
    """Resolve instruments CSV + sibling trades file for *target_date*.

    Resolution order:
    1. Exact dated CSV for *target_date*:
       <B3_DATA_DIR>/b3/boletim_diario/<YYYY-MM-DD>/
       cadastro_instrumentos_<YYYYMMDD>.normalized.csv
    2. Fallback to the most recent CSV (today / yesterday), mirroring
       `resolve_instruments_csv`.
    3. Any glob match in the dated directory.

    Returns (instruments_csv, trades_file_or_None).
    Raises HTTPException 404 if no CSV found.
    """
    from app.etl.orchestration.csv_resolver import (
        CSVNotFoundError,
        find_csv_for_date,
        resolve_instruments_csv,
    )

    data_dir = Path(settings.b3_data_dir)
    # First, try to resolve an instruments CSV for the requested target_date.
    instruments_csv = find_csv_for_date(data_dir, target_date)
    if instruments_csv is None:
        # Fallback to the existing "most recent" behavior (today / yesterday).
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
        "file in local mode). Returns the pipeline summary including row counts and status. "
        "Only supports source_mode='local'; for remote use the scheduler or run scripts manually."
    ),
    responses={
        200: {"description": "ETL run completed successfully."},
        404: {"description": "Instruments CSV not found in B3_DATA_DIR."},
        501: {"description": "source_mode='remote' not supported via API."},
    },
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
        "Only processes dates where a CSV file is available locally. "
        "Only supports source_mode='local'."
    ),
    responses={
        200: {"description": "Backfill completed; check results per date."},
        501: {"description": "source_mode='remote' not supported via API."},
    },
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
