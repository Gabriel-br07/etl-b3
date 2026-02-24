"""ETL trigger router."""

from datetime import date, timedelta

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.constants import SourceMode
from app.db.engine import get_db
from app.etl.orchestration.flow import run_daily_b3_etl
from app.schemas import ETLBackfillRequest, ETLRunRequest

router = APIRouter(prefix="/etl", tags=["ETL"])


@router.post(
    "/run-latest",
    summary="Trigger ETL for the latest available date",
    description=(
        "Runs the B3 daily ETL synchronously for today's date (or the latest available "
        "file in local mode). Use source_mode='local' for local file fallback."
    ),
)
def run_latest(
    body: ETLRunRequest = ETLRunRequest(),
    db: Session = Depends(get_db),  # noqa: ARG001 – kept for future async support
) -> dict:
    result = run_daily_b3_etl(
        target_date=date.today(),
        source_mode=body.source_mode,
    )
    return {"status": "ok", "result": result}


@router.post(
    "/backfill",
    summary="Trigger historical ETL backfill",
    description=(
        "Runs the B3 ETL for a date range. "
        "MVP: iterates day by day synchronously. "
        "TODO: parallelise / async for large ranges."
    ),
)
def run_backfill(body: ETLBackfillRequest) -> dict:
    results = []
    current = body.date_from
    while current <= body.date_to:
        try:
            result = run_daily_b3_etl(
                target_date=current,
                source_mode=body.source_mode,
            )
            results.append({"date": str(current), "status": "ok", "result": result})
        except Exception as exc:  # noqa: BLE001
            results.append({"date": str(current), "status": "error", "error": str(exc)})
        # Advance by one day
        current = current + timedelta(days=1)
    return {"total_dates": len(results), "results": results}
