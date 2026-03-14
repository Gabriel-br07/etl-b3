"""Fact quotes router.

Exposes intraday quote series from the fact_quotes hypertable (DB).
For live intraday data from B3, use GET /quotes/{ticker} or /quotes/{ticker}/intraday instead.
"""

from datetime import date, datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db.engine import get_db
from app.repositories.fact_quote_repository import FactQuoteRepository
from app.schemas import FactQuoteRead

router = APIRouter(prefix="/fact-quotes", tags=["Fact Quotes"])


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


@router.get(
    "/{ticker}/series",
    response_model=list[FactQuoteRead],
    summary="Intraday series from DB",
    description=(
        "Return intraday price points for the ticker from the database (fact_quotes hypertable). "
        "Filter by start and end datetime (ISO 8601). "
        "Use this for persisted intraday data; for live data use GET /quotes/{ticker}/intraday. "
        "Returns an empty list if no data exists for the range."
    ),
    responses={
        200: {"description": "List of intraday quote points (may be empty)."},
    },
)
def get_fact_quote_series(
    ticker: str,
    start: str | None = Query(
        None,
        description="Start datetime (ISO 8601, e.g. 2024-06-14T10:00:00).",
    ),
    end: str | None = Query(
        None,
        description="End datetime (ISO 8601, e.g. 2024-06-14T18:00:00).",
    ),
    limit: int = Query(1000, ge=1, le=5000, description="Maximum number of points to return."),
    db: Session = Depends(get_db),
) -> list[FactQuoteRead]:
    start_dt = _parse_datetime(start)
    end_dt = _parse_datetime(end)
    if start is not None and start_dt is None:
        raise HTTPException(status_code=400, detail="Invalid start datetime; use ISO 8601 format.")
    if end is not None and end_dt is None:
        raise HTTPException(status_code=400, detail="Invalid end datetime; use ISO 8601 format.")
    repo = FactQuoteRepository(db)
    items = repo.get_series(
        ticker=ticker.upper(),
        start=start_dt,
        end=end_dt,
        limit=limit,
    )
    return [FactQuoteRead.model_validate(q) for q in items]


@router.get(
    "/{ticker}/days/{trade_date}",
    response_model=list[FactQuoteRead],
    summary="Intraday points for a trade date",
    description=(
        "Return all intraday quote points for the ticker on the given trade date. "
        "Data is from the database (fact_quotes). "
        "Returns an empty list if no data exists for that day."
    ),
    responses={
        200: {"description": "List of intraday points for the day (may be empty)."},
    },
)
def get_fact_quotes_for_day(
    ticker: str,
    trade_date: date,
    db: Session = Depends(get_db),
) -> list[FactQuoteRead]:
    repo = FactQuoteRepository(db)
    items = repo.get_for_trade_date(ticker=ticker.upper(), trade_date=trade_date)
    return [FactQuoteRead.model_validate(q) for q in items]
