"""Fact quotes router.

Exposes intraday quote series from the fact_quotes hypertable (DB).
For live intraday data from B3, use GET /quotes/{ticker} or /quotes/{ticker}/intraday instead.
"""

from datetime import date, datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db.engine import get_db
from app.repositories.fact_quote_repository import FactQuoteRepository
from app.schemas import CandleRead, FactQuoteRead
from app.use_cases.quotes import candles as candle_uc

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
        400: {"description": "Invalid parameters (e.g. invalid ISO 8601 or end before start)."},
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
    if start_dt is not None and end_dt is not None and end_dt < start_dt:
        raise HTTPException(
            status_code=400,
            detail="end must be greater than or equal to start.",
        )
    repo = FactQuoteRepository(db)
    items = repo.get_series(
        ticker=ticker.upper(),
        start=start_dt,
        end=end_dt,
        limit=limit,
    )
    return [FactQuoteRead.model_validate(q) for q in items]


@router.get(
    "/{ticker}/candles",
    response_model=list[CandleRead],
    summary="Intraday OHLC candles from fact_quotes",
    description=(
        "Aggregates persisted ``fact_quotes`` into time buckets (UTC). "
        "Unlike ``GET /quotes/{ticker}/candles``, this route **only** uses the hypertable — "
        "no ``fact_daily_quotes`` fallback. ``point_count`` is the number of raw quotes per candle."
    ),
    responses={
        200: {"description": "Candles oldest-to-newest."},
        400: {"description": "Invalid interval or time range."},
    },
)
def get_fact_quote_candles(
    ticker: str,
    start: str | None = Query(None, description="Range start (ISO 8601)."),
    end: str | None = Query(None, description="Range end (ISO 8601)."),
    interval: str = Query("15m", description="5m, 15m, or 1h (not 1d)."),
    limit: int = Query(500, ge=1, le=5000),
    db: Session = Depends(get_db),
) -> list[CandleRead]:
    if interval == "1d":
        raise HTTPException(
            status_code=400,
            detail="Use GET /quotes/{ticker}/candles with interval=1d for daily candles.",
        )
    try:
        candle_uc.validate_interval(interval)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    start_dt = _parse_datetime(start)
    end_dt = _parse_datetime(end)
    if start is not None and start_dt is None:
        raise HTTPException(status_code=400, detail="Invalid start datetime; use ISO 8601 format.")
    if end is not None and end_dt is None:
        raise HTTPException(status_code=400, detail="Invalid end datetime; use ISO 8601 format.")
    if start_dt is not None and end_dt is not None and end_dt < start_dt:
        raise HTTPException(status_code=400, detail="end must be >= start.")
    repo = FactQuoteRepository(db)
    # Make the number of raw points fetched proportional to the requested candle limit,
    # while keeping an upper safety cap to avoid excessive queries.
    # This reduces the chance that newer data is silently dropped due to a fixed point limit.
    points_limit = min(50_000, limit * 100)
    points = [
        (p.quoted_at, p.close_price)
        for p in repo.get_series(ticker.upper(), start=start_dt, end=end_dt, limit=points_limit)
    ]
    raw = candle_uc.intraday_candles_from_points(points, interval, limit=limit)
    return [CandleRead(**x) for x in raw]


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
