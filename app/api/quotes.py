"""Quotes router."""

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db.engine import get_db
from app.repositories.quote_repository import QuoteRepository
from app.schemas import DailyQuoteRead

router = APIRouter(prefix="/quotes", tags=["Quotes"])


@router.get(
    "/latest",
    response_model=dict,
    summary="Latest quotes",
    description="Return the most recent available daily quote per ticker.",
)
def get_latest_quotes(
    tickers: str | None = Query(
        None,
        description="Comma-separated list of tickers, e.g. PETR4,VALE3",
    ),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
) -> dict:
    ticker_list = [t.strip().upper() for t in tickers.split(",")] if tickers else None
    repo = QuoteRepository(db)
    items, total = repo.get_latest_per_ticker(tickers=ticker_list, limit=limit, offset=offset)
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": [DailyQuoteRead.model_validate(q) for q in items],
    }


@router.get(
    "/{ticker}/history",
    response_model=list[DailyQuoteRead],
    summary="Quote history for a ticker",
    description="Return historical daily quotes for a specific ticker.",
)
def get_quote_history(
    ticker: str,
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    limit: int = Query(365, ge=1, le=1000),
    db: Session = Depends(get_db),
) -> list[DailyQuoteRead]:
    repo = QuoteRepository(db)
    items = repo.get_history(
        ticker=ticker.upper(),
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )
    if not items:
        raise HTTPException(status_code=404, detail=f"No quotes found for ticker '{ticker}'.")
    return [DailyQuoteRead.model_validate(q) for q in items]
