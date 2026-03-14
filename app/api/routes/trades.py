"""Trades router.

Exposes daily consolidated trades (NegociosConsolidados) from fact_daily_trades.
"""

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db.engine import get_db
from app.repositories.trade_repository import TradeRepository
from app.schemas import PaginatedResponse, TradeRead

router = APIRouter(prefix="/trades", tags=["Trades"])


@router.get(
    "",
    response_model=PaginatedResponse[TradeRead],
    summary="List daily trades",
    description=(
        "Return a paginated list of daily consolidated trades from the database. "
        "Filter by trade_date (exact day), ticker, or start_date/end_date range. "
        "Ordered by trade_date descending, then ticker. "
        "At least one filter (trade_date, ticker, or start_date/end_date) is recommended to avoid large result sets."
    ),
    responses={
        200: {"description": "Paginated list of trades."},
        400: {"description": "Invalid parameters (e.g. end_date before start_date)."},
    },
)
def list_trades(
    trade_date: date | None = Query(
        None,
        description="Filter by exact trade date (YYYY-MM-DD).",
    ),
    ticker: str | None = Query(
        None,
        description="Filter by ticker symbol (e.g. PETR4).",
    ),
    start_date: date | None = Query(
        None,
        description="Start of date range (YYYY-MM-DD). Use with end_date.",
    ),
    end_date: date | None = Query(
        None,
        description="End of date range (YYYY-MM-DD). Use with start_date.",
    ),
    limit: int = Query(50, ge=1, le=500, description="Maximum number of items to return."),
    offset: int = Query(0, ge=0, description="Number of items to skip."),
    db: Session = Depends(get_db),
) -> PaginatedResponse[TradeRead]:
    if start_date is not None and end_date is not None and end_date < start_date:
        raise HTTPException(
            status_code=400,
            detail="end_date must be greater than or equal to start_date.",
        )
    repo = TradeRepository(db)
    items, total = repo.list_trades(
        trade_date=trade_date,
        ticker=ticker,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        offset=offset,
    )
    return PaginatedResponse(
        total=total,
        limit=limit,
        offset=offset,
        items=[TradeRead.model_validate(t) for t in items],
    )


@router.get(
    "/{ticker}/history",
    response_model=list[TradeRead],
    summary="Trade history for a ticker",
    description=(
        "Return historical daily trades for a single ticker. "
        "Optionally filter by start_date and end_date. "
        "Ordered by trade_date descending. Data is from the database (fact_daily_trades)."
    ),
    responses={
        200: {"description": "List of daily trades for the ticker."},
        404: {"description": "No trades found for the ticker in the given range."},
    },
)
def get_trade_history(
    ticker: str,
    start_date: date | None = Query(
        None,
        description="Start of date range (YYYY-MM-DD).",
    ),
    end_date: date | None = Query(
        None,
        description="End of date range (YYYY-MM-DD).",
    ),
    limit: int = Query(365, ge=1, le=1000, description="Maximum number of days to return."),
    db: Session = Depends(get_db),
) -> list[TradeRead]:
    if start_date is not None and end_date is not None and end_date < start_date:
        raise HTTPException(
            status_code=400,
            detail="end_date must be greater than or equal to start_date.",
        )
    repo = TradeRepository(db)
    items = repo.get_history(
        ticker=ticker.upper(),
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )
    if not items:
        raise HTTPException(
            status_code=404,
            detail=f"No trades found for ticker '{ticker}'.",
        )
    return [TradeRead.model_validate(t) for t in items]


@router.get(
    "/{ticker}",
    response_model=TradeRead,
    summary="Get trade by ticker and date",
    description=(
        "Return the single daily trade record for the given ticker and trade_date. "
        "Data is from the database (fact_daily_trades)."
    ),
    responses={
        200: {"description": "The daily trade record."},
        404: {"description": "No trade found for the ticker on the given date."},
    },
)
def get_trade_by_ticker_date(
    ticker: str,
    trade_date: date = Query(
        ...,
        description="Trade date (YYYY-MM-DD).",
    ),
    db: Session = Depends(get_db),
) -> TradeRead:
    repo = TradeRepository(db)
    trade = repo.get_by_ticker_date(ticker.upper(), trade_date)
    if not trade:
        raise HTTPException(
            status_code=404,
            detail=f"Trade not found for ticker '{ticker}' on date {trade_date.isoformat()}.",
        )
    return TradeRead.model_validate(trade)
