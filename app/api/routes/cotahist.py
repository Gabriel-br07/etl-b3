"""COTAHIST read API (fact_cotahist_daily)."""

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db.engine import get_db
from app.repositories.cotahist_read_repository import CotahistReadRepository
from app.schemas import CotahistDailyRead, PaginatedResponse

router = APIRouter(prefix="/cotahist", tags=["COTAHIST"])


def _row_to_read(row) -> CotahistDailyRead:
    return CotahistDailyRead(
        ticker=row.ticker,
        trade_date=row.trade_date,
        open_price=row.open_price,
        min_price=row.min_price,
        max_price=row.max_price,
        avg_price=row.avg_price,
        close_price=row.last_price,
        best_bid=row.best_bid,
        best_ask=row.best_ask,
        trade_count=row.trade_count,
        quantity_total=row.quantity_total,
        volume_financial=row.volume_financial,
        isin=row.isin,
        especi=row.especi,
        tp_merc=row.tp_merc,
        expiration_date=row.expiration_date,
        strike_price=row.strike_price,
    )


@router.get(
    "/{ticker}/history",
    response_model=PaginatedResponse[CotahistDailyRead],
    summary="COTAHIST daily history for a ticker",
    description=(
        "Full COTAHIST type-01 history for the ticker from ``fact_cotahist_daily``. "
        "Multiple rows per calendar date are possible (different B3 natural keys). "
        "Ordered by ``trade_date`` descending, then ``id`` descending. "
        "Returns an empty ``items`` list when no rows match."
    ),
    responses={
        200: {"description": "Paginated COTAHIST rows."},
        400: {"description": "Invalid date range."},
    },
)
def get_cotahist_ticker_history(
    ticker: str,
    start_date: date | None = Query(None, description="Inclusive start trade_date."),
    end_date: date | None = Query(None, description="Inclusive end trade_date."),
    limit: int = Query(100, ge=1, le=1000, description="Page size."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
    db: Session = Depends(get_db),
) -> PaginatedResponse[CotahistDailyRead]:
    if start_date is not None and end_date is not None and end_date < start_date:
        raise HTTPException(status_code=400, detail="end_date must be >= start_date.")
    repo = CotahistReadRepository(db)
    rows, total = repo.list_by_ticker(
        ticker,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        offset=offset,
    )
    return PaginatedResponse(
        total=total,
        limit=limit,
        offset=offset,
        items=[_row_to_read(r) for r in rows],
    )


@router.get(
    "",
    response_model=PaginatedResponse[CotahistDailyRead],
    summary="List COTAHIST rows (filterable)",
    description=(
        "Paginated, filterable collection from ``fact_cotahist_daily``. "
        "All filters combine with AND. "
        "Ordered by ``trade_date`` desc, ``ticker`` asc, ``id`` desc."
    ),
    responses={
        200: {"description": "Paginated COTAHIST rows."},
        400: {"description": "Invalid date range."},
    },
)
def list_cotahist(
    ticker: str | None = Query(None, description="Exact ticker match (uppercased)."),
    trade_date: date | None = Query(None, description="Exact trade date."),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    isin: str | None = Query(None),
    tp_merc: str | None = Query(None, max_length=3),
    cod_bdi: str | None = Query(None, max_length=2),
    expiration_date: date | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
) -> PaginatedResponse[CotahistDailyRead]:
    if start_date is not None and end_date is not None and end_date < start_date:
        raise HTTPException(status_code=400, detail="end_date must be >= start_date.")
    repo = CotahistReadRepository(db)
    rows, total = repo.list_filtered(
        ticker=ticker,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
        isin=isin,
        tp_merc=tp_merc,
        cod_bdi=cod_bdi,
        expiration_date=expiration_date,
        limit=limit,
        offset=offset,
    )
    return PaginatedResponse(
        total=total,
        limit=limit,
        offset=offset,
        items=[_row_to_read(r) for r in rows],
    )
