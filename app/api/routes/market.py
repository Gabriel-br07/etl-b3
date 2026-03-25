"""Market-wide aggregated endpoints."""

from datetime import date

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db.engine import get_db
from app.repositories.market_repository import MarketRepository
from app.schemas import MarketMoverRead, MarketOverviewRead, MarketVolumeRankRead

router = APIRouter(prefix="/market", tags=["Market"])


@router.get(
    "/overview",
    response_model=MarketOverviewRead,
    summary="Market overview for a trading day",
    description=(
        "Consolidated rankings for ``trade_date``. "
        "**top_gainers / top_losers:** from ``fact_daily_quotes`` where ``variation_pct`` is not null, "
        "ordered by variation (desc for gainers, asc for losers), ticker ascending on ties. "
        "**most_traded_by_volume** and **most_traded_by_financial_volume:** from ``fact_daily_trades``, "
        "ordered by ``financial_volume`` descending (nulls last), ticker asc on ties. "
        "**highest_trade_count:** from ``fact_daily_trades``, ordered by ``trade_count`` descending (nulls last). "
        "**traded_assets_count:** distinct tickers in ``fact_daily_trades`` for the date."
    ),
    responses={200: {"description": "Overview payload for the date."}},
)
def get_market_overview(
    trade_date: date = Query(..., description="Trading session date (YYYY-MM-DD).", examples=["2024-06-14"]),
    db: Session = Depends(get_db),
) -> MarketOverviewRead:
    repo = MarketRepository(db)
    raw = repo.overview_for_date(trade_date)
    return MarketOverviewRead(
        trade_date=raw["trade_date"],
        top_gainers=[MarketMoverRead(**x) for x in raw["top_gainers"]],
        top_losers=[MarketMoverRead(**x) for x in raw["top_losers"]],
        most_traded_by_volume=[MarketVolumeRankRead(**x) for x in raw["most_traded_by_volume"]],
        most_traded_by_financial_volume=[
            MarketVolumeRankRead(**x) for x in raw["most_traded_by_financial_volume"]
        ],
        highest_trade_count=[MarketVolumeRankRead(**x) for x in raw["highest_trade_count"]],
        traded_assets_count=raw["traded_assets_count"],
    )
