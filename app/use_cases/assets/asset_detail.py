"""Compose asset coverage and overview payloads."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy.orm import Session

from app.integrations.b3.exceptions import (
    B3TemporaryBlockError,
    B3TickerNotFoundError,
    B3UnexpectedResponseError,
)
from app.repositories.asset_repository import AssetRepository
from app.repositories.cotahist_read_repository import CotahistReadRepository
from app.repositories.fact_quote_repository import FactQuoteRepository
from app.repositories.quote_repository import QuoteRepository
from app.repositories.trade_repository import TradeRepository
from app.schemas import (
    AssetCoverageRead,
    AssetOverviewRead,
    AssetOverviewSectionMeta,
    AssetRead,
    DailyQuoteRead,
    DateRangeRead,
    FactQuoteRead,
    TradeRead,
)
from app.use_cases.quotes.get_latest_snapshot import get_latest_snapshot


def build_asset_coverage(db: Session, ticker: str) -> AssetCoverageRead:
    t = ticker.strip().upper()
    assets = AssetRepository(db)
    quotes = QuoteRepository(db)
    trades = TradeRepository(db)
    facts = FactQuoteRepository(db)
    cot = CotahistReadRepository(db)

    has_master = assets.get_by_ticker(t) is not None
    qmin, qmax = quotes.min_max_trade_dates(t)
    tmin, tmax = trades.min_max_trade_dates(t)
    fmin, fmax = facts.min_max_quoted_at(t)
    cmin, cmax = cot.min_max_trade_dates(t)

    return AssetCoverageRead(
        ticker=t,
        has_asset_master=has_master,
        daily_quotes=DateRangeRead(min_date=qmin, max_date=qmax) if qmin or qmax else None,
        daily_trades=DateRangeRead(min_date=tmin, max_date=tmax) if tmin or tmax else None,
        fact_quotes=DateRangeRead(
            min_date=fmin.date() if fmin else None,
            max_date=fmax.date() if fmax else None,
        )
        if (fmin or fmax)
        else None,
        cotahist=DateRangeRead(min_date=cmin, max_date=cmax) if cmin or cmax else None,
        live_delayed_b3=True,
    )


def build_asset_overview(db: Session, ticker: str) -> AssetOverviewRead:
    t = ticker.strip().upper()
    assets = AssetRepository(db)
    quotes = QuoteRepository(db)
    trades = TradeRepository(db)
    facts = FactQuoteRepository(db)
    cot = CotahistReadRepository(db)

    asset = assets.get_by_ticker(t)
    dq = quotes.get_latest_for_ticker(t)
    dt = trades.get_latest_for_ticker(t)
    fq = facts.get_latest_for_ticker(t)

    live: dict | None = None
    try:
        snap = get_latest_snapshot(t)
        live = {
            "ticker": snap.ticker,
            "trade_date": snap.trade_date.isoformat(),
            "latest_close_price": str(snap.latest_close_price)
            if snap.latest_close_price is not None
            else None,
            "latest_price_fluctuation_pct": str(snap.latest_price_fluctuation_pct)
            if snap.latest_price_fluctuation_pct is not None
            else None,
            "source": snap.source,
            "delayed": snap.delayed,
            "fetched_at": snap.fetched_at.isoformat(),
        }
    except (
        B3TickerNotFoundError,
        B3TemporaryBlockError,
        B3UnexpectedResponseError,
        OSError,
        ValueError,
    ):
        live = None

    qmin, qmax = quotes.min_max_trade_dates(t)
    tmin, tmax = trades.min_max_trade_dates(t)
    cmin, cmax = cot.min_max_trade_dates(t)

    updates: list[datetime] = []
    if asset:
        updates.extend([asset.created_at, asset.updated_at])
    if dq and dq.ingested_at:
        updates.append(dq.ingested_at)
    if dt and dt.ingested_at:
        updates.append(dt.ingested_at)
    if fq and fq.ingested_at:
        updates.append(fq.ingested_at)
    latest_up = max(updates) if updates else None

    sections = {
        "asset": AssetOverviewSectionMeta(has_data=asset is not None),
        "daily_quote": AssetOverviewSectionMeta(has_data=dq is not None),
        "daily_trade": AssetOverviewSectionMeta(has_data=dt is not None),
        "intraday_db": AssetOverviewSectionMeta(has_data=fq is not None),
        "live_b3": AssetOverviewSectionMeta(has_data=live is not None),
    }

    return AssetOverviewRead(
        ticker=t,
        asset=AssetRead.model_validate(asset) if asset else None,
        latest_daily_quote=DailyQuoteRead.model_validate(dq) if dq else None,
        latest_daily_trade=TradeRead.model_validate(dt) if dt else None,
        latest_intraday_db=FactQuoteRead.model_validate(fq) if fq else None,
        live_snapshot=live,
        historical_range_daily_quotes=DateRangeRead(min_date=qmin, max_date=qmax)
        if (qmin or qmax)
        else None,
        historical_range_daily_trades=DateRangeRead(min_date=tmin, max_date=tmax)
        if (tmin or tmax)
        else None,
        historical_range_cotahist=DateRangeRead(min_date=cmin, max_date=cmax)
        if (cmin or cmax)
        else None,
        variation_pct_latest=dq.variation_pct if dq else None,
        financial_volume_latest=(
            dq.financial_volume
            if dq and dq.financial_volume is not None
            else (dt.financial_volume if dt else None)
        ),
        latest_update_at=latest_up,
        sections=sections,
    )
