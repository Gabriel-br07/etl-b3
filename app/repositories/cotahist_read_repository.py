"""Read-only queries for ``fact_cotahist_daily`` (API exposure)."""

from __future__ import annotations

from datetime import date

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db.models import FactCotahistDaily


class CotahistReadRepository:
    """Paginated reads for COTAHIST facts."""

    def __init__(self, db: Session) -> None:
        self.db = db

    def list_by_ticker(
        self,
        ticker: str,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[FactCotahistDaily], int]:
        """Rows for one ticker, ``trade_date`` desc, ``id`` desc; total count."""
        t = ticker.strip().upper()
        base = select(FactCotahistDaily).where(FactCotahistDaily.ticker == t)
        if start_date is not None:
            base = base.where(FactCotahistDaily.trade_date >= start_date)
        if end_date is not None:
            base = base.where(FactCotahistDaily.trade_date <= end_date)
        total = self.db.execute(
            select(func.count()).select_from(base.subquery())
        ).scalar_one()
        ordered = base.order_by(
            FactCotahistDaily.trade_date.desc(),
            FactCotahistDaily.id.desc(),
        )
        items = self.db.execute(ordered.limit(limit).offset(offset)).scalars().all()
        return list(items), total

    def list_filtered(
        self,
        *,
        ticker: str | None = None,
        trade_date: date | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        isin: str | None = None,
        tp_merc: str | None = None,
        cod_bdi: str | None = None,
        expiration_date: date | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[FactCotahistDaily], int]:
        """Filterable collection; same ordering as ``list_by_ticker``."""
        base = select(FactCotahistDaily)
        if ticker:
            base = base.where(FactCotahistDaily.ticker == ticker.strip().upper())
        if trade_date is not None:
            base = base.where(FactCotahistDaily.trade_date == trade_date)
        if start_date is not None:
            base = base.where(FactCotahistDaily.trade_date >= start_date)
        if end_date is not None:
            base = base.where(FactCotahistDaily.trade_date <= end_date)
        if isin:
            base = base.where(FactCotahistDaily.isin == isin.strip().upper())
        if tp_merc:
            base = base.where(FactCotahistDaily.tp_merc == tp_merc.strip())
        if cod_bdi:
            base = base.where(FactCotahistDaily.cod_bdi == cod_bdi.strip())
        if expiration_date is not None:
            base = base.where(FactCotahistDaily.expiration_date == expiration_date)
        total = self.db.execute(
            select(func.count()).select_from(base.subquery())
        ).scalar_one()
        ordered = base.order_by(
            FactCotahistDaily.trade_date.desc(),
            FactCotahistDaily.ticker.asc(),
            FactCotahistDaily.id.desc(),
        )
        items = self.db.execute(ordered.limit(limit).offset(offset)).scalars().all()
        return list(items), total

    def min_max_trade_dates(self, ticker: str) -> tuple[date | None, date | None]:
        t = ticker.strip().upper()
        stmt = select(
            func.min(FactCotahistDaily.trade_date),
            func.max(FactCotahistDaily.trade_date),
        ).where(FactCotahistDaily.ticker == t)
        row = self.db.execute(stmt).one()
        return row[0], row[1]
