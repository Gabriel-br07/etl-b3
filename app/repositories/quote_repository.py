"""Database repository for FactDailyQuote."""

from datetime import date

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.db.models import FactDailyQuote


class QuoteRepository:
    """CRUD and upsert operations for fact_daily_quotes."""

    def __init__(self, db: Session) -> None:
        self.db = db

    def get_latest_per_ticker(
        self,
        tickers: list[str] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[FactDailyQuote], int]:
        """Return latest quote per ticker (most recent trade_date)."""
        # Subquery: max trade_date per ticker
        sub = select(
            FactDailyQuote.ticker,
            func.max(FactDailyQuote.trade_date).label("max_date"),
        )
        if tickers:
            upper = [t.upper() for t in tickers]
            sub = sub.where(FactDailyQuote.ticker.in_(upper))
        sub = sub.group_by(FactDailyQuote.ticker).subquery()

        stmt = (
            select(FactDailyQuote)
            .join(
                sub,
                (FactDailyQuote.ticker == sub.c.ticker)
                & (FactDailyQuote.trade_date == sub.c.max_date),
            )
            .order_by(FactDailyQuote.ticker)
        )
        total = self.db.execute(
            select(func.count()).select_from(stmt.subquery())
        ).scalar_one()
        items = self.db.execute(stmt.limit(limit).offset(offset)).scalars().all()
        return list(items), total

    def get_history(
        self,
        ticker: str,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 365,
    ) -> list[FactDailyQuote]:
        stmt = select(FactDailyQuote).where(
            FactDailyQuote.ticker == ticker.upper()
        )
        if start_date:
            stmt = stmt.where(FactDailyQuote.trade_date >= start_date)
        if end_date:
            stmt = stmt.where(FactDailyQuote.trade_date <= end_date)
        stmt = stmt.order_by(FactDailyQuote.trade_date.desc()).limit(limit)
        return list(self.db.execute(stmt).scalars().all())

    def upsert_many(self, rows: list[dict]) -> int:
        """Upsert quotes by (ticker, trade_date). Returns affected row count."""
        if not rows:
            return 0
        stmt = insert(FactDailyQuote).values(rows)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_quote_ticker_date",
            set_={
                "last_price": stmt.excluded.last_price,
                "min_price": stmt.excluded.min_price,
                "max_price": stmt.excluded.max_price,
                "avg_price": stmt.excluded.avg_price,
                "variation_pct": stmt.excluded.variation_pct,
                "financial_volume": stmt.excluded.financial_volume,
                "trade_count": stmt.excluded.trade_count,
                "source_file_name": stmt.excluded.source_file_name,
                "ingested_at": func.now(),
            },
        )
        result = self.db.execute(stmt)
        # Commit is handled by the caller (managed_session) to provide atomicity
        return result.rowcount
