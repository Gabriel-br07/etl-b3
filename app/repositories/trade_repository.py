"""Database repository for FactDailyTrade (negocios_consolidados)."""

from datetime import date

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.db.models import FactDailyTrade


class TradeRepository:
    """CRUD and upsert operations for fact_daily_trades."""

    def __init__(self, db: Session) -> None:
        self.db = db

    def get_by_ticker_date(self, ticker: str, trade_date: date) -> FactDailyTrade | None:
        stmt = select(FactDailyTrade).where(
            FactDailyTrade.ticker == ticker.upper(),
            FactDailyTrade.trade_date == trade_date,
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def get_history(
        self,
        ticker: str,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 365,
    ) -> list[FactDailyTrade]:
        stmt = select(FactDailyTrade).where(
            FactDailyTrade.ticker == ticker.upper()
        )
        if start_date:
            stmt = stmt.where(FactDailyTrade.trade_date >= start_date)
        if end_date:
            stmt = stmt.where(FactDailyTrade.trade_date <= end_date)
        stmt = stmt.order_by(FactDailyTrade.trade_date.desc()).limit(limit)
        return list(self.db.execute(stmt).scalars().all())

    def upsert_many(self, rows: list[dict]) -> int:
        """Upsert trades by (ticker, trade_date). Returns affected row count."""
        if not rows:
            return 0
        stmt = insert(FactDailyTrade).values(rows)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_daily_trade_ticker_date",
            set_={
                "open_price": stmt.excluded.open_price,
                "close_price": stmt.excluded.close_price,
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
        self.db.commit()
        return result.rowcount

