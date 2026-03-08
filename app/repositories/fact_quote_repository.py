"""Database repository for FactQuote (intraday time-series hypertable)."""

from datetime import date, datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.db.models import FactQuote


class FactQuoteRepository:
    """Insert and query operations for the fact_quotes hypertable."""

    def __init__(self, db: Session) -> None:
        self.db = db

    def get_series(
        self,
        ticker: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[FactQuote]:
        """Return intraday price points for *ticker* in the given time window."""
        stmt = select(FactQuote).where(FactQuote.ticker == ticker.upper())
        if start:
            stmt = stmt.where(FactQuote.quoted_at >= start)
        if end:
            stmt = stmt.where(FactQuote.quoted_at <= end)
        stmt = stmt.order_by(FactQuote.quoted_at).limit(limit)
        return list(self.db.execute(stmt).scalars().all())

    def get_for_trade_date(self, ticker: str, trade_date: date) -> list[FactQuote]:
        stmt = (
            select(FactQuote)
            .where(
                FactQuote.ticker == ticker.upper(),
                FactQuote.trade_date == trade_date,
            )
            .order_by(FactQuote.quoted_at)
        )
        return list(self.db.execute(stmt).scalars().all())

    def insert_many(self, rows: list[dict]) -> int:
        """Insert intraday quote rows.  On conflict (ticker, quoted_at) do nothing
        to preserve idempotency — Timescale hypertable rows are immutable by design.

        Returns the number of rows actually inserted.
        """
        if not rows:
            return 0
        stmt = insert(FactQuote).values(rows)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["ticker", "quoted_at"]
        )
        # For INSERT ... ON CONFLICT DO NOTHING, rowcount is not reliably
        # available on all drivers.  We use rowcount when > 0, else 0.
        result = self.db.execute(stmt)
        # Commit is handled by the caller (managed_session) to provide atomicity
        count = result.rowcount
        return count if count is not None and count >= 0 else 0

