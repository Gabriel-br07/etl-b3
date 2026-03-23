"""Repository for ``fact_cotahist_daily`` (annual COTAHIST ingestion)."""

from __future__ import annotations

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.db.models import FactCotahistDaily


class CotahistQuoteRepository:
    """Bulk upsert COTAHIST rows by natural key (idempotent reruns)."""

    def __init__(self, db: Session) -> None:
        self.db = db

    def upsert_many(self, rows: list[dict]) -> int:
        """Upsert rows on ``uq_cotahist_natural_key``. Returns statement rowcount sum."""
        if not rows:
            return 0
        stmt = insert(FactCotahistDaily).values(rows)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_cotahist_natural_key",
            set_={
                "ticker": stmt.excluded.ticker,
                "nomres": stmt.excluded.nomres,
                "open_price": stmt.excluded.open_price,
                "max_price": stmt.excluded.max_price,
                "min_price": stmt.excluded.min_price,
                "avg_price": stmt.excluded.avg_price,
                "last_price": stmt.excluded.last_price,
                "best_bid": stmt.excluded.best_bid,
                "best_ask": stmt.excluded.best_ask,
                "trade_count": stmt.excluded.trade_count,
                "quantity_total": stmt.excluded.quantity_total,
                "volume_financial": stmt.excluded.volume_financial,
                "strike_price": stmt.excluded.strike_price,
                "expiration_date": stmt.excluded.expiration_date,
                "quotation_factor": stmt.excluded.quotation_factor,
                "strike_points": stmt.excluded.strike_points,
                "source_file_name": stmt.excluded.source_file_name,
                "ingested_at": func.now(),
            },
        )
        result = self.db.execute(stmt)
        return result.rowcount  # ty: ignore[unresolved-attribute]
