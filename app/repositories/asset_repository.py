"""Database repository for DimAsset."""

from sqlalchemy import func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.db.models import DimAsset


class AssetRepository:
    """CRUD and upsert operations for dim_assets."""

    def __init__(self, db: Session) -> None:
        self.db = db

    def get_by_ticker(self, ticker: str) -> DimAsset | None:
        stmt = select(DimAsset).where(DimAsset.ticker == ticker.upper())
        return self.db.execute(stmt).scalar_one_or_none()

    def list_assets(
        self,
        q: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[DimAsset], int]:
        """Return a page of assets and the total count."""
        base = select(DimAsset)
        if q:
            pattern = f"%{q.upper()}%"
            base = base.where(
                or_(
                    func.upper(DimAsset.ticker).like(pattern),
                    func.upper(DimAsset.asset_name).like(pattern),
                )
            )
        total = self.db.execute(
            select(func.count()).select_from(base.subquery())
        ).scalar_one()
        items = self.db.execute(
            base.order_by(DimAsset.ticker).limit(limit).offset(offset)
        ).scalars().all()
        return list(items), total

    def upsert_many(self, rows: list[dict]) -> int:
        """Upsert a list of asset dicts by ticker. Returns affected row count."""
        if not rows:
            return 0
        stmt = insert(DimAsset).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["ticker"],
            set_={
                "asset_name": stmt.excluded.asset_name,
                "isin": stmt.excluded.isin,
                "segment": stmt.excluded.segment,
                "source_file_date": stmt.excluded.source_file_date,
                "updated_at": func.now(),
            },
        )
        result = self.db.execute(stmt)
        self.db.commit()
        return result.rowcount
