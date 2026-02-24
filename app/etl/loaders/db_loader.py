"""DB loading logic – wraps repository upserts with logging and transaction safety."""

from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.repositories.asset_repository import AssetRepository
from app.repositories.quote_repository import QuoteRepository

logger = get_logger(__name__)


def load_assets(db: Session, rows: list[dict]) -> int:
    """Upsert asset rows into dim_assets. Returns affected row count."""
    if not rows:
        logger.warning("load_assets called with empty row list.")
        return 0
    repo = AssetRepository(db)
    count = repo.upsert_many(rows)
    logger.info("Assets upserted: %d rows", count)
    return count


def load_quotes(db: Session, rows: list[dict]) -> int:
    """Upsert quote rows into fact_daily_quotes. Returns affected row count."""
    if not rows:
        logger.warning("load_quotes called with empty row list.")
        return 0
    repo = QuoteRepository(db)
    count = repo.upsert_many(rows)
    logger.info("Quotes upserted: %d rows", count)
    return count
