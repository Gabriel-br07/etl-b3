"""DB loading logic – wraps repository upserts with logging and transaction safety.

Public API
----------
load_assets(db, rows)       → int  (dim_assets upsert)
load_trades(db, rows)       → int  (fact_daily_trades upsert)
load_daily_quotes(db, rows) → int  (fact_daily_quotes upsert – legacy)
load_intraday_quotes(db, rows) → int  (fact_quotes hypertable insert)
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.repositories.asset_repository import AssetRepository

from app.repositories.fact_quote_repository import FactQuoteRepository
from app.repositories.quote_repository import QuoteRepository
from app.repositories.trade_repository import TradeRepository

logger = get_logger(__name__)

# Chunk size for bulk upserts – avoids hitting PostgreSQL parameter limits
_CHUNK_SIZE = 500


def _chunked(lst: list, size: int):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def load_assets(db: Session, rows: list[dict]) -> int:
    """Upsert asset rows into dim_assets. Returns affected row count."""
    if not rows:
        logger.warning("load_assets called with empty row list.")
        return 0
    repo = AssetRepository(db)
    total = 0
    for chunk in _chunked(rows, _CHUNK_SIZE):
        total += repo.upsert_many(chunk)
    logger.info("Assets upserted: %d rows", total)
    return total


def load_trades(db: Session, rows: list[dict]) -> int:
    """Upsert consolidated trade rows into fact_daily_trades. Returns affected row count."""
    if not rows:
        logger.warning("load_trades called with empty row list.")
        return 0
    repo = TradeRepository(db)
    total = 0
    for chunk in _chunked(rows, _CHUNK_SIZE):
        total += repo.upsert_many(chunk)
    logger.info("Trades upserted: %d rows", total)
    return total


def load_daily_quotes(db: Session, rows: list[dict]) -> int:
    """Upsert daily quote rows into fact_daily_quotes (legacy). Returns affected row count."""
    if not rows:
        logger.warning(
            "load_daily_quotes called with empty row list – no rows will be inserted. "
            "Check that transform_daily_quotes produced data and that the CSV "
            "contains columns mappable to the fact_daily_quotes schema."
        )
        return 0
    logger.info(
        "load_daily_quotes: inserting %d rows. First row keys: %s",
        len(rows),
        list(rows[0].keys()),
    )
    repo = QuoteRepository(db)
    total = 0
    for chunk in _chunked(rows, _CHUNK_SIZE):
        total += repo.upsert_many(chunk)
    logger.info("Daily quotes upserted: %d rows", total)
    return total


# Keep the old name as an alias for backward compatibility with existing callers
load_quotes = load_daily_quotes


def load_intraday_quotes(db: Session, rows: list[dict]) -> int:
    """Insert intraday price points into fact_quotes hypertable. Returns inserted row count."""
    if not rows:
        logger.warning(
            "load_intraday_quotes called with empty row list – no rows will be inserted. "
            "Check that transform_jsonl_quotes produced data and that the JSONL "
            "contains the required 'ticker' and 'quoted_at' fields."
        )
        return 0
    logger.info(
        "load_intraday_quotes: inserting %d rows. First row keys: %s",
        len(rows),
        list(rows[0].keys()),
    )
    repo = FactQuoteRepository(db)
    total = 0
    for chunk in _chunked(rows, _CHUNK_SIZE):
        total += repo.insert_many(chunk)
    logger.info("Intraday quotes inserted: %d rows", total)
    return total
