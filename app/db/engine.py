"""SQLAlchemy engine and session factory.

Usage
-----
FastAPI routes:
    from app.db.engine import get_db
    def my_route(db: Session = Depends(get_db)): ...

ETL / background code:
    from app.db.engine import managed_session
    # managed_session is the single-commit authority for ETL batches.
    # Repository methods MUST NOT call db.commit(); instead let the
    # context manager commit once on successful exit to provide atomicity.
    with managed_session() as db:
        repo.upsert_many(rows)
"""

from __future__ import annotations

import logging
import time
from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
    echo=(settings.app_env == "development"),
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def wait_for_db(retries: int = 10, delay: float = 3.0) -> None:
    """Block until the database is reachable or raise after *retries* attempts.

    Called once at application / scheduler startup so the process doesn't crash
    immediately if the DB container is still initialising.
    """
    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("DB connection established (attempt %d/%d)", attempt, retries)
            return
        except OperationalError as exc:
            logger.warning(
                "DB not ready (attempt %d/%d): %s — retrying in %.0fs",
                attempt, retries, exc, delay,
            )
            time.sleep(delay)
    raise RuntimeError(
        f"Could not connect to the database after {retries} attempts. "
        "Check DATABASE_URL and that the db service is healthy."
    )


@contextmanager
def managed_session() -> Generator[Session, None, None]:
    """Context manager that yields a Session and handles commit/rollback/close.

    This context manager is the single-commit authority for ETL and background
    code. Repository implementations used by ETL (for example
    ``AssetRepository``, ``QuoteRepository``, etc.) MUST NOT call
    ``db.commit()`` themselves. Instead, perform multiple repository calls
    inside the same ``with managed_session():`` block and let this manager
    commit once on successful exit — providing atomicity across the batch.

    Example::

        with managed_session() as db:
            etl_repo = ETLRunRepository(db)
            run = etl_repo.start_run(...)
            asset_repo.upsert_many(rows)
            trade_repo.upsert_many(rows)
            etl_repo.finish_run(run, ...)
    """
    db: Session = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency that yields a database session."""
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()
