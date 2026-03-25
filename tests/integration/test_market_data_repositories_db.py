"""Integration tests for cotahist, ETL run listing, and market overview repositories (optional PostgreSQL)."""

from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.db

FIXTURE = Path(__file__).resolve().parent.parent / "fixtures" / "b3" / "cotahist_minimal.txt"


def _db_available() -> bool:
    from app.db.engine import engine

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:
        return False
    return True


@pytest.fixture(scope="module")
def cotahist_seeded_db():
    if not _db_available():
        pytest.skip("Database not reachable")
    from app.db.engine import managed_session
    from app.use_cases.quotes.cotahist_annual_ingestion import ingest_cotahist_txt_file

    with managed_session() as db:
        try:
            db.execute(text("SELECT COUNT(*) FROM fact_cotahist_daily"))
        except Exception as exc:
            pytest.skip(f"fact_cotahist_daily not available: {exc}")
        ingest_cotahist_txt_file(db, FIXTURE, track_in_file_duplicates=False)
    yield


def test_cotahist_read_repository_list_by_ticker(cotahist_seeded_db) -> None:
    from app.db.engine import managed_session
    from app.repositories.cotahist_read_repository import CotahistReadRepository

    with managed_session() as db:
        repo = CotahistReadRepository(db)
        rows, total = repo.list_by_ticker("PETR4", limit=10, offset=0)
        assert total >= 1
        assert rows[0].ticker == "PETR4"


def test_cotahist_min_max_trade_dates(cotahist_seeded_db) -> None:
    from app.db.engine import managed_session
    from app.repositories.cotahist_read_repository import CotahistReadRepository

    with managed_session() as db:
        repo = CotahistReadRepository(db)
        lo, hi = repo.min_max_trade_dates("PETR4")
        assert lo is not None and hi is not None


def test_etl_list_runs_empty_ok() -> None:
    if not _db_available():
        pytest.skip("Database not reachable")
    from app.db.engine import managed_session
    from app.repositories.etl_run_repository import ETLRunRepository

    with managed_session() as db:
        try:
            db.execute(text("SELECT COUNT(*) FROM etl_runs"))
        except Exception as exc:
            pytest.skip(f"etl_runs not available: {exc}")
        repo = ETLRunRepository(db)
        rows, total = repo.list_runs(limit=5, offset=0)
        assert isinstance(rows, list)
        assert total >= 0


def test_market_overview_runs_without_error() -> None:
    if not _db_available():
        pytest.skip("Database not reachable")
    from app.db.engine import managed_session
    from app.repositories.market_repository import MarketRepository

    with managed_session() as db:
        try:
            db.execute(text("SELECT COUNT(*) FROM fact_daily_quotes"))
        except Exception as exc:
            pytest.skip(f"fact_daily_quotes not available: {exc}")
        repo = MarketRepository(db)
        out = repo.overview_for_date(date(2099, 1, 2))
        assert out["traded_assets_count"] == 0
        assert out["top_gainers"] == []
