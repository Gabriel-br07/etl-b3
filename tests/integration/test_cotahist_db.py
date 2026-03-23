"""COTAHIST ingest against PostgreSQL (run with ``-m db`` when DB + schema are available)."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.db

FIXTURE = Path(__file__).resolve().parent.parent / "fixtures" / "b3" / "cotahist_minimal.txt"


def test_cotahist_ingest_idempotent_row_count():
    """Re-ingesting the same file does not grow ``fact_cotahist_daily`` (upsert on natural key)."""
    from app.db.engine import engine, managed_session
    from app.use_cases.quotes.cotahist_annual_ingestion import ingest_cotahist_txt_file

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Database not reachable: {exc}")

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT COUNT(*) FROM fact_cotahist_daily"))
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"fact_cotahist_daily not available: {exc}")

    assert FIXTURE.is_file(), f"Missing fixture: {FIXTURE}"

    with managed_session() as db:
        ingest_cotahist_txt_file(db, FIXTURE, track_in_file_duplicates=False)
    with managed_session() as db:
        count1 = db.execute(text("SELECT COUNT(*) FROM fact_cotahist_daily")).scalar_one()

    with managed_session() as db:
        ingest_cotahist_txt_file(db, FIXTURE, track_in_file_duplicates=False)
    with managed_session() as db:
        count2 = db.execute(text("SELECT COUNT(*) FROM fact_cotahist_daily")).scalar_one()

    assert count1 == count2
    assert count1 >= 1
