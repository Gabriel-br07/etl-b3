"""Optional PostgreSQL smoke tests (run with -m db when DATABASE_URL is reachable)."""

from sqlalchemy import text

import pytest

pytestmark = pytest.mark.db


def test_database_select_one():
    """Verify the configured engine can open a connection and run SELECT 1."""
    from app.db.engine import engine

    try:
        with engine.connect() as conn:
            row = conn.execute(text("SELECT 1")).one()
    except Exception as exc:  # noqa: BLE001 — surface skip reason for developers
        pytest.skip(f"Database not reachable: {exc}")

    assert row[0] == 1
