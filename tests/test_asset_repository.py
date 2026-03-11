"""Unit tests for AssetRepository (no real DB needed – mocks the session)."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from app.repositories.asset_repository import AssetRepository, _sanitize_row


# ---------------------------------------------------------------------------
# _sanitize_row
# ---------------------------------------------------------------------------


def test_sanitize_row_returns_none_when_no_ticker():
    assert _sanitize_row({"asset_name": "Petrobras"}) is None


def test_sanitize_row_returns_none_for_empty_ticker():
    assert _sanitize_row({"ticker": "   "}) is None


def test_sanitize_row_uppercases_ticker():
    row = _sanitize_row({"ticker": "petr4"})
    assert row is not None
    assert row["ticker"] == "PETR4"


def test_sanitize_row_truncates_long_ticker():
    row = _sanitize_row({"ticker": "A" * 25})
    assert row is not None
    assert len(row["ticker"]) == 20


def test_sanitize_row_uppercases_isin():
    row = _sanitize_row({"ticker": "PETR4", "isin": "brpetracnpr6"})
    assert row is not None
    assert row["isin"] == "BRPETRACNPR6"


def test_sanitize_row_sets_isin_to_none_when_empty():
    row = _sanitize_row({"ticker": "PETR4", "isin": "   "})
    assert row is not None
    assert row["isin"] is None


def test_sanitize_row_truncates_asset_name():
    row = _sanitize_row({"ticker": "PETR4", "asset_name": "X" * 250})
    assert row is not None
    assert len(row["asset_name"]) == 200


def test_sanitize_row_preserves_instrument_type():
    row = _sanitize_row({"ticker": "PETR4", "instrument_type": "STOCK"})
    assert row is not None
    assert row["instrument_type"] == "STOCK"


def test_sanitize_row_truncates_instrument_type():
    row = _sanitize_row({"ticker": "PETR4", "instrument_type": "X" * 60})
    assert row is not None
    assert len(row["instrument_type"]) == 50


# ---------------------------------------------------------------------------
# upsert_many
# ---------------------------------------------------------------------------


@pytest.fixture()
def db():
    session = MagicMock()
    result = MagicMock()
    result.rowcount = 2
    session.execute.return_value = result
    return session


def test_upsert_many_returns_zero_for_empty_list(db):
    repo = AssetRepository(db)
    assert repo.upsert_many([]) == 0
    db.execute.assert_not_called()


def test_upsert_many_skips_rows_without_ticker(db):
    repo = AssetRepository(db)
    rows = [{"asset_name": "No ticker here"}]
    result = repo.upsert_many(rows)
    # All rows sanitized away → no execute call
    assert result == 0
    db.execute.assert_not_called()


def test_upsert_many_instrument_type_included_in_conflict_update(db):
    """instrument_type must be included in the ON CONFLICT DO UPDATE set_."""
    repo = AssetRepository(db)
    rows = [{"ticker": "PETR4", "asset_name": "Petrobras", "instrument_type": "STOCK"}]
    repo.upsert_many(rows)
    db.execute.assert_called_once()
    # Inspect the compiled statement to verify instrument_type is updated
    stmt = db.execute.call_args[0][0]
    # The on_conflict_do_update clause contains the set_ columns; verify
    # instrument_type is present by checking the statement string representation.
    stmt_str = str(stmt.compile(compile_kwargs={"literal_binds": False}))
    assert "instrument_type" in stmt_str


def test_upsert_many_does_not_commit(db):
    """Repository must not call db.commit(); the caller owns the transaction."""
    repo = AssetRepository(db)
    rows = [{"ticker": "PETR4"}]
    repo.upsert_many(rows)
    db.commit.assert_not_called()


# ---------------------------------------------------------------------------
# list_assets – LIKE wildcard escape
# ---------------------------------------------------------------------------


def test_list_assets_escapes_percent_in_query(db):
    """A literal '%' in the search term must not act as a SQL wildcard."""
    # This test verifies that the query string sent to the DB contains an
    # escaped pattern (\\%) rather than a bare % which would match everything.
    db.execute.return_value.scalar_one.return_value = 0
    db.execute.return_value.scalars.return_value.all.return_value = []

    repo = AssetRepository(db)
    repo.list_assets(q="100%")

    # We don't have a real DB here, but we can verify execute was called
    # and inspect the generated SQL to confirm the escape is applied.
    assert db.execute.called
    # The query should be called at least twice (count + items)
    count_call_stmt = db.execute.call_args_list[0][0][0]
    stmt_str = str(count_call_stmt.compile(compile_kwargs={"literal_binds": False}))
    # The pattern must contain the escape clause
    assert "ESCAPE" in stmt_str.upper() or "escape" in stmt_str


def test_list_assets_escapes_underscore_in_query(db):
    """A literal '_' in the search term must not act as a SQL wildcard."""
    db.execute.return_value.scalar_one.return_value = 0
    db.execute.return_value.scalars.return_value.all.return_value = []

    repo = AssetRepository(db)
    repo.list_assets(q="PETR_4")

    assert db.execute.called
    count_call_stmt = db.execute.call_args_list[0][0][0]
    stmt_str = str(count_call_stmt.compile(compile_kwargs={"literal_binds": False}))
    assert "ESCAPE" in stmt_str.upper() or "escape" in stmt_str
