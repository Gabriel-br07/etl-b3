"""Unit tests for asset row sanitization and upsert SQL shape."""

from unittest.mock import MagicMock

from app.repositories.asset_repository import AssetRepository, _sanitize_row


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


def test_upsert_many_includes_instrument_type_in_conflict_update():
    """instrument_type must appear in ON CONFLICT DO UPDATE."""
    session = MagicMock()
    result = MagicMock()
    result.rowcount = 1
    session.execute.return_value = result
    repo = AssetRepository(session)
    rows = [{"ticker": "PETR4", "asset_name": "Petrobras", "instrument_type": "STOCK"}]
    repo.upsert_many(rows)
    session.execute.assert_called_once()
    stmt = session.execute.call_args[0][0]
    stmt_str = str(stmt.compile(compile_kwargs={"literal_binds": False}))
    assert "instrument_type" in stmt_str


def test_upsert_many_does_not_commit():
    session = MagicMock()
    session.execute.return_value.rowcount = 1
    repo = AssetRepository(session)
    repo.upsert_many([{"ticker": "PETR4"}])
    session.commit.assert_not_called()
