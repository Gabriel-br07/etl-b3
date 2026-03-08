"""Unit tests for the ETL db_loader module (no real DB needed - mocks repos)."""
from __future__ import annotations
from unittest.mock import MagicMock, patch
import pytest
from app.etl.loaders.db_loader import (
    load_assets,
    load_daily_quotes,
    load_intraday_quotes,
    load_quotes,
    load_trades,
)
@pytest.fixture()
def mock_db():
    return MagicMock()
def test_load_assets_empty_returns_zero(mock_db):
    assert load_assets(mock_db, []) == 0
def test_load_assets_calls_upsert(mock_db):
    rows = [{"ticker": "PETR4", "asset_name": "Petrobras"}]
    with patch("app.etl.loaders.db_loader.AssetRepository") as MockRepo:
        instance = MockRepo.return_value
        instance.upsert_many.return_value = 1
        result = load_assets(mock_db, rows)
    assert result == 1
    MockRepo.assert_called_once_with(mock_db)
    instance.upsert_many.assert_called_once_with(rows)
def test_load_assets_chunks_large_input(mock_db):
    rows = [{"ticker": f"T{i}"} for i in range(1200)]
    with patch("app.etl.loaders.db_loader.AssetRepository") as MockRepo:
        instance = MockRepo.return_value
        instance.upsert_many.return_value = 500
        result = load_assets(mock_db, rows)
    # 1200 rows / 500 chunk_size = 3 chunks (500 + 500 + 200)
    assert instance.upsert_many.call_count == 3
    assert result == 1500  # 3 * 500
def test_load_trades_empty_returns_zero(mock_db):
    assert load_trades(mock_db, []) == 0
def test_load_trades_calls_upsert(mock_db):
    rows = [{"ticker": "VALE3", "trade_date": "2024-06-14"}]
    with patch("app.etl.loaders.db_loader.TradeRepository") as MockRepo:
        instance = MockRepo.return_value
        instance.upsert_many.return_value = 1
        result = load_trades(mock_db, rows)
    assert result == 1
def test_load_daily_quotes_empty_returns_zero(mock_db):
    assert load_daily_quotes(mock_db, []) == 0
def test_load_quotes_alias_is_same_function():
    assert load_quotes is load_daily_quotes
def test_load_intraday_quotes_empty_returns_zero(mock_db):
    assert load_intraday_quotes(mock_db, []) == 0
def test_load_intraday_quotes_calls_insert(mock_db):
    rows = [{"ticker": "PETR4", "quoted_at": "2024-06-14T10:00:00Z"}]
    with patch("app.etl.loaders.db_loader.FactQuoteRepository") as MockRepo:
        instance = MockRepo.return_value
        instance.insert_many.return_value = 1
        result = load_intraday_quotes(mock_db, rows)
    assert result == 1