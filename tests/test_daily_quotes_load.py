"""Unit tests for CSV → fact_daily_quotes load paths.

This module previously contained JSONL parser tests; those were moved to
`tests/test_jsonl_quotes.py` to keep responsibilities clear.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from app.etl.loaders.db_loader import load_daily_quotes, load_intraday_quotes
from app.etl.parsers.jsonl_quotes_parser import parse_jsonl_quotes
from app.etl.transforms.b3_transforms import transform_daily_quotes


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_quotes_df(
    *,
    include_trade_date: bool = True,
    include_open_price: bool = True,
) -> pl.DataFrame:
    """Build a minimal quotes DataFrame that mirrors a parsed normalized CSV."""
    data: dict[str, list] = {
        "ticker": ["PETR4", "VALE3", "ITUB4"],
        "last_price": ["38.45", "62.30", "32.15"],
        "min_price": ["37.80", "61.50", "31.80"],
        "max_price": ["39.10", "63.20", "32.60"],
        "avg_price": ["38.00", "62.00", "32.00"],
        "variation_pct": ["1.5", "-0.3", "0.8"],
        "financial_volume": ["1000000.00", "500000.00", "250000.00"],
        "trade_count": ["200", "150", "80"],
    }
    if include_open_price:
        data["open_price"] = ["37.00", "62.50", "31.50"]
    if include_trade_date:
        data["trade_date"] = ["2024-06-14", "2024-06-14", "2024-06-14"]
    return pl.DataFrame(data)


def _make_jsonl_file(records: list[dict], tmp_path: Path) -> Path:
    p = tmp_path / "daily_fluctuation_20240614T100000.jsonl"
    with open(p, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    return p


# ---------------------------------------------------------------------------
# transform_daily_quotes – happy path
# ---------------------------------------------------------------------------


def test_transform_daily_quotes_basic():
    """Happy path: all expected columns produce correct rows."""
    df = _make_quotes_df()
    rows = transform_daily_quotes(df, "negocios_consolidados_20240614.csv")
    assert len(rows) == 3
    r = next(r for r in rows if r["ticker"] == "PETR4")
    assert r["trade_date"] == date(2024, 6, 14)
    assert r["last_price"] == pytest.approx(38.45, abs=0.01)
    assert r["min_price"] == pytest.approx(37.80, abs=0.01)
    assert r["max_price"] == pytest.approx(39.10, abs=0.01)
    assert r["avg_price"] == pytest.approx(38.00, abs=0.01)
    assert r["variation_pct"] == pytest.approx(1.5, abs=0.001)
    assert r["financial_volume"] == pytest.approx(1_000_000.00, abs=0.01)
    assert r["trade_count"] == pytest.approx(200)


def test_transform_daily_quotes_excludes_open_price():
    """open_price must NOT appear in the output – it's not in fact_daily_quotes."""
    df = _make_quotes_df(include_open_price=True)
    rows = transform_daily_quotes(df, "negocios_consolidados_20240614.csv")
    assert len(rows) > 0
    for row in rows:
        assert "open_price" not in row, "open_price must be stripped from fact_daily_quotes rows"


def test_transform_daily_quotes_no_open_price_input():
    """If the CSV has no open_price column, it should work fine."""
    df = _make_quotes_df(include_open_price=False)
    rows = transform_daily_quotes(df, "negocios_consolidados_20240614.csv")
    assert len(rows) == 3
    for row in rows:
        assert "open_price" not in row


def test_transform_daily_quotes_uppercases_tickers():
    df = pl.DataFrame({
        "ticker": ["petr4", "vale3"],
        "trade_date": ["2024-06-14", "2024-06-14"],
    })
    rows = transform_daily_quotes(df, "test.csv")
    assert all(r["ticker"] == r["ticker"].upper() for r in rows)


def test_transform_daily_quotes_source_file_name():
    df = _make_quotes_df()
    rows = transform_daily_quotes(df, "my_source_file.csv")
    assert all(r["source_file_name"] == "my_source_file.csv" for r in rows)


def test_transform_daily_quotes_deduplicates_by_ticker_date():
    df = pl.DataFrame({
        "ticker": ["PETR4", "PETR4"],
        "trade_date": ["2024-06-14", "2024-06-14"],
        "last_price": ["38.45", "39.00"],
    })
    rows = transform_daily_quotes(df, "test.csv")
    assert len(rows) == 1


# ---------------------------------------------------------------------------
# transform_daily_quotes – trade_date derivation
# ---------------------------------------------------------------------------


def test_transform_daily_quotes_derives_trade_date_from_target_date():
    """trade_date missing → derived from target_date."""
    df = _make_quotes_df(include_trade_date=False)
    target = date(2024, 6, 14)
    rows = transform_daily_quotes(df, "no_date.csv", target_date=target)
    assert len(rows) == 3
    assert all(r["trade_date"] == target for r in rows)


def test_transform_daily_quotes_derives_trade_date_from_filename():
    """trade_date missing, no target_date → extracted from filename."""
    df = _make_quotes_df(include_trade_date=False)
    rows = transform_daily_quotes(df, "negocios_consolidados_20240614.csv")
    assert len(rows) == 3
    assert all(r["trade_date"] == date(2024, 6, 14) for r in rows)


def test_transform_daily_quotes_filename_dash_separator():
    """Date in filename with dash separator."""
    df = _make_quotes_df(include_trade_date=False)
    rows = transform_daily_quotes(df, "NegociosConsolidados_2024-06-14.csv")
    assert len(rows) == 3
    assert rows[0]["trade_date"] == date(2024, 6, 14)


def test_transform_daily_quotes_returns_empty_when_no_date_source():
    """No trade_date column, no target_date, no date in filename → empty list, not silent."""
    df = _make_quotes_df(include_trade_date=False)
    rows = transform_daily_quotes(df, "no_date_at_all.csv")
    assert rows == []


# ---------------------------------------------------------------------------
# transform_daily_quotes – missing required columns
# ---------------------------------------------------------------------------


def test_transform_daily_quotes_returns_empty_when_ticker_missing():
    """ticker is required – missing it must return [] not silently skip."""
    df = pl.DataFrame({
        "trade_date": ["2024-06-14"],
        "last_price": ["38.45"],
    })
    rows = transform_daily_quotes(df, "test.csv", target_date=date(2024, 6, 14))
    assert rows == []


def test_transform_daily_quotes_missing_optional_columns_produce_nulls():
    """Optional price columns not present → filled with None, rows still produced."""
    df = pl.DataFrame({
        "ticker": ["PETR4"],
        "trade_date": ["2024-06-14"],
        # No price columns at all
    })
    rows = transform_daily_quotes(df, "test.csv")
    assert len(rows) == 1
    assert rows[0]["last_price"] is None
    assert rows[0]["min_price"] is None
    assert rows[0]["max_price"] is None
    assert rows[0]["avg_price"] is None


def test_transform_daily_quotes_drops_null_tickers():
    """Rows with null or empty tickers are discarded."""
    df = pl.DataFrame({
        "ticker": ["PETR4", None, "", "VALE3"],
        "trade_date": ["2024-06-14", "2024-06-14", "2024-06-14", "2024-06-14"],
    })
    rows = transform_daily_quotes(df, "test.csv")
    assert len(rows) == 2
    assert {r["ticker"] for r in rows} == {"PETR4", "VALE3"}


# ---------------------------------------------------------------------------
# CSV column mapping → transform_daily_quotes (integration-like)
# ---------------------------------------------------------------------------


def test_transform_daily_quotes_with_mapped_columns():
    """Simulate what parse_trades_file + transform_daily_quotes produces end-to-end
    using already-mapped internal column names (as the parser returns)."""
    df = pl.DataFrame({
        "ticker": ["PETR4"],
        "last_price": ["38,45"],   # comma decimal
        "min_price": ["37.80"],
        "max_price": ["39.10"],
        "avg_price": ["38.00"],
        "variation_pct": ["1.5"],
        "financial_volume": ["1000000"],
        "trade_count": ["200"],
        "open_price": ["37.00"],   # must be stripped
    })
    rows = transform_daily_quotes(df, "negocios_consolidados_20240614.csv")
    assert len(rows) == 1
    r = rows[0]
    assert r["ticker"] == "PETR4"
    assert r["trade_date"] == date(2024, 6, 14)
    assert r["last_price"] == pytest.approx(38.45, abs=0.01)
    assert "open_price" not in r


# JSONL tests moved to tests/test_jsonl_quotes.py


# ---------------------------------------------------------------------------
# load_daily_quotes – loader unit tests (mocked repo)
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_db():
    return MagicMock()


def test_load_daily_quotes_empty_returns_zero(mock_db):
    assert load_daily_quotes(mock_db, []) == 0


def test_load_daily_quotes_calls_quote_repo_upsert(mock_db):
    rows = [
        {
            "ticker": "PETR4",
            "trade_date": date(2024, 6, 14),
            "last_price": Decimal("38.45"),
            "source_file_name": "test.csv",
        }
    ]
    with patch("app.etl.loaders.db_loader.QuoteRepository") as MockRepo:
        instance = MockRepo.return_value
        instance.upsert_many.return_value = 1
        result = load_daily_quotes(mock_db, rows)

    assert result == 1
    MockRepo.assert_called_once_with(mock_db)
    instance.upsert_many.assert_called_once_with(rows)


def test_load_daily_quotes_chunks_large_input(mock_db):
    rows = [{"ticker": f"T{i}", "trade_date": date(2024, 6, 14)} for i in range(1200)]
    with patch("app.etl.loaders.db_loader.QuoteRepository") as MockRepo:
        instance = MockRepo.return_value
        instance.upsert_many.side_effect = [500, 500, 200]
        result = load_daily_quotes(mock_db, rows)

    assert instance.upsert_many.call_count == 3
    assert result == 1200


# ---------------------------------------------------------------------------
# load_intraday_quotes – loader unit tests (mocked repo)
# ---------------------------------------------------------------------------


def test_load_intraday_quotes_empty_returns_zero(mock_db):
    assert load_intraday_quotes(mock_db, []) == 0


def test_load_intraday_quotes_calls_fact_quote_repo(mock_db):
    rows = [
        {
            "ticker": "PETR4",
            "quoted_at": datetime(2024, 6, 14, 10, 0, 0, tzinfo=timezone.utc),
            "trade_date": date(2024, 6, 14),
            "close_price": Decimal("38.45"),
            "price_fluctuation_pct": Decimal("1.2"),
            "source_jsonl": "daily_fluctuation_20240614T100000.jsonl",
        }
    ]
    with patch("app.etl.loaders.db_loader.FactQuoteRepository") as MockRepo:
        instance = MockRepo.return_value
        instance.insert_many.return_value = 1
        result = load_intraday_quotes(mock_db, rows)

    assert result == 1
    MockRepo.assert_called_once_with(mock_db)
    instance.insert_many.assert_called_once_with(rows)


# ---------------------------------------------------------------------------
# End-to-end transform + load (fully mocked DB)
# ---------------------------------------------------------------------------


def test_csv_to_fact_daily_quotes_end_to_end(mock_db):
    """Full path: DataFrame → transform_daily_quotes → load_daily_quotes."""
    df = _make_quotes_df()
    rows = transform_daily_quotes(df, "negocios_consolidados_20240614.csv")

    # Rows must not contain open_price (would cause a DB error)
    assert all("open_price" not in r for r in rows)
    # All rows must have trade_date and ticker
    assert all("ticker" in r and "trade_date" in r for r in rows)

    with patch("app.etl.loaders.db_loader.QuoteRepository") as MockRepo:
        instance = MockRepo.return_value
        instance.upsert_many.return_value = len(rows)
        inserted = load_daily_quotes(mock_db, rows)

    assert inserted == 3


def test_jsonl_to_fact_quotes_end_to_end(tmp_path, mock_db):
    """Full path: JSONL → parse_jsonl_quotes → load_intraday_quotes."""
    records = [
        {
            "ticker_requested": "PETR4",
            "trade_date": "2024-06-14",
            "price_history": [
                {"quote_time": "10:00:00", "close_price": "38.45",
                 "price_fluctuation_percentage": "1.2"},
                {"quote_time": "10:30:00", "close_price": "38.60",
                 "price_fluctuation_percentage": "1.6"},
            ],
        },
        {
            "ticker_requested": "VALE3",
            "trade_date": "2024-06-14",
            "price_history": [
                {"quote_time": "10:00:00", "close_price": "62.30",
                 "price_fluctuation_percentage": "-0.35"},
            ],
        },
    ]
    path = _make_jsonl_file(records, tmp_path)
    rows = parse_jsonl_quotes(path)

    assert len(rows) == 3
    # All rows must have the required FactQuote fields
    for row in rows:
        assert "ticker" in row
        assert "quoted_at" in row
        assert "trade_date" in row

    with patch("app.etl.loaders.db_loader.FactQuoteRepository") as MockRepo:
        instance = MockRepo.return_value
        instance.insert_many.return_value = 3
        inserted = load_intraday_quotes(mock_db, rows)

    assert inserted == 3

