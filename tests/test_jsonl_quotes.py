"""Tests for JSONL → fact_quotes parsing and parsing helpers.

This file consolidates tests that previously lived in `test_daily_quotes_load.py`.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from app.etl.parsers.jsonl_quotes_parser import parse_jsonl_quotes


def _make_jsonl_file(records: list[dict], tmp_path: Path) -> Path:
    p = tmp_path / "daily_fluctuation_20240614T100000.jsonl"
    with open(p, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    return p


def test_parse_jsonl_quotes_returns_fact_quote_compatible_rows(tmp_path):
    """Parsed JSONL rows must have all required FactQuote fields."""
    records = [
        {
            "ticker_requested": "PETR4",
            "trade_date": "2024-06-14",
            "collected_at": "2024-06-14T14:00:00Z",
            "price_history": [
                {
                    "quote_time": "10:00:00",
                    "close_price": "38.45",
                    "price_fluctuation_percentage": "1.2",
                },
                {
                    "quote_time": "10:30:00",
                    "close_price": "38.60",
                    "price_fluctuation_percentage": "1.6",
                },
            ],
        }
    ]
    path = _make_jsonl_file(records, tmp_path)
    rows = parse_jsonl_quotes(path)

    assert len(rows) == 2
    required_keys = {"ticker", "quoted_at", "trade_date", "close_price", "price_fluctuation_pct", "source_jsonl"}
    for row in rows:
        assert required_keys.issubset(row.keys()), (
            f"Missing keys: {required_keys - row.keys()}"
        )


def test_parse_jsonl_quotes_field_values(tmp_path):
    """Verify exact field values match FactQuote column mapping."""
    records = [
        {
            "ticker_requested": "VALE3",
            "trade_date": "2024-06-14",
            "price_history": [
                {
                    "quote_time": "11:15:00",
                    "close_price": "62.30",
                    "price_fluctuation_percentage": "-0.35",
                }
            ],
        }
    ]
    path = _make_jsonl_file(records, tmp_path)
    rows = parse_jsonl_quotes(path)

    assert len(rows) == 1
    r = rows[0]
    # ticker → ticker (uppercased)
    assert r["ticker"] == "VALE3"
    # quote_time → quoted_at (UTC datetime)
    assert r["quoted_at"] == datetime(2024, 6, 14, 11, 15, 0, tzinfo=timezone.utc)
    # trade_date derived from record
    assert r["trade_date"] == date(2024, 6, 14)
    # close_price → close_price
    assert r["close_price"] == Decimal("62.30")
    # price_fluctuation_percentage → price_fluctuation_pct
    assert r["price_fluctuation_pct"] == Decimal("-0.35")
    # source file name
    assert r["source_jsonl"] == path.name


def test_parse_jsonl_quotes_ticker_from_symbol_field(tmp_path):
    """ticker_requested or ticker_returned both resolve the ticker."""
    records = [
        {
            "ticker_returned": "ITUB4",   # only ticker_returned set
            "trade_date": "2024-06-14",
            "price_history": [
                {"quote_time": "10:00:00", "close_price": "32.0",
                 "price_fluctuation_percentage": "0"}
            ],
        }
    ]
    path = _make_jsonl_file(records, tmp_path)
    rows = parse_jsonl_quotes(path)
    assert len(rows) == 1
    assert rows[0]["ticker"] == "ITUB4"


def test_parse_jsonl_quotes_missing_ticker_skipped(tmp_path):
    """Records with no ticker at all must be skipped."""
    records = [
        {
            "trade_date": "2024-06-14",
            "price_history": [
                {"quote_time": "10:00:00", "close_price": "10.0",
                 "price_fluctuation_percentage": "0"}
            ],
        }
    ]
    path = _make_jsonl_file(records, tmp_path)
    rows = parse_jsonl_quotes(path)
    assert rows == []


def test_parse_jsonl_quotes_missing_trade_date_skipped(tmp_path):
    """Records with invalid/missing trade_date must be skipped."""
    records = [
        {
            "ticker_requested": "PETR4",
            "trade_date": "not-a-date",
            "price_history": [{"quote_time": "10:00", "close_price": "38.0",
                                "price_fluctuation_percentage": "0"}],
        }
    ]
    path = _make_jsonl_file(records, tmp_path)
    rows = parse_jsonl_quotes(path)
    assert rows == []


def test_parse_jsonl_quotes_empty_price_history_sentinel(tmp_path):
    """An empty price_history still produces a sentinel row with null prices."""
    records = [
        {
            "ticker_requested": "BBAS3",
            "trade_date": "2024-06-14",
            "price_history": [],
        }
    ]
    path = _make_jsonl_file(records, tmp_path)
    rows = parse_jsonl_quotes(path)
    assert len(rows) == 1
    assert rows[0]["ticker"] == "BBAS3"
    assert rows[0]["close_price"] is None
    assert rows[0]["price_fluctuation_pct"] is None


def test_parse_jsonl_quotes_multiple_tickers(tmp_path):
    """Multiple ticker records with different point counts produce correct total rows."""
    records = [
        {
            "ticker_requested": "PETR4",
            "trade_date": "2024-06-14",
            "price_history": [
                {"quote_time": "10:00", "close_price": "38.45", "price_fluctuation_percentage": "1.0"},
                {"quote_time": "10:30", "close_price": "38.50", "price_fluctuation_percentage": "1.1"},
            ],
        },
        {
            "ticker_requested": "VALE3",
            "trade_date": "2024-06-14",
            "price_history": [
                {"quote_time": "10:00", "close_price": "65.10", "price_fluctuation_percentage": "-0.5"},
            ],
        },
    ]
    path = _make_jsonl_file(records, tmp_path)
    rows = parse_jsonl_quotes(path)
    assert len(rows) == 3
    assert {r["ticker"] for r in rows} == {"PETR4", "VALE3"}

