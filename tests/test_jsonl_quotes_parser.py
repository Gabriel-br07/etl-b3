"""Tests for the JSONL quotes parser."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from app.etl.parsers.jsonl_quotes_parser import parse_jsonl_quotes, _build_quoted_at, _parse_decimal


# ---------------------------------------------------------------------------
# _parse_decimal
# ---------------------------------------------------------------------------

def test_parse_decimal_plain():
    assert _parse_decimal("12.34") == Decimal("12.34")


def test_parse_decimal_comma_separator():
    assert _parse_decimal("12,34") == Decimal("12.34")


def test_parse_decimal_none():
    assert _parse_decimal(None) is None


def test_parse_decimal_invalid():
    assert _parse_decimal("abc") is None


# ---------------------------------------------------------------------------
# _build_quoted_at
# ---------------------------------------------------------------------------

def test_build_quoted_at_with_time():
    trade_date = date(2024, 6, 14)
    result = _build_quoted_at(trade_date, "10:34:00")
    assert result == datetime(2024, 6, 14, 10, 34, 0, tzinfo=timezone.utc)


def test_build_quoted_at_no_time_fallback():
    trade_date = date(2024, 6, 14)
    result = _build_quoted_at(trade_date, None)
    assert result == datetime(2024, 6, 14, 0, 0, 0, tzinfo=timezone.utc)


def test_build_quoted_at_bad_time_fallback():
    trade_date = date(2024, 6, 14)
    result = _build_quoted_at(trade_date, "not_a_time")
    assert result == datetime(2024, 6, 14, 0, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# parse_jsonl_quotes
# ---------------------------------------------------------------------------

def _make_jsonl_file(records: list[dict], tmp_path: Path) -> Path:
    p = tmp_path / "test.jsonl"
    with open(p, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    return p


def test_parse_jsonl_quotes_basic(tmp_path):
    records = [
        {
            "ticker_requested": "PETR4",
            "trade_date": "2024-06-14",
            "collected_at": "2024-06-14T14:00:00Z",
            "price_history": [
                {"quote_time": "10:00:00", "close_price": "38.45", "price_fluctuation_percentage": "1.2"},
                {"quote_time": "10:30:00", "close_price": "38.60", "price_fluctuation_percentage": "1.6"},
            ],
        }
    ]
    path = _make_jsonl_file(records, tmp_path)
    rows = parse_jsonl_quotes(path)

    assert len(rows) == 2
    assert rows[0]["ticker"] == "PETR4"
    assert rows[0]["close_price"] == Decimal("38.45")
    assert rows[0]["trade_date"] == date(2024, 6, 14)
    assert rows[0]["quoted_at"] == datetime(2024, 6, 14, 10, 0, 0, tzinfo=timezone.utc)


def test_parse_jsonl_quotes_empty_price_history(tmp_path):
    records = [
        {
            "ticker_requested": "VALE3",
            "trade_date": "2024-06-14",
            "price_history": [],
        }
    ]
    path = _make_jsonl_file(records, tmp_path)
    rows = parse_jsonl_quotes(path)

    # One sentinel row with null prices
    assert len(rows) == 1
    assert rows[0]["ticker"] == "VALE3"
    assert rows[0]["close_price"] is None


def test_parse_jsonl_quotes_skips_bad_json(tmp_path):
    p = tmp_path / "bad.jsonl"
    p.write_text('{"ticker_requested":"PETR4","trade_date":"2024-06-14","price_history":[]}\n{NOT JSON\n')
    rows = parse_jsonl_quotes(p)
    assert len(rows) == 1  # first line ok, second skipped


def test_parse_jsonl_quotes_skips_missing_ticker(tmp_path):
    records = [{"trade_date": "2024-06-14", "price_history": []}]
    path = _make_jsonl_file(records, tmp_path)
    rows = parse_jsonl_quotes(path)
    assert rows == []


def test_parse_jsonl_quotes_uppercase_ticker(tmp_path):
    records = [
        {
            "ticker_requested": "petr4",
            "trade_date": "2024-06-14",
            "price_history": [{"quote_time": "10:00", "close_price": "10.00", "price_fluctuation_percentage": "0"}],
        }
    ]
    path = _make_jsonl_file(records, tmp_path)
    rows = parse_jsonl_quotes(path)
    assert rows[0]["ticker"] == "PETR4"


def test_parse_jsonl_quotes_multiple_tickers(tmp_path):
    records = [
        {
            "ticker_requested": "PETR4",
            "trade_date": "2024-06-14",
            "price_history": [
                {"quote_time": "10:00", "close_price": "38.45", "price_fluctuation_percentage": "1.0"},
            ],
        },
        {
            "ticker_requested": "VALE3",
            "trade_date": "2024-06-14",
            "price_history": [
                {"quote_time": "10:00", "close_price": "65.10", "price_fluctuation_percentage": "-0.5"},
                {"quote_time": "10:30", "close_price": "65.00", "price_fluctuation_percentage": "-0.7"},
            ],
        },
    ]
    path = _make_jsonl_file(records, tmp_path)
    rows = parse_jsonl_quotes(path)
    assert len(rows) == 3
    tickers = {r["ticker"] for r in rows}
    assert tickers == {"PETR4", "VALE3"}


