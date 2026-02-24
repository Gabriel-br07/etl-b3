"""Tests for transformation functions."""

from datetime import date

import polars as pl
import pytest

from app.etl.transforms.b3_transforms import transform_instruments, transform_trades


def _make_instruments_df():
    return pl.DataFrame(
        {
            "ticker": ["petr4", "VALE3", " ITUB4 ", ""],
            "asset_name": ["PETROBRAS PN", "VALE ON", "ITAU PN", "INVALID"],
            "isin": ["BR123", "BR456", "BR789", None],
            "segment": ["NM", "NM", "NM", None],
        }
    )


def _make_trades_df():
    return pl.DataFrame(
        {
            "ticker": ["PETR4", "VALE3", "ITUB4"],
            "trade_date": ["2024-06-14", "2024-06-14", "2024-06-14"],
            "last_price": ["38,45", "62.30", "32.15"],
            "min_price": ["37.80", "61.50", "31.80"],
            "max_price": ["39.10", "63.20", "32.60"],
        }
    )


def test_transform_instruments_uppercases_tickers():
    df = _make_instruments_df()
    rows = transform_instruments(df, date(2024, 6, 14))
    tickers = [r["ticker"] for r in rows]
    assert all(t == t.upper() for t in tickers)


def test_transform_instruments_drops_empty_tickers():
    df = _make_instruments_df()
    rows = transform_instruments(df, date(2024, 6, 14))
    tickers = [r["ticker"] for r in rows]
    assert "" not in tickers


def test_transform_instruments_deduplicates():
    df = pl.DataFrame(
        {
            "ticker": ["PETR4", "PETR4"],
            "asset_name": ["A", "B"],
        }
    )
    rows = transform_instruments(df, date(2024, 6, 14))
    assert len(rows) == 1


def test_transform_instruments_adds_file_date():
    df = pl.DataFrame({"ticker": ["PETR4"], "asset_name": ["PETROBRAS PN"]})
    rows = transform_instruments(df, date(2024, 6, 14))
    assert rows[0]["source_file_date"] == date(2024, 6, 14)


def test_transform_trades_parses_numeric():
    df = _make_trades_df()
    rows = transform_trades(df, "NegociosConsolidados_2024-06-14.zip")
    petr = next(r for r in rows if r["ticker"] == "PETR4")
    # Comma decimal separator should be handled
    assert petr["last_price"] == pytest.approx(38.45, abs=0.01)


def test_transform_trades_parses_dates():
    df = _make_trades_df()
    rows = transform_trades(df, "test.zip")
    assert rows[0]["trade_date"] == date(2024, 6, 14)


def test_transform_trades_deduplicates():
    df = pl.DataFrame(
        {
            "ticker": ["PETR4", "PETR4"],
            "trade_date": ["2024-06-14", "2024-06-14"],
            "last_price": ["38.45", "39.00"],
        }
    )
    rows = transform_trades(df, "test.zip")
    assert len(rows) == 1


def test_transform_trades_adds_source_file_name():
    df = _make_trades_df()
    rows = transform_trades(df, "my_source_file.zip")
    assert rows[0]["source_file_name"] == "my_source_file.zip"


def test_transform_instruments_empty_df_returns_empty():
    df = pl.DataFrame({"ticker": [], "asset_name": []})
    rows = transform_instruments(df, date(2024, 6, 14))
    assert rows == []
