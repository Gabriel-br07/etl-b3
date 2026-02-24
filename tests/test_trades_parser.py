"""Tests for the trades ZIP/CSV parser."""

import polars as pl
import pytest

from app.etl.parsers.trades_parser import parse_trades_file


def test_parse_trades_returns_dataframe(trades_fixture_path):
    df = parse_trades_file(trades_fixture_path)
    assert isinstance(df, pl.DataFrame)


def test_parse_trades_has_required_columns(trades_fixture_path):
    df = parse_trades_file(trades_fixture_path)
    assert "ticker" in df.columns
    assert "trade_date" in df.columns


def test_parse_trades_row_count(trades_fixture_path):
    df = parse_trades_file(trades_fixture_path)
    assert len(df) == 10


def test_parse_trades_price_columns_present(trades_fixture_path):
    df = parse_trades_file(trades_fixture_path)
    assert "last_price" in df.columns


def test_parse_trades_tickers(trades_fixture_path):
    df = parse_trades_file(trades_fixture_path)
    tickers = df["ticker"].to_list()
    assert "PETR4" in tickers
    assert "VALE3" in tickers
