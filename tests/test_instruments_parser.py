"""Tests for the instruments CSV parser."""

from pathlib import Path

import polars as pl
import pytest

from app.etl.parsers.instruments_parser import parse_instruments_csv


def test_parse_instruments_returns_dataframe(instruments_fixture_path):
    df = parse_instruments_csv(instruments_fixture_path)
    assert isinstance(df, pl.DataFrame)


def test_parse_instruments_has_expected_columns(instruments_fixture_path):
    df = parse_instruments_csv(instruments_fixture_path)
    assert "ticker" in df.columns


def test_parse_instruments_row_count(instruments_fixture_path):
    df = parse_instruments_csv(instruments_fixture_path)
    assert len(df) == 10


def test_parse_instruments_ticker_values(instruments_fixture_path):
    df = parse_instruments_csv(instruments_fixture_path)
    tickers = df["ticker"].to_list()
    assert "PETR4" in tickers
    assert "VALE3" in tickers


def test_parse_instruments_asset_name_present(instruments_fixture_path):
    df = parse_instruments_csv(instruments_fixture_path)
    assert "asset_name" in df.columns
    assert df.filter(pl.col("ticker") == "PETR4")["asset_name"][0] == "PETROBRAS PN N2"


def test_parse_instruments_nonexistent_file_raises():
    with pytest.raises(Exception):
        parse_instruments_csv(Path("/tmp/nonexistent_file.csv"))
