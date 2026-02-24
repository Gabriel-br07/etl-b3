"""Tests for column mapping utilities."""

from app.etl.parsers.column_mapping import (
    INSTRUMENT_COLUMN_MAP,
    TRADE_COLUMN_MAP,
    map_columns,
)


def test_map_columns_instruments_basic():
    columns = ["TckrSymb", "NmOfc", "ISIN", "Sgmt"]
    result = map_columns(columns, INSTRUMENT_COLUMN_MAP)
    assert result["TckrSymb"] == "ticker"
    assert result["NmOfc"] == "asset_name"
    assert result["ISIN"] == "isin"
    assert result["Sgmt"] == "segment"


def test_map_columns_case_insensitive():
    columns = ["tckrsymb", "NMOFCE"]
    result = map_columns(columns, INSTRUMENT_COLUMN_MAP)
    assert "tckrsymb" in result


def test_map_columns_ignores_unknown_columns():
    columns = ["UnknownColumn", "AnotherUnknown"]
    result = map_columns(columns, INSTRUMENT_COLUMN_MAP)
    assert result == {}


def test_map_columns_trade_basic():
    columns = ["TckrSymb", "RptDt", "LastPric", "MinPric", "MaxPric"]
    result = map_columns(columns, TRADE_COLUMN_MAP)
    assert result["TckrSymb"] == "ticker"
    assert result["RptDt"] == "trade_date"
    assert result["LastPric"] == "last_price"
    assert result["MinPric"] == "min_price"
    assert result["MaxPric"] == "max_price"


def test_map_columns_partial_match():
    columns = ["TckrSymb", "UnknownCol", "RptDt"]
    result = map_columns(columns, TRADE_COLUMN_MAP)
    assert len(result) == 2
    assert "TckrSymb" in result
    assert "RptDt" in result
