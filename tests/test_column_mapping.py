# -*- coding: utf-8 -*-
"""Tests for column mapping utilities."""

from app.etl.parsers.column_mapping import (
    INSTRUMENT_COLUMN_MAP,
    TRADE_COLUMN_MAP,
    _normalize_name,
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


# ---------------------------------------------------------------------------
# New: accent-insensitive / variant header tests
# ---------------------------------------------------------------------------


def test_normalize_name_removes_accents():
    # Accented and unaccented variants should normalize to the same canonical form
    assert _normalize_name("Variação") == _normalize_name("Variacao")
    assert _normalize_name("Preço médio") == _normalize_name("Preco medio")
    # And the canonical form itself should be accent-free, lowercase, and space-preserving
    assert _normalize_name("Variação") == "variacao"
    assert _normalize_name("Preço médio") == "preco medio"


def test_normalize_name_case_insensitive():
    assert _normalize_name("TckrSymb") == _normalize_name("tckrsymb")
    assert _normalize_name("RPTDT") == _normalize_name("rptdt")


def test_normalize_name_collapses_whitespace():
    assert _normalize_name("  Data  do  negócio  ") == _normalize_name("Data do negocio")


def test_map_columns_accented_portuguese_trade_date():
    """'Data do negócio' (accented) should map to trade_date."""
    columns = ["TckrSymb", "Data do negócio", "LastPric"]
    result = map_columns(columns, TRADE_COLUMN_MAP)
    assert result.get("Data do negócio") == "trade_date"


def test_map_columns_unaccented_portuguese_trade_date():
    """'Data do negocio' (no accents) should map to trade_date."""
    columns = ["TckrSymb", "Data do negocio", "LastPric"]
    result = map_columns(columns, TRADE_COLUMN_MAP)
    assert result.get("Data do negocio") == "trade_date"


def test_map_columns_portuguese_variation_accented():
    """'Variação' should map to variation_pct."""
    columns = ["TckrSymb", "RptDt", "Variação"]
    result = map_columns(columns, TRADE_COLUMN_MAP)
    assert result.get("Variação") == "variation_pct"


def test_map_columns_portuguese_variation_unaccented():
    """'Variacao' (no accent) should still map to variation_pct."""
    columns = ["TckrSymb", "RptDt", "Variacao"]
    result = map_columns(columns, TRADE_COLUMN_MAP)
    assert result.get("Variacao") == "variation_pct"


def test_map_columns_portuguese_last_price():
    """Portuguese close price header maps to last_price."""
    columns = ["TckrSymb", "RptDt", "Preço de fechamento"]
    result = map_columns(columns, TRADE_COLUMN_MAP)
    assert result.get("Preço de fechamento") == "last_price"


def test_map_columns_portuguese_min_price():
    """Portuguese min price header (accented and unaccented) maps to min_price."""
    for header in ["Preço mínimo", "Preco minimo"]:
        result = map_columns([header], TRADE_COLUMN_MAP)
        assert result.get(header) == "min_price", f"Failed for header: {header!r}"


def test_map_columns_portuguese_financial_volume():
    columns = ["TckrSymb", "RptDt", "Volume financeiro"]
    result = map_columns(columns, TRADE_COLUMN_MAP)
    assert result.get("Volume financeiro") == "financial_volume"


def test_map_columns_portuguese_trade_count():
    columns = ["TckrSymb", "RptDt", "Quantidade de negócios"]
    result = map_columns(columns, TRADE_COLUMN_MAP)
    assert result.get("Quantidade de negócios") == "trade_count"


def test_map_columns_deduplicates_mapped_targets():
    """If two source columns map to the same internal name, only the first wins."""
    columns = ["TckrSymb", "Tckr"]  # both map to "ticker"
    result = map_columns(columns, TRADE_COLUMN_MAP)
    # Only one of the two should appear in the result
    assert len([v for v in result.values() if v == "ticker"]) == 1


def test_map_columns_date_variants():
    """Various date column names (DtNeg, DataPregao, TradeDate) map to trade_date."""
    for col in ["DtNeg", "DataPregao", "TradeDate", "Date"]:
        result = map_columns([col], TRADE_COLUMN_MAP)
        assert result.get(col) == "trade_date", f"Failed for column: {col!r}"


# ---------------------------------------------------------------------------
# UTF-8 lossy / mangled header variants (actual B3 CSV encoding)
# ---------------------------------------------------------------------------


def test_map_columns_mangled_ticker():
    """'Instrumento financeiro' maps to ticker (common mangled variant)."""
    result = map_columns(["Instrumento financeiro"], TRADE_COLUMN_MAP)
    assert result.get("Instrumento financeiro") == "ticker"


def test_map_columns_mangled_last_price():
    """'Preo de fechamento' (lossy-mangled 'Preço de fechamento') maps to last_price."""
    result = map_columns(["Preo de fechamento"], TRADE_COLUMN_MAP)
    assert result.get("Preo de fechamento") == "last_price"


def test_map_columns_mangled_min_price():
    """'Preo mnimo' (lossy-mangled 'Preço mínimo') maps to min_price."""
    result = map_columns(["Preo mnimo"], TRADE_COLUMN_MAP)
    assert result.get("Preo mnimo") == "min_price"


def test_map_columns_mangled_max_price():
    """'Preo mximo' (lossy-mangled 'Preço máximo') maps to max_price."""
    result = map_columns(["Preo mximo"], TRADE_COLUMN_MAP)
    assert result.get("Preo mximo") == "max_price"


def test_map_columns_mangled_avg_price():
    """'Preo mdio' (lossy-mangled 'Preço médio') maps to avg_price."""
    result = map_columns(["Preo mdio"], TRADE_COLUMN_MAP)
    assert result.get("Preo mdio") == "avg_price"


def test_map_columns_mangled_open_price():
    """'Preo de abertura' (lossy-mangled 'Preço de abertura') maps to open_price."""
    result = map_columns(["Preo de abertura"], TRADE_COLUMN_MAP)
    assert result.get("Preo de abertura") == "open_price"


def test_map_columns_mangled_variation_oscilao():
    """'Oscilao' (mangled 'Oscilação') maps to variation_pct."""
    result = map_columns(["Oscilao"], TRADE_COLUMN_MAP)
    assert result.get("Oscilao") == "variation_pct"


def test_map_columns_mangled_variation_variao():
    """'Variao' (mangled 'Variação') maps to variation_pct."""
    result = map_columns(["Variao"], TRADE_COLUMN_MAP)
    assert result.get("Variao") == "variation_pct"


def test_map_columns_mangled_trade_count():
    """'Quantidade de negcios' (mangled 'Quantidade de negócios') maps to trade_count."""
    result = map_columns(["Quantidade de negcios"], TRADE_COLUMN_MAP)
    assert result.get("Quantidade de negcios") == "trade_count"


def test_map_columns_full_actual_csv_row():
    """Simulate the exact header row from the real B3 normalized CSV file
    and verify all key price/volume fields are mapped."""
    # Example of actual headers from a real B3 normalized CSV file; the date
    # in the filename (e.g. negocios_consolidados_20260309.normalized.csv)
    # is illustrative and shown as read by Polars with encoding='utf8-lossy'.
    actual_headers = [
        "Instrumento financeiro",
        "Cdigo ISIN",
        "Segmento",
        "Preo de abertura",
        "Preo mnimo",
        "Preo mximo",
        "Preo mdio",
        "Preo de fechamento",
        "Oscilao",
        "Ajuste",
        "Ajuste de referncia",
        "Ajuste do dia anterior",
        "Preo de referncia",
        "Variao",
        "Quantidade de negcios",
        "Quantidade de contratos",
        "Volume financeiro",
    ]
    result = map_columns(actual_headers, TRADE_COLUMN_MAP)
    assert result.get("Instrumento financeiro") == "ticker"
    assert result.get("Preo de fechamento") == "last_price"
    assert result.get("Preo mnimo") == "min_price"
    assert result.get("Preo mximo") == "max_price"
    assert result.get("Preo mdio") == "avg_price"
    assert result.get("Preo de abertura") == "open_price"
    # Either Oscilao or Variao maps to variation_pct (first one wins)
    mapped_to_variation = [k for k, v in result.items() if v == "variation_pct"]
    assert len(mapped_to_variation) == 1
    assert result.get("Volume financeiro") == "financial_volume"
    assert result.get("Quantidade de negcios") == "trade_count"

