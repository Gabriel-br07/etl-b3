"""Unit tests for column header → internal field mapping."""

from app.etl.parsers.column_mapping import (
    INSTRUMENT_COLUMN_MAP,
    TRADE_COLUMN_MAP,
    _normalize_name,
    map_columns,
)


def test_normalize_name_removes_accents():
    assert _normalize_name("Variação") == _normalize_name("Variacao")
    assert _normalize_name("Preço médio") == _normalize_name("Preco medio")
    assert _normalize_name("Variação") == "variacao"


def test_normalize_name_collapses_whitespace():
    assert _normalize_name("  Data  do  negócio  ") == _normalize_name("Data do negocio")


def test_map_columns_trade_basic():
    columns = ["TckrSymb", "RptDt", "LastPric", "MinPric", "MaxPric"]
    result = map_columns(columns, TRADE_COLUMN_MAP)
    assert result["TckrSymb"] == "ticker"
    assert result["RptDt"] == "trade_date"
    assert result["LastPric"] == "last_price"


def test_map_columns_accented_portuguese_trade_date():
    columns = ["TckrSymb", "Data do negócio", "LastPric"]
    result = map_columns(columns, TRADE_COLUMN_MAP)
    assert result.get("Data do negócio") == "trade_date"


def test_map_columns_mangled_utf8_lossy_headers():
    """Headers as seen after utf-8-lossy reads still map."""
    actual_headers = [
        "Instrumento financeiro",
        "Preo de fechamento",
        "Preo mnimo",
        "Quantidade de negcios",
    ]
    result = map_columns(actual_headers, TRADE_COLUMN_MAP)
    assert result.get("Instrumento financeiro") == "ticker"
    assert result.get("Preo de fechamento") == "last_price"
    assert result.get("Preo mnimo") == "min_price"
    assert result.get("Quantidade de negcios") == "trade_count"


def test_map_columns_deduplicates_mapped_targets():
    """If two source columns map to the same internal name, the first wins."""
    columns = ["TckrSymb", "Tckr"]
    result = map_columns(columns, TRADE_COLUMN_MAP)
    assert len([v for v in result.values() if v == "ticker"]) == 1


def test_map_columns_instruments_segment():
    columns = ["Instrumento financeiro", "Segmento", "Código ISIN"]
    result = map_columns(columns, INSTRUMENT_COLUMN_MAP)
    assert result["Instrumento financeiro"] == "ticker"
    assert result["Segmento"] == "segment"
    assert result["Código ISIN"] == "isin"
