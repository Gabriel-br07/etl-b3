"""Tests for the dual-source ticker filtering pipeline.

Tests cover:
- Master structural filter rules (Segmento, Mercado, Categoria, date ranges)
- Negocios operational filter rules (trade count > 0, non-null close price)
- Join / intersection behaviour (strict vs fallback)
- Fallback when negocios CSV is absent or missing
- Handling of missing / blank Data fim negócio
- Column name normalisation (accents, casing, whitespace variants)
- Deterministic output ordering (preserves master CSV order)
- Edge cases: empty files, all-fail negocios, all-pass negocios
"""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

import pytest

from app.etl.ingestion.ticker_filter import (
    TickerFilterResult,
    _apply_master_filter,
    _apply_negocios_filter,
    _build_col_index,
    _normalise_key,
    _parse_date,
    build_ticker_filter,
)
from app.etl.ingestion.autodiscover import _find_negocios_sibling

# ---------------------------------------------------------------------------
# Fixture paths
# ---------------------------------------------------------------------------

FIXTURES = Path(__file__).parent / "fixtures"
INSTR_FIXTURE = FIXTURES / "cadastro_instrumentos_filter_test.csv"
NEG_FIXTURE = FIXTURES / "negocios_consolidados_filter_test.csv"

# Expected tickers that pass the structural filter in INSTR_FIXTURE
# (CASH + EQUITY-CASH + SHARES + active dates as of 2026-02-26)
MASTER_EXPECTED = {"PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3", "MGLU3", "WEGE3", "RENT3", "PETR3", "LREN3", "SUZB3"}

# Tickers in negocios fixture that pass operational filter (trade_count>0, close_price non-null)
NEGOCIOS_ACTIVE = {"PETR4", "VALE3", "ITUB4", "BBDC4", "WEGE3", "RENT3", "SUZB3", "MGLU3"}

# Strict = master ∩ negocios_active
STRICT_EXPECTED = MASTER_EXPECTED & NEGOCIOS_ACTIVE

REFERENCE_DATE = date(2026, 2, 26)


# ---------------------------------------------------------------------------
# Helper: write a minimal CSV to a tmp file
# ---------------------------------------------------------------------------

def _write_csv(path: Path, rows: list[dict], delimiter: str = ";") -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter=delimiter)
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Unit: _normalise_key
# ---------------------------------------------------------------------------

def test_normalise_key_strips_accents():
    assert _normalise_key("Segmento") == "segmento"
    assert _normalise_key("Data início negócio") == "data inicio negocio"
    assert _normalise_key("Preço de fechamento") == "preco de fechamento"
    assert _normalise_key("Quantidade de negócios") == "quantidade de negocios"


def test_normalise_key_strips_whitespace():
    assert _normalise_key("  Segmento  ") == "segmento"


def test_normalise_key_lowercases():
    assert _normalise_key("SEGMENTO") == "segmento"
    assert _normalise_key("MERCADO") == "mercado"


# ---------------------------------------------------------------------------
# Unit: _parse_date
# ---------------------------------------------------------------------------

def test_parse_date_iso():
    assert _parse_date("2024-06-14") == date(2024, 6, 14)


def test_parse_date_br_format():
    assert _parse_date("14/06/2024") == date(2024, 6, 14)


def test_parse_date_empty_returns_none():
    assert _parse_date("") is None
    assert _parse_date(None) is None
    assert _parse_date("   ") is None


def test_parse_date_invalid_returns_none():
    assert _parse_date("not-a-date") is None


# ---------------------------------------------------------------------------
# Unit: _apply_master_filter
# ---------------------------------------------------------------------------

def _master_rows_and_index() -> tuple[list[dict], dict[str, str]]:
    headers = [
        "Instrumento financeiro",
        "Segmento",
        "Mercado",
        "Categoria",
        "Data início negócio",
        "Data fim negócio",
    ]
    rows = [
        # Pass all rules
        {"Instrumento financeiro": "PETR4", "Segmento": "CASH", "Mercado": "EQUITY-CASH",
         "Categoria": "SHARES", "Data início negócio": "2000-01-02", "Data fim negócio": ""},
        # Fail: wrong Segmento
        {"Instrumento financeiro": "XPBR31", "Segmento": "OPTIONS", "Mercado": "EQUITY-CASH",
         "Categoria": "BDR", "Data início negócio": "2020-01-01", "Data fim negócio": ""},
        # Fail: wrong Mercado
        {"Instrumento financeiro": "OPT001", "Segmento": "CASH", "Mercado": "OPTIONS",
         "Categoria": "CALL", "Data início negócio": "2020-01-01", "Data fim negócio": ""},
        # Fail: wrong Categoria
        {"Instrumento financeiro": "FII001", "Segmento": "CASH", "Mercado": "EQUITY-CASH",
         "Categoria": "FII", "Data início negócio": "2020-01-01", "Data fim negócio": ""},
        # Fail: start date in future
        {"Instrumento financeiro": "NEW001", "Segmento": "CASH", "Mercado": "EQUITY-CASH",
         "Categoria": "SHARES", "Data início negócio": "2030-01-01", "Data fim negócio": ""},
        # Fail: end date in the past
        {"Instrumento financeiro": "OLD001", "Segmento": "CASH", "Mercado": "EQUITY-CASH",
         "Categoria": "SHARES", "Data início negócio": "2000-01-01", "Data fim negócio": "2020-12-31"},
        # Pass: end date is today or future
        {"Instrumento financeiro": "VALE3", "Segmento": "CASH", "Mercado": "EQUITY-CASH",
         "Categoria": "SHARES", "Data início negócio": "2002-05-22", "Data fim negócio": "2027-12-31"},
        # Pass: end date missing (still active)
        {"Instrumento financeiro": "ITUB4", "Segmento": "CASH", "Mercado": "EQUITY-CASH",
         "Categoria": "SHARES", "Data início negócio": "1998-04-01", "Data fim negócio": None},
    ]
    col_index = _build_col_index(headers)
    return rows, col_index


def test_master_filter_keeps_passing_rows():
    rows, col_index = _master_rows_and_index()
    result = _apply_master_filter(rows, col_index, REFERENCE_DATE)
    assert "PETR4" in result
    assert "VALE3" in result
    assert "ITUB4" in result


def test_master_filter_drops_wrong_segmento():
    rows, col_index = _master_rows_and_index()
    result = _apply_master_filter(rows, col_index, REFERENCE_DATE)
    assert "XPBR31" not in result


def test_master_filter_drops_wrong_mercado():
    rows, col_index = _master_rows_and_index()
    result = _apply_master_filter(rows, col_index, REFERENCE_DATE)
    assert "OPT001" not in result


def test_master_filter_drops_wrong_categoria():
    rows, col_index = _master_rows_and_index()
    result = _apply_master_filter(rows, col_index, REFERENCE_DATE)
    assert "FII001" not in result


def test_master_filter_drops_future_start_date():
    rows, col_index = _master_rows_and_index()
    result = _apply_master_filter(rows, col_index, REFERENCE_DATE)
    assert "NEW001" not in result


def test_master_filter_drops_expired_end_date():
    rows, col_index = _master_rows_and_index()
    result = _apply_master_filter(rows, col_index, REFERENCE_DATE)
    assert "OLD001" not in result


def test_master_filter_keeps_null_end_date():
    rows, col_index = _master_rows_and_index()
    result = _apply_master_filter(rows, col_index, REFERENCE_DATE)
    assert "ITUB4" in result


def test_master_filter_keeps_future_end_date():
    rows, col_index = _master_rows_and_index()
    result = _apply_master_filter(rows, col_index, REFERENCE_DATE)
    assert "VALE3" in result


def test_master_filter_deduplicates(tmp_path):
    headers = ["Instrumento financeiro", "Segmento", "Mercado", "Categoria",
               "Data início negócio", "Data fim negócio"]
    rows = [
        {"Instrumento financeiro": "PETR4", "Segmento": "CASH", "Mercado": "EQUITY-CASH",
         "Categoria": "SHARES", "Data início negócio": "2000-01-02", "Data fim negócio": ""},
        {"Instrumento financeiro": "PETR4", "Segmento": "CASH", "Mercado": "EQUITY-CASH",
         "Categoria": "SHARES", "Data início negócio": "2000-01-02", "Data fim negócio": ""},
    ]
    col_index = _build_col_index(headers)
    result = _apply_master_filter(rows, col_index, REFERENCE_DATE)
    assert result.count("PETR4") == 1


def test_master_filter_preserves_order():
    rows, col_index = _master_rows_and_index()
    result = _apply_master_filter(rows, col_index, REFERENCE_DATE)
    # PETR4 appears before VALE3 in the rows list
    assert result.index("PETR4") < result.index("VALE3")


def test_master_filter_missing_filter_cols_accepts_all_tickers():
    """When structural filter columns are absent, tickers are kept (graceful degradation)."""
    headers = ["Instrumento financeiro"]
    rows = [
        {"Instrumento financeiro": "PETR4"},
        {"Instrumento financeiro": "VALE3"},
    ]
    col_index = _build_col_index(headers)
    result = _apply_master_filter(rows, col_index, REFERENCE_DATE)
    assert "PETR4" in result
    assert "VALE3" in result


def test_master_filter_missing_ticker_col_returns_empty():
    headers = ["Segmento", "Mercado"]
    rows = [{"Segmento": "CASH", "Mercado": "EQUITY-CASH"}]
    col_index = _build_col_index(headers)
    result = _apply_master_filter(rows, col_index, REFERENCE_DATE)
    assert result == []


def test_master_filter_case_insensitive_values():
    """Filter values should match regardless of casing in the CSV."""
    headers = ["Instrumento financeiro", "Segmento", "Mercado", "Categoria",
               "Data início negócio", "Data fim negócio"]
    rows = [
        {"Instrumento financeiro": "TEST1", "Segmento": "cash", "Mercado": "equity-cash",
         "Categoria": "shares", "Data início negócio": "2000-01-01", "Data fim negócio": ""},
        {"Instrumento financeiro": "TEST2", "Segmento": "Cash", "Mercado": "Equity-Cash",
         "Categoria": "Shares", "Data início negócio": "2000-01-01", "Data fim negócio": ""},
    ]
    col_index = _build_col_index(headers)
    result = _apply_master_filter(rows, col_index, REFERENCE_DATE)
    assert "TEST1" in result
    assert "TEST2" in result


# ---------------------------------------------------------------------------
# Unit: _apply_negocios_filter
# ---------------------------------------------------------------------------

def _negocios_rows_and_index() -> tuple[list[dict], dict[str, str]]:
    headers = ["Instrumento financeiro", "Quantidade de negócios", "Preço de fechamento"]
    rows = [
        {"Instrumento financeiro": "PETR4", "Quantidade de negócios": "15420", "Preço de fechamento": "38.45"},
        {"Instrumento financeiro": "VALE3", "Quantidade de negócios": "9830",  "Preço de fechamento": "62.30"},
        {"Instrumento financeiro": "ZERONEGS", "Quantidade de negócios": "0", "Preço de fechamento": "15.00"},
        {"Instrumento financeiro": "NOPRICE",  "Quantidade de negócios": "1200", "Preço de fechamento": ""},
        {"Instrumento financeiro": "NULLPRC",  "Quantidade de negócios": "500", "Preço de fechamento": None},
        {"Instrumento financeiro": "BOTHNULL", "Quantidade de negócios": "0",  "Preço de fechamento": ""},
    ]
    col_index = _build_col_index(headers)
    return rows, col_index


def test_negocios_filter_keeps_active_tickers():
    rows, col_index = _negocios_rows_and_index()
    result = _apply_negocios_filter(rows, col_index)
    assert "PETR4" in result
    assert "VALE3" in result


def test_negocios_filter_drops_zero_trades():
    rows, col_index = _negocios_rows_and_index()
    result = _apply_negocios_filter(rows, col_index)
    assert "ZERONEGS" not in result


def test_negocios_filter_drops_empty_close_price():
    rows, col_index = _negocios_rows_and_index()
    result = _apply_negocios_filter(rows, col_index)
    assert "NOPRICE" not in result


def test_negocios_filter_drops_null_close_price():
    rows, col_index = _negocios_rows_and_index()
    result = _apply_negocios_filter(rows, col_index)
    assert "NULLPRC" not in result


def test_negocios_filter_returns_set():
    rows, col_index = _negocios_rows_and_index()
    result = _apply_negocios_filter(rows, col_index)
    assert isinstance(result, set)


def test_negocios_filter_missing_ticker_col_returns_empty():
    headers = ["Quantidade de negócios", "Preço de fechamento"]
    rows = [{"Quantidade de negócios": "100", "Preço de fechamento": "10.00"}]
    col_index = _build_col_index(headers)
    result = _apply_negocios_filter(rows, col_index)
    assert result == set()


def test_negocios_filter_missing_optional_cols_accepts_all_with_ticker():
    """When operational filter columns are absent, all tickers are accepted."""
    headers = ["Instrumento financeiro"]
    rows = [
        {"Instrumento financeiro": "PETR4"},
        {"Instrumento financeiro": "VALE3"},
    ]
    col_index = _build_col_index(headers)
    result = _apply_negocios_filter(rows, col_index)
    assert "PETR4" in result
    assert "VALE3" in result


def test_negocios_filter_comma_decimal_in_trade_count():
    """Trade counts with comma decimal separators should be handled."""
    headers = ["Instrumento financeiro", "Quantidade de negócios", "Preço de fechamento"]
    rows = [{"Instrumento financeiro": "PETR4", "Quantidade de negócios": "1.500", "Preço de fechamento": "38,45"}]
    col_index = _build_col_index(headers)
    result = _apply_negocios_filter(rows, col_index)
    assert "PETR4" in result


# ---------------------------------------------------------------------------
# Integration: build_ticker_filter with fixture files
# ---------------------------------------------------------------------------

def test_build_ticker_filter_strict_is_subset_of_master():
    result = build_ticker_filter(INSTR_FIXTURE, trades_csv=NEG_FIXTURE, reference_date=REFERENCE_DATE)
    strict_set = set(result.strict_filtered_tickers)
    master_set = set(result.fallback_master_tickers)
    assert strict_set.issubset(master_set)


def test_build_ticker_filter_master_contains_expected_tickers():
    result = build_ticker_filter(INSTR_FIXTURE, trades_csv=NEG_FIXTURE, reference_date=REFERENCE_DATE)
    master_set = set(result.fallback_master_tickers)
    for ticker in MASTER_EXPECTED:
        assert ticker in master_set, f"Expected {ticker} in master list"


def test_build_ticker_filter_master_excludes_non_cash():
    result = build_ticker_filter(INSTR_FIXTURE, trades_csv=NEG_FIXTURE, reference_date=REFERENCE_DATE)
    master_set = set(result.fallback_master_tickers)
    assert "XPBR31" not in master_set       # OPTIONS segmento
    assert "PETRH280" not in master_set     # CALL categoria
    assert "FUTURE01" not in master_set     # DERIVATIVES


def test_build_ticker_filter_master_excludes_expired():
    result = build_ticker_filter(INSTR_FIXTURE, trades_csv=NEG_FIXTURE, reference_date=REFERENCE_DATE)
    master_set = set(result.fallback_master_tickers)
    assert "OLDTICK" not in master_set      # end date 2020-12-31 < ref_date


def test_build_ticker_filter_master_excludes_not_yet_started():
    result = build_ticker_filter(INSTR_FIXTURE, trades_csv=NEG_FIXTURE, reference_date=REFERENCE_DATE)
    master_set = set(result.fallback_master_tickers)
    assert "FUTURET" not in master_set      # start date 2030-01-01 > ref_date


def test_build_ticker_filter_strict_excludes_zero_trade_tickers():
    result = build_ticker_filter(INSTR_FIXTURE, trades_csv=NEG_FIXTURE, reference_date=REFERENCE_DATE)
    strict_set = set(result.strict_filtered_tickers)
    assert "ZERONEGS" not in strict_set
    assert "NOPRICE" not in strict_set


def test_build_ticker_filter_master_only_tickers():
    result = build_ticker_filter(INSTR_FIXTURE, trades_csv=NEG_FIXTURE, reference_date=REFERENCE_DATE)
    # master_only = in master but NOT in strict
    strict_set = set(result.strict_filtered_tickers)
    master_set = set(result.fallback_master_tickers)
    master_only_set = set(result.master_only_tickers)
    assert master_only_set == master_set - strict_set


def test_build_ticker_filter_strict_plus_master_only_equals_master():
    result = build_ticker_filter(INSTR_FIXTURE, trades_csv=NEG_FIXTURE, reference_date=REFERENCE_DATE)
    combined = set(result.strict_filtered_tickers) | set(result.master_only_tickers)
    assert combined == set(result.fallback_master_tickers)


def test_build_ticker_filter_preserves_master_order():
    """Strict and fallback lists must respect the order from the instruments CSV."""
    result = build_ticker_filter(INSTR_FIXTURE, trades_csv=NEG_FIXTURE, reference_date=REFERENCE_DATE)
    fallback = result.fallback_master_tickers
    # PETR4 appears before VALE3 in the fixture CSV
    assert fallback.index("PETR4") < fallback.index("VALE3")


def test_build_ticker_filter_strict_order_matches_master_order():
    """Strict list must preserve the same relative order as the master list."""
    result = build_ticker_filter(INSTR_FIXTURE, trades_csv=NEG_FIXTURE, reference_date=REFERENCE_DATE)
    strict = result.strict_filtered_tickers
    fallback = result.fallback_master_tickers
    # strict is a subset of fallback preserving order
    fallback_filtered = [t for t in fallback if t in set(strict)]
    assert fallback_filtered == strict


# ---------------------------------------------------------------------------
# Fallback behaviour: no trades CSV
# ---------------------------------------------------------------------------

def test_build_ticker_filter_no_trades_strict_equals_master():
    result = build_ticker_filter(INSTR_FIXTURE, reference_date=REFERENCE_DATE)
    assert result.strict_filtered_tickers == result.fallback_master_tickers


def test_build_ticker_filter_no_trades_master_only_is_empty():
    result = build_ticker_filter(INSTR_FIXTURE, reference_date=REFERENCE_DATE)
    assert result.master_only_tickers == []


def test_build_ticker_filter_missing_trades_file_falls_back(tmp_path):
    missing = tmp_path / "nonexistent_negocios.csv"
    result = build_ticker_filter(INSTR_FIXTURE, trades_csv=missing, reference_date=REFERENCE_DATE)
    # Should fall back gracefully (same as no trades file)
    assert result.strict_filtered_tickers == result.fallback_master_tickers
    assert result.master_only_tickers == []


def test_build_ticker_filter_missing_instruments_raises():
    missing = Path("/tmp/nonexistent_instruments.csv")
    with pytest.raises(FileNotFoundError):
        build_ticker_filter(missing, reference_date=REFERENCE_DATE)


# ---------------------------------------------------------------------------
# Column name normalisation (accents / casing / spacing variants)
# ---------------------------------------------------------------------------

def test_build_ticker_filter_tolerates_accent_variants(tmp_path):
    """Columns with slightly different accent forms should still be resolved."""
    instr = tmp_path / "instr.csv"
    # Use 'inicio' without accent – the normaliser should still match
    _write_csv(instr, [
        {
            "Instrumento financeiro": "PETR4",
            "Segmento": "CASH",
            "Mercado": "EQUITY-CASH",
            "Categoria": "SHARES",
            "Data inicio negocio": "2000-01-02",   # no accent
            "Data fim negocio": "",                  # no accent
        }
    ])
    result = build_ticker_filter(instr, reference_date=REFERENCE_DATE)
    assert "PETR4" in result.fallback_master_tickers


def test_build_ticker_filter_tolerates_uppercase_col_names(tmp_path):
    instr = tmp_path / "instr.csv"
    _write_csv(instr, [
        {
            "INSTRUMENTO FINANCEIRO": "PETR4",
            "SEGMENTO": "CASH",
            "MERCADO": "EQUITY-CASH",
            "CATEGORIA": "SHARES",
            "DATA INÍCIO NEGÓCIO": "2000-01-02",
            "DATA FIM NEGÓCIO": "",
        }
    ])
    result = build_ticker_filter(instr, reference_date=REFERENCE_DATE)
    assert "PETR4" in result.fallback_master_tickers


def test_build_ticker_filter_tolerates_extra_whitespace_in_col_names(tmp_path):
    instr = tmp_path / "instr.csv"
    _write_csv(instr, [
        {
            "  Instrumento financeiro  ": "VALE3",
            "  Segmento  ": "CASH",
            "  Mercado  ": "EQUITY-CASH",
            "  Categoria  ": "SHARES",
            "  Data início negócio  ": "2002-05-22",
            "  Data fim negócio  ": "",
        }
    ])
    result = build_ticker_filter(instr, reference_date=REFERENCE_DATE)
    assert "VALE3" in result.fallback_master_tickers


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_build_ticker_filter_empty_instruments_returns_empty_lists(tmp_path):
    instr = tmp_path / "empty_instr.csv"
    _write_csv(instr, [])
    result = build_ticker_filter(instr, reference_date=REFERENCE_DATE)
    assert result.strict_filtered_tickers == []
    assert result.fallback_master_tickers == []
    assert result.master_only_tickers == []


def test_build_ticker_filter_all_negocios_fail(tmp_path):
    """When no negocios ticker passes the operational filter, strict list is empty."""
    neg = tmp_path / "neg.csv"
    _write_csv(neg, [
        {"Instrumento financeiro": "PETR4", "Quantidade de negócios": "0", "Preço de fechamento": ""},
        {"Instrumento financeiro": "VALE3", "Quantidade de negócios": "0", "Preço de fechamento": ""},
    ])
    result = build_ticker_filter(INSTR_FIXTURE, trades_csv=neg, reference_date=REFERENCE_DATE)
    assert result.strict_filtered_tickers == []
    # fallback master is still populated
    assert len(result.fallback_master_tickers) > 0
    # master_only == full master
    assert set(result.master_only_tickers) == set(result.fallback_master_tickers)


def test_build_ticker_filter_returns_ticker_filter_result_type():
    result = build_ticker_filter(INSTR_FIXTURE, trades_csv=NEG_FIXTURE, reference_date=REFERENCE_DATE)
    assert isinstance(result, TickerFilterResult)
    assert isinstance(result.strict_filtered_tickers, list)
    assert isinstance(result.fallback_master_tickers, list)
    assert isinstance(result.master_only_tickers, list)


def test_build_ticker_filter_all_tickers_uppercase():
    result = build_ticker_filter(INSTR_FIXTURE, trades_csv=NEG_FIXTURE, reference_date=REFERENCE_DATE)
    for ticker in result.strict_filtered_tickers + result.fallback_master_tickers:
        assert ticker == ticker.upper(), f"Ticker {ticker!r} is not uppercased"


def test_build_ticker_filter_no_spaces_in_tickers():
    """Noise rows with spaces should not appear in the output."""
    result = build_ticker_filter(INSTR_FIXTURE, trades_csv=NEG_FIXTURE, reference_date=REFERENCE_DATE)
    for ticker in result.strict_filtered_tickers + result.fallback_master_tickers:
        assert " " not in ticker, f"Ticker {ticker!r} contains a space"


# ---------------------------------------------------------------------------
# _find_negocios_sibling  (auto-discovery used by run_b3_quote_batch.py)
# ---------------------------------------------------------------------------

from app.etl.ingestion.autodiscover import _find_negocios_sibling


def test_find_negocios_sibling_exact_date_match(tmp_path):
    instr = tmp_path / "cadastro_instrumentos_20260226.normalized.csv"
    negocios = tmp_path / "negocios_consolidados_20260226.normalized.csv"
    instr.write_text("header\n", encoding="utf-8")
    negocios.write_text("header\n", encoding="utf-8")

    result = _find_negocios_sibling(instr)
    assert result == negocios


def test_find_negocios_sibling_fallback_to_any_in_folder(tmp_path):
    instr = tmp_path / "cadastro_instrumentos_20260226.normalized.csv"
    negocios = tmp_path / "negocios_consolidados_20260220.normalized.csv"
    instr.write_text("header\n", encoding="utf-8")
    negocios.write_text("header\n", encoding="utf-8")

    result = _find_negocios_sibling(instr)
    assert result == negocios


def test_find_negocios_sibling_picks_most_recent_when_multiple(tmp_path):
    instr = tmp_path / "cadastro_instrumentos_20260226.normalized.csv"
    older = tmp_path / "negocios_consolidados_20260224.normalized.csv"
    newer = tmp_path / "negocios_consolidados_20260225.normalized.csv"
    instr.write_text("header\n", encoding="utf-8")
    older.write_text("header\n", encoding="utf-8")
    newer.write_text("header\n", encoding="utf-8")

    result = _find_negocios_sibling(instr)
    assert result == newer   # alphabetically last = most recent


def test_find_negocios_sibling_returns_none_when_not_found(tmp_path):
    instr = tmp_path / "cadastro_instrumentos_20260226.normalized.csv"
    instr.write_text("header\n", encoding="utf-8")

    result = _find_negocios_sibling(instr)
    assert result is None


def test_find_negocios_sibling_prefers_exact_over_older(tmp_path):
    """Exact date match must win over an older fallback sibling."""
    instr = tmp_path / "cadastro_instrumentos_20260226.normalized.csv"
    exact = tmp_path / "negocios_consolidados_20260226.normalized.csv"
    older = tmp_path / "negocios_consolidados_20260220.normalized.csv"
    instr.write_text("h\n", encoding="utf-8")
    exact.write_text("h\n", encoding="utf-8")
    older.write_text("h\n", encoding="utf-8")

    result = _find_negocios_sibling(instr)
    assert result == exact

