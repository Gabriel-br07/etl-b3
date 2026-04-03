"""Unit tests for B3 boletim CSV reading (locale-safe numeric columns)."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from app.etl.validation.scraping_output_validator import _read_csv, validate_scraped_csv

_FIXTURE_NEGOCIOS = (
    Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "negocios_consolidados_filter_test.csv"
)


def test_read_csv_volume_financeiro_comma_decimal_and_integer(tmp_path: Path) -> None:
    p = tmp_path / "neg.csv"
    p.write_text(
        "col1;Volume financeiro\n"
        "A;12769,99\n"
        "B;1000\n",
        encoding="utf-8",
    )
    df = _read_csv(p)
    assert df["Volume financeiro"].dtype == pl.Float64
    assert df["Volume financeiro"].to_list() == [12769.99, 1000.0]


def test_read_csv_volume_financeiro_thousands_pt_br(tmp_path: Path) -> None:
    p = tmp_path / "neg.csv"
    p.write_text(
        "Instrumento financeiro;Volume financeiro\n"
        "PETR4;1.234,56\n",
        encoding="utf-8",
    )
    df = _read_csv(p)
    assert df["Volume financeiro"].dtype == pl.Float64
    assert df["Volume financeiro"].to_list() == [1234.56]


def test_read_csv_volume_financeiro_dot_decimal_fixture_style(tmp_path: Path) -> None:
    p = tmp_path / "neg.csv"
    p.write_text(
        "Instrumento financeiro;Volume financeiro\n"
        "PETR4;592940.00\n"
        "VALE3;38.45\n",
        encoding="utf-8",
    )
    df = _read_csv(p)
    assert df["Volume financeiro"].dtype == pl.Float64
    assert df["Volume financeiro"].to_list() == [592940.0, 38.45]


def test_read_csv_precos_and_quantidade_coerced(tmp_path: Path) -> None:
    p = tmp_path / "neg.csv"
    p.write_text(
        "Instrumento financeiro;Quantidade de negócios;Preço de fechamento;Volume financeiro\n"
        "X;1.500;10,25;9,99\n",
        encoding="utf-8",
    )
    df = _read_csv(p)
    assert df["Quantidade de negócios"].dtype == pl.Float64
    assert df["Preço de fechamento"].dtype == pl.Float64
    assert df["Volume financeiro"].dtype == pl.Float64
    assert df["Quantidade de negócios"].to_list() == [1500.0]
    assert df["Preço de fechamento"].to_list() == [10.25]
    assert df["Volume financeiro"].to_list() == [9.99]


def test_read_csv_repo_fixture_negocios_still_loads() -> None:
    """Regression: fixture uses dot decimals and spelling variants not in TRADE map."""
    assert _FIXTURE_NEGOCIOS.is_file()
    df = _read_csv(_FIXTURE_NEGOCIOS)
    assert df["Volume financeiro"].dtype == pl.Float64
    assert len(df) >= 1


def test_read_csv_ascii_headers_match_normalized_trade_numeric_columns(tmp_path: Path) -> None:
    """B3 often exports ASCII-only headers; they must normalize like accented TRADE_COLUMN_MAP keys."""
    p = tmp_path / "neg_ascii.csv"
    p.write_text(
        "Instrumento financeiro;Quantidade de negocios;Preco de fechamento;Volume financeiro\n"
        "PETR4;1500;10,25;12769,99\n"
        "VALE3;2000;62,30;1000\n",
        encoding="utf-8",
    )
    df = _read_csv(p)
    assert df["Volume financeiro"].dtype == pl.Float64
    assert df["Preco de fechamento"].dtype == pl.Float64
    assert df["Quantidade de negocios"].dtype == pl.Float64
    assert df["Volume financeiro"].to_list() == [12769.99, 1000.0]
    assert df["Preco de fechamento"].to_list() == [10.25, 62.30]
    assert df["Quantidade de negocios"].to_list() == [1500.0, 2000.0]

    result = validate_scraped_csv(
        path=p,
        required_columns={"Instrumento financeiro"},
        key_columns=["Instrumento financeiro"],
        min_rows=1,
        enforce_unique_keys=False,
    )
    assert result.ok is True


def test_read_csv_regression_many_integers_then_comma_decimal_volume(tmp_path: Path) -> None:
    """Original failure: Polars inferred Int64 from early rows then choked on 12769,99."""
    p = tmp_path / "neg.csv"
    lines = ["col1;Volume financeiro"] + [f"R{i};{1000 + i}" for i in range(15)] + ["Z;12769,99"]
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    df = _read_csv(p)
    assert df["Volume financeiro"].dtype == pl.Float64
    assert df["Volume financeiro"].to_list()[-1] == 12769.99
