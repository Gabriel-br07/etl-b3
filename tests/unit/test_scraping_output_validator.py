from __future__ import annotations

from pathlib import Path

import pytest

from app.etl.validation.scraping_output_validator import validate_scraped_csv


def test_validate_scraped_csv_b3_instrumento_financeiro_header(tmp_path: Path):
    """Cadastro/negócios raw files use ``Instrumento financeiro``, not ``ticker``."""
    p = tmp_path / "cad.csv"
    p.write_text(
        "Instrumento financeiro;Ativo\n"
        "PETR4;PETR\n"
        "VALE3;VALE\n",
        encoding="utf-8",
    )
    result = validate_scraped_csv(
        path=p,
        required_columns={"Instrumento financeiro"},
        key_columns=["Instrumento financeiro"],
        min_rows=1,
    )
    assert result.ok is True
    assert result.rows == 2


def test_validate_scraped_csv_b3_dash_numeric_placeholder(tmp_path: Path):
    """B3 uses '-' in numeric columns; Polars must not infer int then fail on '-'."""
    p = tmp_path / "cad.csv"
    p.write_text("a;b;c\n1;2;10\n3;4;20\n5;6;-\n", encoding="utf-8")
    result = validate_scraped_csv(
        path=p,
        required_columns={"a", "b", "c"},
        key_columns=["a", "b"],
        min_rows=1,
    )
    assert result.ok is True
    assert result.rows == 3


def test_validate_scraped_csv_ok(tmp_path: Path):
    p = tmp_path / "ok.csv"
    p.write_text("ticker;trade_date\nPETR4;2026-03-26\nVALE3;2026-03-26\n", encoding="utf-8")
    result = validate_scraped_csv(
        path=p,
        required_columns={"ticker", "trade_date"},
        key_columns=["ticker", "trade_date"],
        min_rows=1,
    )
    assert result.ok is True
    assert result.rows == 2


def test_validate_scraped_csv_missing_column(tmp_path: Path):
    p = tmp_path / "bad.csv"
    p.write_text("ticker\nPETR4\n", encoding="utf-8")
    with pytest.raises(ValueError):
        validate_scraped_csv(
            path=p,
            required_columns={"ticker", "trade_date"},
            key_columns=["ticker", "trade_date"],
        )


def test_validate_scraped_csv_negocios_allows_repeated_ticker_when_uniqueness_disabled(
    tmp_path: Path,
):
    """Trades-like files may repeat ticker rows and should not fail early validation."""
    p = tmp_path / "negocios.csv"
    p.write_text(
        "Instrumento financeiro;Data do Negocio\n"
        "PETR4;2026-03-26\n"
        "PETR4;2026-03-26\n"
        "VALE3;2026-03-26\n",
        encoding="utf-8",
    )
    result = validate_scraped_csv(
        path=p,
        required_columns={"Instrumento financeiro"},
        key_columns=["Instrumento financeiro"],
        min_rows=1,
        enforce_unique_keys=False,
    )
    assert result.ok is True
    assert result.rows == 3
