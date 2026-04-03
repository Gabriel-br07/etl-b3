"""Integration: validate_scraped_csv with pt-BR numeric cells (realistic negócios CSV)."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from app.etl.validation.scraping_output_validator import _read_csv, validate_scraped_csv


def test_validate_scraped_csv_negocios_locale_volume_financeiro(tmp_path: Path) -> None:
    p = tmp_path / "negocios_consolidados_20260402.normalized.csv"
    p.write_text(
        "Instrumento financeiro;Data do negócio;Volume financeiro;Preço de fechamento\n"
        "PETR4;2026-04-02;12769,99;38,12\n"
        "VALE3;2026-04-02;1000;62,30\n",
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
    assert result.rows == 2

    df = _read_csv(p)
    assert df["Volume financeiro"].dtype == pl.Float64
    assert df["Volume financeiro"].to_list()[0] == 12769.99
    assert df["Preço de fechamento"].to_list() == [38.12, 62.30]


def test_validate_scraped_csv_ascii_headers_pt_br_numbers(tmp_path: Path) -> None:
    p = tmp_path / "neg_ascii.csv"
    p.write_text(
        "Instrumento financeiro;Preco de fechamento;Volume financeiro\n"
        "PETR4;38,12;12769,99\n",
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
    df = _read_csv(p)
    assert df["Preco de fechamento"].dtype == pl.Float64
    assert df["Volume financeiro"].to_list() == [12769.99]
