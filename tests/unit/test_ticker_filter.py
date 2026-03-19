"""Unit tests for dual-source ticker filtering (minimal CSV fixtures on tmp_path)."""

from datetime import date
from pathlib import Path

from app.etl.ingestion.ticker_filter import build_ticker_filter


def _write_cadastro(path: Path, rows: list[dict[str, str]]) -> None:
    headers = [
        "Instrumento financeiro",
        "Segmento",
        "Mercado",
        "Categoria",
        "Data início negócio",
        "Data fim negócio",
    ]
    lines = [";".join(headers)]
    for r in rows:
        lines.append(";".join(r.get(h, "") for h in headers))
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_negocios(path: Path, rows: list[dict[str, str]]) -> None:
    headers = [
        "Instrumento financeiro",
        "Quantidade de negócios",
        "Preço de fechamento",
    ]
    lines = [";".join(headers)]
    for r in rows:
        lines.append(";".join(r.get(h, "") for h in headers))
    path.write_text("\n".join(lines), encoding="utf-8")


def test_build_ticker_filter_strict_intersects_negocios(tmp_path: Path):
    ref = date(2025, 6, 1)
    cad = tmp_path / "cad.csv"
    neg = tmp_path / "neg.csv"
    _write_cadastro(
        cad,
        [
            {
                "Instrumento financeiro": "PETR4",
                "Segmento": "CASH",
                "Mercado": "EQUITY-CASH",
                "Categoria": "SHARES",
                "Data início negócio": "2020-01-01",
                "Data fim negócio": "",
            },
            {
                "Instrumento financeiro": "ZZZZ99",
                "Segmento": "CASH",
                "Mercado": "EQUITY-CASH",
                "Categoria": "SHARES",
                "Data início negócio": "2020-01-01",
                "Data fim negócio": "",
            },
        ],
    )
    _write_negocios(
        neg,
        [
            {
                "Instrumento financeiro": "PETR4",
                "Quantidade de negócios": "100",
                "Preço de fechamento": "38,50",
            },
        ],
    )
    result = build_ticker_filter(cad, trades_csv=neg, reference_date=ref)
    assert result.strict_filtered_tickers == ["PETR4"]
    assert result.fallback_master_tickers == ["PETR4", "ZZZZ99"]
    assert result.master_only_tickers == ["ZZZZ99"]


def test_build_ticker_filter_without_negocios_strict_equals_master(tmp_path: Path):
    ref = date(2025, 6, 1)
    cad = tmp_path / "cad.csv"
    _write_cadastro(
        cad,
        [
            {
                "Instrumento financeiro": "VALE3",
                "Segmento": "CASH",
                "Mercado": "EQUITY-CASH",
                "Categoria": "SHARES",
                "Data início negócio": "2020-01-01",
                "Data fim negócio": "",
            },
        ],
    )
    result = build_ticker_filter(cad, trades_csv=None, reference_date=ref)
    assert result.strict_filtered_tickers == ["VALE3"]
    assert result.fallback_master_tickers == ["VALE3"]
    assert result.master_only_tickers == []


def test_build_ticker_filter_excludes_not_yet_trading(tmp_path: Path):
    ref = date(2025, 6, 1)
    cad = tmp_path / "cad.csv"
    _write_cadastro(
        cad,
        [
            {
                "Instrumento financeiro": "NEW1",
                "Segmento": "CASH",
                "Mercado": "EQUITY-CASH",
                "Categoria": "SHARES",
                "Data início negócio": "2025-12-01",
                "Data fim negócio": "",
            },
        ],
    )
    result = build_ticker_filter(cad, trades_csv=None, reference_date=ref)
    assert result.fallback_master_tickers == []


def test_build_ticker_filter_excludes_wrong_segment(tmp_path: Path):
    ref = date(2025, 6, 1)
    cad = tmp_path / "cad.csv"
    _write_cadastro(
        cad,
        [
            {
                "Instrumento financeiro": "PETR4",
                "Segmento": "FUTURES",
                "Mercado": "EQUITY-CASH",
                "Categoria": "SHARES",
                "Data início negócio": "2020-01-01",
                "Data fim negócio": "",
            },
        ],
    )
    result = build_ticker_filter(cad, trades_csv=None, reference_date=ref)
    assert result.fallback_master_tickers == []
