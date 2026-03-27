"""Prefect validation tasks for scraper outputs."""

from __future__ import annotations

from pathlib import Path

from prefect import task

from app.etl.validation.scraping_output_validator import validate_scraped_csv

# Raw B3 boletim CSVs use Portuguese headers; internal names (ticker, trade_date)
# are applied only after parse_instruments_csv / parse_trades_file.
_CADASTRO_TICKER_COL = "Instrumento financeiro"
_NEGOCIOS_TICKER_COL = "Instrumento financeiro"


@task(name="validate-scraped-outputs")
def validate_outputs_task(
    *,
    cadastro_csv: Path,
    negocios_csv: Path,
) -> dict:
    """Validate required scraped outputs before transform/load handoff."""
    cadastro = validate_scraped_csv(
        path=cadastro_csv,
        required_columns={_CADASTRO_TICKER_COL},
        key_columns=[_CADASTRO_TICKER_COL],
        min_rows=1,
    )
    negocios = validate_scraped_csv(
        path=negocios_csv,
        required_columns={_NEGOCIOS_TICKER_COL},
        key_columns=[_NEGOCIOS_TICKER_COL],
        min_rows=1,
    )
    return {
        "cadastro_rows": cadastro.rows,
        "negocios_rows": negocios.rows,
        "ok": True,
    }
