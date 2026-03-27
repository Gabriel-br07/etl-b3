"""Prefect tasks for ETL handoff to transformation/load pipelines."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from prefect import task

from app.etl.orchestration.pipeline import (
    run_cotahist_historical_pipeline,
    run_daily_quotes_pipeline,
    run_instruments_and_trades_pipeline,
    run_intraday_quotes_pipeline,
)


def _resolve_latest_jsonl_from_report(report_path: Path) -> Path:
    folder = report_path.parent
    candidates = sorted(folder.glob("daily_fluctuation_*.jsonl"))
    if not candidates:
        raise FileNotFoundError(f"No daily_fluctuation JSONL found in {folder}")
    return candidates[-1]


@task(name="trigger-transform-load-handoff", retries=1, retry_delay_seconds=30)
def trigger_transform_load_task(
    *,
    target_date: date,
    cadastro_csv: Path,
    negocios_csv: Path,
    intraday_report_path: Path | None = None,
    cotahist_txt_paths: list[Path] | None = None,
) -> dict:
    """Run downstream ETL entrypoints after scraper validation."""
    result_main = run_instruments_and_trades_pipeline(cadastro_csv, negocios_csv, target_date)
    result_daily = run_daily_quotes_pipeline(negocios_csv, target_date)
    result_intraday = None
    if intraday_report_path is not None:
        jsonl_path = _resolve_latest_jsonl_from_report(intraday_report_path)
        result_intraday = run_intraday_quotes_pipeline(jsonl_path)
    result_cotahist = None
    if cotahist_txt_paths:
        result_cotahist = run_cotahist_historical_pipeline(cotahist_txt_paths, record_audit=True)
    return {
        "instruments_trades": result_main,
        "daily_quotes": result_daily,
        "intraday_quotes": result_intraday,
        "cotahist": result_cotahist,
    }
