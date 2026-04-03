"""Prefect tasks for ETL handoff to transformation/load pipelines."""

from __future__ import annotations

from datetime import date
from pathlib import Path
import re

from prefect import task

from app.etl.orchestration.pipeline import (
    run_cotahist_historical_pipeline,
    run_daily_quotes_pipeline,
    run_instruments_and_trades_pipeline,
    run_intraday_quotes_pipeline,
)


def _resolve_latest_jsonl_from_report(report_path: Path) -> Path:
    m = re.match(r"report_(\d{8}T\d{6})\.csv$", report_path.name)
    if m is None:
        raise FileNotFoundError(f"Cannot derive JSONL artifact from report name: {report_path.name}")
    timestamp = m.group(1)
    jsonl_path = report_path.parent / f"daily_fluctuation_{timestamp}.jsonl"
    if not jsonl_path.exists():
        raise FileNotFoundError(f"JSONL artifact not found for report {report_path.name}: {jsonl_path}")
    return jsonl_path


def run_registry_handoff_impl(
    *,
    target_date: date,
    cadastro_csv: Path,
    negocios_csv: Path,
) -> dict:
    """Instruments+trades and daily quotes loads (no intraday, no cotahist)."""
    result_main = run_instruments_and_trades_pipeline(cadastro_csv, negocios_csv, target_date)
    result_daily = run_daily_quotes_pipeline(negocios_csv, target_date)
    return {
        "instruments_trades": result_main,
        "daily_quotes": result_daily,
    }


def run_intraday_handoff_impl(intraday_report_path: Path) -> dict:
    jsonl_path = _resolve_latest_jsonl_from_report(intraday_report_path)
    result_intraday = run_intraday_quotes_pipeline(jsonl_path)
    return {"intraday_quotes": result_intraday}


@task(name="handoff-registry-loads", retries=1, retry_delay_seconds=30)
def handoff_registry_loads_task(
    *,
    target_date: date,
    cadastro_csv: Path,
    negocios_csv: Path,
) -> dict:
    """Load cadastro + negócios artifacts into core tables and daily quotes."""
    return run_registry_handoff_impl(
        target_date=target_date,
        cadastro_csv=cadastro_csv,
        negocios_csv=negocios_csv,
    )


@task(name="handoff-intraday-loads", retries=1, retry_delay_seconds=30)
def handoff_intraday_load_task(*, intraday_report_path: Path) -> dict:
    """Load intraday JSONL produced by the quote batch (report CSV path as anchor)."""
    return run_intraday_handoff_impl(intraday_report_path)


@task(name="trigger-transform-load-handoff", retries=1, retry_delay_seconds=30)
def trigger_transform_load_task(
    *,
    target_date: date,
    cadastro_csv: Path,
    negocios_csv: Path,
    intraday_report_path: Path | None = None,
    cotahist_txt_paths: list[Path] | None = None,
) -> dict:
    """Run downstream ETL entrypoints after scraper validation (combined path)."""
    out: dict = run_registry_handoff_impl(
        target_date=target_date,
        cadastro_csv=cadastro_csv,
        negocios_csv=negocios_csv,
    )
    if intraday_report_path is not None:
        out.update(run_intraday_handoff_impl(intraday_report_path))
    else:
        out["intraday_quotes"] = None
    result_cotahist = None
    if cotahist_txt_paths:
        result_cotahist = run_cotahist_historical_pipeline(cotahist_txt_paths, record_audit=True)
    out["cotahist"] = result_cotahist
    return out
