"""Prefect flow for the daily scraping pipeline."""

from __future__ import annotations

from datetime import date
import os

from prefect import flow, get_run_logger

from app.etl.orchestration.prefect.tasks.handoff_tasks import trigger_transform_load_task
from app.etl.orchestration.prefect.tasks.scraping_tasks import (
    run_intraday_quote_batch_task,
    scrape_cadastro_task,
    scrape_cotahist_task,
    scrape_negocios_task,
)
from app.etl.orchestration.prefect.tasks.validation_tasks import validate_outputs_task


@flow(name="daily-scraping-flow")
def daily_scraping_flow(
    target_date: date | None = None,
    run_intraday: bool = True,
    run_cotahist: bool = True,
) -> dict:
    """Daily scraping orchestration with explicit task dependencies."""
    logger = get_run_logger()
    td = target_date or date.today()
    cadastro_csv = scrape_cadastro_task(td)
    negocios_csv = scrape_negocios_task(td)
    cotahist_txt_paths = scrape_cotahist_task(td, enabled=run_cotahist)
    validation = validate_outputs_task(cadastro_csv=cadastro_csv, negocios_csv=negocios_csv)

    intraday_report = None
    if run_intraday:
        intraday_report = run_intraday_quote_batch_task(
            instruments_csv=cadastro_csv,
            target_date=td,
            trades_csv=negocios_csv,
        )

    handoff = trigger_transform_load_task(
        target_date=td,
        cadastro_csv=cadastro_csv,
        negocios_csv=negocios_csv,
        intraday_report_path=intraday_report,
        cotahist_txt_paths=cotahist_txt_paths,
    )
    summary = {
        "target_date": td.isoformat(),
        "validation": validation,
        "handoff": handoff,
    }
    logger.info("daily scraping flow completed: %s", summary)
    return summary


def default_daily_parameters() -> dict:
    return {
        "run_intraday": os.environ.get("PREFECT_RUN_INTRADAY", "true").lower() in ("1", "true", "yes"),
        "run_cotahist": os.environ.get("PREFECT_RUN_COTAHIST", "true").lower() in ("1", "true", "yes"),
    }
