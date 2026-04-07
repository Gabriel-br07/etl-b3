"""Prefect flows for B3 lightweight ETL (registry, intraday, bootstrap, combined)."""

from __future__ import annotations

from datetime import date
import os

from prefect import flow, get_run_logger

from app.etl.orchestration.csv_resolver import (
    CSVNotFoundError,
    find_negocios_sibling,
    resolve_instruments_csv,
)
from app.etl.orchestration.market_hours import is_within_b3_quote_window
from app.etl.orchestration.prefect.tasks.handoff_tasks import (
    handoff_intraday_load_task,
    handoff_registry_loads_task,
    trigger_transform_load_task,
)
from app.etl.orchestration.prefect.tasks.scraping_tasks import (
    run_intraday_quote_batch_task,
    scrape_cadastro_task,
    scrape_cotahist_task,
    scrape_negocios_task,
)
from app.etl.orchestration.prefect.tasks.validation_tasks import validate_outputs_task


@flow(name="daily-registry-flow")
def daily_registry_flow(target_date: date | None = None) -> dict:
    """Daily cadastro + negócios scrape, validation, and registry DB loads (08:00 path)."""
    logger = get_run_logger()
    td = target_date or date.today()
    cadastro_csv = scrape_cadastro_task(td)
    negocios_csv = scrape_negocios_task(td)
    validation = validate_outputs_task(cadastro_csv=cadastro_csv, negocios_csv=negocios_csv)
    handoff = handoff_registry_loads_task(
        target_date=td,
        cadastro_csv=cadastro_csv,
        negocios_csv=negocios_csv,
    )
    summary = {
        "target_date": td.isoformat(),
        "validation": validation,
        "handoff": handoff,
    }
    logger.info("daily registry flow completed: %s", summary)
    return summary


@flow(name="intraday-quotes-flow")
def intraday_quotes_flow(target_date: date | None = None) -> dict:
    """Intraday quote batch + load, only meaningful inside the configured B3 quote window."""
    logger = get_run_logger()
    td = target_date or date.today()
    if not is_within_b3_quote_window():
        logger.info("intraday flow skipped: outside B3 quote window")
        return {"skipped": True, "reason": "outside_quote_window", "target_date": td.isoformat()}

    try:
        instruments = resolve_instruments_csv(retry_count=0, retry_delay_seconds=0)
    except CSVNotFoundError as exc:
        logger.warning("intraday flow skipped: instruments CSV not found: %s", exc)
        return {"skipped": True, "reason": "instruments_csv_missing", "target_date": td.isoformat()}

    negocios = find_negocios_sibling(instruments)
    report = run_intraday_quote_batch_task(
        instruments_csv=instruments,
        target_date=td,
        trades_csv=negocios,
    )
    handoff = handoff_intraday_load_task(intraday_report_path=report)
    summary = {
        "skipped": False,
        "target_date": td.isoformat(),
        "instruments_csv": str(instruments),
        "report": str(report),
        "handoff": handoff,
    }
    logger.info("intraday quotes flow completed: %s", summary)
    return summary


@flow(name="lightweight-bootstrap-flow")
def lightweight_bootstrap_flow(target_date: date | None = None) -> dict:
    """One-shot startup path: cadastro, negócios, registry loads, intraday batch + intraday load."""
    logger = get_run_logger()
    td = target_date or date.today()
    cadastro_csv = scrape_cadastro_task(td)
    negocios_csv = scrape_negocios_task(td)
    validation = validate_outputs_task(cadastro_csv=cadastro_csv, negocios_csv=negocios_csv)
    reg_handoff = handoff_registry_loads_task(
        target_date=td,
        cadastro_csv=cadastro_csv,
        negocios_csv=negocios_csv,
    )
    report = run_intraday_quote_batch_task(
        instruments_csv=cadastro_csv,
        target_date=td,
        trades_csv=negocios_csv,
    )
    intra_handoff = handoff_intraday_load_task(intraday_report_path=report)
    summary = {
        "target_date": td.isoformat(),
        "validation": validation,
        "registry_handoff": reg_handoff,
        "intraday_handoff": intra_handoff,
    }
    logger.info("lightweight bootstrap flow completed: %s", summary)
    return summary


@flow(name="daily-scraping-flow")
def daily_scraping_flow(
    target_date: date | None = None,
    run_intraday: bool = True,
    run_cotahist: bool = False,
) -> dict:
    """Combined daily pipeline (manual / legacy): optional intraday and COTAHIST in one flow."""
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
    """Env defaults for **legacy** ``daily_scraping_flow`` / tests only.

    Not used by ``python -m app.etl.orchestration.prefect.serve`` (deployments
    use ``daily_registry_flow`` and ``intraday_quotes_flow`` separately).
    """
    return {
        "run_intraday": os.environ.get("PREFECT_RUN_INTRADAY", "true").lower() in ("1", "true", "yes"),
        "run_cotahist": os.environ.get("PREFECT_RUN_COTAHIST", "false").lower()
        in ("1", "true", "yes"),
    }
