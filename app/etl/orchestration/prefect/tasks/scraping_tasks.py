"""Prefect tasks for scraper execution."""

from __future__ import annotations

import logging
import time
from datetime import date
from pathlib import Path

from prefect import get_run_logger, task
from prefect.exceptions import MissingContextError
from prefect.runtime import task_run
from sqlalchemy.exc import InterfaceError, OperationalError

from app.core.config import settings
from app.core.logging import get_logger
from app.integrations.b3.cotahist_downloader import download_cotahist_zip, extract_cotahist_txt
from app.scraping.b3.scraper import BoletimDiarioScraper
from app.scraping.b3.scraper_negocios import NegociosConsolidadosScraper
from app.use_cases.quotes.batch_ingestion import run_batch_quote_ingestion

from app.etl.orchestration.prefect.tasks.audit_tasks import finish_scraper_audit, start_scraper_audit


def _current_retry_count() -> int:
    attempt = getattr(task_run, "run_count", 1) or 1
    return max(0, int(attempt) - 1)


def _run_logger_or_app() -> logging.Logger | logging.LoggerAdapter[logging.Logger]:
    """Prefect run logger when inside a flow/task; std logger for `.fn()` / CLI."""
    try:
        return get_run_logger()
    except MissingContextError:
        return get_logger(__name__)


def _is_audit_db_unavailable(exc: BaseException) -> bool:
    """True when scraper audit rows cannot be persisted (e.g. DB down)."""
    return isinstance(exc, (OperationalError, InterfaceError))


@task(name="scrape-cadastro", retries=2, retry_delay_seconds=[30, 60])
def scrape_cadastro_task(target_date: date) -> Path:
    logger = get_run_logger()
    retry_count = _current_retry_count()
    audit_id = start_scraper_audit(
        scraper_name="cadastro_instrumentos",
        target_date=target_date,
        retry_count=retry_count,
        status="retrying" if retry_count > 0 else "running",
    )
    try:
        result = BoletimDiarioScraper().scrape(target_date)[0]
        out = result.normalized_file_path or result.file_path
        finish_scraper_audit(
            audit_id=audit_id,
            status="success",
            retry_count=retry_count,
            output_path=str(out),
            output_file_name=out.name,
        )
        logger.info("cadastro scrape done: %s", out)
        return out
    except Exception as exc:  # noqa: BLE001
        finish_scraper_audit(
            audit_id=audit_id,
            status="failed",
            retry_count=retry_count,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        raise


@task(name="scrape-negocios", retries=2, retry_delay_seconds=[30, 60])
def scrape_negocios_task(target_date: date) -> Path:
    logger = get_run_logger()
    retry_count = _current_retry_count()
    audit_id = start_scraper_audit(
        scraper_name="negocios_consolidados",
        target_date=target_date,
        retry_count=retry_count,
        status="retrying" if retry_count > 0 else "running",
    )
    try:
        result = NegociosConsolidadosScraper().scrape(target_date)[0]
        out = result.normalized_file_path or result.file_path
        finish_scraper_audit(
            audit_id=audit_id,
            status="success",
            retry_count=retry_count,
            output_path=str(out),
            output_file_name=out.name,
        )
        logger.info("negocios scrape done: %s", out)
        return out
    except Exception as exc:  # noqa: BLE001
        finish_scraper_audit(
            audit_id=audit_id,
            status="failed",
            retry_count=retry_count,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        raise


@task(name="scrape-cotahist", retries=1, retry_delay_seconds=120)
def scrape_cotahist_task(target_date: date, enabled: bool = True) -> list[Path] | None:
    """Download and extract annual COTAHIST ZIPs for each year in configured range.

    Years default to ``b3_cotahist_year_start`` … ``b3_cotahist_year_end`` (e.g. 1986–2026).
    Missing years (404) are logged and skipped; at least one successful year is required.
    """
    log = get_logger(__name__)
    retry_count = _current_retry_count()
    audit_id = start_scraper_audit(
        scraper_name="cotahist",
        target_date=target_date,
        retry_count=retry_count,
        status="retrying" if retry_count > 0 else "running",
    )
    if not enabled:
        finish_scraper_audit(audit_id=audit_id, status="skipped", retry_count=retry_count)
        return None
    try:
        lo = int(settings.b3_cotahist_year_start)
        hi = int(settings.b3_cotahist_year_end)
        if lo > hi:
            lo, hi = hi, lo
        txt_paths: list[Path] = []
        skipped_years: list[int] = []
        for i, year in enumerate(range(lo, hi + 1)):
            if i > 0 and settings.b3_cotahist_delay_between_years > 0:
                time.sleep(float(settings.b3_cotahist_delay_between_years))
            year_dir = Path(settings.b3_cotahist_annual_dir) / str(year)
            zip_path = year_dir / f"COTAHIST_A{year}.zip"
            try:
                download_cotahist_zip(year, zip_path)
                txt_path = extract_cotahist_txt(zip_path, year_dir)
                txt_paths.append(txt_path)
                log.info("cotahist annual fetch done year=%s path=%s", year, txt_path)
            except FileNotFoundError as exc:
                log.warning("cotahist year=%s not available (skipped): %s", year, exc)
                skipped_years.append(year)
            except Exception:
                raise

        if not txt_paths:
            msg = (
                f"No COTAHIST files downloaded for years {lo}-{hi} "
                f"(all missing or unreachable; skipped={skipped_years})"
            )
            raise RuntimeError(msg)

        finish_scraper_audit(
            audit_id=audit_id,
            status="success",
            retry_count=retry_count,
            output_path=str(txt_paths[-1]),
            output_file_name=txt_paths[-1].name,
            metadata_json={
                "year_range": [lo, hi],
                "paths": [str(p) for p in txt_paths],
                "years_ok": [int(p.parent.name) for p in txt_paths],
                "years_skipped_404": skipped_years,
            },
        )
        return txt_paths
    except Exception as exc:  # noqa: BLE001
        finish_scraper_audit(
            audit_id=audit_id,
            status="failed",
            retry_count=retry_count,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        raise


@task(name="run-intraday-quote-batch", retries=2, retry_delay_seconds=[30, 90])
def run_intraday_quote_batch_task(
    *,
    instruments_csv: Path,
    target_date: date,
    trades_csv: Path | None = None,
    filter_mode: str = "strict",
    output_dir: str | None = None,
) -> Path:
    """Execute quote batch use-case and return generated report path."""
    logger = _run_logger_or_app()
    retry_count = _current_retry_count()
    audit_id: int | None = None
    try:
        audit_id = start_scraper_audit(
            scraper_name="daily_fluctuation_history",
            target_date=target_date,
            retry_count=retry_count,
            status="retrying" if retry_count > 0 else "running",
        )
    except Exception as audit_exc:  # noqa: BLE001
        if _is_audit_db_unavailable(audit_exc):
            logger.warning(
                "scraper audit unavailable (DB); running quote batch without audit: %s",
                audit_exc,
            )
            report = run_batch_quote_ingestion(
                instruments_csv=instruments_csv,
                trades_csv=trades_csv,
                filter_mode=filter_mode,
                reference_date=target_date,
                output_dir=output_dir,
            )
            logger.info("intraday quote batch done (no audit): report=%s", report)
            return report
        raise

    try:
        report = run_batch_quote_ingestion(
            instruments_csv=instruments_csv,
            trades_csv=trades_csv,
            filter_mode=filter_mode,
            reference_date=target_date,
            output_dir=output_dir,
        )
        finish_scraper_audit(
            audit_id=audit_id,
            status="success",
            retry_count=retry_count,
            output_path=str(report),
            output_file_name=report.name,
        )
        logger.info("intraday quote batch done: report=%s", report)
        return report
    except Exception as exc:  # noqa: BLE001
        try:
            finish_scraper_audit(
                audit_id=audit_id,
                status="failed",
                retry_count=retry_count,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
        except Exception as audit_exc:  # noqa: BLE001
            logger.warning(
                "failed to persist scraper audit failure for audit_id=%s after quote batch error %s: %s",
                audit_id,
                type(exc).__name__,
                audit_exc,
            )
        raise
