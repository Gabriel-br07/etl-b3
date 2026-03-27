"""Task-level audit helpers for scraper execution records."""

from __future__ import annotations

from datetime import date

from prefect.runtime import flow_run, task_run

from app.core.logging import get_logger
from app.db.engine import managed_session
from app.repositories.scraper_run_audit_repository import ScraperRunAuditRepository

logger = get_logger(__name__)


def _flow_task_ids() -> tuple[str | None, str | None]:
    flow_id = getattr(flow_run, "id", None)
    task_id = getattr(task_run, "id", None)
    return (str(flow_id) if flow_id else None, str(task_id) if task_id else None)


def start_scraper_audit(
    *,
    scraper_name: str,
    target_date: date | None,
    retry_count: int,
    status: str = "running",
    metadata_json: dict | None = None,
) -> int:
    """Insert audit-start row and return row id."""
    flow_run_id, task_run_id = _flow_task_ids()
    with managed_session() as db:
        repo = ScraperRunAuditRepository(db)
        row = repo.start(
            scraper_name=scraper_name,
            target_date=target_date,
            flow_run_id=flow_run_id,
            task_run_id=task_run_id,
            retry_count=retry_count,
            status=status,
            metadata_json=metadata_json,
        )
        db.flush()
        return int(row.id)


def finish_scraper_audit(
    *,
    audit_id: int,
    status: str,
    retry_count: int,
    output_path: str | None = None,
    output_file_name: str | None = None,
    records_count: int | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
    metadata_json: dict | None = None,
) -> None:
    """Update audit row with final status/details."""
    with managed_session() as db:
        from app.db.models import ScraperRunAudit

        row = db.get(ScraperRunAudit, audit_id)
        if row is None:
            logger.warning(
                "finish_scraper_audit: missing audit row for id=%s status=%s retry_count=%s",
                audit_id,
                status,
                retry_count,
            )
            return
        repo = ScraperRunAuditRepository(db)
        repo.finish(
            row,
            status=status,
            retry_count=retry_count,
            output_path=output_path,
            output_file_name=output_file_name,
            records_count=records_count,
            error_type=error_type,
            error_message=error_message,
            metadata_json=metadata_json,
        )
