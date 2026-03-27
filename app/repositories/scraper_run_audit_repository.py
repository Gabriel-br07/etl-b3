"""Repository for scraper execution audit rows."""

from __future__ import annotations

from datetime import date, datetime, timezone

from sqlalchemy.orm import Session

from app.db.models import ScraperRunAudit


class ScraperRunAuditRepository:
    """Persist and update scraper-run audit records."""

    def __init__(self, db: Session) -> None:
        self.db = db

    def start(
        self,
        *,
        scraper_name: str,
        target_date: date | None,
        flow_run_id: str | None,
        task_run_id: str | None,
        retry_count: int = 0,
        status: str = "running",
        metadata_json: dict | None = None,
    ) -> ScraperRunAudit:
        row = ScraperRunAudit(
            scraper_name=scraper_name,
            target_date=target_date,
            flow_run_id=flow_run_id,
            task_run_id=task_run_id,
            status=status,
            retry_count=retry_count,
            started_at=datetime.now(tz=timezone.utc),
            metadata_json=metadata_json,
        )
        self.db.add(row)
        return row

    def finish(
        self,
        row: ScraperRunAudit,
        *,
        status: str,
        retry_count: int | None = None,
        output_path: str | None = None,
        output_file_name: str | None = None,
        records_count: int | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
        metadata_json: dict | None = None,
    ) -> ScraperRunAudit:
        row.status = status
        row.finished_at = datetime.now(tz=timezone.utc)
        if row.started_at and row.finished_at:
            row.duration_ms = int((row.finished_at - row.started_at).total_seconds() * 1000)
        if retry_count is not None:
            row.retry_count = retry_count
        if output_path is not None:
            row.output_path = output_path
        if output_file_name is not None:
            row.output_file_name = output_file_name
        if records_count is not None:
            row.records_count = records_count
        if error_type is not None:
            row.error_type = error_type
        if error_message is not None:
            row.error_message = error_message
        if metadata_json is not None:
            row.metadata_json = metadata_json
        return row
