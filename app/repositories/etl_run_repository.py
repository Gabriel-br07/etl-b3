"""Database repository for ETLRun audit records."""

from __future__ import annotations

from datetime import date, datetime, timezone

from sqlalchemy.orm import Session

from app.core.constants import ETLStatus
from app.db.models import ETLRun


class ETLRunRepository:
    """Persist ETL execution audit records.

    Note: this repository no longer commits the session. The caller (for
    example `managed_session`) is responsible for committing or rolling back
    to provide atomicity across multiple repository calls.
    """

    def __init__(self, db: Session) -> None:
        self.db = db

    def start_run(
        self,
        pipeline_name: str,
        *,
        source_file: str | None = None,
        source_date: date | None = None,
    ) -> ETLRun:
        """Create and persist an ETL run record with RUNNING status.

        The caller must commit the session to persist the row to the DB.
        """
        run = ETLRun(
            pipeline_name=pipeline_name,
            flow_name=pipeline_name,  # kept for backward compat
            status=ETLStatus.RUNNING,
            started_at=datetime.now(tz=timezone.utc),
            source_file=source_file,
            source_date=source_date,
        )
        self.db.add(run)
        # Caller commits to allow atomic ETL transactions
        return run

    def finish_run(
        self,
        run: ETLRun,
        status: ETLStatus,
        message: str | None = None,
        *,
        rows_inserted: int | None = None,
        rows_failed: int | None = None,
    ) -> ETLRun:
        """Update a run record with final status, timing, and row counts.

        The caller must commit the session to persist the changes.
        """
        run.status = status
        run.finished_at = datetime.now(tz=timezone.utc)
        run.message = message
        if rows_inserted is not None:
            run.rows_inserted = rows_inserted
        if rows_failed is not None:
            run.rows_failed = rows_failed
        return run
