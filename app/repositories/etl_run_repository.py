"""Database repository for ETLRun audit records."""

from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.core.constants import ETLStatus
from app.db.models import ETLRun


class ETLRunRepository:
    """Persist ETL execution audit records."""

    def __init__(self, db: Session) -> None:
        self.db = db

    def start_run(self, flow_name: str) -> ETLRun:
        run = ETLRun(
            flow_name=flow_name,
            status=ETLStatus.RUNNING,
            started_at=datetime.now(tz=timezone.utc),
        )
        self.db.add(run)
        self.db.commit()
        self.db.refresh(run)
        return run

    def finish_run(
        self,
        run: ETLRun,
        status: ETLStatus,
        message: str | None = None,
    ) -> ETLRun:
        run.status = status
        run.finished_at = datetime.now(tz=timezone.utc)
        run.message = message
        self.db.commit()
        self.db.refresh(run)
        return run
