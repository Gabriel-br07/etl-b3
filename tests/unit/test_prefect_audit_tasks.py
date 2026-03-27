from __future__ import annotations

import logging
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from app.etl.orchestration.prefect.tasks.audit_tasks import finish_scraper_audit


@contextmanager
def _fake_session(db):
    yield db


def test_finish_scraper_audit_logs_when_row_missing(caplog):
    db = MagicMock()
    db.get.return_value = None
    with patch(
        "app.etl.orchestration.prefect.tasks.audit_tasks.managed_session",
        return_value=_fake_session(db),
    ):
        with caplog.at_level(logging.WARNING):
            finish_scraper_audit(audit_id=999, status="success", retry_count=0)
    assert "missing audit row" in caplog.text.lower()
