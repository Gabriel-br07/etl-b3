"""Unit tests for ETLRunRepository - uses PostgreSQL-compatible patterns,
verified against an in-process mock session to avoid needing a real DB.
"""
from __future__ import annotations
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch, call
import pytest
from app.core.constants import ETLStatus
from app.db.models import ETLRun
from app.repositories.etl_run_repository import ETLRunRepository


@pytest.fixture()
def db():
    """Return a mock session that records interactions."""
    session = MagicMock()
    # Make refresh() copy run state (simulate DB returning the object)
    session.refresh.side_effect = lambda obj: None
    return session


# ---------------------------------------------------------------------------
# start_run
# ---------------------------------------------------------------------------


def test_start_run_adds_and_does_not_commit(db):
    repo = ETLRunRepository(db)
    run = repo.start_run("my_pipeline")
    db.add.assert_called_once()
    # Repository no longer commits; caller owns the transaction
    assert not db.commit.called
    # refresh isn't required by the repo
    assert not db.refresh.called


def test_start_run_sets_pipeline_name(db):
    repo = ETLRunRepository(db)
    captured = []
    db.add.side_effect = lambda obj: captured.append(obj)
    repo.start_run("instruments_and_trades")
    assert captured[0].pipeline_name == "instruments_and_trades"
    assert captured[0].flow_name == "instruments_and_trades"


def test_start_run_sets_running_status(db):
    repo = ETLRunRepository(db)
    captured = []
    db.add.side_effect = lambda obj: captured.append(obj)
    repo.start_run("test")
    assert captured[0].status == ETLStatus.RUNNING


def test_start_run_sets_started_at(db):
    repo = ETLRunRepository(db)
    before = datetime.now(tz=timezone.utc)
    captured = []
    db.add.side_effect = lambda obj: captured.append(obj)
    repo.start_run("test")
    after = datetime.now(tz=timezone.utc)
    assert before <= captured[0].started_at <= after


def test_start_run_with_source_info(db):
    repo = ETLRunRepository(db)
    captured = []
    db.add.side_effect = lambda obj: captured.append(obj)
    repo.start_run(
        "instruments_and_trades",
        source_file="/data/instruments.csv",
        source_date=date(2024, 6, 14),
    )
    assert captured[0].source_file == "/data/instruments.csv"
    assert captured[0].source_date == date(2024, 6, 14)


def test_start_run_finished_at_is_none(db):
    repo = ETLRunRepository(db)
    captured = []
    db.add.side_effect = lambda obj: captured.append(obj)
    repo.start_run("test")
    assert captured[0].finished_at is None


# ---------------------------------------------------------------------------
# finish_run
# ---------------------------------------------------------------------------


def _make_run() -> ETLRun:
    run = ETLRun(
        pipeline_name="test",
        flow_name="test",
        status=ETLStatus.RUNNING,
        started_at=datetime.now(tz=timezone.utc),
    )
    return run


def test_finish_run_sets_success(db):
    repo = ETLRunRepository(db)
    run = _make_run()
    repo.finish_run(run, ETLStatus.SUCCESS)
    assert run.status == ETLStatus.SUCCESS
    assert run.finished_at is not None
    # Repository does not commit; caller is responsible
    assert not db.commit.called


def test_finish_run_sets_rows_inserted(db):
    repo = ETLRunRepository(db)
    run = _make_run()
    repo.finish_run(run, ETLStatus.SUCCESS, rows_inserted=100, rows_failed=0)
    assert run.rows_inserted == 100
    assert run.rows_failed == 0


def test_finish_run_failure_with_message(db):
    repo = ETLRunRepository(db)
    run = _make_run()
    repo.finish_run(run, ETLStatus.FAILED, message="oops", rows_inserted=0, rows_failed=5)
    assert run.status == ETLStatus.FAILED
    assert run.message == "oops"
    assert run.rows_failed == 5


def test_finish_run_without_optional_fields(db):
    repo = ETLRunRepository(db)
    run = _make_run()
    repo.finish_run(run, ETLStatus.SUCCESS)
    assert run.rows_inserted is None
    assert run.rows_failed is None
    assert run.message is None

