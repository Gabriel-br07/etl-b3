"""Unit tests: COTAHIST historical pipeline audit and stop-on-failure (mocked DB)."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.core.constants import ETLStatus
from app.etl.orchestration.pipeline import run_cotahist_historical_pipeline
from app.use_cases.quotes.cotahist_annual_ingestion import CotahistIngestSummary


def _make_managed_session_factory(mock_dbs: list[MagicMock]):
    idx = 0

    @contextmanager
    def _one_session():
        nonlocal idx
        db = mock_dbs[idx]
        idx += 1
        yield db

    def factory():
        return _one_session()

    return factory


def test_historical_pipeline_success_single_audit_and_finish_with_null_message() -> None:
    p0 = Path("a/2000/COTAHIST_A2000.TXT")
    p1 = Path("b/2001/COTAHIST_A2001.TXT")
    mock_dbs = [MagicMock() for _ in range(4)]
    run_obj = MagicMock()
    run_obj.id = 42

    ingest_calls: list[Path] = []

    def fake_ingest(db, txt_path, **kwargs):
        ingest_calls.append(Path(txt_path))
        assert kwargs.get("batch_size") == 20_000
        assert kwargs.get("progress_heartbeat") is False
        return CotahistIngestSummary(db_upsert_operations=3)

    with (
        patch(
            "app.etl.orchestration.pipeline.managed_session",
            side_effect=_make_managed_session_factory(mock_dbs),
        ),
        patch("app.etl.orchestration.pipeline.ETLRunRepository") as RepoCls,
        patch("app.etl.orchestration.pipeline.ingest_cotahist_txt_file", side_effect=fake_ingest),
    ):
        repo_inst = RepoCls.return_value
        repo_inst.start_run.return_value = run_obj
        repo_inst.get_by_id.return_value = run_obj

        out = run_cotahist_historical_pipeline([p0, p1], record_audit=True)

    assert out["status"] == ETLStatus.SUCCESS
    assert out["rows_upsert_ops"] == 6
    assert out["files"] == 2
    assert out["windows"] == 1
    assert ingest_calls == [p0, p1]

    repo_inst.start_run.assert_called_once()
    assert repo_inst.start_run.call_args.kwargs["source_file"] == "cotahist_historical:2000-2001"
    assert repo_inst.start_run.call_args.args[0] == "cotahist_historical"
    repo_inst.finish_run.assert_called_once()
    fin = repo_inst.finish_run.call_args
    assert fin.args[1] == ETLStatus.SUCCESS
    assert fin.kwargs.get("message") is None
    assert fin.kwargs.get("rows_inserted") == 6


def test_historical_pipeline_failure_stops_and_marks_failed_with_message() -> None:
    paths = [
        Path("a/2000/COTAHIST_A2000.TXT"),
        Path("b/2001/COTAHIST_A2001.TXT"),
        Path("c/2002/COTAHIST_A2002.TXT"),
        Path("d/2003/COTAHIST_A2003.TXT"),
    ]
    mock_dbs = [MagicMock() for _ in range(8)]
    run_obj = MagicMock()
    run_obj.id = 7
    calls = 0

    def fake_ingest(db, txt_path, **kwargs):
        nonlocal calls
        calls += 1
        if calls >= 3:
            raise RuntimeError("boom")
        return CotahistIngestSummary(db_upsert_operations=1)

    with (
        patch(
            "app.etl.orchestration.pipeline.managed_session",
            side_effect=_make_managed_session_factory(mock_dbs),
        ),
        patch("app.etl.orchestration.pipeline.ETLRunRepository") as RepoCls,
        patch("app.etl.orchestration.pipeline.ingest_cotahist_txt_file", side_effect=fake_ingest),
    ):
        repo_inst = RepoCls.return_value
        repo_inst.start_run.return_value = run_obj
        repo_inst.get_by_id.return_value = run_obj

        out = run_cotahist_historical_pipeline(paths, record_audit=True)

    assert out["status"] == ETLStatus.FAILED
    assert "boom" in out["error"]
    assert calls == 3
    repo_inst.finish_run.assert_called_once()
    fin = repo_inst.finish_run.call_args
    assert fin.args[1] == ETLStatus.FAILED
    assert fin.kwargs.get("message") == "boom"


def test_historical_pipeline_record_audit_false_skips_repository() -> None:
    p0 = Path("a/2000/COTAHIST_A2000.TXT")
    mock_dbs = [MagicMock()]
    with (
        patch(
            "app.etl.orchestration.pipeline.managed_session",
            side_effect=_make_managed_session_factory(mock_dbs),
        ),
        patch("app.etl.orchestration.pipeline.ETLRunRepository") as RepoCls,
        patch(
            "app.etl.orchestration.pipeline.ingest_cotahist_txt_file",
            return_value=CotahistIngestSummary(db_upsert_operations=0),
        ),
    ):
        out = run_cotahist_historical_pipeline([p0], record_audit=False)
    assert out["status"] == ETLStatus.SUCCESS
    RepoCls.assert_not_called()


@pytest.mark.parametrize("record_audit", [True, False])
def test_historical_pipeline_empty_paths_no_ingest_no_crash(record_audit: bool) -> None:
    with (
        patch("app.etl.orchestration.pipeline.managed_session") as ms,
        patch("app.etl.orchestration.pipeline.ingest_cotahist_txt_file") as ingest,
        patch("app.etl.orchestration.pipeline.ETLRunRepository") as RepoCls,
    ):
        out = run_cotahist_historical_pipeline([], record_audit=record_audit)
    assert out["status"] == ETLStatus.SUCCESS
    assert out["rows_upsert_ops"] == 0
    ms.assert_not_called()
    ingest.assert_not_called()
    if record_audit:
        RepoCls.assert_not_called()
