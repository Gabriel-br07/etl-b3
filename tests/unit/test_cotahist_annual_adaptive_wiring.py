"""Unit tests for adaptive wiring in annual COTAHIST pipeline."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

from app.core.constants import ETLStatus
from app.etl.orchestration.pipeline import run_cotahist_annual_pipeline


def _managed_session_with_single_db(mock_db: MagicMock):
    @contextmanager
    def _one_session():
        yield mock_db

    def factory():
        return _one_session()

    return factory


def test_annual_pipeline_propagates_track_in_file_duplicates_flag(tmp_path: Path) -> None:
    txt_path = tmp_path / "COTAHIST_A2024.TXT"
    txt_path.write_text("dummy", encoding="utf-8")
    db = MagicMock()
    db.execute.return_value.scalar_one.return_value = True

    with (
        patch(
            "app.etl.orchestration.pipeline.managed_session",
            side_effect=_managed_session_with_single_db(db),
        ),
        patch("app.etl.orchestration.pipeline.orchestrate_adaptive_cotahist_load") as orchestrate,
    ):
        orchestrate.return_value = {
            "final_metrics": {
                "rows_upsert_ops": 1,
                "normalized_valid": 1,
                "normalized_invalid": 0,
                "in_file_duplicate_keys": 1,
            }
        }
        out = run_cotahist_annual_pipeline(
            txt_path,
            record_audit=False,
            track_in_file_duplicates=True,
        )

    assert out["status"] == ETLStatus.SUCCESS
    assert orchestrate.call_args.kwargs["track_in_file_duplicates"] is True


def test_annual_pipeline_uses_safe_has_dupes_true_in_wiring(tmp_path: Path) -> None:
    txt_path = tmp_path / "COTAHIST_A2024.TXT"
    txt_path.write_text("dummy", encoding="utf-8")
    db = MagicMock()
    db.execute.return_value.scalar_one.return_value = True

    with (
        patch(
            "app.etl.orchestration.pipeline.managed_session",
            side_effect=_managed_session_with_single_db(db),
        ),
        patch("app.etl.orchestration.pipeline.orchestrate_adaptive_cotahist_load") as orchestrate,
    ):
        orchestrate.return_value = {"final_metrics": {"rows_upsert_ops": 0}}
        run_cotahist_annual_pipeline(
            txt_path,
            record_audit=False,
            track_in_file_duplicates=False,
        )

    assert orchestrate.call_args.kwargs["has_dupes"] is True

