"""Pyramid tip: full COTAHIST historical pipeline with real DB (``-m db``)."""

from __future__ import annotations

import shutil
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.db

FIXTURE = Path(__file__).resolve().parent.parent / "fixtures" / "b3" / "cotahist_minimal.txt"


def _require_db_tables() -> None:
    from app.db.engine import engine

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT COUNT(*) FROM fact_cotahist_daily"))
            conn.execute(text("SELECT COUNT(*) FROM etl_runs"))
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Database or schema unavailable: {exc}")


def _year_files(tmp_path: Path, years: list[int]) -> list[Path]:
    paths = []
    for y in years:
        d = tmp_path / str(y)
        d.mkdir(parents=True)
        p = d / f"COTAHIST_A{y}.TXT"
        shutil.copyfile(FIXTURE, p)
        paths.append(p)
    return paths


def test_tip_happy_path_four_years_two_windows_single_audit(tmp_path: Path) -> None:
    _require_db_tables()
    from app.db.engine import managed_session
    from app.etl.orchestration.pipeline import run_cotahist_historical_pipeline

    paths = _year_files(tmp_path, [2000, 2001, 2002, 2003])

    with managed_session() as db:
        before = db.execute(
            text("SELECT COUNT(*) FROM etl_runs WHERE pipeline_name = :n"),
            {"n": "cotahist_historical"},
        ).scalar_one()

    out = run_cotahist_historical_pipeline(paths, record_audit=True)

    assert out["status"] == "success"
    assert out["files"] == 4
    assert out["windows"] == 2
    assert out["rows_upsert_ops"] >= 1

    with managed_session() as db:
        after = db.execute(
            text("SELECT COUNT(*) FROM etl_runs WHERE pipeline_name = :n"),
            {"n": "cotahist_historical"},
        ).scalar_one()
        assert after == before + 1


def test_tip_failure_after_first_window_marks_audit_failed(tmp_path: Path) -> None:
    _require_db_tables()
    from app.db.engine import managed_session
    from app.etl.orchestration.pipeline import run_cotahist_historical_pipeline
    from app.use_cases.quotes.cotahist_annual_ingestion import ingest_cotahist_txt_file as real_ingest

    paths = _year_files(tmp_path, [2000, 2001, 2002, 2003])
    n = 0

    def fail_when_2002(db, txt_path, **kwargs):
        nonlocal n
        n += 1
        if "2002" in str(txt_path):
            raise RuntimeError("tip_injected_failure")
        return real_ingest(db, txt_path, **kwargs)

    with managed_session() as db:
        before = db.execute(
            text("SELECT COUNT(*) FROM etl_runs WHERE pipeline_name = :n"),
            {"n": "cotahist_historical"},
        ).scalar_one()

    with patch("app.etl.orchestration.pipeline.ingest_cotahist_txt_file", side_effect=fail_when_2002):
        out = run_cotahist_historical_pipeline(paths, record_audit=True)

    assert out["status"] == "failed"
    assert "tip_injected_failure" in out["error"]

    with managed_session() as db:
        after = db.execute(
            text("SELECT COUNT(*) FROM etl_runs WHERE pipeline_name = :n"),
            {"n": "cotahist_historical"},
        ).scalar_one()
        assert after == before + 1
        status, msg = db.execute(
            text(
                "SELECT status, message FROM etl_runs "
                "WHERE pipeline_name = :n ORDER BY id DESC LIMIT 1",
            ),
            {"n": "cotahist_historical"},
        ).one()
        assert status == "failed"
        assert msg == "tip_injected_failure"
