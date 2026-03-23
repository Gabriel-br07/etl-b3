"""COTAHIST historical pipeline against PostgreSQL (``-m db`` when DB + schema exist)."""

from __future__ import annotations

import shutil
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.db

FIXTURE = Path(__file__).resolve().parent.parent / "fixtures" / "b3" / "cotahist_minimal.txt"


def _db_or_skip():
    from app.db.engine import engine

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Database not reachable: {exc}")
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT COUNT(*) FROM fact_cotahist_daily"))
            conn.execute(text("SELECT COUNT(*) FROM etl_runs"))
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Required tables missing: {exc}")


def _cotahist_txt_for_years(tmp_path: Path, years: list[int]) -> list[Path]:
    assert FIXTURE.is_file()
    out: list[Path] = []
    for y in years:
        d = tmp_path / str(y)
        d.mkdir(parents=True)
        dst = d / f"COTAHIST_A{y}.TXT"
        shutil.copyfile(FIXTURE, dst)
        out.append(dst)
    return out


def test_historical_single_etl_run_success_message_null(tmp_path: Path) -> None:
    _db_or_skip()
    from app.db.engine import managed_session
    from app.etl.orchestration.pipeline import run_cotahist_historical_pipeline

    paths = _cotahist_txt_for_years(tmp_path, [2000, 2001])

    with managed_session() as db:
        before = db.execute(
            text("SELECT COUNT(*) FROM etl_runs WHERE pipeline_name = :n"),
            {"n": "cotahist_historical"},
        ).scalar_one()

    result = run_cotahist_historical_pipeline(paths, record_audit=True)
    assert result["status"] == "success"

    with managed_session() as db:
        after = db.execute(
            text("SELECT COUNT(*) FROM etl_runs WHERE pipeline_name = :n"),
            {"n": "cotahist_historical"},
        ).scalar_one()
        assert after == before + 1
        row = db.execute(
            text(
                "SELECT status, message, source_file FROM etl_runs "
                "WHERE pipeline_name = :n ORDER BY id DESC LIMIT 1",
            ),
            {"n": "cotahist_historical"},
        ).one()
        assert row[0] == "success"
        assert row[1] is None
        assert row[2] == "cotahist_historical:2000-2001"


def test_historical_idempotent_row_count_and_refreshes_ingested_at(tmp_path: Path) -> None:
    _db_or_skip()
    from app.db.engine import managed_session
    from app.etl.orchestration.pipeline import run_cotahist_historical_pipeline

    paths = _cotahist_txt_for_years(tmp_path, [2000, 2001])

    run_cotahist_historical_pipeline(paths, record_audit=True)
    with managed_session() as db:
        count1 = db.execute(text("SELECT COUNT(*) FROM fact_cotahist_daily")).scalar_one()
        t1 = db.execute(
            text(
                "SELECT ingested_at FROM fact_cotahist_daily "
                "WHERE trade_date = :trade_date AND codneg = :codneg "
                "ORDER BY ingested_at DESC LIMIT 1"
            ),
            {"trade_date": "2024-01-02", "codneg": "PETR4"},
        ).scalar_one()

    run_cotahist_historical_pipeline(paths, record_audit=True)
    with managed_session() as db:
        count2 = db.execute(text("SELECT COUNT(*) FROM fact_cotahist_daily")).scalar_one()
        t2 = db.execute(
            text(
                "SELECT ingested_at FROM fact_cotahist_daily "
                "WHERE trade_date = :trade_date AND codneg = :codneg "
                "ORDER BY ingested_at DESC LIMIT 1"
            ),
            {"trade_date": "2024-01-02", "codneg": "PETR4"},
        ).scalar_one()

    assert count1 == count2
    assert count1 >= 1
    assert t2 >= t1


def test_historical_last_file_in_window_wins_source_file_name(tmp_path: Path) -> None:
    _db_or_skip()
    from app.db.engine import managed_session
    from app.etl.orchestration.pipeline import run_cotahist_historical_pipeline

    paths = _cotahist_txt_for_years(tmp_path, [2000, 2001])

    # Ensure there are no pre-existing rows for these source files so we only
    # assert against data ingested by this test run.
    with managed_session() as db:
        db.execute(
            text(
                "DELETE FROM fact_cotahist_daily "
                "WHERE source_file_name IN (:f1, :f2)"
            ),
            {"f1": "COTAHIST_A2000.TXT", "f2": "COTAHIST_A2001.TXT"},
        )

    run_cotahist_historical_pipeline(paths, record_audit=False)

    with managed_session() as db:
        src = db.execute(
            text(
                "SELECT source_file_name FROM fact_cotahist_daily "
                "WHERE source_file_name IN (:f1, :f2) "
                "ORDER BY source_file_name DESC "
                "LIMIT 1"
            ),
            {"f1": "COTAHIST_A2000.TXT", "f2": "COTAHIST_A2001.TXT"},
        ).scalar_one()

    assert src == "COTAHIST_A2001.TXT"


def test_historical_failure_marks_single_run_failed_with_message(tmp_path: Path) -> None:
    _db_or_skip()
    from app.db.engine import managed_session
    from app.etl.orchestration.pipeline import run_cotahist_historical_pipeline
    from app.use_cases.quotes.cotahist_annual_ingestion import CotahistIngestSummary

    paths = _cotahist_txt_for_years(tmp_path, [2000, 2001, 2002])

    calls = 0

    def boom_on_second(db, txt_path, **kwargs):
        nonlocal calls
        calls += 1
        if calls >= 2:
            raise RuntimeError("injected_db_failure")
        return CotahistIngestSummary(db_upsert_operations=1)

    with managed_session() as db:
        before = db.execute(
            text("SELECT COUNT(*) FROM etl_runs WHERE pipeline_name = :n"),
            {"n": "cotahist_historical"},
        ).scalar_one()

    with patch(
        "app.etl.orchestration.pipeline.ingest_cotahist_txt_file",
        side_effect=boom_on_second,
    ):
        out = run_cotahist_historical_pipeline(paths, record_audit=True)

    assert out["status"] == "failed"
    assert "injected_db_failure" in out["error"]
    assert calls == 2

    with managed_session() as db:
        after = db.execute(
            text("SELECT COUNT(*) FROM etl_runs WHERE pipeline_name = :n"),
            {"n": "cotahist_historical"},
        ).scalar_one()
        assert after == before + 1
        row = db.execute(
            text(
                "SELECT status, message FROM etl_runs "
                "WHERE pipeline_name = :n ORDER BY id DESC LIMIT 1",
            ),
            {"n": "cotahist_historical"},
        ).one()
        assert row[0] == "failed"
        assert row[1] == "injected_db_failure"


def test_historical_partial_success_keeps_first_window_data(tmp_path: Path) -> None:
    _db_or_skip()
    from app.db.engine import managed_session
    from app.etl.orchestration.pipeline import run_cotahist_historical_pipeline
    from app.use_cases.quotes.cotahist_annual_ingestion import ingest_cotahist_txt_file as real_ingest

    paths = _cotahist_txt_for_years(tmp_path, [2000, 2001, 2002])
    calls = 0

    def fail_on_third(db, txt_path, **kwargs):
        nonlocal calls
        calls += 1
        if calls >= 3:
            raise RuntimeError("stop_after_two_files")
        return real_ingest(db, txt_path, **kwargs)

    with patch(
        "app.etl.orchestration.pipeline.ingest_cotahist_txt_file",
        side_effect=fail_on_third,
    ):
        run_cotahist_historical_pipeline(paths, record_audit=False)

    with managed_session() as db:
        n = db.execute(text("SELECT COUNT(*) FROM fact_cotahist_daily")).scalar_one()
    assert n >= 1
