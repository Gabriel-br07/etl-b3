from __future__ import annotations

from pathlib import Path

import pytest

from app.etl.orchestration.prefect.tasks.handoff_tasks import _resolve_latest_jsonl_from_report


def test_resolve_jsonl_uses_same_timestamp_as_report(tmp_path: Path):
    day_dir = tmp_path / "2026-03-27"
    day_dir.mkdir(parents=True, exist_ok=True)
    report = day_dir / "report_20260327T120000.csv"
    report.write_text("ticker,status\nPETR4,200\n", encoding="utf-8")

    expected = day_dir / "daily_fluctuation_20260327T120000.jsonl"
    expected.write_text('{"ticker":"PETR4"}\n', encoding="utf-8")
    unrelated_newer = day_dir / "daily_fluctuation_20260327T130000.jsonl"
    unrelated_newer.write_text('{"ticker":"VALE3"}\n', encoding="utf-8")

    resolved = _resolve_latest_jsonl_from_report(report)
    assert resolved == expected


def test_resolve_jsonl_raises_when_report_timestamp_match_missing(tmp_path: Path):
    day_dir = tmp_path / "2026-03-27"
    day_dir.mkdir(parents=True, exist_ok=True)
    report = day_dir / "report_20260327T120000.csv"
    report.write_text("ticker,status\nPETR4,200\n", encoding="utf-8")
    # Unrelated artifact from another run should not be accepted.
    (day_dir / "daily_fluctuation_20260327T130000.jsonl").write_text(
        '{"ticker":"VALE3"}\n', encoding="utf-8"
    )

    with pytest.raises(FileNotFoundError):
        _resolve_latest_jsonl_from_report(report)
