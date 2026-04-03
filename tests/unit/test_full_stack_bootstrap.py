"""Tests for marker-gated full_stack_bootstrap wrapper."""

from __future__ import annotations

from subprocess import CompletedProcess
from unittest.mock import patch

from app.etl.orchestration.prefect import full_stack_bootstrap as fsb


def test_main_skips_when_run_disabled(monkeypatch, tmp_path):
    monkeypatch.setenv("RUN_FULL_STACK_BOOTSTRAP", "false")
    assert fsb.main() == 0


def test_main_skips_when_marker_exists(monkeypatch, tmp_path):
    marker = tmp_path / ".bootstrap_full_v1.done"
    marker.write_text("x", encoding="utf-8")
    monkeypatch.setenv("STACK_FULL_BOOTSTRAP_MARKER", str(marker))
    with patch.object(fsb.subprocess, "run") as mock_run:
        assert fsb.main() == 0
    mock_run.assert_not_called()


def test_main_runs_worker_and_writes_marker(monkeypatch, tmp_path):
    marker = tmp_path / ".bootstrap_full_v1.done"
    monkeypatch.setenv("STACK_FULL_BOOTSTRAP_MARKER", str(marker))
    worker = tmp_path / "cotahist_worker.py"
    worker.write_text("# stub\n", encoding="utf-8")
    monkeypatch.setenv("COTAHIST_WORKER_SCRIPT", str(worker))
    monkeypatch.setenv("APP_ROOT", str(tmp_path))

    with patch.object(
        fsb.subprocess,
        "run",
        return_value=CompletedProcess(["py", str(worker)], 0),
    ) as mock_run:
        assert fsb.main() == 0
    mock_run.assert_called_once()
    assert marker.is_file()


def test_main_force_reruns_when_marker_exists(monkeypatch, tmp_path):
    monkeypatch.setenv("FORCE_FULL_STACK_BOOTSTRAP", "true")
    marker = tmp_path / ".bootstrap_full_v1.done"
    marker.write_text("old", encoding="utf-8")
    monkeypatch.setenv("STACK_FULL_BOOTSTRAP_MARKER", str(marker))
    worker = tmp_path / "cotahist_worker.py"
    worker.write_text("# stub\n", encoding="utf-8")
    monkeypatch.setenv("COTAHIST_WORKER_SCRIPT", str(worker))
    monkeypatch.setenv("APP_ROOT", str(tmp_path))

    with patch.object(
        fsb.subprocess,
        "run",
        return_value=CompletedProcess(["py", str(worker)], 0),
    ):
        assert fsb.main() == 0


def test_main_returns_worker_exit_code(monkeypatch, tmp_path):
    marker = tmp_path / ".bootstrap_full_v1.done"
    monkeypatch.setenv("STACK_FULL_BOOTSTRAP_MARKER", str(marker))
    worker = tmp_path / "cotahist_worker.py"
    worker.write_text("# stub\n", encoding="utf-8")
    monkeypatch.setenv("COTAHIST_WORKER_SCRIPT", str(worker))
    monkeypatch.setenv("APP_ROOT", str(tmp_path))

    with patch.object(
        fsb.subprocess,
        "run",
        return_value=CompletedProcess(["py", str(worker)], 3),
    ):
        assert fsb.main() == 3
    assert not marker.exists()
