"""Tests for stack bootstrap marker and env toggles."""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

from app.etl.orchestration.prefect import bootstrap as bootstrap_module


def test_main_skips_when_disabled(monkeypatch, tmp_path):
    monkeypatch.setenv("RUN_STACK_BOOTSTRAP", "false")
    assert bootstrap_module.main() == 0


def test_main_skips_when_marker_exists(monkeypatch, tmp_path):
    monkeypatch.setenv("RUN_STACK_BOOTSTRAP", "true")
    marker = tmp_path / ".bootstrap_light_v1.done"
    marker.write_text("done", encoding="utf-8")
    monkeypatch.setenv("STACK_BOOTSTRAP_MARKER", str(marker))
    with patch.object(bootstrap_module, "lightweight_bootstrap_flow") as mock_flow:
        assert bootstrap_module.main() == 0
    mock_flow.assert_not_called()


def test_main_runs_flow_and_writes_marker(monkeypatch, tmp_path):
    monkeypatch.setenv("RUN_STACK_BOOTSTRAP", "true")
    monkeypatch.setenv("SKIP_STACK_BOOTSTRAP_IF_FRESH", "false")
    marker = tmp_path / ".bootstrap_light_v1.done"
    monkeypatch.setenv("STACK_BOOTSTRAP_MARKER", str(marker))
    with patch.object(bootstrap_module, "lightweight_bootstrap_flow") as mock_flow:
        assert bootstrap_module.main() == 0
    mock_flow.assert_called_once()
    assert marker.is_file()


def test_main_skips_flow_when_fresh_cadastro_writes_marker(monkeypatch, tmp_path):
    """Today's cadastro exists → skip Playwright bootstrap but write marker."""
    monkeypatch.setenv("RUN_STACK_BOOTSTRAP", "true")
    monkeypatch.setenv("SKIP_STACK_BOOTSTRAP_IF_FRESH", "true")
    raw = tmp_path / "raw"
    day = date.today()
    day_folder = raw / "b3" / "boletim_diario" / day.isoformat()
    day_folder.mkdir(parents=True)
    compact = day.isoformat().replace("-", "")
    csv_path = day_folder / f"cadastro_instrumentos_{compact}.normalized.csv"
    csv_path.write_text("ticker\nX", encoding="utf-8")
    monkeypatch.setenv("B3_DATA_DIR", str(raw))
    marker = tmp_path / ".bootstrap_light_v1.done"
    monkeypatch.setenv("STACK_BOOTSTRAP_MARKER", str(marker))
    with patch.object(bootstrap_module, "lightweight_bootstrap_flow") as mock_flow:
        assert bootstrap_module.main() == 0
    mock_flow.assert_not_called()
    assert marker.is_file()


def test_main_force_reruns_when_marker_exists(monkeypatch, tmp_path):
    monkeypatch.setenv("RUN_STACK_BOOTSTRAP", "true")
    monkeypatch.setenv("SKIP_STACK_BOOTSTRAP_IF_FRESH", "false")
    monkeypatch.setenv("FORCE_STACK_BOOTSTRAP", "true")
    marker = tmp_path / ".bootstrap_light_v1.done"
    marker.write_text("old", encoding="utf-8")
    monkeypatch.setenv("STACK_BOOTSTRAP_MARKER", str(marker))
    with patch.object(bootstrap_module, "lightweight_bootstrap_flow") as mock_flow:
        assert bootstrap_module.main() == 0
    mock_flow.assert_called_once()


def test_main_returns_one_on_flow_failure(monkeypatch, tmp_path):
    monkeypatch.setenv("RUN_STACK_BOOTSTRAP", "true")
    monkeypatch.setenv("SKIP_STACK_BOOTSTRAP_IF_FRESH", "false")
    marker = tmp_path / ".bootstrap_light_v1.done"
    monkeypatch.setenv("STACK_BOOTSTRAP_MARKER", str(marker))
    with patch.object(
        bootstrap_module,
        "lightweight_bootstrap_flow",
        side_effect=RuntimeError("boom"),
    ):
        assert bootstrap_module.main() == 1
    assert not marker.exists()
