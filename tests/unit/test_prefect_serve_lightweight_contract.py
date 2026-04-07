"""Ensure default Prefect serve path stays lightweight (no heavy-flow imports)."""

from __future__ import annotations

from pathlib import Path


def test_serve_module_does_not_register_cotahist_flow():
    root = Path(__file__).resolve().parents[2]
    text = (root / "app" / "etl" / "orchestration" / "prefect" / "serve.py").read_text(encoding="utf-8")
    assert "cotahist_flow" not in text
    assert "daily_registry_flow" in text
    assert "intraday_quotes_flow" in text
