"""Prefect flow for annual/historical COTAHIST loading."""

from __future__ import annotations

from pathlib import Path

from prefect import flow

from app.etl.orchestration.pipeline import run_cotahist_annual_pipeline, run_cotahist_historical_pipeline


@flow(name="cotahist-flow")
def cotahist_flow(paths: list[str]) -> dict:
    """Load one or many cotahist TXT files via existing pipeline functions."""
    txt_paths = [Path(p).resolve() for p in paths]
    if not txt_paths:
        return {"status": "skipped", "reason": "no files"}
    if len(txt_paths) == 1:
        return run_cotahist_annual_pipeline(txt_paths[0], record_audit=True)
    return run_cotahist_historical_pipeline(txt_paths, record_audit=True)
