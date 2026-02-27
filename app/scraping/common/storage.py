"""Filesystem helpers for deterministic output paths.

All scrapers should obtain their output directories through this module
so that path-building logic is centralised and testable.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path


def daily_output_dir(base_dir: str | Path, site: str, target_date: date) -> Path:
    """Return (and create) the output directory for a specific site and date.

    Structure: ``<base_dir>/<site>/boletim_diario/<YYYY-MM-DD>/``

    Args:
        base_dir: Root directory (e.g. ``data/raw``).
        site: Site identifier (e.g. ``b3``).
        target_date: Business date.

    Returns:
        The resolved, existing :class:`Path`.
    """
    path = Path(base_dir) / site / "boletim_diario" / target_date.strftime("%Y-%m-%d")
    path.mkdir(parents=True, exist_ok=True)
    return path


def screenshots_dir(base_dir: str | Path, site: str) -> Path:
    """Return (and create) the screenshots directory for *site*."""
    path = Path(base_dir) / site
    path.mkdir(parents=True, exist_ok=True)
    return path


def traces_dir(base_dir: str | Path, site: str) -> Path:
    """Return (and create) the trace-recording directory for *site*."""
    path = Path(base_dir) / site
    path.mkdir(parents=True, exist_ok=True)
    return path
