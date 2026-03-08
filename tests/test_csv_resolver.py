from __future__ import annotations

import time
from datetime import date, timedelta
from pathlib import Path

import pytest

from app.etl.orchestration.csv_resolver import (
    resolve_instruments_csv,
    CSVNotFoundError,
)


# Use a fixed date for all tests to avoid flakiness around midnight/CI timezones
FIXED_TODAY = date(2025, 6, 14)


def patch_resolver_today(monkeypatch, fixed: date = FIXED_TODAY) -> None:
    """Replace the `date` used inside the csv_resolver module with a small
    stub whose today() returns a fixed date.

    This ensures resolver logic that depends on date.today() is deterministic
    in tests.
    """

    class _FixedDate:
        @staticmethod
        def today():
            return fixed

    monkeypatch.setattr("app.etl.orchestration.csv_resolver.date", _FixedDate)


def make_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("ok")


def test_exact_path_match(tmp_path: Path, monkeypatch):
    # Ensure the resolver uses a fixed today
    patch_resolver_today(monkeypatch)

    # Create today's folder and exact-named file
    today = FIXED_TODAY
    date_str = today.isoformat()
    compact = date_str.replace("-", "")
    folder = tmp_path / "b3" / "boletim_diario" / date_str
    file = folder / f"cadastro_instrumentos_{compact}.normalized.csv"
    make_file(file)

    # Resolver should find exact file immediately
    resolved = resolve_instruments_csv(data_dir=tmp_path, retry_count=0, retry_delay_seconds=0)
    assert resolved == file


def test_glob_fallback_match(tmp_path: Path, monkeypatch):
    # Ensure deterministic date inside resolver
    patch_resolver_today(monkeypatch)

    # Create today's folder with a file that doesn't match exact name but does match glob
    today = FIXED_TODAY
    date_str = today.isoformat()
    folder = tmp_path / "b3" / "boletim_diario" / date_str
    file1 = folder / "cadastro_instrumentos_extra.normalized.csv"
    file2 = folder / "cadastro_instrumentos_20240101.normalized.csv"
    make_file(file1)
    make_file(file2)

    resolved = resolve_instruments_csv(data_dir=tmp_path, retry_count=0, retry_delay_seconds=0)
    # Should pick the alphabetically last candidate
    assert resolved.name == sorted([file1.name, file2.name])[-1]


def test_yesterday_fallback(tmp_path: Path, monkeypatch):
    # Ensure deterministic date inside resolver
    patch_resolver_today(monkeypatch)

    # Create yesterday's folder with an exact file, today's missing
    today = FIXED_TODAY
    yesterday = today - timedelta(days=1)
    y_str = yesterday.isoformat()
    y_compact = y_str.replace("-", "")

    y_file = tmp_path / "b3" / "boletim_diario" / y_str / f"cadastro_instrumentos_{y_compact}.normalized.csv"
    make_file(y_file)

    resolved = resolve_instruments_csv(data_dir=tmp_path, retry_count=0, retry_delay_seconds=0)
    assert resolved == y_file


def test_not_found_raises_after_retries(tmp_path: Path, monkeypatch):
    # Ensure deterministic date inside resolver
    patch_resolver_today(monkeypatch)

    # No files for today or yesterday -> should raise CSVNotFoundError
    # Monkeypatch time.sleep to avoid delays if resolver attempts retries
    monkeypatch.setattr(time, "sleep", lambda s: None)

    with pytest.raises(CSVNotFoundError):
        resolve_instruments_csv(data_dir=tmp_path, retry_count=1, retry_delay_seconds=0)
