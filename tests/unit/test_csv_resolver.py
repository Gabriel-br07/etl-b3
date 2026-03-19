"""Unit tests for instruments CSV discovery (deterministic dates)."""

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.etl.orchestration.csv_resolver import (
    CSVNotFoundError,
    find_csv_for_date,
    resolve_instruments_csv,
)


def test_find_csv_for_date_exact_match(tmp_path: Path):
    target = date(2025, 6, 14)
    folder = tmp_path / "b3" / "boletim_diario" / target.isoformat()
    folder.mkdir(parents=True)
    expected = folder / "cadastro_instrumentos_20250614.normalized.csv"
    expected.write_text("ok", encoding="utf-8")
    resolved = find_csv_for_date(tmp_path, target)
    assert resolved == expected


def test_find_csv_for_date_glob_fallback(tmp_path: Path):
    target = date(2025, 6, 14)
    folder = tmp_path / "b3" / "boletim_diario" / target.isoformat()
    folder.mkdir(parents=True)
    alt = folder / "cadastro_instrumentos_extra.normalized.csv"
    alt.write_text("ok", encoding="utf-8")
    resolved = find_csv_for_date(tmp_path, target)
    assert resolved == alt


def test_resolve_instruments_csv_prefers_today(monkeypatch, tmp_path: Path):
    fixed = date(2025, 6, 14)

    class _Date:
        @staticmethod
        def today():
            return fixed

    monkeypatch.setattr("app.etl.orchestration.csv_resolver.date", _Date)
    monkeypatch.setattr("app.etl.orchestration.csv_resolver.time.sleep", MagicMock())

    today_folder = tmp_path / "b3" / "boletim_diario" / fixed.isoformat()
    today_folder.mkdir(parents=True)
    today_file = today_folder / "cadastro_instrumentos_20250614.normalized.csv"
    today_file.write_text("x", encoding="utf-8")

    resolved = resolve_instruments_csv(data_dir=tmp_path, retry_count=0, retry_delay_seconds=0)
    assert resolved == today_file


def test_resolve_instruments_csv_yesterday_fallback(monkeypatch, tmp_path: Path):
    fixed = date(2025, 6, 14)
    yesterday = date(2025, 6, 13)

    class _Date:
        @staticmethod
        def today():
            return fixed

    monkeypatch.setattr("app.etl.orchestration.csv_resolver.date", _Date)
    monkeypatch.setattr("app.etl.orchestration.csv_resolver.time.sleep", MagicMock())

    y_folder = tmp_path / "b3" / "boletim_diario" / yesterday.isoformat()
    y_folder.mkdir(parents=True)
    y_file = y_folder / "cadastro_instrumentos_20250613.normalized.csv"
    y_file.write_text("x", encoding="utf-8")

    resolved = resolve_instruments_csv(data_dir=tmp_path, retry_count=0, retry_delay_seconds=0)
    assert resolved == y_file


def test_resolve_instruments_csv_raises_when_missing(monkeypatch, tmp_path: Path):
    fixed = date(2025, 6, 14)

    class _Date:
        @staticmethod
        def today():
            return fixed

    monkeypatch.setattr("app.etl.orchestration.csv_resolver.date", _Date)
    monkeypatch.setattr("app.etl.orchestration.csv_resolver.time.sleep", MagicMock())

    with pytest.raises(CSVNotFoundError):
        resolve_instruments_csv(data_dir=tmp_path, retry_count=0, retry_delay_seconds=0)
