"""Tests for the local file source adapter."""

from datetime import date
from pathlib import Path

import pytest

from app.etl.ingestion.local_adapter import LocalFileAdapter


FIXTURE_DIR = Path(__file__).parent / "fixtures"
TARGET_DATE = date(2024, 6, 14)


def test_local_adapter_resolves_instruments(tmp_path):
    # Copy fixture to tmp_path with the right naming
    import shutil
    src = FIXTURE_DIR / "CadInstrumento_2024-06-14.csv"
    shutil.copy(src, tmp_path / "CadInstrumento_2024-06-14.csv")

    adapter = LocalFileAdapter(data_dir=tmp_path)
    result = adapter.get_instruments_file(TARGET_DATE)

    assert result.path.exists()
    assert result.source_name == "CadInstrumento"
    assert result.source_url is None
    assert result.file_date == TARGET_DATE


def test_local_adapter_resolves_trades(tmp_path):
    import shutil
    src = FIXTURE_DIR / "NegociosConsolidados_2024-06-14.zip"
    shutil.copy(src, tmp_path / "NegociosConsolidados_2024-06-14.zip")

    adapter = LocalFileAdapter(data_dir=tmp_path)
    result = adapter.get_trades_file(TARGET_DATE)

    assert result.path.exists()
    assert result.source_name == "NegociosConsolidados"
    assert result.source_url is None


def test_local_adapter_missing_file_raises(tmp_path):
    adapter = LocalFileAdapter(data_dir=tmp_path)
    with pytest.raises(FileNotFoundError):
        adapter.get_instruments_file(TARGET_DATE)


def test_local_adapter_fallback_to_no_date_file(tmp_path):
    """Adapter should also resolve files without a date suffix."""
    import shutil
    src = FIXTURE_DIR / "CadInstrumento_2024-06-14.csv"
    shutil.copy(src, tmp_path / "CadInstrumento.csv")

    adapter = LocalFileAdapter(data_dir=tmp_path)
    result = adapter.get_instruments_file(date(2099, 1, 1))  # date not in filename
    assert result.path.name == "CadInstrumento.csv"
