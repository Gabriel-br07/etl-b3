"""Shared pytest fixtures and configuration."""

import pytest

try:
    from fastapi.testclient import TestClient
    from app.main import app as _app
    _APP_AVAILABLE = True
except Exception:  # noqa: BLE001
    _APP_AVAILABLE = False
    _app = None  # type: ignore[assignment]


@pytest.fixture(scope="session")
def client():
    """FastAPI test client (no real DB required for unit tests)."""
    if not _APP_AVAILABLE:
        pytest.skip("FastAPI app unavailable (missing dependency)")
    from fastapi.testclient import TestClient
    return TestClient(_app, raise_server_exceptions=False)


@pytest.fixture
def instruments_fixture_path():
    """Path to the instruments CSV fixture."""
    from pathlib import Path

    return Path(__file__).parent / "fixtures" / "CadInstrumento_2024-06-14.csv"


@pytest.fixture
def trades_fixture_path():
    """Path to the trades ZIP fixture."""
    from pathlib import Path

    return Path(__file__).parent / "fixtures" / "NegociosConsolidados_2024-06-14.zip"
