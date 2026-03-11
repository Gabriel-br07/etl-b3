"""Shared pytest fixtures and configuration."""

import pytest

# Make FastAPI/TestClient optional: do not skip the entire test session when it's absent.
TestClient = None
try:
    from fastapi.testclient import TestClient as _TestClient  # type: ignore
    TestClient = _TestClient
except Exception:
    TestClient = None

_app = None
_APP_AVAILABLE = False
_APP_SKIP_REASON = None

# Try to import the app; treat import errors as "app unavailable" but don't skip tests.
try:
    from app.main import app as _app  # type: ignore
    _APP_AVAILABLE = True
    _APP_SKIP_REASON = None
except (ImportError, ModuleNotFoundError) as exc:  # only catch import-related errors
    _APP_AVAILABLE = False
    _app = None  # type: ignore[assignment]
    _APP_SKIP_REASON = f"FastAPI app unavailable (import error): {exc!r}"


@pytest.fixture(scope="session")
def client():
    """FastAPI test client (no real DB required for unit tests)."""
    if not _APP_AVAILABLE or TestClient is None:
        pytest.skip(_APP_SKIP_REASON or "FastAPI app unavailable (missing dependency)")
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
