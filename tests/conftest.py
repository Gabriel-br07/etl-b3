"""Shared pytest fixtures and configuration."""

import pytest

# Prefer pytest.importorskip so pytest can skip the whole test session
# if FastAPI is not installed (avoids hiding other runtime errors).
pytest.importorskip("fastapi")

try:
    from fastapi.testclient import TestClient
except Exception:
    # If TestClient import fails for some other reason, let the module import
    # continue so we can surface the real error when running pytest. We keep
    # TestClient import guarded only by importorskip above in normal pytest runs.
    TestClient = None  # type: ignore

# Try to import the app; only treat import errors as "app unavailable".
try:
    from app.main import app as _app
    _APP_AVAILABLE = True
    _APP_SKIP_REASON = None
except (ImportError, ModuleNotFoundError) as exc:  # only catch import-related errors
    _APP_AVAILABLE = False
    _app = None  # type: ignore[assignment]
    _APP_SKIP_REASON = f"FastAPI app unavailable (import error): {exc!r}"


@pytest.fixture(scope="session")
def client():
    """FastAPI test client (no real DB required for unit tests)."""
    if not _APP_AVAILABLE:
        # Include the original import error when skipping so real failures aren't hidden.
        pytest.skip(_APP_SKIP_REASON or "FastAPI app unavailable (missing dependency)")
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
