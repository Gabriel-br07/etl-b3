"""Shared pytest fixtures and configuration."""

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture(scope="session")
def client():
    """FastAPI test client (no real DB required for unit tests)."""
    return TestClient(app, raise_server_exceptions=False)


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
