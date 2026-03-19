"""Shared pytest fixtures for the test suite."""

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture(scope="module")
def client() -> TestClient:
    """FastAPI test client (no real DB required for most route tests)."""
    return TestClient(app, raise_server_exceptions=False)
