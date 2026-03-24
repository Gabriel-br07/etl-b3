"""Black-box API smoke tests against a running server (set E2E_BASE_URL)."""

import os

import httpx
import pytest

pytestmark = pytest.mark.e2e


@pytest.fixture(scope="module")
def e2e_base_url() -> str:
    base = os.environ.get("E2E_BASE_URL", "").strip().rstrip("/")
    if not base:
        pytest.skip("E2E_BASE_URL is not set (e.g. http://127.0.0.1:8000)")
    return base


def test_health_returns_ok(e2e_base_url: str):
    response = httpx.get(f"{e2e_base_url}/health", timeout=15.0)
    assert response.status_code == 200
    data = response.json()
    assert data.get("status") == "ok"


def test_openapi_available(e2e_base_url: str):
    response = httpx.get(f"{e2e_base_url}/openapi.json", timeout=15.0)
    assert response.status_code == 200
    schema = response.json()
    assert "openapi" in schema
    paths = schema.get("paths", {})
    assert "/cotahist/{ticker}/history" in paths
    assert "/etl/runs" in paths
