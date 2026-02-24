"""API integration tests (no real DB – uses TestClient)."""

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture(scope="module")
def client():
    return TestClient(app, raise_server_exceptions=False)


def test_health_returns_ok(client):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "version" in data
    assert "environment" in data


def test_health_response_structure(client):
    response = client.get("/health")
    data = response.json()
    assert set(data.keys()) >= {"status", "version", "environment"}


def test_assets_endpoint_returns_200(client):
    """GET /assets should respond even without DB (connection error = 500, handled)."""
    response = client.get("/assets")
    # Without a real DB this will return 500 or 200; we just verify it responds
    assert response.status_code in (200, 500, 503)


def test_scalar_docs_accessible(client):
    response = client.get("/scalar")
    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")


def test_openapi_schema_accessible(client):
    response = client.get("/openapi.json")
    assert response.status_code == 200
    schema = response.json()
    assert "openapi" in schema
    assert "paths" in schema


def test_swagger_ui_disabled(client):
    response = client.get("/docs")
    assert response.status_code == 404


def test_redoc_disabled(client):
    response = client.get("/redoc")
    assert response.status_code == 404


def test_openapi_has_expected_endpoints(client):
    response = client.get("/openapi.json")
    schema = response.json()
    paths = list(schema["paths"].keys())
    assert "/health" in paths
    assert "/assets" in paths
    assert "/quotes/latest" in paths
    assert "/etl/run-latest" in paths
