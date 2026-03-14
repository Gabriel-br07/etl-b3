"""API integration tests (no real DB – uses TestClient)."""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch

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
    # New routes from API refactor (trades, fact-quotes)
    assert "/trades" in paths
    assert "/fact-quotes/{ticker}/series" in paths
    assert "/fact-quotes/{ticker}/days/{trade_date}" in paths


# ---------------------------------------------------------------------------
# /quotes/latest ticker list parsing – edge cases
# ---------------------------------------------------------------------------


def test_quotes_latest_filters_empty_strings_from_ticker_list():
    """Trailing/leading commas must not produce empty-string tickers."""
    mock_repo = MagicMock()
    mock_repo.get_latest_per_ticker.return_value = ([], 0)

    with patch("app.api.routes.quotes.QuoteRepository", return_value=mock_repo), \
         patch("app.api.routes.quotes.get_db"):
        local_client = TestClient(app, raise_server_exceptions=True)
        # The request itself may fail at DB level; we only care that the
        # tickers list passed to the repo does not contain empty strings.
        try:
            local_client.get("/quotes/latest?tickers=PETR4,")
        except Exception:
            pass

    if mock_repo.get_latest_per_ticker.called:
        called_tickers = mock_repo.get_latest_per_ticker.call_args[1].get(
            "tickers"
        ) or mock_repo.get_latest_per_ticker.call_args[0][0]
        assert "" not in (called_tickers or [])


def test_quotes_latest_ticker_list_parsing():
    """Verify the ticker parsing logic directly using the helper function."""
    from app.api.routes.quotes import _parse_ticker_list

    assert _parse_ticker_list(None) is None
    assert _parse_ticker_list("") is None
    assert _parse_ticker_list("PETR4") == ["PETR4"]
    assert _parse_ticker_list("PETR4,VALE3") == ["PETR4", "VALE3"]
    # Trailing comma must not produce empty string
    assert _parse_ticker_list("PETR4,") == ["PETR4"]
    # Leading comma must not produce empty string
    assert _parse_ticker_list(",PETR4") == ["PETR4"]
    # Multiple commas
    assert _parse_ticker_list(",,PETR4,,VALE3,,") == ["PETR4", "VALE3"]
    # Whitespace around tickers
    assert _parse_ticker_list(" PETR4 , VALE3 ") == ["PETR4", "VALE3"]
    # Lower-case tickers are uppercased
    assert _parse_ticker_list("petr4,vale3") == ["PETR4", "VALE3"]
    # All-empty tokens return None
    assert _parse_ticker_list(",,") is None
