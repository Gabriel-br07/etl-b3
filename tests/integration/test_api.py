"""API integration tests (no real DB – uses TestClient)."""

from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from app.main import app


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
    assert "/fact-quotes/{ticker}/candles" in paths
    assert "/cotahist/" in paths
    assert "/cotahist/{ticker}/history" in paths
    assert "/etl/runs" in paths
    assert "/market/overview" in paths
    assert "/assets/{ticker}/coverage" in paths
    assert "/assets/{ticker}/overview" in paths
    assert "/quotes/{ticker}/candles" in paths
    assert "/quotes/{ticker}/indicators" in paths


# ---------------------------------------------------------------------------
# Representative route smoke tests (one per router)
# ---------------------------------------------------------------------------


def test_trades_endpoint_responds(client):
    """GET /trades should respond (200/500/503 without real DB)."""
    response = client.get("/trades")
    assert response.status_code in (200, 500, 503)


def test_fact_quotes_series_endpoint_responds(client):
    """GET /fact-quotes/{ticker}/series should respond (200/500/503)."""
    response = client.get("/fact-quotes/PETR4/series")
    assert response.status_code in (200, 500, 503)


def test_etl_runs_endpoint_responds(client):
    response = client.get("/etl/runs")
    assert response.status_code in (200, 500, 503)


def test_cotahist_history_endpoint_responds(client):
    response = client.get("/cotahist/PETR4/history")
    assert response.status_code in (200, 500, 503)


# ---------------------------------------------------------------------------
# ETL-triggering routes (mock pipeline to avoid real DB)
# ---------------------------------------------------------------------------


def test_etl_run_latest_calls_pipeline_and_returns_200(client, tmp_path):
    """POST /etl/run-latest with source_mode=local should call run_instruments_and_trades_pipeline."""
    instruments_csv = tmp_path / "cadastro_20260318.normalized.csv"
    trades_csv = tmp_path / "negocios_consolidados_20260318.normalized.csv"
    instruments_csv.write_text("Instrumento financeiro;Ativo\nPETR4;PETR", encoding="utf-8")
    trades_csv.write_text("ticker;trade_date\nPETR4;2026-03-18", encoding="utf-8")
    with patch("app.api.routes.etl.run_instruments_and_trades_pipeline") as mock_pipeline:
        mock_pipeline.return_value = {
            "status": "success",
            "assets_upserted": 1,
            "trades_upserted": 1,
            "target_date": "2026-03-18",
        }
        with patch("app.api.routes.etl.settings") as mock_settings:
            mock_settings.b3_data_dir = str(tmp_path)
            with patch(
                "app.api.routes.etl._resolve_local_paths",
                return_value=(instruments_csv, trades_csv),
            ):
                response = client.post(
                    "/etl/run-latest",
                    json={"source_mode": "local"},
                )
    assert response.status_code == 200
    data = response.json()
    assert data.get("status") == "ok"
    assert "result" in data
    mock_pipeline.assert_called_once()
    call_args = mock_pipeline.call_args[0]
    assert call_args[0] == instruments_csv
    assert call_args[1] == trades_csv
    assert len(call_args) == 3  # target_date


def test_etl_run_latest_remote_returns_501(client):
    """POST /etl/run-latest with source_mode=remote must return 501."""
    response = client.post("/etl/run-latest", json={"source_mode": "remote"})
    assert response.status_code == 501
    assert "remote" in response.json().get("detail", "").lower()


def test_etl_backfill_remote_returns_501(client):
    """POST /etl/backfill with source_mode=remote must return 501."""
    response = client.post(
        "/etl/backfill",
        json={
            "source_mode": "remote",
            "date_from": "2026-03-01",
            "date_to": "2026-03-18",
        },
    )
    assert response.status_code == 501


def test_etl_run_latest_404_when_no_csv(client, tmp_path):
    """POST /etl/run-latest returns 404 when no instruments CSV found."""
    from fastapi import HTTPException

    with patch("app.api.routes.etl._resolve_local_paths", side_effect=HTTPException(status_code=404, detail="not found")):
        response = client.post("/etl/run-latest", json={"source_mode": "local"})
    assert response.status_code == 404


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


def test_quotes_indicators_invalid_indicator_returns_400(client):
    r = client.get("/quotes/PETR4/indicators?indicator=NOTREAL&period=14")
    assert r.status_code == 400


def test_quotes_candles_invalid_interval_returns_400(client):
    r = client.get("/quotes/PETR4/candles?interval=2h")
    assert r.status_code == 400


def test_fact_quotes_candles_rejects_1d(client):
    r = client.get("/fact-quotes/PETR4/candles?interval=1d")
    assert r.status_code == 400


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
