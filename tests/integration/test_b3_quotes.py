"""Integration tests for B3 quote client, parsers, services, routes, and batch ingestion.

Coverage:
- B3QuoteClient: successful fetch, 403→warm+retry, 429→warm+retry,
  persistent block, 404, non-JSON body, timeout.
- Parser (legacy flat format): happy-path normalisation, missing trade_date,
  empty payload, bad payload, comma-decimal separator.
- Parser (real B3 payload – TradgFlr/lstQtn shape):
  parse_intraday_series, parse_latest_snapshot, empty lstQtn, missing TradgFlr.
- B3QuoteService: cache hit / miss, cache bypass when ttl=0,
  get_latest_snapshot, get_intraday_series.
- Use cases: get_daily_fluctuation, get_latest_snapshot, get_intraday_series.
- Batch ingestion (intraday series):
  - one per-ticker CSV per success (one row per lstQtn point)
  - manifest CSV with one row per ticker
  - empty lstQtn → header-only per-ticker CSV
  - failed ticker → manifest row with request_succeeded=false, no per-ticker CSV
  - batch resilience (one failure does not abort the rest)
  - timestamped filename convention
  - output path correctness
- API routes:
  GET /quotes/{ticker}/snapshot  → 200, 404, 503, 502  (legacy, NormalizedQuote)
  GET /quotes/{ticker}           → 200, 404, 503, 502  (LatestSnapshotQuote)
  GET /quotes/{ticker}/intraday  → 200, 404, 503, 502  (IntradaySeriesQuote)

Real B3 endpoints are NEVER contacted.  All HTTP is mocked via ``respx``.
"""

from __future__ import annotations

import csv
from collections.abc import Generator
from pathlib import Path
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
import respx

from app.integrations.b3.client import B3QuoteClient
from app.integrations.b3.exceptions import (
    B3TemporaryBlockError,
    B3TickerNotFoundError,
    B3UnexpectedResponseError,
)
from app.integrations.b3.models import (
    IntradaySeriesQuote,
    LatestSnapshotQuote,
    NormalizedQuote,
)
from app.integrations.b3.parser import (
    _to_date,
    _to_decimal,
    parse_daily_fluctuation,
    parse_intraday_series,
    parse_latest_snapshot,
)
from app.integrations.b3.service import B3QuoteService
from app.etl.ingestion.ticker_reader import (
    TickerColumnMissingError,
    read_tickers_from_csv,
)
from app.use_cases.quotes.batch_ingestion import run_batch_quote_ingestion
from app.use_cases.quotes.get_daily_fluctuation import get_daily_fluctuation
from app.use_cases.quotes.get_intraday_series import get_intraday_series as uc_get_intraday_series
from app.use_cases.quotes.get_latest_snapshot import get_latest_snapshot as uc_get_latest_snapshot

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

TICKER = "PETR4"

# Legacy flat payload – still used by the backward-compatible parser path.
SAMPLE_RAW_PAYLOAD: dict[str, Any] = {
    "codNeg": "PETR4",
    "tradeDate": "2024-06-14",
    "lastPrice": "38.45",
    "minPrice": "37.90",
    "maxPrice": "38.90",
    "avgPrice": "38.22",
    "oscillationVal": "1.25",
}

# Real B3 DailyFluctuationHistory payload shape (BizSts / Msg / TradgFlr).
REAL_B3_PAYLOAD: dict[str, Any] = {
    "BizSts": {"cd": "OK"},
    "Msg": {"dtTm": "2024-06-14T15:30:00"},
    "TradgFlr": {
        "date": "2024-06-14",
        "scty": {
            "symb": "PETR4",
            "lstQtn": [
                {"closPric": 37.90, "dtTm": "2024-06-14T10:01:00", "prcFlcn": -0.10},
                {"closPric": 38.00, "dtTm": "2024-06-14T10:02:00", "prcFlcn": 0.26},
                {"closPric": 38.45, "dtTm": "2024-06-14T15:29:00", "prcFlcn": 0.52},
            ],
        },
    },
}

BASE_URL = "https://sistemaswebb3-listados.b3.com.br/mdsProxy/proxy/api/v1"
FLUCTUATION_URL = f"{BASE_URL}/DailyFluctuationHistory/{TICKER}"
WARM_URL = (
    "https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/"
    "market-data/consultas/mercado-a-vista/variacao-diaria/mercado-continuo/"
)


@pytest.fixture()
def b3_quote_client() -> Generator[B3QuoteClient, None, None]:
    """B3 HTTP client for respx-based tests (distinct from FastAPI TestClient in conftest)."""
    c = B3QuoteClient(
        base_url=BASE_URL,
        warm_url=WARM_URL,
        timeout=5.0,
        http2=False,
    )
    yield c
    c.close()


# ---------------------------------------------------------------------------
# _to_decimal helper
# ---------------------------------------------------------------------------


def test_to_decimal_valid():
    assert _to_decimal("38.45") == Decimal("38.45")


def test_to_decimal_comma_separator():
    assert _to_decimal("38,45") == Decimal("38.45")


def test_to_decimal_none():
    assert _to_decimal(None) is None


def test_to_decimal_empty_string():
    assert _to_decimal("") is None


def test_to_decimal_dash():
    assert _to_decimal("-") is None


# ---------------------------------------------------------------------------
# _to_date helper
# ---------------------------------------------------------------------------


def test_to_date_iso():
    assert _to_date("2024-06-14") == date(2024, 6, 14)


def test_to_date_br_format():
    assert _to_date("14/06/2024") == date(2024, 6, 14)


def test_to_date_none():
    assert _to_date(None) is None


def test_to_date_garbage():
    assert _to_date("not-a-date") is None


# ---------------------------------------------------------------------------
# Parser – happy path
# ---------------------------------------------------------------------------


def test_parse_daily_fluctuation_happy_path():
    quote = parse_daily_fluctuation(SAMPLE_RAW_PAYLOAD, requested_ticker=TICKER)

    assert quote.ticker == "PETR4"
    assert quote.trade_date == date(2024, 6, 14)
    assert quote.last_price == Decimal("38.45")
    assert quote.min_price == Decimal("37.90")
    assert quote.max_price == Decimal("38.90")
    assert quote.average_price == Decimal("38.22")
    assert quote.oscillation_pct == Decimal("1.25")
    assert quote.source == "b3_public_internal_endpoint"
    assert quote.delayed is True
    assert quote.raw_data is not None


def test_parse_uses_requested_ticker_as_fallback():
    payload = {**SAMPLE_RAW_PAYLOAD}
    payload.pop("codNeg", None)
    payload.pop("ticker", None)
    quote = parse_daily_fluctuation(payload, requested_ticker="VALE3")
    assert quote.ticker == "VALE3"


def test_parse_unwraps_results_list():
    wrapped = {"results": [SAMPLE_RAW_PAYLOAD]}
    quote = parse_daily_fluctuation(wrapped, requested_ticker=TICKER)
    assert quote.ticker == "PETR4"
    assert quote.last_price == Decimal("38.45")


# ---------------------------------------------------------------------------
# Parser – error cases
# ---------------------------------------------------------------------------


def test_parse_raises_on_empty_payload():
    with pytest.raises(B3UnexpectedResponseError, match="empty payload"):
        parse_daily_fluctuation({}, requested_ticker=TICKER)


def test_parse_raises_on_none_payload():
    with pytest.raises(B3UnexpectedResponseError):
        parse_daily_fluctuation(None, requested_ticker=TICKER)  # type: ignore[arg-type]


def test_parse_uses_today_when_trade_date_missing(monkeypatch):
    payload = {**SAMPLE_RAW_PAYLOAD}
    payload.pop("tradeDate", None)
    today = date(2024, 6, 14)
    monkeypatch.setattr("app.integrations.b3.parser.date", type("_FakeDate", (), {"today": staticmethod(lambda: today)}))
    quote = parse_daily_fluctuation(payload, requested_ticker=TICKER)
    assert quote.trade_date == today


# ---------------------------------------------------------------------------
# B3QuoteClient – happy path
# ---------------------------------------------------------------------------


@respx.mock
def test_client_get_daily_fluctuation_success(b3_quote_client):
    respx.get(FLUCTUATION_URL).mock(
        return_value=httpx.Response(200, json=SAMPLE_RAW_PAYLOAD)
    )
    data = b3_quote_client.get_daily_fluctuation_history(TICKER)
    assert data == SAMPLE_RAW_PAYLOAD


@respx.mock
def test_client_uppercases_ticker(b3_quote_client):
    url = f"{BASE_URL}/DailyFluctuationHistory/PETR4"
    respx.get(url).mock(return_value=httpx.Response(200, json=SAMPLE_RAW_PAYLOAD))
    data = b3_quote_client.get_daily_fluctuation_history("petr4")
    assert data == SAMPLE_RAW_PAYLOAD


# ---------------------------------------------------------------------------
# B3QuoteClient – 403 → warm session → success
# ---------------------------------------------------------------------------


@respx.mock
def test_client_403_warms_and_retries_success(b3_quote_client):
    # First call returns 403; after warm-up second call returns 200.
    respx.get(WARM_URL).mock(return_value=httpx.Response(200, text="ok"))
    fluctuation_route = respx.get(FLUCTUATION_URL)
    fluctuation_route.side_effect = [
        httpx.Response(403, text="Forbidden"),
        httpx.Response(200, json=SAMPLE_RAW_PAYLOAD),
    ]
    data = b3_quote_client.get_daily_fluctuation_history(TICKER)
    assert data == SAMPLE_RAW_PAYLOAD
    assert fluctuation_route.call_count == 2


# ---------------------------------------------------------------------------
# B3QuoteClient – 429 → warm session → success
# ---------------------------------------------------------------------------


@respx.mock
def test_client_429_warms_and_retries_success(b3_quote_client):
    respx.get(WARM_URL).mock(return_value=httpx.Response(200, text="ok"))
    fluctuation_route = respx.get(FLUCTUATION_URL)
    fluctuation_route.side_effect = [
        httpx.Response(429, text="Too Many Requests"),
        httpx.Response(200, json=SAMPLE_RAW_PAYLOAD),
    ]
    data = b3_quote_client.get_daily_fluctuation_history(TICKER)
    assert data == SAMPLE_RAW_PAYLOAD


# ---------------------------------------------------------------------------
# B3QuoteClient – persistent block raises B3TemporaryBlockError
# ---------------------------------------------------------------------------


@respx.mock
def test_client_persistent_403_raises_temporary_block(b3_quote_client):
    respx.get(WARM_URL).mock(return_value=httpx.Response(200, text="ok"))
    respx.get(FLUCTUATION_URL).mock(return_value=httpx.Response(403, text="Forbidden"))
    with pytest.raises(B3TemporaryBlockError) as exc_info:
        b3_quote_client.get_daily_fluctuation_history(TICKER)
    assert exc_info.value.ticker == TICKER
    assert exc_info.value.status_code == 403


@respx.mock
def test_client_persistent_429_raises_temporary_block(b3_quote_client):
    respx.get(WARM_URL).mock(return_value=httpx.Response(200, text="ok"))
    respx.get(FLUCTUATION_URL).mock(return_value=httpx.Response(429, text="Too Many"))
    with pytest.raises(B3TemporaryBlockError) as exc_info:
        b3_quote_client.get_daily_fluctuation_history(TICKER)
    assert exc_info.value.status_code == 429


# ---------------------------------------------------------------------------
# B3QuoteClient – 404 raises B3TickerNotFoundError
# ---------------------------------------------------------------------------


@respx.mock
def test_client_404_raises_ticker_not_found(b3_quote_client):
    respx.get(FLUCTUATION_URL).mock(return_value=httpx.Response(404, text="Not Found"))
    with pytest.raises(B3TickerNotFoundError) as exc_info:
        b3_quote_client.get_daily_fluctuation_history(TICKER)
    assert exc_info.value.ticker == TICKER


# ---------------------------------------------------------------------------
# B3QuoteClient – non-2xx unexpected raises B3UnexpectedResponseError
# ---------------------------------------------------------------------------


@respx.mock
def test_client_500_raises_unexpected_response(b3_quote_client):
    respx.get(FLUCTUATION_URL).mock(return_value=httpx.Response(500, text="Server Error"))
    with pytest.raises(B3UnexpectedResponseError) as exc_info:
        b3_quote_client.get_daily_fluctuation_history(TICKER)
    assert exc_info.value.status_code == 500


# ---------------------------------------------------------------------------
# B3QuoteClient – non-JSON body raises B3UnexpectedResponseError
# ---------------------------------------------------------------------------


@respx.mock
def test_client_non_json_body_raises_unexpected_response(b3_quote_client):
    respx.get(FLUCTUATION_URL).mock(
        return_value=httpx.Response(200, text="<html>CF Challenge</html>")
    )
    with pytest.raises(B3UnexpectedResponseError, match="not valid JSON"):
        b3_quote_client.get_daily_fluctuation_history(TICKER)


# ---------------------------------------------------------------------------
# B3QuoteClient – timeout raises B3UnexpectedResponseError
# ---------------------------------------------------------------------------


@respx.mock
def test_client_timeout_raises_unexpected_response(b3_quote_client):
    respx.get(FLUCTUATION_URL).mock(side_effect=httpx.TimeoutException("timed out"))
    with pytest.raises(B3UnexpectedResponseError, match="timed out"):
        b3_quote_client.get_daily_fluctuation_history(TICKER)


# ---------------------------------------------------------------------------
# B3QuoteService – cache miss fetches from client
# ---------------------------------------------------------------------------


def test_service_cache_miss_fetches():
    mock_client = MagicMock(spec=B3QuoteClient)
    mock_client.get_daily_fluctuation_history.return_value = SAMPLE_RAW_PAYLOAD

    svc = B3QuoteService(client=mock_client, cache_ttl=300)
    quote = svc.get_quote_snapshot(TICKER)

    assert quote.ticker == "PETR4"
    mock_client.get_daily_fluctuation_history.assert_called_once_with(TICKER)


def test_service_cache_hit_does_not_refetch():
    mock_client = MagicMock(spec=B3QuoteClient)
    mock_client.get_daily_fluctuation_history.return_value = SAMPLE_RAW_PAYLOAD

    svc = B3QuoteService(client=mock_client, cache_ttl=300)
    svc.get_quote_snapshot(TICKER)
    svc.get_quote_snapshot(TICKER)  # second call should use cache

    mock_client.get_daily_fluctuation_history.assert_called_once()


def test_service_cache_bypass_when_ttl_zero():
    mock_client = MagicMock(spec=B3QuoteClient)
    mock_client.get_daily_fluctuation_history.return_value = SAMPLE_RAW_PAYLOAD

    svc = B3QuoteService(client=mock_client, cache_ttl=0)
    svc.get_quote_snapshot(TICKER)
    svc.get_quote_snapshot(TICKER)

    assert mock_client.get_daily_fluctuation_history.call_count == 2


def test_service_invalidate_cache_forces_refetch():
    mock_client = MagicMock(spec=B3QuoteClient)
    mock_client.get_daily_fluctuation_history.return_value = SAMPLE_RAW_PAYLOAD

    svc = B3QuoteService(client=mock_client, cache_ttl=300)
    svc.get_quote_snapshot(TICKER)
    svc.invalidate_cache(TICKER)
    svc.get_quote_snapshot(TICKER)

    assert mock_client.get_daily_fluctuation_history.call_count == 2


# ---------------------------------------------------------------------------
# Use case
# ---------------------------------------------------------------------------


def test_use_case_returns_normalized_quote():
    mock_svc = MagicMock(spec=B3QuoteService)
    expected = NormalizedQuote(
        ticker="PETR4",
        trade_date=date(2024, 6, 14),
        last_price=Decimal("38.45"),
        min_price=None,
        max_price=None,
        average_price=None,
        oscillation_pct=None,
        source="b3_public_internal_endpoint",
        delayed=True,
        fetched_at=datetime.now(UTC),
        raw_data=None,
    )
    mock_svc.get_quote_snapshot.return_value = expected

    result = get_daily_fluctuation(TICKER, service=mock_svc)

    assert result is expected
    mock_svc.get_quote_snapshot.assert_called_once_with("PETR4")


def test_use_case_uppercases_ticker():
    mock_svc = MagicMock(spec=B3QuoteService)
    mock_svc.get_quote_snapshot.return_value = NormalizedQuote(
        ticker="VALE3",
        trade_date=date(2024, 6, 14),
        last_price=None,
        min_price=None,
        max_price=None,
        average_price=None,
        oscillation_pct=None,
        source="b3_public_internal_endpoint",
        delayed=True,
        fetched_at=datetime.now(UTC),
        raw_data=None,
    )
    get_daily_fluctuation("vale3", service=mock_svc)
    mock_svc.get_quote_snapshot.assert_called_once_with("VALE3")


# ---------------------------------------------------------------------------
# API route – /quotes/{ticker}/snapshot
# ---------------------------------------------------------------------------


def _make_quote(ticker: str = "PETR4") -> NormalizedQuote:
    return NormalizedQuote(
        ticker=ticker,
        trade_date=date(2024, 6, 14),
        last_price=Decimal("38.45"),
        min_price=Decimal("37.90"),
        max_price=Decimal("38.90"),
        average_price=Decimal("38.22"),
        oscillation_pct=Decimal("1.25"),
        source="b3_public_internal_endpoint",
        delayed=True,
        fetched_at=datetime(2024, 6, 14, 12, 0, 0, tzinfo=UTC),
        raw_data=None,
    )


def test_route_snapshot_200(client):
    with patch(
        "app.api.routes.quotes.get_daily_fluctuation",
        return_value=_make_quote(),
    ):
        resp = client.get("/quotes/PETR4/snapshot")

    assert resp.status_code == 200
    data = resp.json()
    assert data["ticker"] == "PETR4"
    assert data["last_price"] == "38.45"
    assert data["source"] == "b3_public_internal_endpoint"
    assert data["delayed"] is True


def test_route_snapshot_404(client):
    with patch(
        "app.api.routes.quotes.get_daily_fluctuation",
        side_effect=B3TickerNotFoundError("XXXXXX"),
    ):
        resp = client.get("/quotes/XXXXXX/snapshot")

    assert resp.status_code == 404
    detail = resp.json()["detail"]
    assert isinstance(detail, dict)
    assert detail.get("ticker") == "XXXXXX"
def test_route_snapshot_503_temporary_block(client):
    with patch(
        "app.api.routes.quotes.get_daily_fluctuation",
        side_effect=B3TemporaryBlockError(403, "PETR4"),
    ):
        resp = client.get("/quotes/PETR4/snapshot")

    assert resp.status_code == 503
    detail = resp.json()["detail"]
    assert detail["error"] == "upstream_temporary_block"


def test_route_snapshot_502_unexpected(client):
    with patch(
        "app.api.routes.quotes.get_daily_fluctuation",
        side_effect=B3UnexpectedResponseError("bad JSON", status_code=200),
    ):
        resp = client.get("/quotes/PETR4/snapshot")

    assert resp.status_code == 502
    detail = resp.json()["detail"]
    assert detail["error"] == "upstream_unexpected_response"


# ===========================================================================
# Real B3 payload – parse_intraday_series
# ===========================================================================


def test_parse_intraday_series_happy_path():
    series = parse_intraday_series(REAL_B3_PAYLOAD, requested_ticker=TICKER)

    assert series.ticker == "PETR4"
    assert series.trade_date == date(2024, 6, 14)
    assert series.message_datetime == "2024-06-14T15:30:00"
    assert len(series.points) == 3
    assert series.source == "b3_public_internal_endpoint"
    assert series.delayed is True

    # Points are ordered oldest-first (as returned by B3)
    first = series.points[0]
    assert first.time == "2024-06-14T10:01:00"
    assert first.close_price == Decimal("37.90")
    assert first.price_fluctuation_pct == Decimal("-0.10")

    last = series.points[-1]
    assert last.time == "2024-06-14T15:29:00"
    assert last.close_price == Decimal("38.45")
    assert last.price_fluctuation_pct == Decimal("0.52")


def test_parse_intraday_series_empty_lst_qtn():
    """parse_intraday_series must succeed with an empty points list."""
    payload = {
        "BizSts": {"cd": "OK"},
        "Msg": {"dtTm": "2024-06-14T15:30:00"},
        "TradgFlr": {
            "date": "2024-06-14",
            "scty": {"symb": "PETR4", "lstQtn": []},
        },
    }
    series = parse_intraday_series(payload, requested_ticker=TICKER)
    assert series.ticker == "PETR4"
    assert series.points == []


def test_parse_intraday_series_missing_scty():
    """parse_intraday_series succeeds even when scty is absent (graceful empty)."""
    payload = {
        "BizSts": {"cd": "OK"},
        "Msg": {"dtTm": "2024-06-14T15:30:00"},
        "TradgFlr": {"date": "2024-06-14"},
    }
    series = parse_intraday_series(payload, requested_ticker=TICKER)
    assert series.ticker == "PETR4"
    assert series.points == []


def test_parse_intraday_series_raises_on_empty_payload():
    with pytest.raises(B3UnexpectedResponseError, match="empty payload"):
        parse_intraday_series({}, requested_ticker=TICKER)


def test_parse_intraday_series_uses_requested_ticker_fallback():
    payload = {
        "BizSts": {"cd": "OK"},
        "Msg": {"dtTm": "2024-06-14T15:30:00"},
        "TradgFlr": {
            "date": "2024-06-14",
            "scty": {"lstQtn": []},  # no symb
        },
    }
    series = parse_intraday_series(payload, requested_ticker="VALE3")
    assert series.ticker == "VALE3"


def test_parse_intraday_series_preserves_raw_data():
    series = parse_intraday_series(REAL_B3_PAYLOAD, requested_ticker=TICKER)
    assert series.raw_data is not None
    assert "TradgFlr" in series.raw_data


# ===========================================================================
# Real B3 payload – parse_latest_snapshot
# ===========================================================================


def test_parse_latest_snapshot_happy_path():
    snapshot = parse_latest_snapshot(REAL_B3_PAYLOAD, requested_ticker=TICKER)

    assert snapshot.ticker == "PETR4"
    assert snapshot.trade_date == date(2024, 6, 14)
    assert snapshot.message_datetime == "2024-06-14T15:30:00"
    # Must pick the LAST item in lstQtn
    assert snapshot.latest_time == "2024-06-14T15:29:00"
    assert snapshot.latest_close_price == Decimal("38.45")
    assert snapshot.latest_price_fluctuation_pct == Decimal("0.52")
    assert snapshot.source == "b3_public_internal_endpoint"
    assert snapshot.delayed is True


def test_parse_latest_snapshot_empty_lst_qtn():
    """Latest snapshot must not raise when lstQtn is empty; all latest_* are None."""
    payload = {
        "BizSts": {"cd": "OK"},
        "Msg": {"dtTm": "2024-06-14T15:30:00"},
        "TradgFlr": {
            "date": "2024-06-14",
            "scty": {"symb": "PETR4", "lstQtn": []},
        },
    }
    snapshot = parse_latest_snapshot(payload, requested_ticker=TICKER)
    assert snapshot.latest_time is None
    assert snapshot.latest_close_price is None
    assert snapshot.latest_price_fluctuation_pct is None


def test_parse_latest_snapshot_single_point():
    """With a single lstQtn point, latest_* should map to that one item."""
    payload = {
        "BizSts": {"cd": "OK"},
        "Msg": {"dtTm": "2024-06-14T10:01:00"},
        "TradgFlr": {
            "date": "2024-06-14",
            "scty": {
                "symb": "VALE3",
                "lstQtn": [
                    {"closPric": "55.00", "dtTm": "2024-06-14T10:01:00", "prcFlcn": "0.10"},
                ],
            },
        },
    }
    snapshot = parse_latest_snapshot(payload, requested_ticker="VALE3")
    assert snapshot.ticker == "VALE3"
    assert snapshot.latest_close_price == Decimal("55.00")
    assert snapshot.latest_price_fluctuation_pct == Decimal("0.10")


def test_parse_latest_snapshot_raises_on_empty_payload():
    with pytest.raises(B3UnexpectedResponseError, match="empty payload"):
        parse_latest_snapshot({}, requested_ticker=TICKER)


# ===========================================================================
# parse_daily_fluctuation auto-detects real payload shape
# ===========================================================================


def test_parse_daily_fluctuation_real_payload_maps_to_normalized_quote():
    """parse_daily_fluctuation must handle real payload and return NormalizedQuote."""
    quote = parse_daily_fluctuation(REAL_B3_PAYLOAD, requested_ticker=TICKER)

    assert quote.ticker == "PETR4"
    assert quote.trade_date == date(2024, 6, 14)
    # last_price comes from the last lstQtn point's closPric
    assert quote.last_price == Decimal("38.45")
    # oscillation_pct comes from the last lstQtn point's prcFlcn
    assert quote.oscillation_pct == Decimal("0.52")
    assert quote.source == "b3_public_internal_endpoint"
    assert quote.delayed is True


# ===========================================================================
# B3QuoteService – get_latest_snapshot
# ===========================================================================


def test_service_get_latest_snapshot_returns_correct_model():
    mock_client = MagicMock(spec=B3QuoteClient)
    mock_client.get_daily_fluctuation_history.return_value = REAL_B3_PAYLOAD

    svc = B3QuoteService(client=mock_client, cache_ttl=0)
    snapshot = svc.get_latest_snapshot(TICKER)

    assert isinstance(snapshot, LatestSnapshotQuote)
    assert snapshot.ticker == "PETR4"
    assert snapshot.latest_close_price == Decimal("38.45")


def test_service_get_latest_snapshot_cache_hit():
    mock_client = MagicMock(spec=B3QuoteClient)
    mock_client.get_daily_fluctuation_history.return_value = REAL_B3_PAYLOAD

    svc = B3QuoteService(client=mock_client, cache_ttl=300)
    svc.get_latest_snapshot(TICKER)
    svc.get_latest_snapshot(TICKER)  # second call must use cache

    mock_client.get_daily_fluctuation_history.assert_called_once()


def test_service_get_latest_snapshot_cache_bypass_when_ttl_zero():
    mock_client = MagicMock(spec=B3QuoteClient)
    mock_client.get_daily_fluctuation_history.return_value = REAL_B3_PAYLOAD

    svc = B3QuoteService(client=mock_client, cache_ttl=0)
    svc.get_latest_snapshot(TICKER)
    svc.get_latest_snapshot(TICKER)

    assert mock_client.get_daily_fluctuation_history.call_count == 2


# ===========================================================================
# B3QuoteService – get_intraday_series
# ===========================================================================


def test_service_get_intraday_series_returns_correct_model():
    mock_client = MagicMock(spec=B3QuoteClient)
    mock_client.get_daily_fluctuation_history.return_value = REAL_B3_PAYLOAD

    svc = B3QuoteService(client=mock_client, cache_ttl=0)
    series = svc.get_intraday_series(TICKER)

    assert isinstance(series, IntradaySeriesQuote)
    assert series.ticker == "PETR4"
    assert len(series.points) == 3


def test_service_get_intraday_series_cache_hit():
    mock_client = MagicMock(spec=B3QuoteClient)
    mock_client.get_daily_fluctuation_history.return_value = REAL_B3_PAYLOAD

    svc = B3QuoteService(client=mock_client, cache_ttl=300)
    svc.get_intraday_series(TICKER)
    svc.get_intraday_series(TICKER)

    mock_client.get_daily_fluctuation_history.assert_called_once()


def test_service_invalidate_cache_clears_all_types():
    mock_client = MagicMock(spec=B3QuoteClient)
    mock_client.get_daily_fluctuation_history.return_value = REAL_B3_PAYLOAD

    svc = B3QuoteService(client=mock_client, cache_ttl=300)
    svc.get_intraday_series(TICKER)
    svc.get_latest_snapshot(TICKER)
    # Both are cached – 2 fetches so far
    assert mock_client.get_daily_fluctuation_history.call_count == 2

    svc.invalidate_cache(TICKER)

    svc.get_intraday_series(TICKER)
    svc.get_latest_snapshot(TICKER)
    # After invalidation both must be re-fetched
    assert mock_client.get_daily_fluctuation_history.call_count == 4


# ===========================================================================
# Use case – get_latest_snapshot
# ===========================================================================


def _make_latest_snapshot(ticker: str = "PETR4") -> LatestSnapshotQuote:
    return LatestSnapshotQuote(
        ticker=ticker,
        trade_date=date(2024, 6, 14),
        message_datetime="2024-06-14T15:30:00",
        latest_time="2024-06-14T15:29:00",
        latest_close_price=Decimal("38.45"),
        latest_price_fluctuation_pct=Decimal("0.52"),
        source="b3_public_internal_endpoint",
        delayed=True,
        fetched_at=datetime(2024, 6, 14, 15, 30, 0, tzinfo=UTC),
        raw_data=None,
    )


def _make_intraday_series(ticker: str = "PETR4") -> IntradaySeriesQuote:
    from app.integrations.b3.models import IntradayPoint
    return IntradaySeriesQuote(
        ticker=ticker,
        trade_date=date(2024, 6, 14),
        message_datetime="2024-06-14T15:30:00",
        points=[
            IntradayPoint(time="2024-06-14T10:01:00", close_price=Decimal("37.90"), price_fluctuation_pct=Decimal("-0.10")),
            IntradayPoint(time="2024-06-14T15:29:00", close_price=Decimal("38.45"), price_fluctuation_pct=Decimal("0.52")),
        ],
        source="b3_public_internal_endpoint",
        delayed=True,
        fetched_at=datetime(2024, 6, 14, 15, 30, 0, tzinfo=UTC),
        raw_data=None,
    )


def test_use_case_get_latest_snapshot_delegates_to_service():
    mock_svc = MagicMock(spec=B3QuoteService)
    expected = _make_latest_snapshot()
    mock_svc.get_latest_snapshot.return_value = expected

    result = uc_get_latest_snapshot(TICKER, service=mock_svc)

    assert result is expected
    mock_svc.get_latest_snapshot.assert_called_once_with("PETR4")


def test_use_case_get_latest_snapshot_uppercases_ticker():
    mock_svc = MagicMock(spec=B3QuoteService)
    mock_svc.get_latest_snapshot.return_value = _make_latest_snapshot("VALE3")

    uc_get_latest_snapshot("vale3", service=mock_svc)
    mock_svc.get_latest_snapshot.assert_called_once_with("VALE3")


# ===========================================================================
# Use case – get_intraday_series
# ===========================================================================


def test_use_case_get_intraday_series_delegates_to_service():
    mock_svc = MagicMock(spec=B3QuoteService)
    expected = _make_intraday_series()
    mock_svc.get_intraday_series.return_value = expected

    result = uc_get_intraday_series(TICKER, service=mock_svc)

    assert result is expected
    mock_svc.get_intraday_series.assert_called_once_with("PETR4")


def test_use_case_get_intraday_series_uppercases_ticker():
    mock_svc = MagicMock(spec=B3QuoteService)
    mock_svc.get_intraday_series.return_value = _make_intraday_series()

    uc_get_intraday_series("petr4", service=mock_svc)
    mock_svc.get_intraday_series.assert_called_once_with("PETR4")


# ===========================================================================
# API route – GET /quotes/{ticker}  (LatestSnapshotQuote)
# ===========================================================================


def test_route_latest_snapshot_200(client):
    with patch(
        "app.api.routes.quotes.get_latest_snapshot",
        return_value=_make_latest_snapshot(),
    ):
        resp = client.get("/quotes/PETR4")

    assert resp.status_code == 200
    data = resp.json()
    assert data["ticker"] == "PETR4"
    assert data["latest_close_price"] == "38.45"
    assert data["latest_price_fluctuation_pct"] == "0.52"
    assert data["latest_time"] == "2024-06-14T15:29:00"
    assert data["source"] == "b3_public_internal_endpoint"
    assert data["delayed"] is True


def test_route_latest_snapshot_404(client):
    with patch(
        "app.api.routes.quotes.get_latest_snapshot",
        side_effect=B3TickerNotFoundError("XXXXXX"),
    ):
        resp = client.get("/quotes/XXXXXX")

    assert resp.status_code == 404
    detail = resp.json()["detail"]
    assert isinstance(detail, dict)
    assert detail.get("ticker") == "XXXXXX"


def test_route_latest_snapshot_503(client):
    with patch(
        "app.api.routes.quotes.get_latest_snapshot",
        side_effect=B3TemporaryBlockError(429, "PETR4"),
    ):
        resp = client.get("/quotes/PETR4")

    assert resp.status_code == 503
    assert resp.json()["detail"]["error"] == "upstream_temporary_block"


def test_route_intraday_series_502(client):
    with patch(
        "app.api.routes.quotes.get_intraday_series",
        side_effect=B3UnexpectedResponseError("bad shape", status_code=200),
    ):
        resp = client.get("/quotes/PETR4/intraday")

    assert resp.status_code == 502
    assert resp.json()["detail"]["error"] == "upstream_unexpected_response"


# ===========================================================================
# Ticker reader – read_tickers_from_csv
# ===========================================================================


def _write_instruments_csv(rows: list[dict], path: Path) -> None:
    """Helper: write a minimal normalized instruments CSV to *path*."""
    fieldnames = ["Instrumento financeiro", "Ativo", "Descricao do ativo"]
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)


def test_read_tickers_happy_path(tmp_path):
    csv_path = tmp_path / "instruments.csv"
    _write_instruments_csv(
        [
            {"Instrumento financeiro": "PETR4", "Ativo": "PETR", "Descricao do ativo": "Petrobras"},
            {"Instrumento financeiro": "VALE3", "Ativo": "VALE", "Descricao do ativo": "Vale"},
            {"Instrumento financeiro": "WEGE3", "Ativo": "WEGE", "Descricao do ativo": "WEG"},
        ],
        csv_path,
    )
    tickers = read_tickers_from_csv(csv_path)
    assert tickers == ["PETR4", "VALE3", "WEGE3"]


def test_read_tickers_deduplicates(tmp_path):
    csv_path = tmp_path / "instruments.csv"
    _write_instruments_csv(
        [
            {"Instrumento financeiro": "PETR4", "Ativo": "", "Descricao do ativo": ""},
            {"Instrumento financeiro": "PETR4", "Ativo": "", "Descricao do ativo": ""},
            {"Instrumento financeiro": "VALE3", "Ativo": "", "Descricao do ativo": ""},
        ],
        csv_path,
    )
    tickers = read_tickers_from_csv(csv_path)
    assert tickers == ["PETR4", "VALE3"]
    assert len(tickers) == 2


def test_read_tickers_skips_empty_values(tmp_path):
    csv_path = tmp_path / "instruments.csv"
    _write_instruments_csv(
        [
            {"Instrumento financeiro": "", "Ativo": "", "Descricao do ativo": ""},
            {"Instrumento financeiro": "PETR4", "Ativo": "", "Descricao do ativo": ""},
            {"Instrumento financeiro": "   ", "Ativo": "", "Descricao do ativo": ""},
        ],
        csv_path,
    )
    tickers = read_tickers_from_csv(csv_path)
    assert tickers == ["PETR4"]


def test_read_tickers_skips_footer_noise(tmp_path):
    """Long strings with spaces (footer notices) must be filtered out."""
    csv_path = tmp_path / "instruments.csv"
    _write_instruments_csv(
        [
            {"Instrumento financeiro": "PETR4", "Ativo": "", "Descricao do ativo": ""},
            {
                "Instrumento financeiro": "Informamos que a tabela acima nao sera publicada",
                "Ativo": "",
                "Descricao do ativo": "",
            },
        ],
        csv_path,
    )
    tickers = read_tickers_from_csv(csv_path)
    assert tickers == ["PETR4"]


def test_read_tickers_uppercases(tmp_path):
    csv_path = tmp_path / "instruments.csv"
    _write_instruments_csv(
        [{"Instrumento financeiro": "petr4", "Ativo": "", "Descricao do ativo": ""}],
        csv_path,
    )
    tickers = read_tickers_from_csv(csv_path)
    assert tickers == ["PETR4"]


def test_read_tickers_missing_column_raises(tmp_path):
    csv_path = tmp_path / "wrong_columns.csv"
    with open(csv_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["colA", "colB"], delimiter=";")
        writer.writeheader()
        writer.writerow({"colA": "X", "colB": "Y"})

    with pytest.raises(TickerColumnMissingError, match="Instrumento financeiro"):
        read_tickers_from_csv(csv_path)


# ===========================================================================
# Batch ingestion – run_batch_quote_ingestion (intraday series)
# ===========================================================================


def _make_b3_payload(ticker: str = "PETR4") -> dict:
    return {
        "BizSts": {"cd": "OK"},
        "Msg": {"dtTm": "2026-02-26T15:30:00"},
        "TradgFlr": {
            "date": "2026-02-26",
            "scty": {
                "symb": ticker,
                "lstQtn": [
                    {"closPric": 37.90, "dtTm": "2026-02-26T10:01:00", "prcFlcn": -0.10},
                    {"closPric": 38.45, "dtTm": "2026-02-26T15:29:00", "prcFlcn": 0.52},
                ],
            },
        },
    }


def _read_report(report_path: Path) -> list[dict]:
    """Read the report CSV and return rows as a list of dicts."""
    with open(report_path, encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _read_jsonl(output_dir: Path) -> list[dict]:
    """Read all lines from the single JSONL file under output_dir."""
    import json as _json
    matches = list(output_dir.rglob("daily_fluctuation_*.jsonl"))
    if not matches:
        return []
    with open(matches[0], encoding="utf-8") as fh:
        return [_json.loads(line) for line in fh if line.strip()]


# ---------------------------------------------------------------------------
# JSONL output: one line per ticker
# ---------------------------------------------------------------------------


def test_batch_ingestion_jsonl_has_one_record_per_ticker(tmp_path):
    """JSONL must contain exactly one JSON line per successfully fetched ticker."""
    instruments_csv = tmp_path / "instruments.csv"
    _write_instruments_csv(
        [{"Instrumento financeiro": "PETR4", "Ativo": "", "Descricao do ativo": ""}],
        instruments_csv,
    )
    mock_client = MagicMock(spec=B3QuoteClient)
    mock_client.get_daily_fluctuation_history.return_value = _make_b3_payload("PETR4")

    run_batch_quote_ingestion(
        instruments_csv=instruments_csv,
        reference_date=date(2026, 2, 26),
        output_dir=tmp_path / "out",
        client=mock_client,
    )

    records = _read_jsonl(tmp_path / "out")
    assert len(records) == 1
    rec = records[0]
    assert rec["ticker_requested"] == "PETR4"
    assert rec["ticker_returned"] == "PETR4"
    assert rec["trade_date"] == "2026-02-26"
    assert rec["business_status_code"] == "OK"
    assert rec["message_datetime"] == "2026-02-26T15:30:00"
    assert rec["points_count"] == 2
    assert rec["collected_at"] != ""


def test_batch_ingestion_jsonl_price_history_array(tmp_path):
    """Each JSONL record must have a price_history array with all lstQtn points."""
    instruments_csv = tmp_path / "instruments.csv"
    _write_instruments_csv(
        [{"Instrumento financeiro": "PETR4", "Ativo": "", "Descricao do ativo": ""}],
        instruments_csv,
    )
    mock_client = MagicMock(spec=B3QuoteClient)
    mock_client.get_daily_fluctuation_history.return_value = _make_b3_payload("PETR4")

    run_batch_quote_ingestion(
        instruments_csv=instruments_csv,
        reference_date=date(2026, 2, 26),
        output_dir=tmp_path / "out",
        client=mock_client,
    )

    records = _read_jsonl(tmp_path / "out")
    history = records[0]["price_history"]
    assert len(history) == 2

    assert history[0]["quote_time"] == "2026-02-26T10:01:00"
    assert history[0]["close_price"] == "37.9"
    assert history[0]["price_fluctuation_percentage"] == "-0.1"

    assert history[1]["quote_time"] == "2026-02-26T15:29:00"
    assert history[1]["close_price"] == "38.45"
    assert history[1]["price_fluctuation_percentage"] == "0.52"


def test_batch_ingestion_jsonl_multiple_tickers(tmp_path):
    """All tickers must be written as separate lines in one JSONL file."""
    instruments_csv = tmp_path / "instruments.csv"
    _write_instruments_csv(
        [
            {"Instrumento financeiro": "PETR4", "Ativo": "", "Descricao do ativo": ""},
            {"Instrumento financeiro": "VALE3", "Ativo": "", "Descricao do ativo": ""},
        ],
        instruments_csv,
    )
    mock_client = MagicMock(spec=B3QuoteClient)
    mock_client.get_daily_fluctuation_history.side_effect = [
        _make_b3_payload("PETR4"),
        _make_b3_payload("VALE3"),
    ]

    run_batch_quote_ingestion(
        instruments_csv=instruments_csv,
        reference_date=date(2026, 2, 26),
        output_dir=tmp_path / "out",
        client=mock_client,
    )

    records = _read_jsonl(tmp_path / "out")
    assert len(records) == 2
    tickers = {r["ticker_requested"] for r in records}
    assert tickers == {"PETR4", "VALE3"}


def test_batch_ingestion_jsonl_filename_and_location(tmp_path):
    """JSONL must be placed under {output_dir}/{reference_date}/daily_fluctuation_*.jsonl."""
    instruments_csv = tmp_path / "instruments.csv"
    _write_instruments_csv(
        [{"Instrumento financeiro": "PETR4", "Ativo": "", "Descricao do ativo": ""}],
        instruments_csv,
    )
    mock_client = MagicMock(spec=B3QuoteClient)
    mock_client.get_daily_fluctuation_history.return_value = _make_b3_payload("PETR4")

    run_batch_quote_ingestion(
        instruments_csv=instruments_csv,
        reference_date=date(2026, 2, 26),
        output_dir=tmp_path / "out",
        client=mock_client,
    )

    matches = list((tmp_path / "out").rglob("daily_fluctuation_*.jsonl"))
    assert len(matches) == 1
    assert matches[0].parent.name == "2026-02-26"


# ---------------------------------------------------------------------------
# Report CSV
# ---------------------------------------------------------------------------


def test_batch_ingestion_report_has_one_row_per_ticker(tmp_path):
    """Report CSV must have one row per attempted ticker."""
    instruments_csv = tmp_path / "instruments.csv"
    _write_instruments_csv(
        [
            {"Instrumento financeiro": "PETR4", "Ativo": "", "Descricao do ativo": ""},
            {"Instrumento financeiro": "VALE3", "Ativo": "", "Descricao do ativo": ""},
        ],
        instruments_csv,
    )
    mock_client = MagicMock(spec=B3QuoteClient)
    mock_client.get_daily_fluctuation_history.side_effect = [
        _make_b3_payload("PETR4"),
        _make_b3_payload("VALE3"),
    ]

    report = run_batch_quote_ingestion(
        instruments_csv=instruments_csv,
        reference_date=date(2026, 2, 26),
        output_dir=tmp_path / "out",
        client=mock_client,
    )

    rows = _read_report(report)
    assert len(rows) == 2
    assert rows[0]["ticker_requested"] == "PETR4"
    assert rows[0]["request_succeeded"] == "true"
    assert rows[0]["points_count"] == "2"
    assert rows[0]["ticker_returned"] == "PETR4"

    assert rows[1]["ticker_requested"] == "VALE3"
    assert rows[1]["request_succeeded"] == "true"


def test_batch_ingestion_report_path_under_reference_date_folder(tmp_path):
    """Report is placed under {output_dir}/{reference_date}/report_*.csv."""
    instruments_csv = tmp_path / "instruments.csv"
    _write_instruments_csv(
        [{"Instrumento financeiro": "PETR4", "Ativo": "", "Descricao do ativo": ""}],
        instruments_csv,
    )
    mock_client = MagicMock(spec=B3QuoteClient)
    mock_client.get_daily_fluctuation_history.return_value = _make_b3_payload("PETR4")

    report = run_batch_quote_ingestion(
        instruments_csv=instruments_csv,
        reference_date=date(2026, 2, 26),
        output_dir=tmp_path / "out",
        client=mock_client,
    )

    assert report.exists()
    assert report.parent.name == "2026-02-26"
    assert report.name.startswith("report_")
    assert report.suffix == ".csv"


def test_batch_ingestion_report_columns(tmp_path):
    """Report CSV must have the required columns including http_status."""
    instruments_csv = tmp_path / "instruments.csv"
    _write_instruments_csv(
        [{"Instrumento financeiro": "PETR4", "Ativo": "", "Descricao do ativo": ""}],
        instruments_csv,
    )
    mock_client = MagicMock(spec=B3QuoteClient)
    mock_client.get_daily_fluctuation_history.return_value = _make_b3_payload("PETR4")

    report = run_batch_quote_ingestion(
        instruments_csv=instruments_csv,
        reference_date=date(2026, 2, 26),
        output_dir=tmp_path / "out",
        client=mock_client,
    )

    with open(report, encoding="utf-8", newline="") as fh:
        fieldnames = csv.DictReader(fh).fieldnames

    expected = {
        "ticker_requested", "ticker_returned", "trade_date",
        "http_status", "request_succeeded", "points_count", "error_message",
    }
    assert set(fieldnames) == expected


# ---------------------------------------------------------------------------
# Failure handling
# ---------------------------------------------------------------------------


def test_batch_ingestion_failure_row_in_report(tmp_path):
    """A failed ticker produces a report row with request_succeeded=false."""
    instruments_csv = tmp_path / "instruments.csv"
    _write_instruments_csv(
        [{"Instrumento financeiro": "XXXXXX", "Ativo": "", "Descricao do ativo": ""}],
        instruments_csv,
    )
    mock_client = MagicMock(spec=B3QuoteClient)
    mock_client.get_daily_fluctuation_history.side_effect = B3TickerNotFoundError("XXXXXX")

    report = run_batch_quote_ingestion(
        instruments_csv=instruments_csv,
        reference_date=date(2026, 2, 26),
        output_dir=tmp_path / "out",
        client=mock_client,
    )

    rows = _read_report(report)
    assert len(rows) == 1
    assert rows[0]["ticker_requested"] == "XXXXXX"
    assert rows[0]["request_succeeded"] == "false"
    assert "XXXXXX" in rows[0]["error_message"]
    assert rows[0]["ticker_returned"] == ""
    assert rows[0]["points_count"] == ""

    # Failed tickers must NOT appear in the JSONL
    records = _read_jsonl(tmp_path / "out")
    assert all(r["ticker_requested"] != "XXXXXX" for r in records)


def test_batch_ingestion_does_not_abort_on_single_failure(tmp_path):
    """One ticker failing must not prevent other tickers from being processed."""
    instruments_csv = tmp_path / "instruments.csv"
    _write_instruments_csv(
        [
            {"Instrumento financeiro": "GOOD1", "Ativo": "", "Descricao do ativo": ""},
            {"Instrumento financeiro": "BAD2", "Ativo": "", "Descricao do ativo": ""},
            {"Instrumento financeiro": "GOOD3", "Ativo": "", "Descricao do ativo": ""},
        ],
        instruments_csv,
    )
    mock_client = MagicMock(spec=B3QuoteClient)
    mock_client.get_daily_fluctuation_history.side_effect = [
        _make_b3_payload("GOOD1"),
        B3TemporaryBlockError(429, "BAD2"),
        _make_b3_payload("GOOD3"),
    ]

    report = run_batch_quote_ingestion(
        instruments_csv=instruments_csv,
        reference_date=date(2026, 2, 26),
        output_dir=tmp_path / "out",
        client=mock_client,
    )

    rows = _read_report(report)
    assert len(rows) == 3
    assert rows[0]["request_succeeded"] == "true"
    assert rows[1]["request_succeeded"] == "false"
    assert rows[2]["request_succeeded"] == "true"

    # JSONL should only contain the 2 successful tickers
    records = _read_jsonl(tmp_path / "out")
    assert len(records) == 2
    tickers_in_jsonl = {r["ticker_requested"] for r in records}
    assert tickers_in_jsonl == {"GOOD1", "GOOD3"}


# ---------------------------------------------------------------------------
# Empty ticker list (entrypoint contract)
# ---------------------------------------------------------------------------


def test_batch_ingestion_empty_ticker_list_writes_report_only(tmp_path):
    """When instruments CSV has no valid tickers, report has header only and JSONL is empty."""
    instruments_csv = tmp_path / "instruments.csv"
    _write_instruments_csv(
        [
            {"Instrumento financeiro": "", "Ativo": "", "Descricao do ativo": ""},
            {"Instrumento financeiro": "   ", "Ativo": "", "Descricao do ativo": ""},
        ],
        instruments_csv,
    )
    mock_client = MagicMock(spec=B3QuoteClient)
    report = run_batch_quote_ingestion(
        instruments_csv=instruments_csv,
        reference_date=date(2026, 2, 26),
        output_dir=tmp_path / "out",
        client=mock_client,
    )
    rows = _read_report(report)
    assert len(rows) == 0
    mock_client.get_daily_fluctuation_history.assert_not_called()
    records = _read_jsonl(tmp_path / "out")
    assert len(records) == 0


# ---------------------------------------------------------------------------
# Empty lstQtn
# ---------------------------------------------------------------------------


def test_batch_ingestion_empty_lstqtn_writes_empty_price_history(tmp_path):
    """A ticker with empty lstQtn writes a JSONL record with price_history=[] and points_count=0."""
    instruments_csv = tmp_path / "instruments.csv"
    _write_instruments_csv(
        [{"Instrumento financeiro": "PETR4", "Ativo": "", "Descricao do ativo": ""}],
        instruments_csv,
    )
    empty_payload = {
        "BizSts": {"cd": "OK"},
        "Msg": {"dtTm": "2026-02-26T15:30:00"},
        "TradgFlr": {
            "date": "2026-02-26",
            "scty": {"symb": "PETR4", "lstQtn": []},
        },
    }
    mock_client = MagicMock(spec=B3QuoteClient)
    mock_client.get_daily_fluctuation_history.return_value = empty_payload

    report = run_batch_quote_ingestion(
        instruments_csv=instruments_csv,
        reference_date=date(2026, 2, 26),
        output_dir=tmp_path / "out",
        client=mock_client,
    )

    # Report: request succeeded, 0 points
    rows = _read_report(report)
    assert rows[0]["request_succeeded"] == "true"
    assert rows[0]["points_count"] == "0"

    # JSONL: record exists with empty price_history
    records = _read_jsonl(tmp_path / "out")
    assert len(records) == 1
    assert records[0]["price_history"] == []
    assert records[0]["points_count"] == 0

# API route – GET /quotes/{ticker}/intraday  (IntradaySeriesQuote)
# ===========================================================================


def test_route_intraday_series_200(client):
    with patch(
        "app.api.routes.quotes.get_intraday_series",
        return_value=_make_intraday_series(),
    ):
        resp = client.get("/quotes/PETR4/intraday")

    assert resp.status_code == 200
    data = resp.json()
    assert data["ticker"] == "PETR4"
    assert data["point_count"] == 2
    assert len(data["points"]) == 2
    assert data["points"][0]["close_price"] == "37.90"
    assert data["points"][-1]["close_price"] == "38.45"
    assert data["source"] == "b3_public_internal_endpoint"
    assert data["delayed"] is True


def test_route_intraday_series_empty_points(client):
    """A 200 with an empty points list is valid (market closed / pre-open)."""
    from app.integrations.b3.models import IntradaySeriesQuote
    empty_series = IntradaySeriesQuote(
        ticker="PETR4",
        trade_date=date(2024, 6, 14),
        message_datetime=None,
        points=[],
        source="b3_public_internal_endpoint",
        delayed=True,
        fetched_at=datetime(2024, 6, 14, 9, 0, 0, tzinfo=UTC),
        raw_data=None,
    )
    with patch("app.api.routes.quotes.get_intraday_series", return_value=empty_series):
        resp = client.get("/quotes/PETR4/intraday")

    assert resp.status_code == 200
    assert resp.json()["point_count"] == 0
    assert resp.json()["points"] == []


def test_route_intraday_series_404(client):
    with patch(
        "app.api.routes.quotes.get_intraday_series",
        side_effect=B3TickerNotFoundError("XXXXXX"),
    ):
        resp = client.get("/quotes/XXXXXX/intraday")

    assert resp.status_code == 404
    detail = resp.json()["detail"]
    assert isinstance(detail, dict)
    assert detail.get("ticker") == "XXXXXX"


def test_route_intraday_series_503(client):
    with patch(
        "app.api.routes.quotes.get_intraday_series",
        side_effect=B3TemporaryBlockError(403, "PETR4"),
    ):
        resp = client.get("/quotes/PETR4/intraday")

    assert resp.status_code == 503
    assert resp.json()["detail"]["error"] == "upstream_temporary_block"

