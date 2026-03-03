"""Parser / normaliser for B3 DailyFluctuationHistory raw payloads.

Responsibility: convert a raw ``dict`` (straight from the HTTP response)
into validated domain models.  This is the *only* module that knows about
raw B3 API field names.

Real B3 payload shape::

    {
        "BizSts":   {"cd": "OK"},
        "Msg":      {"dtTm": "2024-06-14T15:30:00"},
        "TradgFlr": {
            "date": "2024-06-14",
            "scty": {
                "symb": "PETR4",
                "lstQtn": [
                    {"closPric": 38.45, "dtTm": "2024-06-14T10:01:00", "prcFlcn": 0.52},
                    ...
                ]
            }
        }
    }

The module also supports a legacy flat format (``codNeg``, ``lastPrice``, etc.)
used by tests that predate the real payload discovery.

Raises ``B3UnexpectedResponseError`` when the payload is missing required
fields or cannot be parsed, so callers never receive half-formed data.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from app.integrations.b3.exceptions import B3UnexpectedResponseError
from app.integrations.b3.models import (
    IntradayPoint,
    IntradaySeriesQuote,
    LatestSnapshotQuote,
    NormalizedQuote,
    RawDailyFluctuation,
    RawDailyFluctuationResponse,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _to_decimal(value: Any) -> Decimal | None:
    """Safely coerce *value* to Decimal, returning None on failure."""
    if value is None:
        return None
    try:
        # B3 uses comma as decimal separator in some locales; normalise first.
        clean = str(value).strip().replace(",", ".").replace(" ", "")
        if clean in ("", "-", "N/A"):
            return None
        return Decimal(clean)
    except InvalidOperation:
        logger.debug("Could not convert %r to Decimal – treating as None.", value)
        return None


def _to_date(value: Any) -> date | None:
    """Parse a date string in either ISO (YYYY-MM-DD) or BR (DD/MM/YYYY) format."""
    if value is None:
        return None
    raw = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    logger.debug("Could not parse date string %r – returning None.", raw)
    return None


def _resolve_ticker(raw: RawDailyFluctuation, fallback: str) -> str:
    """Return the ticker from the raw model, falling back to the request ticker."""
    return (raw.codNeg or raw.ticker or fallback).strip().upper()


def _unwrap_payload(data: dict[str, Any]) -> dict[str, Any]:
    """Unwrap ``{"results": [{...}]}`` nesting used by some B3 endpoints."""
    if "results" in data and isinstance(data["results"], list) and data["results"]:
        logger.debug("Unwrapping nested 'results' list from B3 payload.")
        return data["results"][0]
    return data


def _is_real_payload(data: dict[str, Any]) -> bool:
    """Return True when *data* looks like the real TradgFlr-based payload."""
    return "TradgFlr" in data or "BizSts" in data


# ---------------------------------------------------------------------------
# Real-payload parsers
# ---------------------------------------------------------------------------


def _parse_real_intraday(
    raw_payload: dict[str, Any],
    *,
    requested_ticker: str,
) -> tuple[RawDailyFluctuationResponse, str, date, str | None]:
    """Validate the real payload and return the parsed envelope + key fields.

    Returns:
        (envelope, ticker, trade_date, message_datetime)

    Raises:
        B3UnexpectedResponseError: if the payload is structurally invalid.
    """
    try:
        envelope = RawDailyFluctuationResponse.model_validate(raw_payload)
    except Exception as exc:
        raise B3UnexpectedResponseError(
            f"Failed to validate B3 payload for '{requested_ticker}': {exc}",
            raw_body=str(raw_payload)[:500],
        ) from exc

    # Resolve ticker
    symb: str | None = None
    if envelope.TradgFlr and envelope.TradgFlr.scty:
        symb = envelope.TradgFlr.scty.symb
    ticker = (symb or requested_ticker).strip().upper()

    # Resolve trade date
    date_str: str | None = envelope.TradgFlr.date if envelope.TradgFlr else None
    trade_date = _to_date(date_str)
    if trade_date is None:
        logger.warning(
            "trade_date missing in B3 payload for ticker '%s'; falling back to today.",
            ticker,
        )
        trade_date = date.today()

    # Resolve message datetime (informational)
    msg_dt: str | None = envelope.Msg.dtTm if envelope.Msg else None

    return envelope, ticker, trade_date, msg_dt


def parse_intraday_series(
    raw_payload: dict[str, Any],
    *,
    requested_ticker: str,
) -> IntradaySeriesQuote:
    """Parse a real B3 DailyFluctuationHistory payload into a full intraday series.

    Args:
        raw_payload: The JSON-decoded response body from the B3 API.
        requested_ticker: The ticker that was requested (used as fallback).

    Returns:
        A validated :class:`IntradaySeriesQuote` with all minute-level points.

    Raises:
        B3UnexpectedResponseError: Empty payload, missing structure, or parse error.
    """
    if not raw_payload:
        raise B3UnexpectedResponseError(
            f"B3 returned an empty payload for ticker '{requested_ticker}'.",
            raw_body=str(raw_payload),
        )

    envelope, ticker, trade_date, msg_dt = _parse_real_intraday(
        raw_payload, requested_ticker=requested_ticker
    )

    lst_qtn = (
        envelope.TradgFlr.scty.lstQtn
        if envelope.TradgFlr and envelope.TradgFlr.scty
        else []
    )

    if not lst_qtn:
        logger.warning(
            "lstQtn is empty in B3 payload for ticker '%s' date='%s'.",
            ticker, trade_date,
        )

    points = [
        IntradayPoint(
            time=qp.dtTm,
            close_price=_to_decimal(qp.closPric),
            price_fluctuation_pct=_to_decimal(qp.prcFlcn),
        )
        for qp in lst_qtn
    ]

    return IntradaySeriesQuote(
        ticker=ticker,
        trade_date=trade_date,
        message_datetime=msg_dt,
        points=points,
        source="b3_public_internal_endpoint",
        delayed=True,
        raw_data=envelope.raw_data,
    )


def parse_latest_snapshot(
    raw_payload: dict[str, Any],
    *,
    requested_ticker: str,
) -> LatestSnapshotQuote:
    """Parse a real B3 payload and return only the *last* point in ``lstQtn``.

    The last item in the list is the most recent intraday quote available.

    Args:
        raw_payload: The JSON-decoded response body from the B3 API.
        requested_ticker: The ticker that was requested (used as fallback).

    Returns:
        A validated :class:`LatestSnapshotQuote`.

    Raises:
        B3UnexpectedResponseError: Empty payload, missing structure, or parse error.
    """
    if not raw_payload:
        raise B3UnexpectedResponseError(
            f"B3 returned an empty payload for ticker '{requested_ticker}'.",
            raw_body=str(raw_payload),
        )

    envelope, ticker, trade_date, msg_dt = _parse_real_intraday(
        raw_payload, requested_ticker=requested_ticker
    )

    lst_qtn = (
        envelope.TradgFlr.scty.lstQtn
        if envelope.TradgFlr and envelope.TradgFlr.scty
        else []
    )

    last_point = lst_qtn[-1] if lst_qtn else None

    if last_point is None:
        logger.warning(
            "lstQtn is empty; latest_close_price will be None for ticker '%s'.",
            ticker,
        )

    return LatestSnapshotQuote(
        ticker=ticker,
        trade_date=trade_date,
        message_datetime=msg_dt,
        latest_time=last_point.dtTm if last_point else None,
        latest_close_price=_to_decimal(last_point.closPric) if last_point else None,
        latest_price_fluctuation_pct=_to_decimal(last_point.prcFlcn) if last_point else None,
        source="b3_public_internal_endpoint",
        delayed=True,
        raw_data=envelope.raw_data,
    )


# ---------------------------------------------------------------------------
# Legacy flat-payload parser (backward-compatible)
# ---------------------------------------------------------------------------


def _parse_legacy_flat(
    raw_payload: dict[str, Any],
    *,
    requested_ticker: str,
) -> NormalizedQuote:
    """Parse the legacy flat payload format into a ``NormalizedQuote``."""
    try:
        flat = _unwrap_payload(raw_payload)
        raw = RawDailyFluctuation.model_validate(flat)
    except Exception as exc:
        raise B3UnexpectedResponseError(
            f"Failed to validate raw B3 payload for '{requested_ticker}': {exc}",
            raw_body=str(raw_payload)[:500],
        ) from exc

    ticker = _resolve_ticker(raw, fallback=requested_ticker)
    trade_date = _to_date(raw.trade_date)

    if trade_date is None:
        logger.warning(
            "trade_date is missing from B3 payload for ticker '%s'. "
            "Falling back to today's date. raw_data=%s",
            ticker,
            raw.raw_data,
        )
        trade_date = date.today()

    return NormalizedQuote(
        ticker=ticker,
        trade_date=trade_date,
        last_price=_to_decimal(raw.last_price),
        min_price=_to_decimal(raw.min_price),
        max_price=_to_decimal(raw.max_price),
        average_price=_to_decimal(raw.average_price),
        oscillation_pct=_to_decimal(raw.oscillation_val),
        source="b3_public_internal_endpoint",
        delayed=True,
        raw_data=raw.raw_data,
    )


# ---------------------------------------------------------------------------
# Public API (primary entry point)
# ---------------------------------------------------------------------------


def parse_daily_fluctuation(
    raw_payload: dict[str, Any],
    *,
    requested_ticker: str,
) -> NormalizedQuote:
    """Parse and normalise a B3 DailyFluctuationHistory payload.

    Detects payload format automatically:

    - **Real format** (``TradgFlr`` / ``BizSts`` keys): extracts the latest
      point from ``lstQtn`` and maps it onto the ``NormalizedQuote`` fields.
    - **Legacy flat format** (``codNeg``, ``lastPrice``, etc.): handled by
      the legacy parser for backward compatibility with existing tests.

    Args:
        raw_payload: The JSON-decoded response body from the B3 API.
        requested_ticker: The ticker that was requested (fallback for missing
            ticker in payload).

    Returns:
        A fully validated :class:`NormalizedQuote`.

    Raises:
        B3UnexpectedResponseError: Empty payload, missing structure, parse error.
    """
    if not raw_payload:
        raise B3UnexpectedResponseError(
            f"B3 returned an empty payload for ticker '{requested_ticker}'.",
            raw_body=str(raw_payload),
        )

    if _is_real_payload(raw_payload):
        # Real payload: derive a NormalizedQuote from the last lstQtn point.
        snapshot = parse_latest_snapshot(raw_payload, requested_ticker=requested_ticker)
        return NormalizedQuote(
            ticker=snapshot.ticker,
            trade_date=snapshot.trade_date,
            last_price=snapshot.latest_close_price,
            min_price=None,
            max_price=None,
            average_price=None,
            oscillation_pct=snapshot.latest_price_fluctuation_pct,
            source=snapshot.source,
            delayed=snapshot.delayed,
            fetched_at=snapshot.fetched_at,
            raw_data=snapshot.raw_data,
        )

    # Legacy flat format (codNeg / lastPrice / etc.)
    return _parse_legacy_flat(raw_payload, requested_ticker=requested_ticker)

