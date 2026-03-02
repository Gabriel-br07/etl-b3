"""Pydantic models for the B3 DailyFluctuationHistory integration.

Three layers:
- Raw API models: mirror the *real* JSON shape returned by the B3 endpoint.
- Normalised output models: clean, typed domain objects consumed by the
  service and API layers.
- ``NormalizedQuote``: legacy snapshot model kept for backward compatibility
  with existing routes, use cases and tests.

Real B3 payload structure (GET DailyFluctuationHistory/{ticker})::

    {
        "BizSts": {"cd": "..."},
        "Msg":    {"dtTm": "2024-06-14T15:30:00"},
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

All raw models use ``extra="allow"`` so that new fields from future API
changes are preserved in ``model_extra`` rather than silently dropped.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Raw API models – mirrors the real B3 JSON shape exactly
# ---------------------------------------------------------------------------


class RawBizSts(BaseModel):
    """Business status block from the B3 response envelope."""

    model_config = ConfigDict(extra="allow")

    cd: str | None = Field(None, description="Business status code, e.g. 'OK'.")


class RawMsg(BaseModel):
    """Message metadata block (timestamp of the response generation)."""

    model_config = ConfigDict(extra="allow")

    dtTm: str | None = Field(None, description="ISO datetime string of the message.")


class RawQuotePoint(BaseModel):
    """A single minute-level intraday quote point from ``lstQtn``."""

    model_config = ConfigDict(extra="allow")

    closPric: Any = Field(None, description="Close price at this point.")
    dtTm: str | None = Field(None, description="ISO datetime string of this point.")
    prcFlcn: Any = Field(None, description="Price fluctuation percentage at this point.")


class RawSecurity(BaseModel):
    """Security block inside the trading floor, holds the quote list."""

    model_config = ConfigDict(extra="allow")

    symb: str | None = Field(None, description="Ticker symbol, e.g. 'PETR4'.")
    lstQtn: list[RawQuotePoint] = Field(
        default_factory=list,
        description="Minute-level intraday quote points for the session.",
    )


class RawTradingFloor(BaseModel):
    """Trading floor block containing date and security data."""

    model_config = ConfigDict(extra="allow")

    date: str | None = Field(None, description="Trade date string (YYYY-MM-DD or similar).")
    scty: RawSecurity | None = Field(None, description="Security / quote data.")


class RawDailyFluctuationResponse(BaseModel):
    """Top-level raw B3 DailyFluctuationHistory response.

    Field names follow the exact casing of the B3 JSON response.
    Unknown top-level fields are preserved in ``model_extra``.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    BizSts: RawBizSts | None = Field(None, description="Business status envelope.")
    Msg: RawMsg | None = Field(None, description="Message metadata.")
    TradgFlr: RawTradingFloor | None = Field(None, description="Trading floor data.")

    @property
    def raw_data(self) -> dict[str, Any]:
        """Return the full raw dict including extra fields for debugging."""
        return {**self.model_dump(by_alias=True), **(self.model_extra or {})}


# ---------------------------------------------------------------------------
# Legacy raw model – kept for backward compatibility with existing parser tests
# ---------------------------------------------------------------------------


class RawDailyFluctuation(BaseModel):
    """Legacy flat mapping used by older parser code.

    Kept so that existing tests that construct this model directly continue
    to work.  New code should use ``RawDailyFluctuationResponse`` instead.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    ticker: str | None = Field(None, alias="ticker")
    codNeg: str | None = Field(None, alias="codNeg")
    trade_date: str | None = Field(None, alias="tradeDate")
    last_price: str | None = Field(None, alias="lastPrice")
    min_price: str | None = Field(None, alias="minPrice")
    max_price: str | None = Field(None, alias="maxPrice")
    average_price: str | None = Field(None, alias="avgPrice")
    oscillation_val: str | None = Field(None, alias="oscillationVal")
    results: list[dict[str, Any]] | None = None

    @property
    def raw_data(self) -> dict[str, Any]:
        """Return the full raw dict including extra fields for debugging."""
        return {**self.model_dump(by_alias=True), **(self.model_extra or {})}


# ---------------------------------------------------------------------------
# Normalised intraday output models
# ---------------------------------------------------------------------------


class IntradayPoint(BaseModel):
    """A single normalised intraday quote point (one item from ``lstQtn``)."""

    model_config = ConfigDict(extra="forbid")

    time: str | None = Field(None, description="ISO datetime string of this point.")
    close_price: Decimal | None = Field(None, description="Close price at this point.")
    price_fluctuation_pct: Decimal | None = Field(
        None, description="Price fluctuation percentage vs previous close."
    )


class IntradaySeriesQuote(BaseModel):
    """Full intraday series response – all minute-level points for the session."""

    model_config = ConfigDict(extra="forbid")

    ticker: str = Field(..., description="Uppercased B3 ticker symbol, e.g. PETR4.")
    trade_date: date = Field(..., description="Reference trade date for this series.")
    message_datetime: str | None = Field(
        None, description="Datetime of the B3 response message."
    )
    points: list[IntradayPoint] = Field(
        default_factory=list,
        description="Minute-level intraday quote points, ordered oldest-first.",
    )
    source: str = Field(
        default="b3_public_internal_endpoint",
        description="Identifier of the data origin.",
    )
    delayed: bool = Field(
        default=True,
        description="B3 public endpoint data is always delayed (not real-time).",
    )
    fetched_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="UTC timestamp when the data was fetched.",
    )
    raw_data: dict[str, Any] | None = Field(
        None,
        description="Full raw API payload, kept for debugging.",
    )


class LatestSnapshotQuote(BaseModel):
    """Latest-point snapshot – derived from the *last* item in ``lstQtn``."""

    model_config = ConfigDict(extra="forbid")

    ticker: str = Field(..., description="Uppercased B3 ticker symbol, e.g. PETR4.")
    trade_date: date = Field(..., description="Reference trade date.")
    message_datetime: str | None = Field(
        None, description="Datetime of the B3 response message."
    )
    latest_time: str | None = Field(
        None, description="Datetime of the most recent quote point."
    )
    latest_close_price: Decimal | None = Field(
        None, description="Close price of the most recent quote point."
    )
    latest_price_fluctuation_pct: Decimal | None = Field(
        None, description="Price fluctuation % of the most recent quote point."
    )
    source: str = Field(
        default="b3_public_internal_endpoint",
        description="Identifier of the data origin.",
    )
    delayed: bool = Field(
        default=True,
        description="B3 public endpoint data is always delayed (not real-time).",
    )
    fetched_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="UTC timestamp when the data was fetched.",
    )
    raw_data: dict[str, Any] | None = Field(
        None,
        description="Full raw API payload, kept for debugging.",
    )


# ---------------------------------------------------------------------------
# Legacy normalised output model – kept for backward compatibility
# ---------------------------------------------------------------------------


class NormalizedQuote(BaseModel):
    """Legacy clean quote snapshot consumed by the service and API layers.

    Kept for backward compatibility with existing routes, use cases and tests.
    New code should prefer ``LatestSnapshotQuote`` or ``IntradaySeriesQuote``.
    """

    model_config = ConfigDict(extra="forbid")

    ticker: str = Field(..., description="Uppercased B3 ticker symbol, e.g. PETR4")
    trade_date: date = Field(..., description="Reference trade date for this snapshot")
    last_price: Decimal | None = Field(None, description="Last traded price")
    min_price: Decimal | None = Field(None, description="Session minimum price")
    max_price: Decimal | None = Field(None, description="Session maximum price")
    average_price: Decimal | None = Field(None, description="Session VWAP / average price")
    oscillation_pct: Decimal | None = Field(
        None, description="Price oscillation percentage vs previous close"
    )
    source: str = Field(
        default="b3_public_internal_endpoint",
        description="Identifier of the data origin.",
    )
    delayed: bool = Field(
        default=True,
        description="B3 public endpoint data is always delayed (not real-time).",
    )
    fetched_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="UTC timestamp when the snapshot was fetched.",
    )
    raw_data: dict[str, Any] | None = Field(
        None,
        description="Full raw API payload, kept for debugging and forward-compatibility.",
        exclude=False,
    )
