"""Pydantic schemas for the ETL-B3 API."""

from datetime import date, datetime
from decimal import Decimal
from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Asset schemas
# ---------------------------------------------------------------------------


class AssetBase(BaseModel):
    ticker: str = Field(..., examples=["PETR4"])
    asset_name: str | None = Field(None, examples=["PETROBRAS PN N2"])
    isin: str | None = Field(None, examples=["BRPETRACNPR6"])
    segment: str | None = Field(None, examples=["Novo Mercado"])
    source_file_date: date | None = None


class AssetCreate(AssetBase):
    pass


class AssetRead(AssetBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    created_at: datetime
    updated_at: datetime


# ---------------------------------------------------------------------------
# Daily quote schemas
# ---------------------------------------------------------------------------


class DailyQuoteBase(BaseModel):
    ticker: str = Field(..., examples=["PETR4"])
    trade_date: date = Field(..., examples=["2024-06-14"])
    last_price: Decimal | None = Field(None, examples=["38.45"])
    min_price: Decimal | None = None
    max_price: Decimal | None = None
    avg_price: Decimal | None = None
    variation_pct: Decimal | None = None
    financial_volume: Decimal | None = None
    trade_count: int | None = None
    source_file_name: str | None = None


class DailyQuoteRead(DailyQuoteBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    ingested_at: datetime


# ---------------------------------------------------------------------------
# Trade schemas
# ---------------------------------------------------------------------------


class TradeRead(BaseModel):
    """Daily consolidated trade (NegociosConsolidados) for one ticker/date."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    ticker: str = Field(..., examples=["PETR4"])
    trade_date: date = Field(..., examples=["2024-06-14"])
    open_price: Decimal | None = None
    close_price: Decimal | None = None
    min_price: Decimal | None = None
    max_price: Decimal | None = None
    avg_price: Decimal | None = None
    variation_pct: Decimal | None = None
    financial_volume: Decimal | None = None
    trade_count: int | None = None
    source_file_name: str | None = None
    ingested_at: datetime


# ---------------------------------------------------------------------------
# Fact quote (intraday) schemas
# ---------------------------------------------------------------------------


class FactQuoteRead(BaseModel):
    """Single intraday quote point from the fact_quotes hypertable."""

    model_config = ConfigDict(from_attributes=True)

    ticker: str = Field(..., examples=["PETR4"])
    quoted_at: datetime = Field(..., description="Timestamp of the quote (timezone-aware).")
    trade_date: date = Field(..., examples=["2024-06-14"])
    close_price: Decimal | None = None
    price_fluctuation_pct: Decimal | None = None


# ---------------------------------------------------------------------------
# ETL Run schemas
# ---------------------------------------------------------------------------


class ETLRunRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    flow_name: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    message: str | None


class ETLRunRequest(BaseModel):
    """Request body for triggering an ETL run."""

    source_mode: str = Field(
        "local",
        examples=["local", "remote"],
        description="Use 'local' to read from B3_DATA_DIR, 'remote' to download.",
    )


class ETLBackfillRequest(BaseModel):
    """Request body for backfill ETL."""

    date_from: date = Field(..., examples=["2024-06-01"])
    date_to: date = Field(..., examples=["2024-06-14"])
    source_mode: str = Field("local", examples=["local", "remote"])


# ---------------------------------------------------------------------------
# Generic pagination / response wrappers
# ---------------------------------------------------------------------------


class PaginatedResponse(BaseModel, Generic[T]):
    """Generic paginated list response."""

    total: int
    limit: int
    offset: int
    items: list[T] = Field(default_factory=list)


class HealthResponse(BaseModel):
    status: str = Field(..., examples=["ok"])
    version: str
    environment: str
