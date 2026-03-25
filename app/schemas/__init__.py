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


# ---------------------------------------------------------------------------
# COTAHIST (fact_cotahist_daily)
# ---------------------------------------------------------------------------


class CotahistDailyRead(BaseModel):
    """One COTAHIST type-01 row as exposed by the API.

    ``close_price`` is the COTAHIST last/settlement price (DB column ``last_price``).
    """

    ticker: str = Field(..., examples=["PETR4"])
    trade_date: date = Field(..., examples=["2024-01-02"])
    open_price: Decimal | None = None
    min_price: Decimal | None = None
    max_price: Decimal | None = None
    avg_price: Decimal | None = None
    close_price: Decimal | None = Field(
        None,
        description="Last / settlement price from COTAHIST (maps from last_price in DB).",
        examples=["38.45"],
    )
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    trade_count: int | None = None
    quantity_total: Decimal | None = None
    volume_financial: Decimal | None = None
    isin: str = Field(..., examples=["BRPETRACNOR9"])
    especi: str = Field(..., examples=["ON"])
    tp_merc: str = Field(..., examples=["010"])
    expiration_date: date | None = None
    strike_price: Decimal | None = None


# ---------------------------------------------------------------------------
# ETL runs list
# ---------------------------------------------------------------------------


class ETLRunListItem(BaseModel):
    """Paginated ETL execution row for observability APIs."""

    run_id: int = Field(..., description="Primary key of the run (etl_runs.id).")
    pipeline_name: str
    status: str
    started_at: datetime = Field(
        ...,
        description="When the run started (use as audit timestamp; there is no separate created_at).",
    )
    finished_at: datetime | None = None
    duration_seconds: float | None = Field(
        None,
        description="finished_at - started_at in seconds when both are set.",
    )
    source_date: date | None = None
    source_file: str | None = None
    message: str | None = Field(
        None,
        description=(
            "Free-form message for the run: typically error text when status is failed, "
            "or a summary/other note when status is successful; may be null."
        ),
    )
    rows_inserted: int | None = None
    rows_failed: int | None = None


# ---------------------------------------------------------------------------
# Candles
# ---------------------------------------------------------------------------


class CandleRead(BaseModel):
    """OHLC candle bucket."""

    interval: str = Field(..., examples=["1d", "15m"])
    start_time: datetime = Field(..., description="Bucket start (timezone-aware).")
    end_time: datetime = Field(..., description="Bucket end (timezone-aware).")
    open: Decimal | None = None
    high: Decimal | None = None
    low: Decimal | None = None
    close: Decimal | None = None
    volume: Decimal | None = Field(
        None,
        description="Financial volume when available (e.g. fact_daily_quotes); null for intraday buckets.",
    )
    point_count: int | None = Field(
        None,
        description="Number of raw quotes aggregated into this candle (fact-quotes route).",
    )


# ---------------------------------------------------------------------------
# Indicators
# ---------------------------------------------------------------------------


class IndicatorValuePoint(BaseModel):
    """Single timestamped indicator value."""

    as_of: date = Field(..., description="Trade date the value applies to.")
    value: Decimal | None = Field(None, description="Null when not enough history for the period.")


class IndicatorSeriesRead(BaseModel):
    ticker: str
    indicator: str = Field(..., examples=["SMA", "EMA", "RSI"])
    period: int = Field(..., ge=2, examples=[14])
    source_range: dict[str, date | None] = Field(
        ...,
        description="Input close series span: keys ``start`` and ``end`` (trade_date).",
        examples=[{"start": "2024-01-02", "end": "2024-06-14"}],
    )
    values: list[IndicatorValuePoint]


# ---------------------------------------------------------------------------
# Market overview
# ---------------------------------------------------------------------------


class MarketMoverRead(BaseModel):
    ticker: str
    variation_pct: Decimal | None = None
    last_price: Decimal | None = None


class MarketVolumeRankRead(BaseModel):
    ticker: str
    trade_count: int | None = None
    financial_volume: Decimal | None = None


class MarketOverviewRead(BaseModel):
    trade_date: date
    top_gainers: list[MarketMoverRead] = Field(
        default_factory=list,
        description="From fact_daily_quotes: highest variation_pct (nulls excluded); tie-break ticker asc.",
    )
    top_losers: list[MarketMoverRead] = Field(
        default_factory=list,
        description="From fact_daily_quotes: lowest variation_pct (nulls excluded); tie-break ticker asc.",
    )
    most_traded_by_volume: list[MarketVolumeRankRead] = Field(
        default_factory=list,
        description=(
            "From fact_daily_trades: highest financial_volume (BRL turnover); tie-break ticker asc. "
            "Same ordering as most_traded_by_financial_volume; distinct from highest_trade_count."
        ),
    )
    most_traded_by_financial_volume: list[MarketVolumeRankRead] = Field(
        default_factory=list,
        description=(
            "From fact_daily_trades: highest financial_volume; tie-break ticker asc "
            "(same ranking as most_traded_by_volume)."
        ),
    )
    highest_trade_count: list[MarketVolumeRankRead] = Field(
        default_factory=list,
        description="Same ordering as most_traded_by_volume (trade_count desc).",
    )
    traded_assets_count: int = Field(
        0,
        description="Distinct tickers with a fact_daily_trades row on trade_date.",
    )


# ---------------------------------------------------------------------------
# Asset coverage / overview
# ---------------------------------------------------------------------------


class DateRangeRead(BaseModel):
    min_date: date | None = None
    max_date: date | None = None


class AssetCoverageRead(BaseModel):
    ticker: str
    has_asset_master: bool = False
    daily_quotes: DateRangeRead | None = None
    daily_trades: DateRangeRead | None = None
    fact_quotes: DateRangeRead | None = None
    cotahist: DateRangeRead | None = None
    live_delayed_b3: bool = Field(
        True,
        description="Whether the API can call B3 delayed public endpoints for this ticker (best-effort).",
    )


class AssetOverviewSectionMeta(BaseModel):
    has_data: bool = False


class AssetOverviewRead(BaseModel):
    ticker: str
    asset: AssetRead | None = None
    latest_daily_quote: DailyQuoteRead | None = None
    latest_daily_trade: TradeRead | None = None
    latest_intraday_db: FactQuoteRead | None = Field(
        None,
        description="Latest persisted intraday point (fact_quotes).",
    )
    live_snapshot: dict | None = Field(
        None,
        description="Delayed B3 snapshot; null if upstream failed or unavailable.",
    )
    historical_range_daily_quotes: DateRangeRead | None = None
    historical_range_daily_trades: DateRangeRead | None = None
    historical_range_cotahist: DateRangeRead | None = None
    variation_pct_latest: Decimal | None = Field(
        None,
        description="From latest daily quote when present.",
    )
    financial_volume_latest: Decimal | None = None
    latest_update_at: datetime | None = Field(
        None,
        description="Most recent ingested_at among included DB sections.",
    )
    sections: dict[str, AssetOverviewSectionMeta] = Field(
        default_factory=dict,
        description="Which logical sections returned data (e.g. asset, daily_quote, daily_trade).",
    )
