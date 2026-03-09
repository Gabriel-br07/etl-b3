"""SQLAlchemy ORM models.

Tables
------
dim_assets          – instrument master data (one row per ticker)
fact_daily_trades   – consolidated trading data by asset/date (negocios consolidados)
fact_daily_quotes   – daily consolidated quotes (legacy name kept for compatibility)
fact_quotes         – intraday time-series quotes; converted to TimescaleDB hypertable
etl_runs            – ETL observability / audit log
"""

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, foreign


class Base(DeclarativeBase):
    """Shared declarative base for all models."""


# ---------------------------------------------------------------------------
# Dimension tables
# ---------------------------------------------------------------------------


class DimAsset(Base):
    """Dimension table for listed instruments / assets (CadInstrumento)."""

    __tablename__ = "dim_assets"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(20), unique=True, nullable=False, index=True)
    asset_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    isin: Mapped[str | None] = mapped_column(String(20), nullable=True)
    segment: Mapped[str | None] = mapped_column(String(100), nullable=True)
    instrument_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")
    source_file_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


# ---------------------------------------------------------------------------
# Fact tables
# ---------------------------------------------------------------------------


class FactDailyTrade(Base):
    """Fact table for consolidated daily trading activity (NegociosConsolidados).

    One row per (ticker, trade_date).  Loaded from the negocios_consolidados
    normalized CSV produced by the daily scraper.
    """

    __tablename__ = "fact_daily_trades"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    # Soft FK: plain BigInteger reference to dim_assets.id. Not enforced by the
    # ORM/DB schema here — use an index for lookups. If you prefer a real
    # FK constraint, restore ForeignKey("dim_assets.id", ondelete="SET NULL").
    asset_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    ticker: Mapped[str] = mapped_column(String(20), nullable=False)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    open_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    close_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    min_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    max_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    avg_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    variation_pct: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    financial_volume: Mapped[Decimal | None] = mapped_column(Numeric(24, 2), nullable=True)
    trade_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    source_file_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Keep an ORM relationship for convenience but explicitly declare the
    # foreign_keys argument since there is no DB-level ForeignKey to infer.
    asset: Mapped["DimAsset | None"] = relationship(
        DimAsset,
        lazy="noload",
        foreign_keys=[asset_id],
        primaryjoin=DimAsset.id == foreign(asset_id),
    )

    __table_args__ = (
        UniqueConstraint("ticker", "trade_date", name="uq_daily_trade_ticker_date"),
        Index("ix_fact_daily_trades_asset_id", "asset_id"),
        Index("ix_fact_daily_trades_ticker", "ticker"),
        Index("ix_fact_daily_trades_ticker_date", "ticker", "trade_date"),
        Index("ix_fact_daily_trades_trade_date", "trade_date"),
    )


class FactDailyQuote(Base):
    """Fact table for daily consolidated quotes (legacy / compatibility).

    Kept for backward compatibility with existing API endpoints and migrations.
    """

    __tablename__ = "fact_daily_quotes"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    # Soft FK: plain BigInteger reference to dim_assets.id. Not enforced by the
    # ORM/DB schema here — use an index for lookups. If you prefer a real
    # FK constraint, restore ForeignKey("dim_assets.id", ondelete="SET NULL").
    asset_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    ticker: Mapped[str] = mapped_column(String(20), nullable=False)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    last_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    min_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    max_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    avg_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    variation_pct: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    financial_volume: Mapped[Decimal | None] = mapped_column(Numeric(24, 2), nullable=True)
    trade_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    source_file_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    asset: Mapped["DimAsset | None"] = relationship(
        DimAsset,
        lazy="noload",
        foreign_keys=[asset_id],
        primaryjoin=DimAsset.id == foreign(asset_id),
    )

    __table_args__ = (
        UniqueConstraint("ticker", "trade_date", name="uq_quote_ticker_date"),
        Index("ix_fact_daily_quotes_asset_id", "asset_id"),
        Index("ix_fact_daily_quotes_ticker", "ticker"),
        Index("ix_fact_daily_quotes_ticker_date", "ticker", "trade_date"),
        Index("ix_fact_daily_quotes_trade_date_idx", "trade_date"),
    )


class FactQuote(Base):
    """Intraday time-series quotes sourced from DailyFluctuationHistory JSONL.

    This table is converted into a TimescaleDB hypertable (partitioned on
    ``quoted_at``) by the Alembic migration.  One row = one price point for
    a ticker at a specific timestamp.
    """

    __tablename__ = "fact_quotes"

    # TimescaleDB hypertables require that the partitioning column be part of the
    # primary key.  We use a composite PK (ticker, quoted_at).
    ticker: Mapped[str] = mapped_column(String(20), primary_key=True, nullable=False)
    quoted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), primary_key=True, nullable=False
    )
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    close_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    price_fluctuation_pct: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    source_jsonl: Mapped[str | None] = mapped_column(String(255), nullable=True)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (

        Index("ix_fact_quotes_trade_date", "trade_date"),
    )


# ---------------------------------------------------------------------------
# Observability / audit
# ---------------------------------------------------------------------------


class ETLRun(Base):
    """Audit log for ETL flow executions."""

    __tablename__ = "etl_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    pipeline_name: Mapped[str] = mapped_column(String(100), nullable=False)
    # Keep flow_name as alias column for backward compat
    flow_name: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    rows_inserted: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rows_failed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_file: Mapped[str | None] = mapped_column(String(500), nullable=True)
    source_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index("ix_etl_runs_pipeline_started", "pipeline_name", "started_at"),
        Index("ix_etl_runs_status", "status"),
    )

