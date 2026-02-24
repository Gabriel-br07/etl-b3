"""SQLAlchemy ORM models."""

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    Index,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Shared declarative base for all models."""


class DimAsset(Base):
    """Dimension table for listed instruments / assets."""

    __tablename__ = "dim_assets"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(20), unique=True, nullable=False, index=True)
    asset_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    isin: Mapped[str | None] = mapped_column(String(20), nullable=True)
    segment: Mapped[str | None] = mapped_column(String(100), nullable=True)
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


class FactDailyQuote(Base):
    """Fact table for daily consolidated quotes."""

    __tablename__ = "fact_daily_quotes"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
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

    __table_args__ = (
        UniqueConstraint("ticker", "trade_date", name="uq_quote_ticker_date"),
        Index("ix_fact_daily_quotes_ticker_date", "ticker", "trade_date"),
    )


class ETLRun(Base):
    """Audit log for ETL flow executions."""

    __tablename__ = "etl_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    flow_name: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
