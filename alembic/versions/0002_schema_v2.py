"""Schema v2: fact_daily_trades, fact_quotes (hypertable), enrich etl_runs.
Revision ID: 0002_schema_v2
Revises: 0001_initial_schema
Create Date: 2026-03-07 00:00:00.000000
"""
from typing import Sequence, Union
import sqlalchemy as sa
from alembic import op
revision: str = "0002_schema_v2"
down_revision: Union[str, None] = "0001_initial_schema"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None
def upgrade() -> None:
    # 1. Extend dim_assets
    op.add_column("dim_assets", sa.Column("instrument_type", sa.String(50), nullable=True))
    op.add_column(
        "dim_assets",
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
    )
    # 2. Add asset_id column to fact_daily_quotes (soft FK: create plain BigInteger)
    op.add_column(
        "fact_daily_quotes",
        sa.Column(
            "asset_id",
            sa.BigInteger(),
            nullable=True,
        ),
    )
    op.create_index("ix_fact_daily_quotes_asset_id", "fact_daily_quotes", ["asset_id"])
    op.create_index("ix_fact_daily_quotes_trade_date_idx", "fact_daily_quotes", ["trade_date"])
    # 3. Create fact_daily_trades (asset_id is a plain BigInteger, no FK constraint)
    op.create_table(
        "fact_daily_trades",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column(
            "asset_id",
            sa.BigInteger(),
            nullable=True,
        ),
        sa.Column("ticker", sa.String(20), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("open_price", sa.Numeric(18, 6), nullable=True),
        sa.Column("close_price", sa.Numeric(18, 6), nullable=True),
        sa.Column("min_price", sa.Numeric(18, 6), nullable=True),
        sa.Column("max_price", sa.Numeric(18, 6), nullable=True),
        sa.Column("avg_price", sa.Numeric(18, 6), nullable=True),
        sa.Column("variation_pct", sa.Numeric(10, 4), nullable=True),
        sa.Column("financial_volume", sa.Numeric(24, 2), nullable=True),
        sa.Column("trade_count", sa.BigInteger(), nullable=True),
        sa.Column("source_file_name", sa.String(255), nullable=True),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ticker", "trade_date", name="uq_daily_trade_ticker_date"),
    )
    op.create_index("ix_fact_daily_trades_asset_id", "fact_daily_trades", ["asset_id"])
    op.create_index("ix_fact_daily_trades_ticker", "fact_daily_trades", ["ticker"])
    op.create_index("ix_fact_daily_trades_trade_date", "fact_daily_trades", ["trade_date"])
    op.create_index("ix_fact_daily_trades_ticker_date", "fact_daily_trades", ["ticker", "trade_date"])
    # 4. Create fact_quotes + TimescaleDB hypertable
    op.create_table(
        "fact_quotes",
        sa.Column("ticker", sa.String(20), nullable=False),
        sa.Column("quoted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("close_price", sa.Numeric(18, 6), nullable=True),
        sa.Column("price_fluctuation_pct", sa.Numeric(10, 4), nullable=True),
        sa.Column("source_jsonl", sa.String(255), nullable=True),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("ticker", "quoted_at"),
    )
    op.create_index("ix_fact_quotes_ticker_quoted_at", "fact_quotes", ["ticker", "quoted_at"])
    op.create_index("ix_fact_quotes_trade_date", "fact_quotes", ["trade_date"])
    # Attempt to enable TimescaleDB and create hypertable only when available/allowed.
    # This keeps migrations compatible with plain PostgreSQL and managed instances where
    # CREATE EXTENSION is disallowed.
    op.execute(
        """
        DO $$
        BEGIN
            -- Only attempt to create the extension if it's available on this server.
            IF EXISTS (
                SELECT 1
                FROM pg_available_extensions
                WHERE name = 'timescaledb'
            ) THEN
                BEGIN
                    CREATE EXTENSION IF NOT EXISTS timescaledb;
                EXCEPTION
                    WHEN insufficient_privilege THEN
                        -- Ignore lack of privilege; run without TimescaleDB.
                        NULL;
                END;
            END IF;
        END
        $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            -- Only create hypertable if TimescaleDB is installed and create_hypertable is available.
            IF EXISTS (
                   SELECT 1
                   FROM pg_extension
                   WHERE extname = 'timescaledb'
               )
               AND EXISTS (
                   SELECT 1
                   FROM pg_proc
                   WHERE proname = 'create_hypertable'
               ) THEN
                PERFORM create_hypertable(
                    'fact_quotes',
                    'quoted_at',
                    if_not_exists => TRUE,
                    migrate_data  => TRUE
                );
            END IF;
        END
        $$;
        """
    )
    # 5. Extend etl_runs
    op.add_column("etl_runs", sa.Column("pipeline_name", sa.String(100), nullable=True))
    op.add_column("etl_runs", sa.Column("rows_inserted", sa.Integer(), nullable=True))
    op.add_column("etl_runs", sa.Column("rows_failed", sa.Integer(), nullable=True))
    op.add_column("etl_runs", sa.Column("source_file", sa.String(500), nullable=True))
    op.add_column("etl_runs", sa.Column("source_date", sa.Date(), nullable=True))
    op.execute("UPDATE etl_runs SET pipeline_name = flow_name WHERE pipeline_name IS NULL")
    op.alter_column("etl_runs", "pipeline_name", nullable=False)
    op.create_index("ix_etl_runs_pipeline_started", "etl_runs", ["pipeline_name", "started_at"])
    op.create_index("ix_etl_runs_status", "etl_runs", ["status"])
def downgrade() -> None:
    op.drop_index("ix_etl_runs_status", table_name="etl_runs")
    op.drop_index("ix_etl_runs_pipeline_started", table_name="etl_runs")
    op.drop_column("etl_runs", "source_date")
    op.drop_column("etl_runs", "source_file")
    op.drop_column("etl_runs", "rows_failed")
    op.drop_column("etl_runs", "rows_inserted")
    op.drop_column("etl_runs", "pipeline_name")
    op.drop_index("ix_fact_quotes_trade_date", table_name="fact_quotes")
    op.drop_index("ix_fact_quotes_ticker_quoted_at", table_name="fact_quotes")
    op.drop_table("fact_quotes")
    op.drop_index("ix_fact_daily_trades_ticker_date", table_name="fact_daily_trades")
    op.drop_index("ix_fact_daily_trades_trade_date", table_name="fact_daily_trades")
    op.drop_index("ix_fact_daily_trades_ticker", table_name="fact_daily_trades")
    op.drop_index("ix_fact_daily_trades_asset_id", table_name="fact_daily_trades")
    op.drop_table("fact_daily_trades")
    op.drop_index("ix_fact_daily_quotes_trade_date_idx", table_name="fact_daily_quotes")
    op.drop_index("ix_fact_daily_quotes_asset_id", table_name="fact_daily_quotes")
    op.drop_column("fact_daily_quotes", "asset_id")
    op.drop_column("dim_assets", "is_active")
    op.drop_column("dim_assets", "instrument_type")
