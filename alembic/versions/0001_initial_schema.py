"""Initial schema: dim_assets, fact_daily_quotes, etl_runs.

Revision ID: 0001_initial_schema
Revises: 
Create Date: 2024-06-14 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0001_initial_schema"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # dim_assets
    op.create_table(
        "dim_assets",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("ticker", sa.String(length=20), nullable=False),
        sa.Column("asset_name", sa.String(length=200), nullable=True),
        sa.Column("isin", sa.String(length=20), nullable=True),
        sa.Column("segment", sa.String(length=100), nullable=True),
        sa.Column("source_file_date", sa.Date(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ticker"),
    )
    op.create_index("ix_dim_assets_ticker", "dim_assets", ["ticker"])

    # fact_daily_quotes
    op.create_table(
        "fact_daily_quotes",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("ticker", sa.String(length=20), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("last_price", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("min_price", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("max_price", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("avg_price", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("variation_pct", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("financial_volume", sa.Numeric(precision=24, scale=2), nullable=True),
        sa.Column("trade_count", sa.BigInteger(), nullable=True),
        sa.Column("source_file_name", sa.String(length=255), nullable=True),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ticker", "trade_date", name="uq_quote_ticker_date"),
    )
    op.create_index("ix_fact_daily_quotes_ticker", "fact_daily_quotes", ["ticker"])
    op.create_index("ix_fact_daily_quotes_trade_date", "fact_daily_quotes", ["trade_date"])
    op.create_index(
        "ix_fact_daily_quotes_ticker_date", "fact_daily_quotes", ["ticker", "trade_date"]
    )

    # etl_runs
    op.create_table(
        "etl_runs",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("flow_name", sa.String(length=100), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("message", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("etl_runs")
    op.drop_index("ix_fact_daily_quotes_ticker_date", table_name="fact_daily_quotes")
    op.drop_index("ix_fact_daily_quotes_trade_date", table_name="fact_daily_quotes")
    op.drop_index("ix_fact_daily_quotes_ticker", table_name="fact_daily_quotes")
    op.drop_table("fact_daily_quotes")
    op.drop_index("ix_dim_assets_ticker", table_name="dim_assets")
    op.drop_table("dim_assets")
