"""fact_cotahist_daily: annual COTAHIST type-01 facts.

Revision ID: 0003_fact_cotahist_daily
Revises: 0002_schema_v2
Create Date: 2026-03-20 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0003_fact_cotahist_daily"
down_revision: Union[str, None] = "0002_schema_v2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "fact_cotahist_daily",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("ticker", sa.String(length=20), nullable=False),
        sa.Column("codneg", sa.String(length=12), nullable=False),
        sa.Column("cod_bdi", sa.String(length=2), nullable=False),
        sa.Column("tp_merc", sa.String(length=3), nullable=False),
        sa.Column("nomres", sa.String(length=12), nullable=True),
        sa.Column("especi", sa.String(length=10), nullable=False),
        sa.Column("prazo_term_days", sa.Integer(), nullable=False),
        sa.Column("moeda_ref", sa.String(length=4), nullable=False),
        sa.Column("open_price", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("max_price", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("min_price", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("avg_price", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("last_price", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("best_bid", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("best_ask", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("trade_count", sa.BigInteger(), nullable=True),
        sa.Column("quantity_total", sa.Numeric(precision=24, scale=4), nullable=True),
        sa.Column("volume_financial", sa.Numeric(precision=24, scale=4), nullable=True),
        sa.Column("strike_price", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("ind_opc", sa.String(length=1), nullable=False),
        sa.Column("expiration_date", sa.Date(), nullable=True),
        sa.Column("expiration_key", sa.String(length=8), nullable=False),
        sa.Column("quotation_factor", sa.Integer(), nullable=True),
        sa.Column("strike_points", sa.Numeric(precision=24, scale=8), nullable=True),
        sa.Column("isin", sa.String(length=12), nullable=False),
        sa.Column("distribution_num", sa.String(length=3), nullable=False),
        sa.Column("source_file_name", sa.String(length=255), nullable=True),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "trade_date",
            "codneg",
            "cod_bdi",
            "tp_merc",
            "especi",
            "prazo_term_days",
            "moeda_ref",
            "expiration_key",
            "isin",
            "distribution_num",
            "ind_opc",
            name="uq_cotahist_natural_key",
        ),
    )
    op.create_index("ix_fact_cotahist_trade_date", "fact_cotahist_daily", ["trade_date"])
    op.create_index("ix_fact_cotahist_codneg", "fact_cotahist_daily", ["codneg"])
    op.create_index(
        "ix_fact_cotahist_ticker_date",
        "fact_cotahist_daily",
        ["ticker", "trade_date"],
    )


def downgrade() -> None:
    op.drop_index("ix_fact_cotahist_ticker_date", table_name="fact_cotahist_daily")
    op.drop_index("ix_fact_cotahist_codneg", table_name="fact_cotahist_daily")
    op.drop_index("ix_fact_cotahist_trade_date", table_name="fact_cotahist_daily")
    op.drop_table("fact_cotahist_daily")
