"""Add scraper_run_audit table for scraper observability.

Revision ID: 0004_scraper_run_audit
Revises: 0003_fact_cotahist_daily
Create Date: 2026-03-26 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0004_scraper_run_audit"
down_revision: Union[str, None] = "0003_fact_cotahist_daily"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "scraper_run_audit",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("scraper_name", sa.String(length=100), nullable=False),
        sa.Column("target_date", sa.Date(), nullable=True),
        sa.Column("flow_run_id", sa.String(length=64), nullable=True),
        sa.Column("task_run_id", sa.String(length=64), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column("output_path", sa.String(length=500), nullable=True),
        sa.Column("output_file_name", sa.String(length=255), nullable=True),
        sa.Column("records_count", sa.Integer(), nullable=True),
        sa.Column("error_type", sa.String(length=100), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_scraper_run_audit_name_date_started",
        "scraper_run_audit",
        ["scraper_name", "target_date", "started_at"],
    )
    op.create_index(
        "ix_scraper_run_audit_status_started",
        "scraper_run_audit",
        ["status", "started_at"],
    )
    op.create_index(
        "ix_scraper_run_audit_flow_task",
        "scraper_run_audit",
        ["flow_run_id", "task_run_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_scraper_run_audit_flow_task", table_name="scraper_run_audit")
    op.drop_index("ix_scraper_run_audit_status_started", table_name="scraper_run_audit")
    op.drop_index("ix_scraper_run_audit_name_date_started", table_name="scraper_run_audit")
    op.drop_table("scraper_run_audit")
