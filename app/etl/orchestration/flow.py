"""Prefect flow for daily B3 ETL pipeline.

Flow: run_daily_b3_etl
Steps:
  1. Resolve source files (local or remote)
  2. Parse instruments CSV
  3. Parse trades file
  4. Transform instruments
  5. Transform trades
  6. Load assets
  7. Load quotes
  8. Persist ETL run audit record
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
from prefect import flow, get_run_logger, task

from app.core.constants import ETLStatus, SourceMode
from app.db.engine import SessionLocal, managed_session
from app.etl.ingestion.local_adapter import LocalFileAdapter
from app.etl.ingestion.remote_adapter import RemoteAdapter
from app.etl.loaders.db_loader import load_assets, load_daily_quotes as load_quotes

from app.etl.parsers.instruments_parser import parse_instruments_csv
from app.etl.parsers.trades_parser import parse_trades_file
from app.etl.transforms.b3_transforms import transform_instruments, transform_trades
from app.repositories.etl_run_repository import ETLRunRepository
from app.db.models import ETLRun


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(name="resolve-instruments-file", retries=2, retry_delay_seconds=5)
def resolve_instruments(mode: str, target_date: date) -> Path:
    logger = get_run_logger()
    adapter = RemoteAdapter() if mode == SourceMode.REMOTE else LocalFileAdapter()
    src = adapter.get_instruments_file(target_date)
    logger.info("Instruments file resolved: %s (url=%s)", src.path, src.source_url)
    return src.path


@task(name="resolve-trades-file", retries=2, retry_delay_seconds=5)
def resolve_trades(mode: str, target_date: date) -> Path:
    logger = get_run_logger()
    adapter = RemoteAdapter() if mode == SourceMode.REMOTE else LocalFileAdapter()
    src = adapter.get_trades_file(target_date)
    logger.info("Trades file resolved: %s (url=%s)", src.path, src.source_url)
    return src.path


@task(name="parse-instruments")
def parse_instruments_task(path: Path) -> pl.DataFrame:
    return parse_instruments_csv(path)


@task(name="parse-trades")
def parse_trades_task(path: Path) -> pl.DataFrame:
    return parse_trades_file(path)


@task(name="transform-instruments")
def transform_instruments_task(df: pl.DataFrame, file_date: date) -> list[dict]:
    return transform_instruments(df, file_date)


@task(name="transform-trades")
def transform_trades_task(df: pl.DataFrame, file_name: str) -> list[dict]:
    return transform_trades(df, file_name)


@task(name="load-assets-db")
def load_assets_task(rows: list[dict]) -> int:
    db = SessionLocal()
    try:
        total = load_assets(db, rows)
        # Persist the upserts performed by the repositories used in load_assets
        db.commit()
        return total
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


@task(name="load-quotes-db")
def load_quotes_task(rows: list[dict]) -> int:
    db = SessionLocal()
    try:
        total = load_quotes(db, rows)
        # Persist the upserts performed by the repositories used in load_quotes
        db.commit()
        return total
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="run-daily-b3-etl")
def run_daily_b3_etl(
    target_date: date | None = None,
    source_mode: str = SourceMode.LOCAL,
) -> dict:
    """Daily B3 ETL flow.

    Args:
        target_date: Date to process.  Defaults to today.
        source_mode: "local" (reads from B3_DATA_DIR) or "remote" (downloads).

    Returns:
        Summary dict with row counts.
    """
    logger = get_run_logger()
    if target_date is None:
        target_date = date.today()

    started_at = datetime.now(tz=timezone.utc)
    db = SessionLocal()
    etl_repo = ETLRunRepository(db)
    run = etl_repo.start_run("run_daily_b3_etl", source_date=target_date)

    # Persist the started run immediately so there is an audit record even if
    # subsequent steps fail. Store only the run id so we don't pass a detached
    # ORM instance between sessions; audit updates will use a fresh session.
    try:
        db.commit()
    except Exception:
        db.rollback()
        db.close()
        raise

    run_id = run.id
    # Close the session that created the run to avoid accidentally reusing
    # the ORM instance across other sessions/tasks.
    db.close()

    try:
        instruments_path = resolve_instruments(source_mode, target_date)
        trades_path = resolve_trades(source_mode, target_date)

        instruments_df = parse_instruments_task(instruments_path)
        trades_df = parse_trades_task(trades_path)

        asset_rows = transform_instruments_task(instruments_df, target_date)
        quote_rows = transform_trades_task(trades_df, trades_path.name)

        assets_count = load_assets_task(asset_rows)
        quotes_count = load_quotes_task(quote_rows)

        summary = {
            "target_date": str(target_date),
            "assets_upserted": assets_count,
            "quotes_upserted": quotes_count,
            "source_mode": source_mode,
        }

        # Record final status in an independent audit transaction to avoid
        # session/transaction mixing with worker tasks that used their own DB
        # sessions.
        try:
            with managed_session() as audit_db:
                audit_repo = ETLRunRepository(audit_db)
                run_obj = audit_db.get(ETLRun, run_id)
                if run_obj is None:
                    logger.warning(
                        "[flow] could not reload ETL run id=%s to record success",
                        run_id,
                    )
                else:
                    audit_repo.finish_run(run_obj, ETLStatus.SUCCESS, str(summary), rows_inserted=assets_count + quotes_count)
        except Exception:
            # If audit recording fails, log and raise so the outer handler can
            # surface the error (we avoid swallowing the problem).
            logger.exception("[flow] failed to record ETL success audit for run id=%s", run_id)
            raise

        logger.info("ETL completed: %s", summary)
        return summary

    except Exception as exc:  # noqa: BLE001
        # Record failure and persist the audit record in its own transaction
        try:
            with managed_session() as audit_db:
                audit_repo = ETLRunRepository(audit_db)
                run_obj = audit_db.get(ETLRun, run_id)
                if run_obj is None:
                    logger.warning(
                        "[flow] could not reload ETL run id=%s to record failure",
                        run_id,
                    )
                else:
                    audit_repo.finish_run(run_obj, ETLStatus.FAILED, str(exc))
        except Exception:
            # If audit logging fails, log but avoid masking the original
            # exception.
            logger.exception("[flow] failed to record ETL failure audit for run id=%s", run_id)

        logger.error("ETL failed: %s", exc)
        raise
    finally:
        # Ensure any leftover DB session is closed (db may have already been closed).
        try:
            db.close()
        except Exception:
            pass
