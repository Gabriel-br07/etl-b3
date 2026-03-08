"""Standalone ETL pipeline functions for the scheduler.

These functions replace the Prefect-based flow when running inside the Docker
scheduler (no Prefect server required).  Each function is independently
callable, testable, and returns a summary dict.

Pipeline entry points
---------------------
run_instruments_and_trades_pipeline(instruments_csv, trades_file, target_date)
    Loads dim_assets + fact_daily_trades from normalized CSVs.

run_intraday_quotes_pipeline(jsonl_path)
    Loads fact_quotes hypertable from a JSONL file.

Both functions record ETL run audit rows in etl_runs.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

from app.core.constants import ETLStatus
from app.db.engine import managed_session
from app.etl.loaders.db_loader import load_assets, load_intraday_quotes, load_trades
from app.etl.parsers.instruments_parser import parse_instruments_csv
from app.etl.parsers.jsonl_quotes_parser import parse_jsonl_quotes
from app.etl.parsers.trades_parser import parse_trades_file
from app.etl.transforms.b3_transforms import transform_instruments, transform_trades
from app.repositories.etl_run_repository import ETLRunRepository

logger = logging.getLogger(__name__)


def run_instruments_and_trades_pipeline(
    instruments_csv: Path,
    trades_file: Path | None,
    target_date: date,
) -> dict:
    """Parse normalized instruments + trades CSVs and load into the database.

    Args:
        instruments_csv: Path to the normalized instruments CSV.
        trades_file:     Path to the normalized trades file (CSV or ZIP).
                         Pass None to skip trade loading.
        target_date:     Reference date for the data (used in audit row).

    Returns:
        Summary dict with keys: target_date, assets_upserted, trades_upserted, status.
    """
    logger.info(
        "[etl_pipeline] starting instruments+trades load  date=%s  csv=%s  trades=%s",
        target_date, instruments_csv, trades_file,
    )

    summary: dict
    with managed_session() as db:
        etl_repo = ETLRunRepository(db)
        run = etl_repo.start_run(
            "instruments_and_trades",
            source_file=str(instruments_csv),
            source_date=target_date,
        )

        assets_upserted = 0
        trades_upserted = 0

        try:
            # --- instruments ---
            instruments_df = parse_instruments_csv(instruments_csv)
            asset_rows = transform_instruments(instruments_df, target_date)
            assets_upserted = load_assets(db, asset_rows)

            # --- trades ---
            if trades_file is not None and trades_file.exists():
                trades_df = parse_trades_file(trades_file)
                trade_rows = transform_trades(trades_df, trades_file.name)
                trades_upserted = load_trades(db, trade_rows)
            elif trades_file is not None:
                logger.warning(
                    "[etl_pipeline] trades file not found: %s - skipping trade load",
                    trades_file,
                )

            etl_repo.finish_run(
                run,
                ETLStatus.SUCCESS,
                rows_inserted=assets_upserted + trades_upserted,
                rows_failed=0,
            )

            summary = {
                "target_date": str(target_date),
                "assets_upserted": assets_upserted,
                "trades_upserted": trades_upserted,
                "status": ETLStatus.SUCCESS,
            }
            logger.info("[etl_pipeline] instruments+trades load done: %s", summary)

        except Exception as exc:  # noqa: BLE001 - we intentionally handle exceptions here
            logger.exception("[etl_pipeline] instruments+trades load FAILED: %s", exc)

            # Ensure any partial work is rolled back before persisting the failure audit
            try:
                db.rollback()
            except Exception:
                # best-effort rollback; continue to try to persist failure
                pass

            # Record the failed run and persist that audit row in the same session.
            try:
                etl_repo.finish_run(
                    run,
                    ETLStatus.FAILED,
                    message=str(exc),
                    rows_inserted=assets_upserted + trades_upserted,
                    rows_failed=1,
                )
                try:
                    db.commit()
                except Exception:
                    db.rollback()
            except Exception:
                # If recording the failure itself fails, ensure rollback and let the
                # caller know by returning a failure summary containing the error.
                try:
                    db.rollback()
                except Exception:
                    pass

            summary = {
                "target_date": str(target_date),
                "assets_upserted": assets_upserted,
                "trades_upserted": trades_upserted,
                "status": ETLStatus.FAILED,
                "error": str(exc),
            }

    return summary


def run_intraday_quotes_pipeline(jsonl_path: Path) -> dict:
    """Parse a DailyFluctuationHistory JSONL and load into the fact_quotes hypertable.

    Args:
        jsonl_path: Path to the JSONL output file from run_b3_quote_batch.py.

    Returns:
        Summary dict with keys: source_file, rows_inserted, status.
    """
    logger.info("[etl_pipeline] starting intraday quotes load  jsonl=%s", jsonl_path)

    summary: dict
    with managed_session() as db:
        etl_repo = ETLRunRepository(db)
        run = etl_repo.start_run(
            "intraday_quotes",
            source_file=str(jsonl_path),
        )

        rows_inserted = 0
        try:
            rows = parse_jsonl_quotes(jsonl_path)
            rows_inserted = load_intraday_quotes(db, rows)

            etl_repo.finish_run(
                run,
                ETLStatus.SUCCESS,
                rows_inserted=rows_inserted,
                rows_failed=0,
            )

            summary = {
                "source_file": jsonl_path.name,
                "rows_inserted": rows_inserted,
                "status": ETLStatus.SUCCESS,
            }
            logger.info("[etl_pipeline] intraday quotes load done: %s", summary)

        except Exception as exc:  # noqa: BLE001 - intentional handling
            logger.exception("[etl_pipeline] intraday quotes load FAILED: %s", exc)

            # Rollback partial work before recording failure
            try:
                db.rollback()
            except Exception:
                pass

            try:
                etl_repo.finish_run(
                    run,
                    ETLStatus.FAILED,
                    message=str(exc),
                    rows_inserted=rows_inserted,
                    rows_failed=1,
                )
                try:
                    db.commit()
                except Exception:
                    db.rollback()
            except Exception:
                try:
                    db.rollback()
                except Exception:
                    pass

            summary = {
                "source_file": jsonl_path.name,
                "rows_inserted": rows_inserted,
                "status": ETLStatus.FAILED,
                "error": str(exc),
            }

    return summary

