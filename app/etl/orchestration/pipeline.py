"""Standalone ETL pipeline functions for the scheduler.

These functions replace the Prefect-based flow when running inside the Docker
scheduler (no Prefect server required).  Each function is independently
callable, testable, and returns a summary dict.

Pipeline entry points
---------------------
run_instruments_and_trades_pipeline(instruments_csv, trades_file, target_date)
    Loads dim_assets + fact_daily_trades from normalized CSVs.

run_daily_quotes_pipeline(quotes_csv, target_date)
    Loads fact_daily_quotes from a normalized negocios_consolidados CSV.

run_intraday_quotes_pipeline(jsonl_path)
    Loads fact_quotes hypertable from a JSONL file.

run_cotahist_annual_pipeline(txt_path)
    Loads fact_cotahist_daily from a COTAHIST_A*.TXT fixed-width file.

By default each function records ``etl_runs`` audit rows. ``scripts/run_etl.py``
uses ``record_audit=True`` for every step so CLI runs are auditable. Pass
``record_audit=False`` only for ad-hoc callers that intentionally skip ``etl_runs``
(e.g. tests or custom batch tools).
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

from app.core.constants import ETLStatus
from app.db.engine import managed_session
from app.etl.loaders.db_loader import load_assets, load_daily_quotes, load_intraday_quotes, load_trades
from app.etl.parsers.instruments_parser import parse_instruments_csv
from app.etl.parsers.jsonl_quotes_parser import parse_jsonl_quotes
from app.etl.parsers.trades_parser import parse_trades_file
from app.etl.transforms.b3_transforms import (
    transform_daily_quotes,
    transform_instruments,
    transform_jsonl_quotes,
    transform_trades,
)
from app.repositories.etl_run_repository import ETLRunRepository
from app.use_cases.quotes.cotahist_annual_ingestion import ingest_cotahist_txt_file

logger = logging.getLogger(__name__)


def run_instruments_and_trades_pipeline(
    instruments_csv: Path,
    trades_file: Path | None,
    target_date: date,
    *,
    record_audit: bool = True,
) -> dict:
    """Parse normalized instruments + trades CSVs and load into the database.

    Args:
        instruments_csv: Path to the normalized instruments CSV.
        trades_file:     Path to the normalized trades file (CSV or ZIP).
                         Pass None to skip trade loading.
        target_date:     Reference date for the data (used in audit row).
        record_audit:    When False, skip all ``etl_runs`` writes (CLI batch mode).

    Returns:
        Summary dict with keys: target_date, assets_upserted, trades_upserted, status.
    """
    logger.info(
        "[etl_pipeline] starting instruments+trades load  date=%s  csv=%s  trades=%s",
        target_date, instruments_csv, trades_file,
    )

    run_id: int | None = None
    if record_audit:
        with managed_session() as audit_db:
            etl_repo = ETLRunRepository(audit_db)
            run = etl_repo.start_run(
                "instruments_and_trades",
                source_file=str(instruments_csv),
                source_date=target_date,
            )
            audit_db.flush()
            run_id = run.id

    assets_upserted = 0
    trades_upserted = 0

    # Transaction 2: perform the actual data load. Do NOT catch exceptions
    # inside this managed_session() so that the session's context manager can
    # rollback automatically on error.
    try:
        with managed_session() as db:
            # --- instruments ---
            instruments_df = parse_instruments_csv(instruments_csv)

            if len(instruments_df) == 0:
                # Distinguish between a likely header-mapping failure (no columns)
                # and a valid header with zero data rows.
                if getattr(instruments_df, "columns", None) is not None and len(instruments_df.columns) == 0:
                    raise ValueError(
                        f"[etl_pipeline] instruments parser returned 0 rows and 0 columns from {instruments_csv}. "
                        "This suggests the CSV header did not match any known column mapping. "
                        "Check that the normalized CSV starts at the real header row and not "
                        "at a preamble/descriptive line."
                    )
                else:
                    raise ValueError(
                        f"[etl_pipeline] instruments parser returned 0 data rows from {instruments_csv}. "
                        "The file may be empty, contain only a header, or all rows may have been filtered "
                        "out by upstream normalization."
                    )

            asset_rows = transform_instruments(instruments_df, target_date)

            if not asset_rows:
                raise ValueError(
                    f"[etl_pipeline] transform_instruments returned 0 rows from {instruments_csv}. "
                    "All rows were filtered out (check ticker/date columns)."
                )

            assets_upserted = load_assets(db, asset_rows)

            # --- trades ---
            if trades_file is not None and trades_file.exists():
                trades_df = parse_trades_file(trades_file)
                trade_rows = transform_trades(trades_df, trades_file.name, target_date=target_date)

                # Adapt transform_trades() output to fact_daily_trades schema:
                # map last_price -> close_price and drop last_price to avoid
                # passing unknown columns into the loader/ORM.
                fact_trade_rows = []
                for row in trade_rows:
                    # Ensure we have a mutable dict copy
                    mapped_row = dict(row)
                    if "last_price" in mapped_row and "close_price" not in mapped_row:
                        mapped_row["close_price"] = mapped_row["last_price"]
                        del mapped_row["last_price"]
                    fact_trade_rows.append(mapped_row)

                trades_upserted = load_trades(db, fact_trade_rows)
            elif trades_file is not None:
                logger.warning(
                    "[etl_pipeline] trades file not found: %s - skipping trade load",
                    trades_file,
                )

    except Exception as exc:
        # The data transaction was rolled back by managed_session(); now
        # record the FAILED audit in its own transaction.
        logger.exception("[etl_pipeline] instruments+trades load FAILED: %s", exc)
        if record_audit and run_id is not None:
            try:
                with managed_session() as audit_db:
                    etl_repo = ETLRunRepository(audit_db)
                    run_obj = etl_repo.get_by_id(run_id)
                    if run_obj is None:
                        logger.warning(
                            "[etl_pipeline] could not reload ETL run id=%s to record failure",
                            run_id,
                        )
                    else:
                        etl_repo.finish_run(
                            run_obj,
                            ETLStatus.FAILED,
                            message=str(exc),
                            rows_inserted=assets_upserted + trades_upserted,
                            rows_failed=1,
                        )
            except Exception:
                logger.exception(
                    "[etl_pipeline] instruments+trades load FAILED but could not record audit run"
                )

        return {
            "target_date": str(target_date),
            "assets_upserted": assets_upserted,
            "trades_upserted": trades_upserted,
            "status": ETLStatus.FAILED,
            "error": str(exc),
        }

    if record_audit and run_id is not None:
        with managed_session() as audit_db:
            etl_repo = ETLRunRepository(audit_db)
            run_obj = etl_repo.get_by_id(run_id)
            if run_obj is None:
                logger.warning(
                    "[etl_pipeline] could not reload ETL run id=%s to record success",
                    run_id,
                )
            else:
                etl_repo.finish_run(
                    run_obj,
                    ETLStatus.SUCCESS,
                    message=None,
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
    return summary


def run_daily_quotes_pipeline(
    quotes_csv: Path,
    target_date: date,
    *,
    record_audit: bool = True,
) -> dict:
    """Parse a normalized negocios_consolidados CSV and load into fact_daily_quotes.

    The CSV is expected to contain columns that map (via TRADE_COLUMN_MAP) to:
        ticker, last_price, min_price, max_price, avg_price,
        variation_pct, financial_volume, trade_count

    ``open_price`` is intentionally ignored – it is not in the
    ``fact_daily_quotes`` schema.

    If ``trade_date`` is absent from the CSV it is derived from
    ``target_date`` or the filename (negocios_consolidados_YYYYMMDD).

    Args:
        quotes_csv:  Path to the normalized quotes CSV file.
        target_date: Reference date used as trade_date fallback and in the
                     audit row.
        record_audit: When False, skip all ``etl_runs`` writes.

    Returns:
        Summary dict with keys: target_date, quotes_upserted, status.
    """
    logger.info(
        "[etl_pipeline] starting daily quotes load  date=%s  csv=%s",
        target_date,
        quotes_csv,
    )

    run_id: int | None = None
    if record_audit:
        with managed_session() as audit_db:
            etl_repo = ETLRunRepository(audit_db)
            run = etl_repo.start_run(
                "daily_quotes",
                source_file=str(quotes_csv),
                source_date=target_date,
            )
            audit_db.flush()
            run_id = run.id

    quotes_upserted = 0

    try:
        with managed_session() as db:
            logger.info(
                "[etl_pipeline] parsing quotes CSV: %s", quotes_csv
            )
            quotes_df = parse_trades_file(quotes_csv)
            logger.info(
                "[etl_pipeline] Input columns detected: %s",
                quotes_df.columns,
            )
            quote_rows = transform_daily_quotes(
                quotes_df, quotes_csv.name, target_date=target_date
            )
            if not quote_rows:
                logger.warning(
                    "[etl_pipeline] transform_daily_quotes returned 0 rows for %s",
                    quotes_csv.name,
                )
            else:
                logger.info(
                    "[etl_pipeline] Mapped columns: %s",
                    [k for k in quote_rows[0].keys() if k != "source_file_name"],
                )
            quotes_upserted = load_daily_quotes(db, quote_rows)

    except Exception as exc:
        logger.exception("[etl_pipeline] daily quotes load FAILED: %s", exc)
        if record_audit and run_id is not None:
            try:
                with managed_session() as audit_db:
                    etl_repo = ETLRunRepository(audit_db)
                    run_obj = etl_repo.get_by_id(run_id)
                    if run_obj is None:
                        logger.warning(
                            "[etl_pipeline] could not reload ETL run id=%s to record failure",
                            run_id,
                        )
                    else:
                        etl_repo.finish_run(
                            run_obj,
                            ETLStatus.FAILED,
                            message=str(exc),
                            rows_inserted=quotes_upserted,
                            rows_failed=1,
                        )
            except Exception:
                logger.exception(
                    "[etl_pipeline] daily quotes load FAILED but could not record audit run"
                )

        return {
            "target_date": str(target_date),
            "quotes_upserted": quotes_upserted,
            "status": ETLStatus.FAILED,
            "error": str(exc),
        }

    if record_audit and run_id is not None:
        with managed_session() as audit_db:
            etl_repo = ETLRunRepository(audit_db)
            run_obj = etl_repo.get_by_id(run_id)
            if run_obj is None:
                logger.warning(
                    "[etl_pipeline] could not reload ETL run id=%s to record success",
                    run_id,
                )
            else:
                etl_repo.finish_run(
                    run_obj,
                    ETLStatus.SUCCESS,
                    message=None,
                    rows_inserted=quotes_upserted,
                    rows_failed=0,
                )

    summary = {
        "target_date": str(target_date),
        "quotes_upserted": quotes_upserted,
        "status": ETLStatus.SUCCESS,
    }
    logger.info("[etl_pipeline] daily quotes load done: %s", summary)
    return summary


def run_intraday_quotes_pipeline(jsonl_path: Path, *, record_audit: bool = True) -> dict:
    """Parse a DailyFluctuationHistory JSONL and load into the fact_quotes hypertable.

    Args:
        jsonl_path: Path to the JSONL output file from run_b3_quote_batch.py.
        record_audit: When False, skip all ``etl_runs`` writes.

    Returns:
        Summary dict with keys: source_file, rows_inserted, status.
    """
    logger.info("[etl_pipeline] starting intraday quotes load  jsonl=%s", jsonl_path)

    run_id: int | None = None
    if record_audit:
        with managed_session() as audit_db:
            etl_repo = ETLRunRepository(audit_db)
            run = etl_repo.start_run(
                "intraday_quotes",
                source_file=str(jsonl_path),
            )
            audit_db.flush()
            run_id = run.id

    rows_inserted = 0

    try:
        # Run the actual data load in a separate transaction. Any exception
        # raised here will cause this managed_session() to roll back.
        with managed_session() as db:
            parsed_rows = parse_jsonl_quotes(jsonl_path)
            logger.info(
                "[etl_pipeline] JSONL parsed: %d raw rows from %s",
                len(parsed_rows),
                jsonl_path.name,
            )
            if not parsed_rows:
                logger.warning(
                    "[etl_pipeline] parse_jsonl_quotes returned 0 rows for %s – "
                    "nothing to transform or load.",
                    jsonl_path.name,
                )
            rows = transform_jsonl_quotes(parsed_rows, source_name=jsonl_path.name)
            if not rows:
                logger.warning(
                    "[etl_pipeline] transform_jsonl_quotes returned 0 rows for %s – "
                    "loader will not insert any data.",
                    jsonl_path.name,
                )
            else:
                logger.info(
                    "[etl_pipeline] transform_jsonl_quotes produced %d rows; "
                    "sample keys: %s",
                    len(rows),
                    list(rows[0].keys()),
                )
            rows_inserted = load_intraday_quotes(db, rows)

    except Exception as exc:
        logger.exception("[etl_pipeline] intraday quotes load FAILED: %s", exc)

        if record_audit and run_id is not None:
            try:
                with managed_session() as audit_db:
                    etl_repo = ETLRunRepository(audit_db)
                    run_obj = etl_repo.get_by_id(run_id)
                    if run_obj is None:
                        logger.warning(
                            "[etl_pipeline] could not reload ETL run id=%s to record failure",
                            run_id,
                        )
                    else:
                        etl_repo.finish_run(
                            run_obj,
                            ETLStatus.FAILED,
                            message=str(exc),
                            rows_inserted=rows_inserted,
                            rows_failed=1,
                        )
            except Exception:
                logger.exception(
                    "[etl_pipeline] intraday quotes load FAILED but could not record audit run"
                )

        return {
            "source_file": jsonl_path.name,
            "rows_inserted": rows_inserted,
            "status": ETLStatus.FAILED,
            "error": str(exc),
        }

    if record_audit and run_id is not None:
        with managed_session() as audit_db:
            etl_repo = ETLRunRepository(audit_db)
            run_obj = etl_repo.get_by_id(run_id)
            if run_obj is None:
                logger.warning(
                    "[etl_pipeline] could not reload ETL run id=%s to record success",
                    run_id,
                )
            else:
                etl_repo.finish_run(
                    run_obj,
                    ETLStatus.SUCCESS,
                    message=None,
                    rows_inserted=rows_inserted,
                    rows_failed=0,
                )

    summary = {
        "source_file": jsonl_path.name,
        "rows_inserted": rows_inserted,
        "status": ETLStatus.SUCCESS,
    }
    logger.info("[etl_pipeline] intraday quotes load done: %s", summary)
    return summary


def run_cotahist_annual_pipeline(
    txt_path: Path,
    *,
    record_audit: bool = True,
    track_in_file_duplicates: bool = False,
) -> dict:
    """Parse a COTAHIST annual TXT and upsert into ``fact_cotahist_daily``.

    Success leaves ``etl_runs.message`` NULL (stats go to logs only). Failures
    persist ``str(exc)`` in ``message``. Use ``record_audit=False`` only to skip
    one ``etl_runs`` row per file (e.g. very large unattended backfills); the
    ``run_etl.py`` CLI keeps audit enabled.
    """
    logger.info("[etl_pipeline] starting COTAHIST annual load  txt=%s", txt_path)

    run_id: int | None = None
    if record_audit:
        with managed_session() as audit_db:
            etl_repo = ETLRunRepository(audit_db)
            run = etl_repo.start_run(
                "cotahist_annual",
                source_file=str(txt_path),
            )
            audit_db.flush()
            run_id = run.id

    rows_touched = 0
    ingest_summary = None

    try:
        with managed_session() as db:
            ingest_summary = ingest_cotahist_txt_file(
                db,
                txt_path,
                track_in_file_duplicates=track_in_file_duplicates,
            )
            rows_touched = ingest_summary.db_upsert_operations
    except Exception as exc:
        logger.exception("[etl_pipeline] COTAHIST annual load FAILED: %s", exc)
        if record_audit and run_id is not None:
            try:
                with managed_session() as audit_db:
                    etl_repo = ETLRunRepository(audit_db)
                    run_obj = etl_repo.get_by_id(run_id)
                    if run_obj is None:
                        logger.warning(
                            "[etl_pipeline] could not reload ETL run id=%s to record failure",
                            run_id,
                        )
                    else:
                        etl_repo.finish_run(
                            run_obj,
                            ETLStatus.FAILED,
                            message=str(exc),
                            rows_inserted=rows_touched,
                            rows_failed=1,
                        )
            except Exception:
                logger.exception(
                    "[etl_pipeline] COTAHIST annual FAILED but could not record audit run"
                )
        return {
            "source_file": txt_path.name,
            "status": ETLStatus.FAILED,
            "error": str(exc),
            "rows_upsert_ops": rows_touched,
        }

    if ingest_summary:
        logger.info(
            "[etl_pipeline] COTAHIST stats file=%s valid=%s invalid=%s dup_keys=%s rows_upsert_ops=%s",
            txt_path.name,
            ingest_summary.normalized_valid,
            ingest_summary.normalized_invalid,
            ingest_summary.in_file_duplicate_keys,
            rows_touched,
        )

    if record_audit and run_id is not None:
        with managed_session() as audit_db:
            etl_repo = ETLRunRepository(audit_db)
            run_obj = etl_repo.get_by_id(run_id)
            if run_obj is None:
                logger.warning(
                    "[etl_pipeline] could not reload ETL run id=%s to record success",
                    run_id,
                )
            else:
                etl_repo.finish_run(
                    run_obj,
                    ETLStatus.SUCCESS,
                    message=None,
                    rows_inserted=rows_touched,
                    rows_failed=0,
                )

    summary = {
        "source_file": txt_path.name,
        "status": ETLStatus.SUCCESS,
        "rows_upsert_ops": rows_touched,
        "normalized_valid": ingest_summary.normalized_valid if ingest_summary else 0,
        "normalized_invalid": ingest_summary.normalized_invalid if ingest_summary else 0,
        "in_file_duplicate_keys": ingest_summary.in_file_duplicate_keys if ingest_summary else 0,
    }
    logger.info("[etl_pipeline] COTAHIST annual load done: %s", summary)
    return summary
