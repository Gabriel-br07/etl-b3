"""Batch intraday-series ingestion use case.

Reads ticker symbols from a normalized B3 instruments CSV, fetches the
DailyFluctuationHistory endpoint for each ticker, and writes:

  1. One **JSONL file** with all tickers merged – each line is a JSON object
     containing the ticker metadata and the full ``lstQtn`` price history as
     an array (``price_history``).
  2. A **report CSV** summarising every attempted ticker: HTTP status, whether
     the request succeeded, and how many data points were collected.

JSONL line schema
-----------------
{
  "ticker_requested":              str,
  "ticker_returned":               str,
  "business_status_code":          str,
  "message_datetime":              str | null,
  "trade_date":                    str,   // YYYY-MM-DD
  "collected_at":                  str,   // ISO-8601 UTC
  "points_count":                  int,
  "price_history": [
    {
      "quote_time":                    str | null,
      "close_price":                   str | null,
      "price_fluctuation_percentage":  str | null
    },
    ...
  ]
}

Report CSV columns
------------------
- ticker_requested
- ticker_returned
- trade_date
- http_status          : HTTP status code returned (200, 404, etc.) or "ERROR"
- request_succeeded    : "true" / "false"
- points_count         : number of intraday price points (empty on failure)
- error_message        : empty on success; exception message on failure

Output paths
------------
JSONL (all tickers in one file):
  {output_dir}/{reference_date}/daily_fluctuation_{YYYYMMDDTHHMMSS}.jsonl

Report CSV:
  {output_dir}/{reference_date}/report_{YYYYMMDDTHHMMSS}.csv
"""

from __future__ import annotations

import csv
import json
import logging
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from app.etl.ingestion.ticker_filter import TickerFilterResult, build_ticker_filter
from app.etl.ingestion.ticker_reader import read_tickers_from_csv
from app.integrations.b3.client import B3QuoteClient
from app.integrations.b3.exceptions import B3ClientError
from app.integrations.b3.parser import parse_intraday_series

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CSV column definitions
# ---------------------------------------------------------------------------

REPORT_FIELDNAMES: list[str] = [
    "ticker_requested",
    "ticker_returned",
    "trade_date",
    "http_status",
    "request_succeeded",
    "points_count",
    "error_message",
]


# ---------------------------------------------------------------------------
# Output path helpers
# ---------------------------------------------------------------------------


def _jsonl_output_path(
    reference_date: date,
    collected_at: datetime,
    output_dir: Path,
) -> Path:
    """Return the single JSONL output path for all tickers."""
    timestamp = collected_at.strftime("%Y%m%dT%H%M%S")
    folder = output_dir / reference_date.isoformat()
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"daily_fluctuation_{timestamp}.jsonl"


def _report_output_path(
    reference_date: date,
    collected_at: datetime,
    output_dir: Path,
) -> Path:
    """Return the batch report CSV path for *reference_date*."""
    timestamp = collected_at.strftime("%Y%m%dT%H%M%S")
    folder = output_dir / reference_date.isoformat()
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"report_{timestamp}.csv"


# ---------------------------------------------------------------------------
# JSONL line builder
# ---------------------------------------------------------------------------


def _build_jsonl_record(
    *,
    ticker_requested: str,
    raw_payload: dict[str, Any],
    collected_at: datetime,
) -> dict[str, Any]:
    """Parse *raw_payload* and return a dict ready for JSONL serialisation."""
    series = parse_intraday_series(raw_payload, requested_ticker=ticker_requested)

    biz_sts = raw_payload.get("BizSts") or {}
    msg = raw_payload.get("Msg") or {}

    price_history = [
        {
            "quote_time": point.time or None,
            "close_price": str(point.close_price) if point.close_price is not None else None,
            "price_fluctuation_percentage": (
                str(point.price_fluctuation_pct)
                if point.price_fluctuation_pct is not None
                else None
            ),
        }
        for point in series.points
    ]

    return {
        "ticker_requested": ticker_requested,
        "ticker_returned": series.ticker,
        "business_status_code": str(biz_sts.get("cd") or ""),
        "message_datetime": str(msg.get("dtTm") or "") or None,
        "trade_date": series.trade_date.isoformat(),
        "collected_at": collected_at.isoformat(),
        "points_count": len(series.points),
        "price_history": price_history,
    }


# ---------------------------------------------------------------------------
# Report row builders
# ---------------------------------------------------------------------------


def _report_success_row(
    ticker_requested: str,
    ticker_returned: str,
    trade_date_str: str,
    http_status: int,
    points_count: int,
) -> dict[str, str]:
    return {
        "ticker_requested": ticker_requested,
        "ticker_returned": ticker_returned,
        "trade_date": trade_date_str,
        "http_status": str(http_status),
        "request_succeeded": "true",
        "points_count": str(points_count),
        "error_message": "",
    }


def _report_failure_row(
    ticker_requested: str,
    http_status: int | str,
    error: Exception,
) -> dict[str, str]:
    return {
        "ticker_requested": ticker_requested,
        "ticker_returned": "",
        "trade_date": "",
        "http_status": str(http_status),
        "request_succeeded": "false",
        "points_count": "",
        "error_message": str(error),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def run_batch_quote_ingestion(
    *,
    instruments_csv: Path | str,
    trades_csv: Path | str | None = None,
    filter_mode: str = "strict",
    reference_date: date | None = None,
    output_dir: Path | str | None = None,
    client: B3QuoteClient | None = None,
) -> Path:
    """Fetch DailyFluctuationHistory intraday series for every ticker in *instruments_csv*.

    All ticker records are appended to a single JSONL file (one JSON line per
    ticker).  A report CSV is written at the end summarising HTTP status codes
    and data-point counts.

    Args:
        instruments_csv: Path to the normalized instruments CSV
            (``cadastro_instrumentos_*.normalized.csv``).
        trades_csv: Optional path to the normalized negocios CSV
            (``negocios_consolidados_*.normalized.csv``).  When provided the
            dual-source filter is activated, producing a *strict* ticker list
            (master ∩ negocios) and a *fallback* list (full master).
        filter_mode: ``"strict"`` (default) – use the intersection of master
            and negocios.  ``"fallback"`` – use the full master list regardless
            of negocios.  Ignored when *trades_csv* is ``None``.
        reference_date: Date label used for the output folder.
            Defaults to today.
        output_dir: Root directory for output files.
            Defaults to ``data/raw/b3/daily_fluctuation_history`` relative to
            the project root.
        client: Optional pre-built ``B3QuoteClient`` (useful in tests).

    Returns:
        Path to the written report CSV.
    """
    instruments_csv = Path(instruments_csv)
    ref_date = reference_date or date.today()
    collected_at = datetime.now(UTC)

    if output_dir is None:
        project_root = Path(__file__).resolve().parents[3]
        output_dir = project_root / "data" / "raw" / "b3" / "daily_fluctuation_history"

    output_dir = Path(output_dir)

    jsonl_path = _jsonl_output_path(ref_date, collected_at, output_dir)
    report_path = _report_output_path(ref_date, collected_at, output_dir)

    # ------------------------------------------------------------------
    # Resolve ticker list via dual-source filter or plain reader
    # ------------------------------------------------------------------
    if trades_csv is not None:
        filter_result: TickerFilterResult = build_ticker_filter(
            instruments_csv,
            trades_csv=trades_csv,
            reference_date=ref_date,
        )
        if filter_mode == "fallback":
            tickers = filter_result.fallback_master_tickers
            logger.info(
                "Filter mode=fallback: using %d master tickers "
                "(%d strict, %d master-only).",
                len(tickers),
                len(filter_result.strict_filtered_tickers),
                len(filter_result.master_only_tickers),
            )
        else:
            tickers = filter_result.strict_filtered_tickers
            logger.info(
                "Filter mode=strict: using %d tickers "
                "(%d master, %d master-only excluded).",
                len(tickers),
                len(filter_result.fallback_master_tickers),
                len(filter_result.master_only_tickers),
            )
    else:
        tickers = read_tickers_from_csv(instruments_csv)
        logger.info("No trades CSV provided – using all %d tickers from instruments CSV.", len(tickers))

    total = len(tickers)
    logger.info(
        "Batch intraday series ingestion starting: %d tickers → JSONL '%s' | report '%s'",
        total,
        jsonl_path,
        report_path,
    )

    if client is None:
        http_client = B3QuoteClient()
        own_client = True
    else:
        http_client = client
        own_client = False

    report_rows: list[dict[str, str]] = []
    successes = 0
    failures = 0

    try:
        http_client.warm_session()

        with open(jsonl_path, "w", encoding="utf-8") as jfh:
            for i, ticker in enumerate(tickers, start=1):
                logger.info(
                    "[%d/%d] Fetching intraday series for ticker '%s'", i, total, ticker
                )

                # Track the HTTP status code even for errors
                http_status: int | str = "ERROR"

                try:
                    # Use the public client API and capture status code without
                    # monkey-patching private methods.
                    try:
                        raw_payload = http_client.get_daily_fluctuation_history(ticker)
                        # If no exception is raised, we assume a successful 2xx response.
                        # Prefer an upstream status code exposed by the client, if any,
                        # otherwise fall back to 200 to preserve existing behavior.
                        status = getattr(http_client, "last_status_code", None)
                        http_status = status if status is not None else 200
                    except B3ClientError as exc:
                        # Record the HTTP status code from the exception (if available)
                        # so that the outer handler and report can use it.
                        status = getattr(exc, "status_code", None)
                        if status is not None:
                            http_status = status
                        raise

                    record = _build_jsonl_record(
                        ticker_requested=ticker,
                        raw_payload=raw_payload,
                        collected_at=collected_at,
                    )

                    jfh.write(json.dumps(record, ensure_ascii=False) + "\n")

                    logger.info(
                        "[%d/%d] '%s': %d points — HTTP %s",
                        i, total, ticker, record["points_count"], http_status,
                    )
                    successes += 1
                    report_rows.append(
                        _report_success_row(
                            ticker,
                            record["ticker_returned"],
                            record["trade_date"],
                            int(http_status),
                            record["points_count"],
                        )
                    )

                except B3ClientError as exc:
                    logger.warning(
                        "[%d/%d] Failed for ticker '%s': %s", i, total, ticker, exc
                    )
                    failures += 1
                    report_rows.append(_report_failure_row(ticker, http_status, exc))

                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        "[%d/%d] Unexpected error for ticker '%s': %s",
                        i, total, ticker, exc,
                    )
                    failures += 1
                    report_rows.append(_report_failure_row(ticker, http_status, exc))

    finally:
        if own_client:
            http_client.close()

    # Write the report CSV after all tickers are processed
    with open(report_path, "w", encoding="utf-8", newline="") as rfh:
        writer = csv.DictWriter(rfh, fieldnames=REPORT_FIELDNAMES)
        writer.writeheader()
        writer.writerows(report_rows)

    logger.info(
        "Batch complete: %d succeeded, %d failed out of %d tickers. "
        "JSONL: '%s' | Report: '%s'",
        successes, failures, total, jsonl_path, report_path,
    )
    return report_path
