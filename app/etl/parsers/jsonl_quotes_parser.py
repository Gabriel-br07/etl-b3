"""Parser for DailyFluctuationHistory JSONL output files.

Each line in the JSONL file is a JSON object with structure:
{
  "ticker_requested": str,
  "ticker_returned": str,
  "trade_date": "YYYY-MM-DD",
  "collected_at": ISO-8601 UTC string,
  "price_history": [
    {"quote_time": "HH:MM:SS" | null,
     "close_price": str | null,
     "price_fluctuation_percentage": str | null},
    ...
  ]
}

The parser reads the JSONL and returns a list of flat dicts ready for the
FactQuote upsert, where each dict represents a single (ticker, quoted_at) row.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

logger = logging.getLogger(__name__)

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _parse_decimal(value: str | None) -> Decimal | None:
    """Convert a string to Decimal, handling comma decimal separators."""
    if value is None:
        return None
    try:
        return Decimal(str(value).strip().replace(",", "."))
    except InvalidOperation:
        return None


def _build_quoted_at(trade_date: date, quote_time: str | None) -> datetime:
    """Combine trade_date + quote_time string into an aware UTC datetime.

    If quote_time is missing or unparseable we fall back to midnight UTC on
    trade_date so the row still has a valid timestamp for the hypertable.
    """
    if quote_time:
        # Typical format: "10:34:00" or "10:34"
        parts = quote_time.strip().split(":")
        try:
            hour = int(parts[0])
            minute = int(parts[1]) if len(parts) > 1 else 0
            second = int(parts[2]) if len(parts) > 2 else 0
            return datetime(
                trade_date.year, trade_date.month, trade_date.day,
                hour, minute, second, tzinfo=timezone.utc,
            )
        except (ValueError, IndexError):
            pass
    return datetime(trade_date.year, trade_date.month, trade_date.day, tzinfo=UTC)


def parse_jsonl_quotes(path: Path | str) -> list[dict]:
    """Parse a DailyFluctuationHistory JSONL file into flat quote dicts.

    Each returned dict has keys matching the FactQuote model:
        ticker, quoted_at, trade_date, close_price,
        price_fluctuation_pct, source_jsonl

    Lines that cannot be parsed are skipped with a warning.
    Ticker names are upper-cased.

    Returns:
        List of dicts, one per (ticker, quote_time) price point.
    """
    path = Path(path)
    rows: list[dict] = []
    source_name = path.name
    skipped = 0

    logger.info("Parsing JSONL quotes file: %s", path)

    with open(path, encoding="utf-8") as fh:
        for lineno, raw_line in enumerate(fh, start=1):
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            try:
                record = json.loads(raw_line)
            except json.JSONDecodeError as exc:
                logger.warning("JSONL line %d: JSON decode error – %s", lineno, exc)
                skipped += 1
                continue

            ticker = str(record.get("ticker_requested") or record.get("ticker_returned") or "").upper()
            if not ticker:
                logger.debug("JSONL line %d: missing ticker – skipped", lineno)
                skipped += 1
                continue

            trade_date_raw = record.get("trade_date")
            try:
                trade_date = date.fromisoformat(str(trade_date_raw))
            except (ValueError, TypeError):
                logger.warning(
                    "JSONL line %d ticker=%s: invalid trade_date '%s' – skipped",
                    lineno, ticker, trade_date_raw,
                )
                skipped += 1
                continue

            price_history: list[dict] = record.get("price_history") or []
            if not price_history:
                logger.debug("JSONL line %d ticker=%s: empty price_history", lineno, ticker)
                # Still include a sentinel row so we can track that the ticker was fetched
                rows.append({
                    "ticker": ticker,
                    "quoted_at": _build_quoted_at(trade_date, None),
                    "trade_date": trade_date,
                    "close_price": None,
                    "price_fluctuation_pct": None,
                    "source_jsonl": source_name,
                })
                continue

            for point in price_history:
                quoted_at = _build_quoted_at(trade_date, point.get("quote_time"))
                rows.append({
                    "ticker": ticker,
                    "quoted_at": quoted_at,
                    "trade_date": trade_date,
                    "close_price": _parse_decimal(point.get("close_price")),
                    "price_fluctuation_pct": _parse_decimal(
                        point.get("price_fluctuation_percentage")
                    ),
                    "source_jsonl": source_name,
                })

    total = len(rows)
    logger.info(
        "JSONL parsed: %d price points across ~%d tickers (%d lines skipped)",
        total, total, skipped,
    )
    return rows

