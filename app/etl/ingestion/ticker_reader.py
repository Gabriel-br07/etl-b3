"""Read B3 ticker symbols from a normalized instruments CSV file.

The normalized CSV produced by the B3 boletim diário scraper uses a
semicolon delimiter and has the column ``Instrumento financeiro`` as the
first column containing the ticker symbol.

This module is intentionally simple: it only knows how to read tickers.
All HTTP, parsing, and output concerns live elsewhere.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from app.integrations.b3.constants import MAX_TICKER_LENGTH, TICKER_COLUMN

logger = logging.getLogger(__name__)


class TickerColumnMissingError(ValueError):
    """Raised when the expected ticker column is absent from the CSV."""

    def __init__(self, path: Path, expected_column: str) -> None:
        self.path = path
        self.expected_column = expected_column
        super().__init__(
            f"Column '{expected_column}' not found in '{path}'. "
            f"Make sure the file is a normalized B3 instruments CSV "
            f"(cadastro_instrumentos_*.normalized.csv)."
        )


def _is_valid_ticker(value: str) -> bool:
    """Return True when *value* looks like a real B3 ticker symbol.

    Rejects empty values and suspiciously long strings that are footer
    notes embedded in the CSV (e.g. B3 informational messages at the end).
    """
    return bool(value) and len(value) <= MAX_TICKER_LENGTH and " " not in value


def read_tickers_from_csv(path: Path | str) -> list[str]:
    """Return a deduplicated, ordered list of tickers from the instruments CSV.

    Args:
        path: Path to the normalized instruments CSV
              (e.g. ``cadastro_instrumentos_20260226.normalized.csv``).

    Returns:
        A list of uppercase ticker strings in the order they first appear,
        with duplicates removed and noise/footer rows filtered out.

    Raises:
        TickerColumnMissingError: If the ``Instrumento financeiro`` column
            is absent from the CSV header.
        FileNotFoundError: If *path* does not exist.
    """
    path = Path(path)
    logger.info("Reading tickers from '%s'", path)

    with open(path, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=";")

        if reader.fieldnames is None or TICKER_COLUMN not in reader.fieldnames:
            # Trigger reading the header by accessing fieldnames early.
            # DictReader reads the first row lazily; force it.
            _ = list(reader)
            if TICKER_COLUMN not in (reader.fieldnames or []):
                raise TickerColumnMissingError(path, TICKER_COLUMN)

        seen: set[str] = set()
        tickers: list[str] = []
        skipped = 0

        for row in reader:
            raw = (row.get(TICKER_COLUMN) or "").strip()
            ticker = raw.upper()

            if not _is_valid_ticker(ticker):
                skipped += 1
                continue

            if ticker not in seen:
                seen.add(ticker)
                tickers.append(ticker)

    logger.info(
        "Tickers loaded: %d unique, %d rows skipped (empty/noise)",
        len(tickers),
        skipped,
    )
    return tickers
