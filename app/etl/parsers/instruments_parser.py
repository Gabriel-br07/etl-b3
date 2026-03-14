"""Parser for B3 instruments (CadInstrumento) CSV files.

B3 Cadastro de Instrumentos – Listado
Expected format: semicolon-delimited CSV with header row.
"""

import csv
from pathlib import Path

import polars as pl

from app.core.logging import get_logger
from app.etl.parsers.column_mapping import INSTRUMENT_COLUMN_MAP, map_columns

logger = get_logger(__name__)

# Separators to attempt in order
_SEPARATORS = [";", ",", "\t", "|"]

# Minimum number of columns the real B3 instruments header must have.
# Both known format variants have ≥ 4 columns.
_MIN_HEADER_COLS = 4

# Known header tokens drawn from INSTRUMENT_COLUMN_MAP keys (normalized to lowercase).
# At least one of these must appear in the header row for the parse to proceed.
_KNOWN_HEADER_TOKENS = frozenset({
    "tckrsymb", "tckr", "codneg", "instrumento financeiro", "instrumento",
    "nmofc", "isin", "sgmt", "segmento", "dtrfrn", "ativo",
})


def _validate_header(columns: list[str], path: Path) -> None:
    """Raise ValueError if *columns* do not look like a real instruments header.

    Checks:
    1. Minimum column count (>= _MIN_HEADER_COLS).
    2. At least one known token present (case-insensitive).

    This catches the case where a preamble/descriptive line was mistakenly
    treated as the header, which produces zero column-mapping hits downstream
    and results in 0 rows silently loaded into the database.
    """
    if len(columns) < _MIN_HEADER_COLS:
        raise ValueError(
            f"instruments_parser: file {path.name} header has only {len(columns)} "
            f"column(s) — expected at least {_MIN_HEADER_COLS}. "
            f"Columns found: {columns}. "
            "This usually means a preamble/descriptive line was parsed as the "
            "header. Check that the normalized CSV starts at the real header row."
        )

    lower_cols = {c.strip().lower() for c in columns}
    matched = lower_cols & _KNOWN_HEADER_TOKENS
    if not matched:
        raise ValueError(
            f"instruments_parser: file {path.name} header columns {columns} do not "
            f"contain any known instruments column token. "
            f"Known tokens: {sorted(_KNOWN_HEADER_TOKENS)}. "
            "This usually means a preamble/descriptive line was parsed as the "
            "header, or the file format has changed significantly."
        )


def parse_instruments_csv(path: Path | str) -> pl.DataFrame:
    """Parse a B3 instruments CSV and return a normalised Polars DataFrame.

    Columns returned (subset that mapped successfully):
        ticker, asset_name, isin, segment, source_file_date

    Unknown columns are dropped.  All fields are cast to strings first so
    downstream transforms can handle them uniformly.

    Raises ValueError with a clear message if the header cannot be validated.
    """
    path = Path(path)
    logger.info("[instruments_parser] parsing: %s", path)

    # Log the first few raw lines for diagnostic purposes — critical for
    # identifying Docker vs local discrepancies.
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f, delimiter=";")
            first_lines = [next(reader, None) for _ in range(5)]
        logger.info(
            "[instruments_parser] first 5 rows of %s: %s",
            path.name, [r for r in first_lines if r is not None],
        )
    except Exception as exc:
        logger.warning("[instruments_parser] could not preview %s: %s", path.name, exc)

    df: pl.DataFrame | None = None
    last_error: Exception | None = None

    for sep in _SEPARATORS:
        try:
            candidate = pl.read_csv(
                path,
                separator=sep,
                infer_schema_length=0,  # all columns as Utf8 initially
                ignore_errors=True,
                truncate_ragged_lines=True,
                encoding="utf8-lossy",
            )
            if candidate.width >= _MIN_HEADER_COLS:
                df = candidate
                logger.info(
                    "[instruments_parser] separator=%r produced %d columns",
                    sep, candidate.width,
                )
                break
            else:
                logger.debug(
                    "[instruments_parser] separator=%r produced only %d column(s) — skipping",
                    sep, candidate.width,
                )
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            logger.debug("[instruments_parser] separator=%r failed: %s", sep, exc)
            continue

    if df is None or df.width == 0:
        raise ValueError(
            f"[instruments_parser] Could not parse instruments CSV with any separator: {path}. "
            f"Last error: {last_error}"
        )

    logger.info(
        "[instruments_parser] raw columns (%d): %s",
        df.width, df.columns,
    )

    # Validate that the header looks like a real instruments header before
    # attempting column mapping. This is the key defence against preamble lines
    # being silently treated as headers.
    _validate_header(df.columns, path)

    # Rename to internal names
    rename_map = map_columns(df.columns, INSTRUMENT_COLUMN_MAP)
    logger.info("[instruments_parser] column rename map: %s", rename_map)

    if not rename_map:
        raise ValueError(
            f"[instruments_parser] No columns in {path.name} matched INSTRUMENT_COLUMN_MAP. "
            f"Raw columns: {df.columns}. "
            "Verify that the normalized CSV starts at the real header row and that "
            "INSTRUMENT_COLUMN_MAP contains the expected B3 column names."
        )

    df = df.rename(rename_map)

    # Keep only internal columns we care about
    internal_cols = ["ticker", "asset_name", "isin", "segment", "source_file_date"]
    available = [c for c in internal_cols if c in df.columns]
    df = df.select(available)

    logger.info(
        "[instruments_parser] parsed %d rows, columns: %s",
        len(df), df.columns,
    )
    return df
