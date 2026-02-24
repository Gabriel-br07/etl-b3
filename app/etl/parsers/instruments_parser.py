"""Parser for B3 instruments (CadInstrumento) CSV files.

B3 Cadastro de Instrumentos – Listado
Expected format: semicolon-delimited CSV with header row.
"""

from pathlib import Path

import polars as pl

from app.core.logging import get_logger
from app.etl.parsers.column_mapping import INSTRUMENT_COLUMN_MAP, map_columns

logger = get_logger(__name__)

# Separators to attempt in order
_SEPARATORS = [";", ",", "\t", "|"]


def parse_instruments_csv(path: Path | str) -> pl.DataFrame:
    """Parse a B3 instruments CSV and return a normalised Polars DataFrame.

    Columns returned (subset that mapped successfully):
        ticker, asset_name, isin, segment, source_file_date

    Unknown columns are dropped.  All fields are cast to strings first so
    downstream transforms can handle them uniformly.
    """
    path = Path(path)
    logger.info("Parsing instruments file: %s", path)

    df: pl.DataFrame | None = None
    for sep in _SEPARATORS:
        try:
            df = pl.read_csv(
                path,
                separator=sep,
                infer_schema_length=0,  # all columns as Utf8 initially
                ignore_errors=True,
                truncate_ragged_lines=True,
                encoding="utf8-lossy",
            )
            if df.width > 1:
                break
        except Exception:  # noqa: BLE001
            continue

    if df is None or df.width == 0:
        raise ValueError(f"Could not parse instruments CSV: {path}")

    # Rename to internal names
    rename_map = map_columns(df.columns, INSTRUMENT_COLUMN_MAP)
    if not rename_map:
        logger.warning(
            "No columns matched INSTRUMENT_COLUMN_MAP for %s. Columns: %s",
            path,
            df.columns,
        )
    df = df.rename(rename_map)

    # Keep only internal columns we care about
    internal_cols = ["ticker", "asset_name", "isin", "segment", "source_file_date"]
    available = [c for c in internal_cols if c in df.columns]
    df = df.select(available)

    logger.info("Instruments file parsed: %d rows, columns: %s", len(df), df.columns)
    return df
