"""Parser for B3 consolidated trades (NegociosConsolidados) files.

B3 Negócios Consolidados do Pregão – Listado
Expected format: ZIP archive containing a semicolon-delimited CSV,
or a plain semicolon-delimited CSV.
"""

import io
import zipfile
from pathlib import Path

import polars as pl

from app.core.logging import get_logger
from app.etl.parsers.column_mapping import TRADE_COLUMN_MAP, map_columns

logger = get_logger(__name__)

_SEPARATORS = [";", ",", "\t", "|"]


def _read_csv_bytes(data: bytes) -> pl.DataFrame:
    for sep in _SEPARATORS:
        try:
            df = pl.read_csv(
                io.BytesIO(data),
                separator=sep,
                infer_schema_length=0,
                ignore_errors=True,
                truncate_ragged_lines=True,
                encoding="utf8-lossy",
            )
            if df.width > 1:
                return df
        except Exception:  # noqa: BLE001
            continue
    raise ValueError("Could not parse trades CSV data with any known separator")


def parse_trades_file(path: Path | str) -> pl.DataFrame:
    """Parse a B3 trades file (ZIP or CSV) and return a normalised DataFrame.

    Columns returned (subset that mapped successfully):
        ticker, trade_date, last_price, min_price, max_price, avg_price,
        variation_pct, financial_volume, trade_count
    """
    path = Path(path)
    logger.info("Parsing trades file: %s", path)

    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                raise ValueError(f"No CSV found inside ZIP: {path}")
            # Use the first CSV (B3 ZIPs typically contain one file)
            data = zf.read(csv_names[0])
        df = _read_csv_bytes(data)
    else:
        df = _read_csv_bytes(path.read_bytes())

    # Rename to internal names
    rename_map = map_columns(df.columns, TRADE_COLUMN_MAP)
    if not rename_map:
        logger.warning(
            "No columns matched TRADE_COLUMN_MAP for %s. Columns: %s",
            path,
            df.columns,
        )
    df = df.rename(rename_map)

    internal_cols = [
        "ticker",
        "trade_date",
        "last_price",
        "min_price",
        "max_price",
        "avg_price",
        "variation_pct",
        "financial_volume",
        "trade_count",
    ]
    available = [c for c in internal_cols if c in df.columns]
    df = df.select(available)

    logger.info("Trades file parsed: %d rows, columns: %s", len(df), df.columns)
    return df
