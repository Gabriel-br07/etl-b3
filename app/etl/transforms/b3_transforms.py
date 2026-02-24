"""Polars-based transformation functions for B3 data.

All transform functions are pure (no side effects) and accept/return
Polars DataFrames for easy testing.
"""

from datetime import date

import polars as pl

from app.core.logging import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _safe_cast_numeric(df: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    """Cast string columns to Float64, handling comma decimal separators."""
    exprs = []
    for col in columns:
        if col not in df.columns:
            continue
        exprs.append(
            pl.col(col)
            .str.strip_chars()
            .str.replace(",", ".")
            .cast(pl.Float64, strict=False)
            .alias(col)
        )
    if exprs:
        df = df.with_columns(exprs)
    return df


def _trim_strings(df: pl.DataFrame) -> pl.DataFrame:
    """Trim whitespace from all Utf8/String columns."""
    exprs = [
        pl.col(c).str.strip_chars().alias(c)
        for c in df.columns
        if df[c].dtype in (pl.Utf8, pl.String)
    ]
    if exprs:
        df = df.with_columns(exprs)
    return df


def _parse_date_column(series: pl.Series) -> pl.Series:
    """Try multiple date formats to parse a string date column."""
    formats = ["%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y", "%Y%m%d"]
    for fmt in formats:
        try:
            return series.str.to_date(fmt, strict=False)
        except Exception:  # noqa: BLE001
            continue
    return series.cast(pl.Date, strict=False)


# ---------------------------------------------------------------------------
# Instruments transform
# ---------------------------------------------------------------------------


def transform_instruments(df: pl.DataFrame, file_date: date) -> list[dict]:
    """Normalise instruments DataFrame and return list of dicts for DB upsert.

    Steps:
    1. Trim strings
    2. Uppercase tickers
    3. Drop rows without ticker
    4. Deduplicate by ticker
    5. Parse source_file_date
    6. Convert to list[dict]
    """
    df = _trim_strings(df)

    if "ticker" in df.columns:
        if len(df) == 0:
            return []
        df = df.with_columns(pl.col("ticker").str.to_uppercase().alias("ticker"))
        df = df.filter(pl.col("ticker").is_not_null() & (pl.col("ticker") != ""))
        df = df.unique(subset=["ticker"], keep="first")
    else:
        logger.warning("No 'ticker' column found in instruments DataFrame.")
        return []

    if "source_file_date" in df.columns:
        df = df.with_columns(
            _parse_date_column(df["source_file_date"]).alias("source_file_date")
        )
    else:
        df = df.with_columns(pl.lit(file_date).alias("source_file_date"))

    # Ensure all expected nullable columns exist
    for col in ["asset_name", "isin", "segment"]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

    logger.info("Instruments transformed: %d rows", len(df))
    return df.to_dicts()


# ---------------------------------------------------------------------------
# Trades transform
# ---------------------------------------------------------------------------

_NUMERIC_COLS = [
    "last_price", "min_price", "max_price", "avg_price", "variation_pct",
    "financial_volume", "trade_count",
]


def transform_trades(df: pl.DataFrame, file_name: str) -> list[dict]:
    """Normalise trades DataFrame and return list of dicts for DB upsert.

    Steps:
    1. Trim strings
    2. Uppercase tickers
    3. Parse trade_date
    4. Cast numeric columns
    5. Drop rows without ticker or trade_date
    6. Deduplicate by (ticker, trade_date)
    7. Add source_file_name
    8. Convert to list[dict]
    """
    df = _trim_strings(df)

    if "ticker" not in df.columns or "trade_date" not in df.columns:
        logger.warning("Trades DataFrame missing required columns.")
        return []

    df = df.with_columns(pl.col("ticker").str.to_uppercase().alias("ticker"))
    df = df.with_columns(_parse_date_column(df["trade_date"]).alias("trade_date"))
    df = _safe_cast_numeric(df, _NUMERIC_COLS)

    df = df.filter(
        pl.col("ticker").is_not_null()
        & (pl.col("ticker") != "")
        & pl.col("trade_date").is_not_null()
    )
    df = df.unique(subset=["ticker", "trade_date"], keep="first")
    df = df.with_columns(pl.lit(file_name).alias("source_file_name"))

    # Ensure all expected nullable numeric columns exist
    for col in _NUMERIC_COLS:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Float64).alias(col))

    logger.info("Trades transformed: %d rows", len(df))
    return df.to_dicts()
