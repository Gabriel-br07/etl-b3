"""Validation helpers for scraped CSV outputs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from app.core.logging import get_logger
from app.etl.parsers.column_mapping import TRADE_COLUMN_MAP, _normalize_name
from app.etl.validation.b3_locale_numbers import parse_b3_locale_number

logger = get_logger(__name__)


@dataclass
class ValidationResult:
    """Validation result for a scraped output file."""

    path: Path
    rows: int
    ok: bool
    message: str


def _assert_path_exists(path: Path) -> None:
    if not path.exists() or not path.is_file():
        raise ValueError(f"File does not exist: {path}")
    if path.stat().st_size == 0:
        raise ValueError(f"File is empty: {path}")


# B3 CSVs use "-" for missing numeric / not-applicable cells; empty fields are common.
_B3_NULL_STRINGS = ("-", "")

# TRADE_COLUMN_MAP values treated as numeric in downstream ETL (prices, counts, volume).
_NUMERIC_INTERNAL = frozenset(
    {
        "last_price",
        "min_price",
        "max_price",
        "avg_price",
        "open_price",
        "variation_pct",
        "financial_volume",
        "trade_count",
    }
)

# Normalized forms of TRADE_COLUMN_MAP *keys* that map to numeric internals.
# Matching CSV headers via _normalize_name (same rules as map_columns) covers
# ASCII-only B3 spellings (e.g. "Preco de fechamento" vs "Preço de fechamento").
_LOCALE_NUMERIC_HEADER_NORMALIZED: frozenset[str] = frozenset(
    _normalize_name(k) for k, v in TRADE_COLUMN_MAP.items() if v in _NUMERIC_INTERNAL
)


def _semicolon_headers_first_line(path: Path) -> list[str]:
    """Return header names from the first line (semicolon-separated).

    The file is read once here and again in full by Polars: a deliberate tradeoff
    to know which columns need Utf8 + locale parsing before ``read_csv``. Boletim
    CSVs are small enough that the extra pass is negligible; revisit if files grow
    to very large multi-GB exports.
    """
    with path.open(encoding="utf-8", errors="replace") as f:
        line = f.readline()
    if not line:
        return []
    return line.rstrip("\r\n").split(";")


def _locale_numeric_headers(headers: list[str]) -> list[str]:
    """Headers in file order that match TRADE numeric columns after normalization."""
    return [h for h in headers if _normalize_name(h) in _LOCALE_NUMERIC_HEADER_NORMALIZED]


def _read_csv(path: Path) -> pl.DataFrame:
    """Load a B3-style semicolon CSV with deterministic numeric columns.

    Columns that map to numeric TRADE fields (accent/case-insensitive) are read as
    Utf8 and cast with :func:`parse_b3_locale_number` to Float64 so pt-BR decimals
    and mixed integer/fractional rows do not rely on Polars inference.
    """
    headers = _semicolon_headers_first_line(path)
    locale_numeric = _locale_numeric_headers(headers)
    schema_overrides = {h: pl.Utf8 for h in locale_numeric}
    df = pl.read_csv(
        path,
        separator=";",
        infer_schema_length=10_000,
        null_values=list(_B3_NULL_STRINGS),
        ignore_errors=False,
        truncate_ragged_lines=False,
        encoding="utf8-lossy",
        schema_overrides=schema_overrides,
    )
    if locale_numeric:
        parsed_cols: list[pl.Series] = []
        for h in locale_numeric:
            raw = df[h]
            parsed = raw.map_elements(parse_b3_locale_number, return_dtype=pl.Float64)
            n_rows = df.height
            raw_non_null = int(raw.is_not_null().sum())
            parsed_non_null = int(parsed.is_not_null().sum())
            invalid = raw.is_not_null() & parsed.is_null()
            n_invalid = int(invalid.sum())
            if n_invalid > 0:
                samples = (
                    df.filter(invalid).select(pl.col(h).head(3)).to_series().to_list()
                )
                logger.debug(
                    "locale_numeric_parse path=%s col=%s rows=%s raw_non_null=%s "
                    "parsed_non_null=%s invalid=%s sample_raw=%s",
                    path.name,
                    h,
                    n_rows,
                    raw_non_null,
                    parsed_non_null,
                    n_invalid,
                    samples,
                )
            else:
                logger.debug(
                    "locale_numeric_parse path=%s col=%s rows=%s raw_non_null=%s "
                    "parsed_non_null=%s invalid=0",
                    path.name,
                    h,
                    n_rows,
                    raw_non_null,
                    parsed_non_null,
                )
            parsed_cols.append(parsed.alias(h))
        df = df.with_columns(parsed_cols)
    logger.debug(
        "read scraped csv path=%s rows=%s locale_numeric_cols=%s",
        path.name,
        len(df),
        locale_numeric,
    )
    return df


def validate_scraped_csv(
    *,
    path: Path,
    required_columns: set[str],
    key_columns: list[str],
    min_rows: int = 1,
    max_rows: int | None = None,
    enforce_unique_keys: bool = True,
) -> ValidationResult:
    """Validate CSV existence/schema/keys and light row-count checks."""

    _assert_path_exists(path)
    df = _read_csv(path)

    if len(df) < min_rows:
        raise ValueError(f"Expected at least {min_rows} rows in {path}, got {len(df)}")
    if max_rows is not None and len(df) > max_rows:
        raise ValueError(f"Expected at most {max_rows} rows in {path}, got {len(df)}")

    missing = sorted(required_columns - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns in {path.name}: {missing}")

    for col in key_columns:
        if col not in df.columns:
            raise ValueError(f"Key column missing in {path.name}: {col}")
        nulls = df.filter(pl.col(col).is_null() | (pl.col(col).cast(pl.Utf8).str.strip_chars() == ""))
        if len(nulls) > 0:
            raise ValueError(f"Key column '{col}' has null/blank values in {path.name}")

    if enforce_unique_keys:
        duplicate_count = len(df) - len(df.unique(subset=key_columns, keep="first"))
        if duplicate_count > 0:
            raise ValueError(f"Duplicate key rows found in {path.name} for keys {key_columns}")

    return ValidationResult(path=path, rows=len(df), ok=True, message="ok")
