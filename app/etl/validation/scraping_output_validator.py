"""Validation helpers for scraped CSV outputs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl


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


def _read_csv(path: Path) -> pl.DataFrame:
    return pl.read_csv(
        path,
        separator=";",
        infer_schema_length=10_000,
        null_values=list(_B3_NULL_STRINGS),
        ignore_errors=False,
        truncate_ragged_lines=False,
        encoding="utf8-lossy",
    )


def validate_scraped_csv(
    *,
    path: Path,
    required_columns: set[str],
    key_columns: list[str],
    min_rows: int = 1,
    max_rows: int | None = None,
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

    duplicate_count = len(df) - len(df.unique(subset=key_columns, keep="first"))
    if duplicate_count > 0:
        raise ValueError(f"Duplicate key rows found in {path.name} for keys {key_columns}")

    return ValidationResult(path=path, rows=len(df), ok=True, message="ok")
