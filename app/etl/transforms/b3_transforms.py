"""Polars-based transformation functions for B3 data.

All transform functions are pure (no side effects) and accept/return
Polars DataFrames for easy testing.
"""

import re
from datetime import date, datetime, timezone
from typing import Optional

import polars as pl

from app.core.logging import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Schema constants
# ---------------------------------------------------------------------------

# Columns that are absolutely required – we cannot produce a useful row without them.
ESSENTIAL_TRADE_COLUMNS = {"ticker", "trade_date"}

# Columns that enrich the row but whose absence should not block the pipeline.
OPTIONAL_TRADE_COLUMNS = {
    "open_price",
    "last_price",
    "min_price",
    "max_price",
    "avg_price",
    "variation_pct",
    "financial_volume",
    "trade_count",
}

REQUIRED_TRADE_COLUMNS = ESSENTIAL_TRADE_COLUMNS | OPTIONAL_TRADE_COLUMNS

# fact_daily_quotes schema – open_price does NOT exist in this model.
ESSENTIAL_DAILY_QUOTE_COLUMNS = {"ticker", "trade_date"}
OPTIONAL_DAILY_QUOTE_COLUMNS = {
    "last_price",
    "min_price",
    "max_price",
    "avg_price",
    "variation_pct",
    "financial_volume",
    "trade_count",
}
# Numeric columns present in the fact_daily_quotes model (no open_price).
_DAILY_QUOTE_NUMERIC_COLS = [
    "last_price",
    "min_price",
    "max_price",
    "avg_price",
    "variation_pct",
    "financial_volume",
    "trade_count",
]

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _safe_cast_numeric(df: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    """Cast columns to Float64, handling string columns with comma decimal separators.

    - String/Utf8 columns: strip whitespace, replace comma decimal separator, then cast.
    - Already-numeric columns: cast directly (avoids Polars InvalidOperationError when
      calling .str accessor on a non-string Series).
    - Logs a warning for any column that cannot be cast.
    """
    _STRING_DTYPES = (pl.Utf8, pl.String)
    exprs = []
    for col in columns:
        if col not in df.columns:
            continue
        if df[col].dtype in _STRING_DTYPES:
            exprs.append(
                pl.col(col)
                .str.strip_chars()
                .str.replace(",", ".")
                .cast(pl.Float64, strict=False)
                .alias(col)
            )
        else:
            # Column is already numeric (Int*, Float*, etc.) – cast directly.
            exprs.append(
                pl.col(col)
                .cast(pl.Float64, strict=False)
                .alias(col)
            )
    if exprs:
        df = df.with_columns(exprs)
    return df


def _trim_strings(df: pl.DataFrame) -> pl.DataFrame:
    """Trim whitespace from all Utf8/String columns."""
    _STRING_DTYPES = (pl.Utf8, pl.String)
    exprs = [
        pl.col(c).str.strip_chars().alias(c)
        for c in df.columns
        if df[c].dtype in _STRING_DTYPES
    ]
    if exprs:
        df = df.with_columns(exprs)
    return df


def _parse_date_column(series: pl.Series) -> pl.Series:
    """Try multiple date formats to parse a string date column.

    Uses ``strict=True`` so that a format that does not match any non-null
    value raises an exception and allows the next format to be tried.  With
    ``strict=False`` Polars silently returns ``null`` for non-matching values
    instead of raising, which would cause the function to always return the
    result of the first format and never fall back to later formats.
    """
    formats = ["%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y", "%Y%m%d"]
    for fmt in formats:
        try:
            return series.str.to_date(fmt, strict=True)
        except Exception:  # noqa: BLE001
            continue
    return series.cast(pl.Date, strict=False)


def _ensure_uppercase_string(col_name: str):
    """Return a Polars expression that casts column to Utf8 and uppercases it with alias.

    Accepts column name and returns an expression ready for with_columns.
    """
    return pl.col(col_name).cast(pl.Utf8).str.to_uppercase().alias(col_name)


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
        df = df.with_columns(_ensure_uppercase_string("ticker"))
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
    "open_price",
    "last_price",
    "min_price",
    "max_price",
    "avg_price",
    "variation_pct",
    "financial_volume",
    "trade_count",
]

_DATE_IN_FILENAME_RE = re.compile(r"(\d{4})[-_]?(\d{2})[-_]?(\d{2})")


def _extract_date_from_filename(file_name: str) -> Optional[date]:
    """Try to extract a YYYY-MM-DD date from a filename like *_2024-06-14.*"""
    m = _DATE_IN_FILENAME_RE.search(file_name)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    return None


def transform_trades(
    df: pl.DataFrame,
    file_name: str,
    target_date: Optional[date] = None,
) -> list[dict]:
    """Normalise trades DataFrame and return list of dicts for DB upsert.

    Steps:
    1. Log detected columns
    2. Derive trade_date when missing (from target_date or filename)
    3. Trim strings
    4. Uppercase tickers
    5. Parse trade_date
    6. Cast numeric columns (fill missing optional columns with null)
    7. Drop rows without ticker or trade_date
    8. Deduplicate by (ticker, trade_date)
    9. Add source_file_name
    10. Convert to list[dict]

    Args:
        df:          Parsed trades DataFrame (columns already mapped to internal names).
        file_name:   Original filename – used as source_file_name and for date extraction.
        target_date: Pipeline reference date, used as a fallback for trade_date.
    """
    logger.info("Trades columns detected: %s", df.columns)
    # ------------------------------------------------------------------ #
    # Schema-drift guard: log every missing column upfront                #
    # ------------------------------------------------------------------ #
    missing = REQUIRED_TRADE_COLUMNS - set(df.columns)
    if missing:
        logger.warning(
            "Trades transform missing columns: %s. Available columns: %s",
            sorted(missing),
            df.columns,
        )

    # ------------------------------------------------------------------ #
    # Essential columns: ticker                                           #
    # ------------------------------------------------------------------ #
    if "ticker" not in df.columns:
        logger.error(
            "Trades transform cannot continue: 'ticker' column is absent. "
            "Verify that the CSV header row matches the expected B3 format or that "
            "the column mapping (e.g. TRADE_COLUMN_MAP) includes the source column "
            "name that should map to 'ticker'. Available columns: %s",
            df.columns,
        )
        # Log and return an empty result so callers can treat this input as having no usable data
        # without raising an exception; the pipeline can decide how to handle this condition.
        return []

    # ------------------------------------------------------------------ #
    # Essential columns: trade_date – try to derive if missing            #
    # ------------------------------------------------------------------ #
    if "trade_date" not in df.columns:
        derived: Optional[date] = None
        if target_date is not None:
            derived = target_date
            logger.info(
                "trade_date column missing – deriving from pipeline target_date: %s",
                derived,
            )
        else:
            derived = _extract_date_from_filename(file_name)
            if derived:
                logger.info(
                    "trade_date column missing – extracted from filename '%s': %s",
                    file_name,
                    derived,
                )

        if derived is None:
            logger.error(
                "Trades transform cannot continue: 'trade_date' column is absent "
                "and could not be derived from target_date or filename '%s'. "
                "Available columns: %s",
                file_name,
                df.columns,
            )
            # Return empty list instead of raising so callers can skip this input
            return []

        df = df.with_columns(pl.lit(derived).alias("trade_date"))

    # ------------------------------------------------------------------ #
    # Normalise data                                                      #
    # ------------------------------------------------------------------ #
    df = _trim_strings(df)
    df = df.with_columns(_ensure_uppercase_string("ticker"))
    df = df.with_columns(_parse_date_column(df["trade_date"]).alias("trade_date"))
    df = _safe_cast_numeric(df, _NUMERIC_COLS)

    # Fill missing optional numeric columns with null so downstream code
    # always gets a consistent schema.
    for col in _NUMERIC_COLS:
        if col not in df.columns:
            logger.debug("Optional column '%s' missing – filling with null.", col)
            df = df.with_columns(pl.lit(None).cast(pl.Float64).alias(col))

    df = df.filter(
        pl.col("ticker").is_not_null()
        & (pl.col("ticker") != "")
        & pl.col("trade_date").is_not_null()
    )
    df = df.unique(subset=["ticker", "trade_date"], keep="first")
    df = df.with_columns(pl.lit(file_name).alias("source_file_name"))

    mapped_columns = [c for c in df.columns if c != "source_file_name"]
    logger.info("Mapped columns: %s", mapped_columns)

    if len(df) == 0:
        logger.warning(
            "Trades transform produced 0 rows. "
            "All rows were discarded (check ticker/trade_date nulls)."
        )

    logger.info("Trades transformed: %d rows", len(df))
    return df.to_dicts()


# ---------------------------------------------------------------------------
# Daily quotes transform  (fact_daily_quotes)
# ---------------------------------------------------------------------------


def transform_daily_quotes(
    df: pl.DataFrame,
    file_name: str,
    target_date: Optional[date] = None,
) -> list[dict]:
    """Normalise a trades/quotes DataFrame for loading into fact_daily_quotes.

    This is identical in spirit to ``transform_trades`` but targets the
    ``fact_daily_quotes`` ORM model which does **not** have an ``open_price``
    column.  Only the columns that exist in the DB schema are returned so the
    PostgreSQL INSERT never fails with an unknown-column error.

    Expected CSV → DB mapping
    -------------------------
    ticker            → ticker
    last_price        → last_price
    min_price         → min_price
    max_price         → max_price
    avg_price         → avg_price
    variation_pct     → variation_pct
    financial_volume  → financial_volume
    trade_count       → trade_count
    trade_date        → trade_date  (derived when missing)
    source_file_name  → source_file_name  (added internally)

    ``open_price`` and any other unknown columns are intentionally ignored.

    Args:
        df:          Parsed trades DataFrame with internal column names.
        file_name:   Original filename – used as source_file_name and for
                     date extraction when trade_date is absent.
        target_date: Pipeline reference date, used as a fallback for
                     trade_date.

    Returns:
        list[dict] ready for ``QuoteRepository.upsert_many``.
    """
    # ------------------------------------------------------------------ #
    # 1. Schema validation log                                            #
    # ------------------------------------------------------------------ #
    logger.info(
        "[daily_quotes] Detected columns: %s", df.columns
    )

    # ------------------------------------------------------------------ #
    # 2. Required: ticker                                                 #
    # ------------------------------------------------------------------ #
    if "ticker" not in df.columns:
        logger.error(
            "[daily_quotes] Cannot continue: 'ticker' column is absent. "
            "Available columns: %s",
            df.columns,
        )
        # Log error and stop processing this file; callers handle missing data
        # by receiving an empty list instead of an exception.
        return []

    # ------------------------------------------------------------------ #
    # 3. Required: trade_date – derive when missing                      #
    # ------------------------------------------------------------------ #
    if "trade_date" not in df.columns:
        derived: Optional[date] = None
        if target_date is not None:
            derived = target_date
            logger.info(
                "[daily_quotes] trade_date column missing – deriving from "
                "pipeline target_date: %s",
                derived,
            )
        else:
            derived = _extract_date_from_filename(file_name)
            if derived:
                logger.info(
                    "[daily_quotes] trade_date column missing – extracted from "
                    "filename '%s': %s",
                    file_name,
                    derived,
                )

        if derived is None:
            logger.error(
                "[daily_quotes] Cannot continue: 'trade_date' column is absent "
                "and could not be derived from target_date or filename '%s'. "
                "Available columns: %s",
                file_name,
                df.columns,
            )
            # Return empty list instead of raising so upstream callers can skip
            return []

        df = df.with_columns(pl.lit(derived).alias("trade_date"))

    # ------------------------------------------------------------------ #
    # 4. Normalise strings and cast numeric columns                      #
    # ------------------------------------------------------------------ #
    df = _trim_strings(df)

    # Uppercase tickers (guard: only when column is string type)
    # Use helper to ensure consistent behavior
    df = df.with_columns(_ensure_uppercase_string("ticker"))

    df = df.with_columns(_parse_date_column(df["trade_date"]).alias("trade_date"))
    df = _safe_cast_numeric(df, _DAILY_QUOTE_NUMERIC_COLS)

    # Fill missing optional numeric columns with null so the output schema is
    # always consistent with the fact_daily_quotes model.
    for col in _DAILY_QUOTE_NUMERIC_COLS:
        if col not in df.columns:
            logger.debug(
                "[daily_quotes] Optional column '%s' missing – filling with null.", col
            )
            df = df.with_columns(pl.lit(None).cast(pl.Float64).alias(col))

    # ------------------------------------------------------------------ #
    # 5. Whitelist: keep only fact_daily_quotes schema columns           #
    # open_price and any other unmapped columns are dropped here.        #
    # ------------------------------------------------------------------ #
    _FACT_DAILY_QUOTE_COLS = [
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
    cols_to_keep = [c for c in _FACT_DAILY_QUOTE_COLS if c in df.columns]
    dropped = [c for c in df.columns if c not in _FACT_DAILY_QUOTE_COLS]
    if dropped:
        logger.debug(
            "[daily_quotes] Dropping columns not in fact_daily_quotes schema: %s", dropped
        )
    df = df.select(cols_to_keep)

    # ------------------------------------------------------------------ #
    # 6. Filter: discard rows without required keys                      #
    # ------------------------------------------------------------------ #
    rows_before_filter = len(df)
    df = df.filter(
        pl.col("ticker").is_not_null()
        & (pl.col("ticker") != "")
        & pl.col("trade_date").is_not_null()
    )
    rows_discarded = rows_before_filter - len(df)
    if rows_discarded > 0:
        logger.warning(
            "[daily_quotes] Discarded %d/%d rows: ticker or trade_date was null/empty.",
            rows_discarded,
            rows_before_filter,
        )

    # ------------------------------------------------------------------ #
    # 7. Deduplicate on (ticker, trade_date)                             #
    # ------------------------------------------------------------------ #
    before_dedup = len(df)
    df = df.unique(subset=["ticker", "trade_date"], keep="first")
    if len(df) < before_dedup:
        logger.debug(
            "[daily_quotes] Deduplicated %d → %d rows on (ticker, trade_date).",
            before_dedup,
            len(df),
        )

    # ------------------------------------------------------------------ #
    # 8. Add source_file_name metadata column                            #
    # ------------------------------------------------------------------ #
    df = df.with_columns(pl.lit(file_name).alias("source_file_name"))

    mapped_columns = [c for c in df.columns if c != "source_file_name"]
    logger.info("[daily_quotes] Mapped columns: %s", mapped_columns)

    # ------------------------------------------------------------------ #
    # 9. Silent-failure guard: explain why result is empty               #
    # ------------------------------------------------------------------ #
    if len(df) == 0:
        logger.warning(
            "[daily_quotes] Transform produced 0 rows for '%s'. "
            "Input had %d rows. Possible causes: "
            "(a) no columns matched TRADE_COLUMN_MAP – check CSV headers; "
            "(b) all tickers were null/empty; "
            "(c) trade_date could not be parsed. "
            "Input columns were: %s",
            file_name,
            rows_before_filter,
            cols_to_keep,
        )

    logger.info("[daily_quotes] Daily quotes transformed: %d rows", len(df))
    return df.to_dicts()


# ---------------------------------------------------------------------------
# JSONL quotes transform  (fact_quotes hypertable)
# ---------------------------------------------------------------------------

# Candidate field names for each FactQuote column, in priority order.
# The first name found in the record wins.
_JSONL_TICKER_FIELDS = ("ticker_requested", "ticker_returned", "symbol", "ticker")
_JSONL_QUOTED_AT_FIELDS = ("quoted_at", "timestamp", "time", "dtTm")
_JSONL_CLOSE_PRICE_FIELDS = ("close_price", "closPric", "price", "last_price")
_JSONL_FLUCT_PCT_FIELDS = (
    "price_fluctuation_pct",
    "price_fluctuation_percentage",
    "prcFlcn",
    "variation_pct",
    "oscillation_pct",
)


def _first_value(record: dict, keys: tuple[str, ...]):
    """Return the first non-None value found in *record* for any key in *keys*."""
    for k in keys:
        v = record.get(k)
        if v is not None:
            return v
    return None


def transform_jsonl_quotes(rows: list[dict], source_name: str = "") -> list[dict]:
    """Normalise a list of flat dicts (from parse_jsonl_quotes or similar)
    into rows compatible with the ``fact_quotes`` hypertable schema.

    Field mapping (first matching key wins)
    ----------------------------------------
    ticker_requested | ticker_returned | symbol | ticker  → ticker
    quoted_at | timestamp | time | dtTm                   → quoted_at (UTC datetime)
    close_price | closPric | price | last_price            → close_price (Decimal)
    price_fluctuation_pct | price_fluctuation_percentage
      | prcFlcn | variation_pct | oscillation_pct         → price_fluctuation_pct

    trade_date is taken from the row's ``trade_date`` field if present; otherwise
    it is derived from ``quoted_at.date()``.

    Rows missing ``ticker`` or ``quoted_at`` are discarded with a warning.

    Args:
        rows:        List of flat dicts (e.g. from ``parse_jsonl_quotes``).
        source_name: Label for log messages (typically the JSONL filename).

    Returns:
        list[dict] ready for ``FactQuoteRepository.insert_many``.
    """
    from decimal import Decimal, InvalidOperation

    def _to_decimal(v) -> Decimal | None:
        if v is None:
            return None
        try:
            return Decimal(str(v).strip().replace(",", "."))
        except (InvalidOperation, ValueError):
            return None

    def _to_datetime(v) -> datetime | None:
        """Parse v into a timezone-aware UTC datetime.

        Time-only formats (``%H:%M:%S``, ``%H:%M``) are intentionally
        excluded: they produce ``datetime(1900, 1, 1, …)`` which would be
        stored as a bogus ``1900-01-01`` trade date.  Callers that have
        time-only strings should combine them with a known trade date before
        calling this function (see ``_build_quoted_at`` in the JSONL parser).
        """
        if isinstance(v, datetime):
            if v.tzinfo is None:
                return v.replace(tzinfo=timezone.utc)
            return v
        if v is None:
            return None
        s = str(v).strip()
        for fmt in (
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S%z",
        ):
            try:
                dt = datetime.strptime(s, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
        return None

    logger.info(
        "[jsonl_quotes] transform_jsonl_quotes: %d input rows  source=%s",
        len(rows),
        source_name or "(unknown)",
    )

    if not rows:
        logger.warning(
            "[jsonl_quotes] transform_jsonl_quotes received 0 input rows – nothing to transform."
        )
        return []

    # Log schema of the first row so field names are visible in logs.
    logger.info("[jsonl_quotes] Detected fields in first row: %s", list(rows[0].keys()))

    out: list[dict] = []
    skipped_no_ticker = 0
    skipped_no_quoted_at = 0

    for i, row in enumerate(rows):
        # -- ticker ----------------------------------------------------
        raw_ticker = _first_value(row, _JSONL_TICKER_FIELDS)
        if not raw_ticker:
            logger.debug(
                "[jsonl_quotes] Row %d: no ticker found (checked %s) – skipped",
                i,
                _JSONL_TICKER_FIELDS,
            )
            skipped_no_ticker += 1
            continue
        ticker = str(raw_ticker).strip().upper()
        if not ticker:
            skipped_no_ticker += 1
            continue

        # -- quoted_at -------------------------------------------------
        # Prefer pre-parsed datetime already in the row (from parse_jsonl_quotes).
        raw_qat = _first_value(row, _JSONL_QUOTED_AT_FIELDS)
        quoted_at = _to_datetime(raw_qat)
        if quoted_at is None:
            logger.debug(
                "[jsonl_quotes] Row %d ticker=%s: could not parse quoted_at from %r – skipped",
                i,
                ticker,
                raw_qat,
            )
            skipped_no_quoted_at += 1
            continue

        # -- trade_date ------------------------------------------------
        td = row.get("trade_date")
        if isinstance(td, date) and not isinstance(td, datetime):
            trade_date = td
        else:
            trade_date = quoted_at.date()

        # -- prices ----------------------------------------------------
        close_price = _to_decimal(_first_value(row, _JSONL_CLOSE_PRICE_FIELDS))
        price_fluctuation_pct = _to_decimal(_first_value(row, _JSONL_FLUCT_PCT_FIELDS))

        # -- source ----------------------------------------------------
        src = row.get("source_jsonl") or source_name or None

        out.append(
            {
                "ticker": ticker,
                "quoted_at": quoted_at,
                "trade_date": trade_date,
                "close_price": close_price,
                "price_fluctuation_pct": price_fluctuation_pct,
                "source_jsonl": src,
            }
        )

    # ------------------------------------------------------------------ #
    # Silent-failure guard                                                #
    # ------------------------------------------------------------------ #
    if skipped_no_ticker:
        logger.warning(
            "[jsonl_quotes] Discarded %d rows: ticker field missing/empty "
            "(checked fields: %s)",
            skipped_no_ticker,
            _JSONL_TICKER_FIELDS,
        )
    if skipped_no_quoted_at:
        logger.warning(
            "[jsonl_quotes] Discarded %d rows: quoted_at could not be parsed "
            "(checked fields: %s)",
            skipped_no_quoted_at,
            _JSONL_QUOTED_AT_FIELDS,
        )

    if not out:
        logger.warning(
            "[jsonl_quotes] transform_jsonl_quotes produced 0 rows from %d inputs. "
            "Possible causes: "
            "(a) ticker field missing (none of %s found); "
            "(b) quoted_at field missing or unparseable (none of %s found); "
            "(c) input rows list was empty.",
            len(rows),
            _JSONL_TICKER_FIELDS,
            _JSONL_QUOTED_AT_FIELDS,
        )
    else:
        logger.info(
            "[jsonl_quotes] transform_jsonl_quotes produced %d rows "
            "(skipped: %d no-ticker, %d no-quoted_at)",
            len(out),
            skipped_no_ticker,
            skipped_no_quoted_at,
        )

    return out
