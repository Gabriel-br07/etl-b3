"""Dual-source ticker filtering pipeline for B3 quote ingestion.

Strategy
--------
Two data sources are combined to produce the best possible list of tickers
to query in the B3 DailyFluctuationHistory endpoint:

1. ``cadastro_instrumentos`` (normalized CSV) – **MASTER** source.
   Structural filter: only keep active equity instruments traded in the
   cash segment.

2. ``negocios_consolidados`` (normalized CSV) – **AUXILIARY** source.
   Operational filter: only keep tickers that had real trading activity
   on the most recent available day (positive trade count, non-null close
   price).  Used as a *signal*, not as the sole truth.

Outputs
-------
``TickerFilterResult`` contains two lists:

strict_filtered_tickers
    Tickers that pass the master structural rules **AND** appear as active
    in negocios_consolidados.  Preferred for quote requests – high
    confidence these will return useful data.

fallback_master_tickers
    All tickers that pass the master structural rules, regardless of
    negocios_consolidados.  Used as a safety net when negocios_consolidados
    is stale or unavailable.

The application should consume *strict_filtered_tickers* first and fall back
to *fallback_master_tickers* only when needed (e.g. when the batch is
configured for ``--filter-mode fallback``).

Column contracts (normalized CSV format)
-----------------------------------------
The B3 scraper normalizes raw downloads so that line 3 of the original
file becomes the CSV header.  Column names below are the exact Portuguese
strings present in the normalized output.

cadastro_instrumentos columns used:
    ``Instrumento financeiro``   – ticker symbol
    ``Segmento``                 – must equal "CASH"
    ``Mercado``                  – must equal "EQUITY-CASH"
    ``Categoria``                – must equal "SHARES"
    ``Data início negócio``      – trading start date; must be ≤ reference_date
    ``Data fim negócio``         – trading end date; null/empty = still active

negocios_consolidados columns used:
    ``Instrumento financeiro``   – ticker symbol (join key)
    ``Quantidade de negócios``   – number of trades; must be > 0
    ``Preço de fechamento``      – close price; must be non-null / non-empty

Normalisation notes
-------------------
- All string comparisons are case-insensitive and strip leading/trailing
  whitespace.
- Date columns accept both ``YYYY-MM-DD`` and ``DD/MM/YYYY`` formats.
- Column names are normalised before lookup: stripped, lowercased, and
  accent-normalised via ``unicodedata``.  This guards against minor
  encoding or spacing differences across B3 file versions.
"""

from __future__ import annotations

import csv
import logging
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

from app.integrations.b3.constants import MAX_TICKER_LENGTH

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column name constants  (exact strings in the normalized B3 CSV)
# ---------------------------------------------------------------------------

# --- cadastro_instrumentos ---
_COL_TICKER = "Instrumento financeiro"
_COL_SEGMENTO = "Segmento"
_COL_MERCADO = "Mercado"
_COL_CATEGORIA = "Categoria"
_COL_DATA_INICIO = "Data início negócio"
_COL_DATA_FIM = "Data fim negócio"

# --- negocios_consolidados ---
# Ticker column is shared: "Instrumento financeiro"
_COL_QTD_NEGOCIOS = "Quantidade de negócios"
_COL_PRECO_FECHAMENTO = "Preço de fechamento"

# ---------------------------------------------------------------------------
# Expected filter values (normalised – lower, no accents)
# ---------------------------------------------------------------------------

_SEGMENTO_CASH = "cash"
_MERCADO_EQUITY = "equity-cash"
_CATEGORIA_SHARES = "shares"


# ---------------------------------------------------------------------------
# Date parsing helpers
# ---------------------------------------------------------------------------

_DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ")


def _parse_date(raw: str | None) -> date | None:
    """Parse *raw* as a date, returning ``None`` on blank or unparseable input."""
    if not raw or not raw.strip():
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(raw.strip(), fmt).date()
        except ValueError:
            continue
    logger.debug("Could not parse date string %r – treating as None.", raw)
    return None


# ---------------------------------------------------------------------------
# Column-name normalisation helpers
# ---------------------------------------------------------------------------


def _normalise_key(name: str) -> str:
    """Lower-case, strip, and remove accents from *name* for fuzzy lookup."""
    nfkd = unicodedata.normalize("NFKD", name.strip().lower())
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _build_col_index(headers: list[str]) -> dict[str, str]:
    """Return a mapping {normalised_name: original_name} for all *headers*."""
    return {_normalise_key(h): h for h in headers}


def _resolve_col(col_index: dict[str, str], desired: str) -> str | None:
    """Return the actual header name for *desired* (accent/case insensitive)."""
    return col_index.get(_normalise_key(desired))


# ---------------------------------------------------------------------------
# CSV reading helper
# ---------------------------------------------------------------------------


def _read_normalized_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    """Read a normalized B3 CSV and return (headers, rows).

    Tries UTF-8 first, then Latin-1 as fallback.
    Rows are plain dicts keyed by the original header strings.
    """
    last_error: Exception | None = None
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            with open(path, newline="", encoding=enc) as fh:
                reader = csv.DictReader(fh, delimiter=";")
                # Force header read
                headers = list(reader.fieldnames or [])
                rows = list(reader)
            return headers, rows
        except UnicodeDecodeError as e:
            # Only swallow decoding-related errors here; record last for context
            last_error = e
            continue
    # If we reach here, none of the attempted encodings worked. Raise a clear
    # ValueError and attach the last UnicodeDecodeError as the __cause__ so the
    # original traceback/message is preserved for debugging.
    if last_error is not None:
        raise ValueError(f"Unable to read normalized CSV: {path}") from last_error
    # No decoding error occurred — let the caller know we couldn't read for an
    # unexpected reason (e.g. empty file). Use a generic ValueError.
    raise ValueError(f"Unable to read normalized CSV: {path}")


# ---------------------------------------------------------------------------
# Numeric/value helpers
# ---------------------------------------------------------------------------


def _is_positive_number(raw: str | None) -> bool:
    """Return True when *raw* is a non-empty string that represents a number > 0."""
    if not raw or not raw.strip():
        return False
    try:
        return float(raw.strip().replace(",", ".")) > 0
    except ValueError:
        return False


def _is_non_empty(raw: str | None) -> bool:
    return bool(raw and raw.strip() and raw.strip().lower() not in ("none", "null", "-", ""))


# ---------------------------------------------------------------------------
# Master structural filter
# ---------------------------------------------------------------------------


def _apply_master_filter(
    rows: list[dict[str, Any]],
    col_index: dict[str, str],
    reference_date: date,
) -> list[str]:
    """Apply structural rules to *rows* and return a deduplicated ticker list.

    Rules:
      1. ``Segmento``  == "CASH"          (case-insensitive)
      2. ``Mercado``   == "EQUITY-CASH"   (case-insensitive)
      3. ``Categoria`` == "SHARES"        (case-insensitive)
      4. ``Data início negócio`` ≤ reference_date  (or missing → accept)
      5. ``Data fim negócio`` is null/empty OR ≥ reference_date

    If any of the filter columns are absent from the CSV the rule is skipped
    with a warning (graceful degradation for partial B3 exports or fixture
    files used in tests).
    """
    col_ticker = _resolve_col(col_index, _COL_TICKER)
    col_seg = _resolve_col(col_index, _COL_SEGMENTO)
    col_mkt = _resolve_col(col_index, _COL_MERCADO)
    col_cat = _resolve_col(col_index, _COL_CATEGORIA)
    col_start = _resolve_col(col_index, _COL_DATA_INICIO)
    col_end = _resolve_col(col_index, _COL_DATA_FIM)

    if col_ticker is None:
        logger.warning(
            "Column '%s' not found in cadastro CSV – cannot build master list.",
            _COL_TICKER,
        )
        return []

    missing_cols: list[str] = []
    for name, found in [
        (_COL_SEGMENTO, col_seg),
        (_COL_MERCADO, col_mkt),
        (_COL_CATEGORIA, col_cat),
    ]:
        if found is None:
            missing_cols.append(name)

    if missing_cols:
        logger.warning(
            "Filter columns not found in cadastro CSV (will be skipped): %s",
            missing_cols,
        )

    seen: set[str] = set()
    tickers: list[str] = []
    skipped_seg = skipped_mkt = skipped_cat = skipped_start = skipped_end = 0

    for row in rows:
        raw_ticker = (row.get(col_ticker) or "").strip().upper()
        if not raw_ticker or " " in raw_ticker or len(raw_ticker) > MAX_TICKER_LENGTH:
            continue  # noise / footer rows

        # Rule 1 – Segmento == "CASH"
        if col_seg is not None:
            seg_val = _normalise_key(row.get(col_seg) or "")
            if seg_val != _SEGMENTO_CASH:
                skipped_seg += 1
                continue

        # Rule 2 – Mercado == "EQUITY-CASH"
        if col_mkt is not None:
            mkt_val = _normalise_key(row.get(col_mkt) or "")
            if mkt_val != _MERCADO_EQUITY:
                skipped_mkt += 1
                continue

        # Rule 3 – Categoria == "SHARES"
        if col_cat is not None:
            cat_val = _normalise_key(row.get(col_cat) or "")
            if cat_val != _CATEGORIA_SHARES:
                skipped_cat += 1
                continue

        # Rule 4 – Data início negócio ≤ reference_date
        if col_start is not None:
            start_dt = _parse_date(row.get(col_start))
            if start_dt is not None and start_dt > reference_date:
                skipped_start += 1
                continue

        # Rule 5 – Data fim negócio is null OR ≥ reference_date
        if col_end is not None:
            end_dt = _parse_date(row.get(col_end))
            if end_dt is not None and end_dt < reference_date:
                skipped_end += 1
                continue

        if raw_ticker not in seen:
            seen.add(raw_ticker)
            tickers.append(raw_ticker)

    logger.info(
        "Master filter: kept=%d  skipped(seg=%d mkt=%d cat=%d start=%d end=%d)",
        len(tickers),
        skipped_seg,
        skipped_mkt,
        skipped_cat,
        skipped_start,
        skipped_end,
    )
    return tickers


# ---------------------------------------------------------------------------
# Auxiliary operational filter
# ---------------------------------------------------------------------------


def _apply_negocios_filter(
    rows: list[dict[str, Any]],
    col_index: dict[str, str],
) -> set[str]:
    """Return the set of active tickers from *negocios_consolidados* rows.

    A ticker is considered active when:
      - ``Quantidade de negócios``  > 0
      - ``Preço de fechamento``     is non-null and non-empty
    """
    col_ticker = _resolve_col(col_index, _COL_TICKER)
    col_qtd = _resolve_col(col_index, _COL_QTD_NEGOCIOS)
    col_preco = _resolve_col(col_index, _COL_PRECO_FECHAMENTO)

    if col_ticker is None:
        logger.warning(
            "Column '%s' not found in negocios CSV – operational filter will return empty set.",
            _COL_TICKER,
        )
        return set()

    if col_qtd is None:
        logger.warning("Column '%s' not found in negocios CSV – skipping trade-count filter.", _COL_QTD_NEGOCIOS)
    if col_preco is None:
        logger.warning("Column '%s' not found in negocios CSV – skipping close-price filter.", _COL_PRECO_FECHAMENTO)

    active: set[str] = set()
    skipped_qtd = skipped_preco = 0

    for row in rows:
        raw_ticker = (row.get(col_ticker) or "").strip().upper()
        if not raw_ticker or " " in raw_ticker or len(raw_ticker) > MAX_TICKER_LENGTH:
            continue

        # Filter: Quantidade de negócios > 0
        if col_qtd is not None:
            if not _is_positive_number(row.get(col_qtd)):
                skipped_qtd += 1
                continue

        # Filter: Preço de fechamento non-null
        if col_preco is not None:
            if not _is_non_empty(row.get(col_preco)):
                skipped_preco += 1
                continue

        active.add(raw_ticker)

    logger.info(
        "Negocios filter: active=%d  skipped(qtd=%d preco=%d)",
        len(active),
        skipped_qtd,
        skipped_preco,
    )
    return active


# ---------------------------------------------------------------------------
# Public result dataclass
# ---------------------------------------------------------------------------

@dataclass
class TickerFilterResult:
    """Result of the dual-source filtering pipeline.

    Attributes:
        strict_filtered_tickers:
            Tickers that pass both master structural rules and negocios
            operational filter.  Prefer these for quote requests.
        fallback_master_tickers:
            All tickers that pass master structural rules, regardless of
            negocios.  Use as fallback when strict list is insufficient or
            negocios is unavailable.
        master_only_tickers:
            Tickers in the master list that did NOT appear in negocios
            (informational / diagnostic).
    """

    strict_filtered_tickers: list[str] = field(default_factory=list)
    fallback_master_tickers: list[str] = field(default_factory=list)
    master_only_tickers: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_ticker_filter(
    instruments_csv: Path | str,
    *,
    trades_csv: Path | str | None = None,
    reference_date: date | None = None,
) -> TickerFilterResult:
    """Build filtered ticker lists from the two B3 data sources.

    Args:
        instruments_csv:
            Path to a normalized ``cadastro_instrumentos_*.normalized.csv``
            (UTF-8, semicolon-delimited, Portuguese column names).
        trades_csv:
            Path to a normalized ``negocios_consolidados_*.normalized.csv``
            (UTF-8, semicolon-delimited, Portuguese column names).
            When ``None``, the strict list equals the master list (no
            operational filtering applied).
        reference_date:
            Date used to evaluate ``Data início/fim negócio`` date ranges.
            Defaults to today.

    Returns:
        A :class:`TickerFilterResult` with the three ticker lists.

    Raises:
        FileNotFoundError: If *instruments_csv* does not exist.
        ValueError: If *instruments_csv* cannot be parsed.
    """
    instruments_csv = Path(instruments_csv)
    ref_date = reference_date or date.today()

    if not instruments_csv.exists():
        raise FileNotFoundError(f"Instruments CSV not found: {instruments_csv}")

    # ------------------------------------------------------------------
    # Step 1 – Read and apply master structural filter
    # ------------------------------------------------------------------
    logger.info("Reading cadastro instruments: %s", instruments_csv)
    instr_headers, instr_rows = _read_normalized_csv(instruments_csv)
    logger.info("Cadastro rows read: %d", len(instr_rows))

    instr_col_index = _build_col_index(instr_headers)
    master_tickers = _apply_master_filter(instr_rows, instr_col_index, ref_date)
    logger.info("Master (structural) tickers: %d", len(master_tickers))

    # ------------------------------------------------------------------
    # Step 2 – Read and apply negocios operational filter (if provided)
    # ------------------------------------------------------------------
    if trades_csv is None:
        logger.info(
            "No negocios CSV provided – strict list equals master list (%d tickers).",
            len(master_tickers),
        )
        return TickerFilterResult(
            strict_filtered_tickers=list(master_tickers),
            fallback_master_tickers=list(master_tickers),
            master_only_tickers=[],
        )

    trades_csv = Path(trades_csv)
    if not trades_csv.exists():
        logger.warning(
            "Negocios CSV not found: '%s' – falling back to master list only.", trades_csv
        )
        return TickerFilterResult(
            strict_filtered_tickers=list(master_tickers),
            fallback_master_tickers=list(master_tickers),
            master_only_tickers=[],
        )

    logger.info("Reading negocios consolidados: %s", trades_csv)
    neg_headers, neg_rows = _read_normalized_csv(trades_csv)
    logger.info("Negocios rows read: %d", len(neg_rows))

    neg_col_index = _build_col_index(neg_headers)
    active_tickers = _apply_negocios_filter(neg_rows, neg_col_index)
    logger.info("Negocios operational active tickers: %d", len(active_tickers))

    # ------------------------------------------------------------------
    # Step 3 – Compute strict and master-only lists
    # ------------------------------------------------------------------

    # strict: in master AND in negocios active set, preserving master order
    strict = [t for t in master_tickers if t in active_tickers]

    # master_only: in master but NOT in negocios active set
    master_only = [t for t in master_tickers if t not in active_tickers]

    logger.info(
        "Filter result: strict=%d  fallback_master=%d  master_only=%d",
        len(strict),
        len(master_tickers),
        len(master_only),
    )

    return TickerFilterResult(
        strict_filtered_tickers=strict,
        fallback_master_tickers=master_tickers,
        master_only_tickers=master_only,
    )