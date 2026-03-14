"""Download helpers and CSV normalization for B3 scraping.

This module provides small, well-typed helpers that:
 - persist Playwright Download objects to disk (raw/original)
 - produce a best-effort UTF-8 semicolon-delimited normalized CSV alongside
   the raw file (never overwriting the raw file)

Design goals:
 - Clear result contract: saved_filename/file_path point to raw file; normalized_file_path
   is a separate artifact.
 - Deterministic token-aware header detection for `negocios_consolidados`.
 - Robust encoding fallback and safe temp-file usage.
 - Fine-grained, explicit error handling and informative logging.
"""

from __future__ import annotations

import csv
import io
import re
import tempfile
import unicodedata
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Iterable, List, Optional, Tuple, Literal

from playwright.sync_api import Download, Locator, Page

from app.core.logging import get_logger
from app.scraping.common.base import ScrapeResult
from app.scraping.common.exceptions import DownloadError

logger = get_logger(__name__)

# Candidate delimiters used for detection
CANDIDATE_DELIMITERS: Tuple[str, ...] = (";", ",", "|", "\t")

# Narrow table key type for the tables we explicitly handle
TableKey = Literal["cadastro_instrumentos", "negocios_consolidados"]

# Expected tokens for negocios_consolidados header (normalized form)
_EXPECTED_NEGOCIOS_TOKENS = (
    "instrumento",
    "codigo isin",
    "codigo",
    "isin",
    "segmento",
    "preco",
    "preço",
    "preco de",
    "ultima",
    "volume",
    "quantidade",
    "valor",
    "varia",
)

# Expected tokens for cadastro_instrumentos header (normalized form).
# These correspond to the known B3 CadInstrumento column names across the two
# file format variants (short-code form and long-label form).
_EXPECTED_INSTRUMENTS_TOKENS = (
    "tckrsymb",      # short-code header: TckrSymb
    "tckr",          # short-code header alias
    "codneg",        # legacy: codNeg (normalised)
    "instrumento financeiro",   # long-label header
    "instrumento",
    "nmofc",         # NmOfc
    "isin",          # present in both formats
    "sgmt",          # Sgmt
    "segmento",
    "dtrfrn",        # DtRfrn
    "nmoficial",
    "ativo",
)

INSTRUMENTS_TABLE_VALUE = "InstrumentsEquities@true"
TABLE_CONFIG: dict[str, Tuple[str, str]] = {
    "cadastro_instrumentos": (
        INSTRUMENTS_TABLE_VALUE,
        "cadastro_instrumentos_{date}.csv",
    ),
    "negocios_consolidados": (
        "NegociosConsolidados@true",
        "negocios_consolidados_{date}.csv",
    ),
}


# --------------------- I/O helpers ---------------------
def _ensure_output_dir(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)


def _read_text_with_fallback(path: Path, encodings: Iterable[str] = ("utf-8-sig", "utf-8", "latin-1")) -> Tuple[str, str]:
    """Read text from *path* trying *encodings* in order.

    Returns (text, encoding_used).

    Raises UnicodeDecodeError only if all attempts failed.
    """
    last_exc: Optional[UnicodeDecodeError] = None
    for enc in encodings:
        try:
            text = path.read_text(encoding=enc)
            return text, enc
        except UnicodeDecodeError as exc:
            # Only catch decoding errors to try the next encoding; let I/O errors propagate.
            last_exc = exc
            continue
    if last_exc is not None:
        raise UnicodeDecodeError(
            "utf-8",
            b"",
            0,
            1,
            f"Unable to decode {path} using fallbacks: {encodings}",
        ) from last_exc
    raise UnicodeDecodeError(
        "utf-8",
        b"",
        0,
        1,
        f"Unable to decode {path} using fallbacks: {encodings}",
    )

# --------------------- CSV detection & normalization helpers ---------------------

def _normalize_text_for_match(s: str) -> str:
    """Lowercase, strip diacritics and normalize whitespace for token matching."""
    s_low = s.lower()
    s_norm = unicodedata.normalize("NFKD", s_low)
    s_no_acc = "".join(ch for ch in s_norm if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", s_no_acc).strip()


# Normalize tokens for regex compilation (same normalization used for lines)
_NORMED_NEGOCIOS_TOKENS = [_normalize_text_for_match(t) for t in _EXPECTED_NEGOCIOS_TOKENS]
_NORMED_INSTRUMENTS_TOKENS = [_normalize_text_for_match(t) for t in _EXPECTED_INSTRUMENTS_TOKENS]

# Precompile token regexes with word boundaries to avoid substring/plural matches
_NEGOCIOS_TOKEN_REGEXES = [re.compile(r"\b" + re.escape(tok) + r"\b") for tok in _NORMED_NEGOCIOS_TOKENS]
_INSTRUMENTS_TOKEN_REGEXES = [re.compile(r"\b" + re.escape(tok) + r"\b") for tok in _NORMED_INSTRUMENTS_TOKENS]


def _line_has_expected_token_in_field(line: str, token_regexes: List[re.Pattern], min_fields: int = 3) -> bool:
    """Check candidate *line* by splitting on candidate delimiters and checking
    whether any field matches an expected token (using token_regexes).

    This avoids matching tokens that appear as substrings in long descriptive
    sentences. We require at least *min_fields* fields to consider the line a
    plausible header.
    """
    for sep in CANDIDATE_DELIMITERS:
        if sep not in line:
            continue
        fields = [ _normalize_text_for_match(f) for f in line.split(sep) ]
        # require a minimum number of columns for header plausibility
        if len([f for f in fields if f.strip()]) < min_fields:
            continue
        # check tokens against fields (word-boundary regex)
        for f in fields:
            if not f:
                continue
            for p in token_regexes:
                if p.search(f):
                    return True
    return False


def _contains_expected_negocios_token(line: str) -> bool:
    low = _normalize_text_for_match(line)
    return _line_has_expected_token_in_field(low, _NEGOCIOS_TOKEN_REGEXES, min_fields=3)


def _contains_expected_instruments_token(line: str) -> bool:
    low = _normalize_text_for_match(line)
    return _line_has_expected_token_in_field(low, _INSTRUMENTS_TOKEN_REGEXES, min_fields=3)


def _is_header_like(line: str, min_fields: int = 2) -> bool:
    """Heuristic to check whether *line* looks like a header row.

    Conditions:
      - non-empty and contains one of the candidate delimiters;
      - when split, yields >= min_fields fields;
      - at least one field contains an alphabetic character.
    """
    if not line or not line.strip():
        return False
    if not any(d in line for d in CANDIDATE_DELIMITERS):
        return False
    for sep in CANDIDATE_DELIMITERS:
        if sep in line:
            fields = [f.strip() for f in line.split(sep)]
            if len(fields) < min_fields:
                continue
            if any(any(ch.isalpha() for ch in f) for f in fields):
                return True
    return False


def _looks_like_data_row(fields: List[str]) -> bool:
    """Return True when the majority of *fields* look like typical data values.

    Numeric fields, ISIN-like tokens, or uppercase tickers count as data-like.
    """
    if not fields:
        return False
    data_like = 0
    total = len(fields)
    isin_re = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")
    ticker_re = re.compile(r"^[A-Z0-9]{4,}$")
    num_re = re.compile(r"^[\d.,]+$")
    for f in fields:
        s = f.strip()
        if not s:
            continue
        if num_re.fullmatch(s):
            data_like += 1
            continue
        if isin_re.fullmatch(s):
            data_like += 1
            continue
        if ticker_re.fullmatch(s) and s.upper() == s:
            data_like += 1
            continue
    return (data_like / total) >= 0.5


def _detect_delimiter(sample: str, default: str = ";") -> str:
    """Try csv.Sniffer on the *sample* (a text block) to detect delimiter.

    Falls back to *default* on failure.
    """
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=''.join(CANDIDATE_DELIMITERS))
        delim = dialect.delimiter
        logger.debug("csv.Sniffer detected delimiter '%s'", delim)
        return delim
    except Exception:
        logger.debug("csv.Sniffer failed, falling back to default delimiter '%s'", default)
        return default


def _choose_header_index(lines: List[str], table_key: Optional[str]) -> Tuple[int, bool]:
    """Choose a header index and indicate whether it was token-detected.

    Unified behavior for the known table types:
      - Scan the first N lines (max_scan) for a line that contains a candidate
        delimiter and at least one expected token for the table.
      - If found -> return (idx, True).
      - If not found -> log a warning and fall back to first non-empty line (idx, False).

    Generic fallback for unknown table_key keeps the previous heuristic.
    """
    # Use the same scan window for both tables to avoid divergence
    max_scan = min(len(lines), 10)

    if table_key in ("negocios_consolidados", "cadastro_instrumentos"):
        detector = _contains_expected_negocios_token if table_key == "negocios_consolidados" else _contains_expected_instruments_token
        for idx in range(max_scan):
            raw = lines[idx].strip()
            if not raw:
                continue
            # require that the candidate header contains a delimiter (robustness)
            if not any(d in raw for d in CANDIDATE_DELIMITERS):
                continue
            if detector(raw):
                logger.info(
                    "%s: auto-detected header at index=%d line=%r",
                    table_key, idx, raw[:120],
                )
                return idx, True
        # No token-detected header found in the scan window — warn and fall back
        logger.warning(
            "%s: token detection failed in first %d lines. First 5 lines: %s",
            table_key, max_scan, [l[:80] for l in lines[:5]],
        )
        # Fallback to the first non-empty line for consistent behavior
        for idx in range(len(lines)):
            if lines[idx].strip():
                logger.debug("%s: falling back to first non-empty line index=%d", table_key, idx)
                return idx, False
        return 0, False

    # Generic path: scan for header-like line
    for idx in range(min(len(lines), 10)):
        if _is_header_like(lines[idx]):
            logger.debug("Auto-detected header line index=%d", idx)
            return idx, True
    # fallback
    idx = 2 if len(lines) >= 3 else 0
    logger.debug("Falling back to positional header index=%d", idx)
    return idx, False


def _write_normalized_csv(
    useful_block_text: str, dest_path: Path, detected_delim: str
) -> None:
    """Write normalized UTF-8 semicolon-delimited CSV to dest_path using a safe temp file.

    This function assumes useful_block_text already starts at the desired header row.
    """
    # Create a NamedTemporaryFile in the target directory to avoid cross-device issues
    tmp_name: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="", delete=False,
            dir=str(dest_path.parent), suffix=dest_path.suffix,
        ) as tf:
            tmp_name = Path(tf.name)
            writer = csv.writer(tf, delimiter=";", quoting=csv.QUOTE_MINIMAL)
            reader = csv.reader(io.StringIO(useful_block_text), delimiter=detected_delim)
            for row in reader:
                writer.writerow(row)
        # Atomic replace
        tmp_name.replace(dest_path)
    except Exception as exc:
        logger.exception("Failed to write normalized CSV to %s", dest_path)
        # Clean up temp file if it was created
        if tmp_name is not None and tmp_name.exists():
            try:
                tmp_name.unlink()
            except Exception:
                pass
        raise


def _validate_normalized_csv(dest_path: Path, table_key: Optional[str], token_detected: bool) -> None:
    """Validate the normalized CSV is well-formed and contains a plausible header.

    Raises ValueError on validation failure.
    """
    with dest_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError(f"Normalized CSV {dest_path.name} is empty")

    # Basic checks: header must contain multiple columns and at least one non-empty field
    if len(header) < 2 or not any(h.strip() for h in header):
        raise ValueError(
            f"Normalized CSV {dest_path.name} header has insufficient columns: {header}"
        )

    joined = _normalize_text_for_match(" ".join(header))

    # Unified validation: both table types use token-aware detection; if token_detected
    # was True we expect tokens in the header. If token_detected is False we fall
    # back to positional/heuristic checks (ensure header does not look like a data row).
    if table_key in ("negocios_consolidados", "cadastro_instrumentos"):
        expected_tokens = _EXPECTED_NEGOCIOS_TOKENS if table_key == "negocios_consolidados" else _EXPECTED_INSTRUMENTS_TOKENS
        if token_detected:
            if not any(tok in joined for tok in expected_tokens):
                raise ValueError(
                    f"{table_key} normalized file header did not contain expected tokens. Header: {header}"
                )
            logger.info(
                "%s: header validation passed (token detected). Columns (%d): %s",
                table_key, len(header), header,
            )
        else:
            # Positional fallback: ensure the header is not actually a data row
            if _looks_like_data_row(header) and not any(tok in joined for tok in expected_tokens):
                raise ValueError(
                    f"{table_key} normalized file header appears to be a data row: {header}"
                )
            logger.info(
                "%s: header validation passed (positional fallback). Columns (%d): %s",
                table_key, len(header), header,
            )
        return

    # Generic path: (no special validation)
    logger.debug("Generic CSV validation passed for %s", dest_path.name)


# --------------------- Public API ---------------------
def save_download(
    download: Download,
    output_dir: Path,
    target_date: date,
    filename_template: str,
    source_url: str = "",
    default_delimiter: str = ";",
    table_key: Optional[TableKey] = None,
) -> ScrapeResult:
    """Persist a Playwright Download to disk and produce a normalized CSV artifact.

    Contract:
      - The raw file is saved to output_dir with the application filename derived
        from filename_template; result.file_path/original_file_path and
        result.saved_filename point to this raw file (unchanged).
      - When normalization succeeds, result.normalized_file_path is set. If
        normalization fails, the raw file remains and conversion_error is set.
    """
    failure = download.failure()
    if failure:
        raise DownloadError(f"Playwright reported download failure: {failure}")

    # Ensure destination directory exists
    _ensure_output_dir(output_dir)

    raw_name = filename_template.format(date=target_date.strftime("%Y%m%d"))
    dest = output_dir / raw_name

    logger.info("Saving raw download to %s", dest)
    try:
        download.save_as(dest)
    except Exception as exc:
        logger.exception("Failed to save download to %s", dest)
        raise DownloadError(f"Failed to persist download to {dest}") from exc

    result = ScrapeResult(
        source_url=source_url,
        file_path=dest,
        suggested_filename=download.suggested_filename or "",
        saved_filename=raw_name,  # preserve raw filename in the contract
        original_file_path=dest,
        normalized_file_path=None,
        downloaded_at=datetime.now(UTC),
        target_date=target_date,
        conversion_succeeded=False,
        conversion_error=None,
    )

    # Only attempt normalization for CSV files; do so in a best-effort way
    if dest.suffix.lower() == ".csv":
        norm_path = dest.with_name(dest.stem + ".normalized" + dest.suffix)
        try:
            _normalize_csv_using_stdlib(src_path=dest, dest_path=norm_path, default_delimiter=default_delimiter, table_key=table_key)
            result.normalized_file_path = norm_path
            result.conversion_succeeded = True
            logger.info("Normalization succeeded: %s", norm_path)
        except (UnicodeDecodeError, OSError, csv.Error, ValueError) as exc:
            result.conversion_succeeded = False
            result.conversion_error = str(exc)
            logger.exception("Normalization failed for %s: %s", dest, exc)

    return result


def trigger_csv_export_and_save(
    page: Page,
    csv_button_locator: Locator,
    output_dir: Path,
    target_date: date,
    table_key: Optional[TableKey] = "cadastro_instrumentos",
    timeout_ms: int = 60_000,
    default_delimiter: str = ";",
) -> ScrapeResult:
    """Trigger page download via provided locator and persist the file via :func:`save_download`.

    csv_button_locator is typed as Playwright Locator for clarity.
    """
    _, filename_template = TABLE_CONFIG[table_key]
    source_url = page.url

    logger.info("Triggering CSV download (table=%s) …", table_key)
    with page.expect_download(timeout=timeout_ms) as dl_info:
        csv_button_locator.click()

    download: Download = dl_info.value
    return save_download(
        download=download,
        output_dir=output_dir,
        target_date=target_date,
        filename_template=filename_template,
        source_url=source_url,
        default_delimiter=default_delimiter,
        table_key=table_key,
    )


# --------------------- High-level normalizer ---------------------
def _normalize_csv_using_stdlib(src_path: Path, dest_path: Path, default_delimiter: str = ";", table_key: Optional[TableKey] = None) -> None:
    """Normalize the CSV in *src_path* and write a UTF-8 semicolon-delimited file to *dest_path*.

    Raises UnicodeDecodeError/OSError/csv.Error/ValueError on failure so callers
    can handle/report the issue. Does not modify *src_path*.
    """
    # Read with encoding fallback
    text, used_enc = _read_text_with_fallback(src_path)
    logger.info(
        "[normalize] src=%s  table=%s  encoding=%s  size=%d chars",
        src_path.name, table_key, used_enc, len(text),
    )

    lines = text.splitlines()
    if not lines:
        # Write empty normalized file
        dest_path.write_text("", encoding="utf-8")
        logger.info("Wrote empty normalized file %s", dest_path)
        return

    # Log the first 5 lines for diagnostics (crucial for Docker vs local debugging)
    logger.info(
        "[normalize] %s first 5 raw lines: %s",
        src_path.name, [l[:100] for l in lines[:5]],
    )

    # Choose header index with special handling per table type
    header_idx, token_detected = _choose_header_index(lines, table_key)
    prelim_discarded = header_idx
    logger.info(
        "[normalize] header chosen index=%d token_detected=%s prelim_discarded=%d",
        header_idx, token_detected, prelim_discarded,
    )

    useful_block_lines = lines[header_idx:]
    useful_block_text = "\n".join(useful_block_lines)

    # Detect delimiter from useful block; for negocios prefer ';' when header contains it
    if table_key == "negocios_consolidados" and header_idx < len(lines) and ";" in lines[header_idx]:
        detected_delim = ";"
        logger.debug("negocios_consolidados: forcing delimiter ';' based on header content")
    elif table_key == "cadastro_instrumentos" and header_idx < len(lines) and ";" in lines[header_idx]:
        detected_delim = ";"
        logger.debug("cadastro_instrumentos: forcing delimiter ';' based on header content")
    else:
        detected_delim = _detect_delimiter(useful_block_text, default=default_delimiter)
    logger.info("[normalize] detected delimiter='%s' for %s", detected_delim, src_path.name)

    # Write normalized CSV safely
    _write_normalized_csv(useful_block_text, dest_path, detected_delim)

    # Validate normalized CSV content — raises ValueError on failure
    _validate_normalized_csv(dest_path, table_key, token_detected)

    # Log final columns and file info
    with dest_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader, [])
    logger.info(
        "[normalize] done: %s  columns=%d  header=%s",
        dest_path.name, len(header), header,
    )
