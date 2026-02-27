"""Download-handling helpers for the B3 scraper.

Wraps Playwright's ``expect_download`` context manager and persists the
file to the application's output directory with a deterministic name.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime
from pathlib import Path
import csv
import io

from playwright.sync_api import Download, Page

from app.core.logging import get_logger
from app.scraping.common.base import ScrapeResult
from app.scraping.common.exceptions import DownloadError

logger = get_logger(__name__)

#: Instruments CSV table identifier in the B3 select.
INSTRUMENTS_TABLE_VALUE = "InstrumentsEquities@true"

#: Human-readable name → (select value, filename template)
TABLE_CONFIG: dict[str, tuple[str, str]] = {
    "cadastro_instrumentos": (
        INSTRUMENTS_TABLE_VALUE,
        "cadastro_instrumentos_{date}.csv",
    ),
    # TODO: ajustar select value real se diferente do placeholder abaixo
    "negocios_consolidados": (
        "NegociosConsolidados@true",
        "negocios_consolidados_{date}.csv",
    ),
}


def _stem_from_download(download: Download) -> str:
    """Extract a safe filename stem from the Playwright Download object."""
    suggested = download.suggested_filename or "b3_download"
    # Strip unsafe chars
    return re.sub(r"[^\w.\-]", "_", suggested)


def save_download(
    download: Download,
    output_dir: Path,
    target_date: date,
    filename_template: str,
    source_url: str = "",
    default_delimiter: str = ";",
) -> ScrapeResult:
    """Persist *download* to *output_dir* with a deterministic filename.

    This function now enforces these rules:
      - The raw/original file is saved first and never modified.
      - A normalized copy is produced (UTF-8, semicolon-delimited) and saved
        alongside the raw file; the raw file is never overwritten.
      - If the file has 3+ textual lines, the third line is used as header and
        only lines from there onward are used for delimiter detection and conversion.
      - Delimiter detection uses csv.Sniffer on the useful block with candidate
        delimiters set to ';', ',', '|', '\t'. If Sniffer fails, the provided
        default_delimiter is used.
      - Quoting is preserved using csv.writer quoting rules.

    Returns a ScrapeResult with both suggested_filename (from Playwright)
    and saved_filename (the application-determined name). When normalization
    succeeds for CSV files, `normalized_file_path` is populated and
    `saved_filename` is updated to the normalized filename so downstream
    consumers that rely on `saved_filename` see the canonical output.
    """
    failure = download.failure()
    if failure:
        raise DownloadError(f"Playwright reported download failure: {failure}")

    # Application-determined filename (deterministic) for the raw/original file
    saved_filename = filename_template.format(date=target_date.strftime("%Y%m%d"))
    dest = output_dir / saved_filename

    logger.info("Saving download raw/original → %s", dest)
    # Save raw file first (as produced by Playwright). Keep original bytes.
    download.save_as(dest)

    # Build result metadata now. suggested_filename must be the original suggested
    # filename reported by the browser; saved_filename is our application filename.
    result = ScrapeResult(
        source_url=source_url,
        file_path=dest,
        suggested_filename=download.suggested_filename or "",
        saved_filename=saved_filename,
        original_file_path=dest,
        normalized_file_path=None,
        downloaded_at=datetime.now(UTC),
        target_date=target_date,
        conversion_succeeded=False,
        conversion_error=None,
    )

    # If the downloaded file is a CSV, attempt normalization into a separate file
    if dest.suffix.lower() == ".csv":
        try:
            norm_path = dest.with_name(dest.stem + ".normalized" + dest.suffix)
            _normalize_csv_using_stdlib(
                src_path=dest,
                dest_path=norm_path,
                default_delimiter=default_delimiter,
            )
            result.conversion_succeeded = True
            # Keep file_path pointing to the raw/original file; record normalized path
            result.normalized_file_path = norm_path
            # Update saved_filename to reflect the canonical output (normalized file name)
            result.saved_filename = norm_path.name
            logger.info(
                "Normalized CSV saved: %s (size=%d bytes)",
                norm_path,
                norm_path.stat().st_size,
            )
        except Exception as exc:  # pragma: no cover - conversion is best-effort
            # Keep raw file intact, record the error, and do not raise.
            result.conversion_succeeded = False
            result.conversion_error = str(exc)
            logger.warning(
                "Failed to normalize CSV %s: %s", dest, exc
            )

    logger.info("Download saved: %s  converted=%s", result, result.conversion_succeeded)
    return result


def trigger_csv_export_and_save(
    page: Page,
    csv_button_locator,  # Locator
    output_dir: Path,
    target_date: date,
    table_key: str = "cadastro_instrumentos",
    timeout_ms: int = 60_000,
    default_delimiter: str = ";",
) -> ScrapeResult:
    """Click the CSV export button and capture the download.

    Uses Playwright's ``expect_download`` context manager to reliably
    intercept the file download triggered by the button click.

    Args:
        page: Active Playwright page.
        csv_button_locator: Locator pointing to the CSV icon/button.
        output_dir: Directory to save the file into.
        target_date: Business date for the filename.
        table_key: Key in :data:`TABLE_CONFIG` (default ``cadastro_instrumentos``).
        timeout_ms: Maximum time (ms) to wait for the download to start.

    Returns:
        A :class:`~app.scraping.common.base.ScrapeResult`.
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
    )


def _normalize_csv_using_stdlib(src_path: Path, dest_path: Path, default_delimiter: str = ";") -> None:
    """Normalize CSV in *src_path* and write UTF-8 semicolon-delimited output to *dest_path*.

    Rules implemented:
      - Read the source text using common encodings (utf-8-sig, utf-8, latin-1).
      - If the file has 3+ lines, use line 3 as header and only lines from line 3 onward
        for delimiter detection and conversion. Lines 1 and 2 are never used in the
        detection or fallback conversion.
      - Detect delimiter using csv.Sniffer on the useful block with delimiters candidates
        set to ';,|\t'. If Sniffer fails, fall back to the provided default_delimiter.
      - Preserve quoting by using csv.reader and csv.writer with quoting=csv.QUOTE_MINIMAL.
      - Write output as UTF-8 with delimiter=';'.
    """
    text = None
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = src_path.read_text(encoding=enc)
            break
        except Exception:
            continue
    if text is None:
        raise ValueError("Unable to read CSV file for normalization")

    lines = text.splitlines()
    if not lines:
        # Empty file; write empty normalized file
        dest_path.write_text("", encoding="utf-8")
        return

    # Determine the useful block for detection/conversion
    if len(lines) >= 3:
        # Use line 3 (index 2) as header, and subsequent lines as data
        header_line = lines[2]
        useful_block_lines = lines[2:]
    else:
        # Fewer than 3 lines: use entire file as useful block but still honor rule 5
        header_line = lines[0]
        useful_block_lines = lines

    useful_block_text = "\n".join(useful_block_lines)

    # Ask csv.Sniffer to detect delimiter from the useful block only, using candidate delimiters
    sniffer_sample = useful_block_text[: 32 * 1024]  # limit sample size
    dialect = None
    try:
        # Provide a set of possible delimiters. csv.Sniffer will choose among them.
        dialect = csv.Sniffer().sniff(sniffer_sample, delimiters=';,|\t')
        detected_delim = dialect.delimiter
        logger.debug("csv.Sniffer detected delimiter '%s' for %s", detected_delim, src_path)
    except Exception:
        detected_delim = default_delimiter
        logger.debug("csv.Sniffer failed for %s; falling back to default delimiter '%s'", src_path, detected_delim)

    # Now parse the useful block with the detected delimiter and preserve quoting
    src_io = io.StringIO(useful_block_text)
    reader = csv.reader(src_io, delimiter=detected_delim)

    # Prepare to write normalized file with semicolon delimiter and UTF-8 encoding
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8", newline="") as out_f:
        writer = csv.writer(out_f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            writer.writerow(row)

    # Atomically move normalized file into place
    tmp_path.replace(dest_path)

    # Note: We intentionally do not modify the original src_path (raw/original saved by Playwright)


# End of file
