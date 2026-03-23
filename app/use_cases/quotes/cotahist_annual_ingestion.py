"""Orchestration for annual COTAHIST TXT → ``fact_cotahist_daily``.

Coexistence: this path never writes ``fact_daily_quotes`` (negocios CSV grain).
Re-ingests update rows on ``uq_cotahist_natural_key`` (last successful load wins).
See ``FactCotahistDaily`` and README.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.etl.loaders.db_loader import load_cotahist_quotes
from app.etl.parsers.cotahist_parser import (
    CotahistParseStats,
    iter_cotahist_quote_rows,
    parse_cotahist_file_metadata,
)
from app.etl.transforms.cotahist_transforms import natural_key_tuple, normalize_cotahist_quote

logger = get_logger(__name__)

# Coarse progress for long annual files (terminal visibility without per-batch spam).
_PROGRESS_HEARTBEAT_LINES = 250_000
_PROGRESS_HEARTBEAT_SEC = 45.0


@dataclass
class CotahistIngestSummary:
    """Aggregate counters for one TXT ingest."""

    lines_read: int = 0
    header_rows: int = 0
    trailer_rows: int = 0
    parser_quote_rows: int = 0
    skipped_wrong_length: int = 0
    skipped_unknown_tip: int = 0
    skipped_malformed_quote: int = 0
    normalized_valid: int = 0
    normalized_invalid: int = 0
    in_file_duplicate_keys: int = 0
    db_upsert_operations: int = 0
    seen_keys: set[tuple] = field(default_factory=set)


def _sync_parser_counters(summary: CotahistIngestSummary, pstats: CotahistParseStats) -> None:
    """Copy final parser counters (same stats object is mutated after the last quote yield)."""
    summary.lines_read = pstats.lines_read
    summary.header_rows = pstats.header_rows
    summary.trailer_rows = pstats.trailer_rows
    summary.parser_quote_rows = pstats.quote_rows
    summary.skipped_wrong_length = pstats.skipped_wrong_length
    summary.skipped_unknown_tip = pstats.skipped_unknown_tip
    summary.skipped_malformed_quote = pstats.skipped_malformed_quote


def ingest_cotahist_txt_file(
    db: Session,
    txt_path: Path | str,
    *,
    source_file_name: str | None = None,
    track_in_file_duplicates: bool = False,
    batch_size: int = 500,
    progress_heartbeat: bool = True,
) -> CotahistIngestSummary:
    """Stream-parse *txt_path*, normalize, and upsert in batches."""
    txt_path = Path(txt_path)
    src = source_file_name or txt_path.name
    summary = CotahistIngestSummary()
    batch: list[dict] = []
    last_hb_lines = 0
    last_hb_time = time.monotonic()
    last_pstats: CotahistParseStats | None = None

    for raw, pstats in iter_cotahist_quote_rows(txt_path):
        last_pstats = pstats

        row = normalize_cotahist_quote(raw, source_file_name=src)
        if row is None:
            summary.normalized_invalid += 1
            continue
        summary.normalized_valid += 1
        key = natural_key_tuple(row)
        if track_in_file_duplicates:
            if key in summary.seen_keys:
                summary.in_file_duplicate_keys += 1
            summary.seen_keys.add(key)
        batch.append(row)
        if progress_heartbeat and pstats.lines_read > 0:
            now = time.monotonic()
            if (pstats.lines_read - last_hb_lines >= _PROGRESS_HEARTBEAT_LINES) or (
                now - last_hb_time >= _PROGRESS_HEARTBEAT_SEC
            ):
                logger.info(
                    "[cotahist_ingest] heartbeat file=%s lines_read=%s quote_rows=%s",
                    txt_path.name,
                    pstats.lines_read,
                    pstats.quote_rows,
                )
                last_hb_lines = pstats.lines_read
                last_hb_time = now
        if len(batch) >= batch_size:
            summary.db_upsert_operations += load_cotahist_quotes(db, batch)
            batch.clear()

    if batch:
        summary.db_upsert_operations += load_cotahist_quotes(db, batch)

    if last_pstats is not None:
        _sync_parser_counters(summary, last_pstats)
    else:
        _, _, meta_stats = parse_cotahist_file_metadata(txt_path)
        _sync_parser_counters(summary, meta_stats)

    logger.info(
        "[cotahist_ingest] file=%s valid=%s invalid=%s dup_keys=%s db_ops=%s",
        txt_path.name,
        summary.normalized_valid,
        summary.normalized_invalid,
        summary.in_file_duplicate_keys,
        summary.db_upsert_operations,
    )
    return summary


def parse_cotahist_txt_stats_only(
    txt_path: Path | str,
    *,
    track_in_file_duplicates: bool = False,
) -> CotahistIngestSummary:
    """Parse and normalise without DB — fills validation counters only.

    Duplicate-key counting is optional (default off) so large annual files do not
    retain millions of keys in memory during Stage 1 validation.
    """
    txt_path = Path(txt_path)
    summary = CotahistIngestSummary()
    last_pstats: CotahistParseStats | None = None
    for raw, pstats in iter_cotahist_quote_rows(txt_path):
        last_pstats = pstats
        row = normalize_cotahist_quote(raw, source_file_name=txt_path.name)
        if row is None:
            summary.normalized_invalid += 1
        else:
            summary.normalized_valid += 1
            if track_in_file_duplicates:
                key = natural_key_tuple(row)
                if key in summary.seen_keys:
                    summary.in_file_duplicate_keys += 1
                summary.seen_keys.add(key)
    if last_pstats is not None:
        _sync_parser_counters(summary, last_pstats)
    else:
        _, _, meta_stats = parse_cotahist_file_metadata(txt_path)
        _sync_parser_counters(summary, meta_stats)
    return summary
