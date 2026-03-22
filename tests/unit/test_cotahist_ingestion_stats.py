"""Stats-only ingest path (no database)."""

from __future__ import annotations

from pathlib import Path

from app.use_cases.quotes.cotahist_annual_ingestion import parse_cotahist_txt_stats_only

FIXTURE = Path(__file__).resolve().parent.parent / "fixtures" / "b3" / "cotahist_minimal.txt"


def test_parse_stats_duplicate_keys_in_fixture():
    s = parse_cotahist_txt_stats_only(FIXTURE)
    assert s.normalized_valid == 2
    assert s.in_file_duplicate_keys == 1
