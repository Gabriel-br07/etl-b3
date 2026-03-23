"""Pure helpers for COTAHIST historical backfill: sort, 2-year windows, batching."""

from __future__ import annotations

import re
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import TypeVar

T = TypeVar("T")

COTAHIST_HISTORICAL_INGEST_BATCH_SIZE = 20_000

_COTAHIST_TXT_YEAR = re.compile(r"COTAHIST_A(\d{4})\.TXT$", re.I)


def cotahist_txt_glob_sort_key(path: Path) -> tuple[int, int | str, str]:
    """Sort key for ``**/COTAHIST_A*.TXT`` globs (matches CLI discovery order)."""
    m = _COTAHIST_TXT_YEAR.search(path.name)
    if m:
        return (0, int(m.group(1)), str(path))
    return (1, path.name.lower(), str(path))


def cotahist_year_from_path(path: Path) -> int:
    """Return the 4-digit year from a canonical ``COTAHIST_A{year}.TXT`` filename."""
    m = _COTAHIST_TXT_YEAR.search(path.name)
    if not m:
        msg = f"Expected filename matching COTAHIST_AYYYY.TXT, got {path.name!r}"
        raise ValueError(msg)
    return int(m.group(1))


def _cotahist_path_sort_key(path: Path) -> tuple[int, int, str]:
    return (0, cotahist_year_from_path(path), str(path))


def sort_cotahist_paths(paths: Sequence[Path]) -> list[Path]:
    """Deterministic order: ascending year, then full path string."""
    return sorted(paths, key=_cotahist_path_sort_key)


def group_paths_into_two_year_windows(paths: Sequence[Path]) -> list[list[Path]]:
    """Group files into consecutive 2-year windows (e.g. 2000–2001, then 2002–2003).

    Within each window, paths are ordered by :func:`sort_cotahist_paths`.
    """
    if not paths:
        return []
    ordered = sort_cotahist_paths(paths)

    # Determine the minimum year present to anchor 2-year calendar windows.
    years = [cotahist_year_from_path(p) for p in ordered]
    min_year = min(years)

    # Bucket paths into 2-year ranges: [min_year, min_year+1], [min_year+2, min_year+3], ...
    buckets: dict[int, list[Path]] = {}
    for path, year in zip(ordered, years):
        bucket_idx = (year - min_year) // 2
        if bucket_idx not in buckets:
            buckets[bucket_idx] = []
        buckets[bucket_idx].append(path)

    # Assemble windows in ascending calendar order of their 2-year ranges.
    windows: list[list[Path]] = []
    for idx in sorted(buckets):
        windows.append(buckets[idx])
    return windows


def iter_batches(items: Sequence[T], batch_size: int) -> Iterator[list[T]]:
    """Yield contiguous slices of *items* of length *batch_size* (last may be smaller)."""
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    n = len(items)
    for i in range(0, n, batch_size):
        yield list(items[i : i + batch_size])
