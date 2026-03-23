"""Unit tests: COTAHIST historical load planning (2-year windows, batching, sort)."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.etl.orchestration.cotahist_historical_planning import (
    COTAHIST_HISTORICAL_INGEST_BATCH_SIZE,
    cotahist_txt_glob_sort_key,
    cotahist_year_from_path,
    group_paths_into_two_year_windows,
    iter_batches,
    sort_cotahist_paths,
)


def test_ingest_batch_size_is_fifty_thousand() -> None:
    assert COTAHIST_HISTORICAL_INGEST_BATCH_SIZE == 50_000


@pytest.mark.parametrize(
    ("length", "expected_lens"),
    [
        (0, []),
        (1, [1]),
        (50_000, [50_000]),
        (50_001, [50_000, 1]),
        (100_000, [50_000, 50_000]),
        (100_001, [50_000, 50_000, 1]),
    ],
)
def test_iter_batches_splits_at_historical_batch_size(length: int, expected_lens: list[int]) -> None:
    items = list(range(length))
    batches = list(iter_batches(items, COTAHIST_HISTORICAL_INGEST_BATCH_SIZE))
    assert [len(b) for b in batches] == expected_lens
    if batches:
        assert sum(len(b) for b in batches) == length


def test_iter_batches_small_remainder() -> None:
    batches = list(iter_batches([1, 2, 3], 2))
    assert batches == [[1, 2], [3]]


def test_cotahist_year_from_path_canonical() -> None:
    p = Path("data/cotahist/2000/COTAHIST_A2000.TXT")
    assert cotahist_year_from_path(p) == 2000


def test_cotahist_year_from_path_case_insensitive() -> None:
    p = Path("COTAHIST_A2024.txt")
    assert cotahist_year_from_path(p) == 2024


def test_cotahist_year_from_path_rejects_non_canonical_name() -> None:
    with pytest.raises(ValueError, match="COTAHIST_A"):
        cotahist_year_from_path(Path("foo.TXT"))


def test_sort_cotahist_paths_deterministic() -> None:
    paths = [
        Path("z/2002/COTAHIST_A2002.TXT"),
        Path("a/2000/COTAHIST_A2000.TXT"),
        Path("m/2001/COTAHIST_A2001.TXT"),
    ]
    assert sort_cotahist_paths(paths) == [
        Path("a/2000/COTAHIST_A2000.TXT"),
        Path("m/2001/COTAHIST_A2001.TXT"),
        Path("z/2002/COTAHIST_A2002.TXT"),
    ]


def test_sort_cotahist_paths_same_year_tie_breaker() -> None:
    p1 = Path("b/2000/COTAHIST_A2000.TXT")
    p2 = Path("a/2000/COTAHIST_A2000.TXT")
    assert sort_cotahist_paths([p1, p2]) == [p2, p1]


@pytest.mark.parametrize(
    ("paths", "expected_windows"),
    [
        (
            [],
            [],
        ),
        (
            [Path("y/2000/COTAHIST_A2000.TXT")],
            [[Path("y/2000/COTAHIST_A2000.TXT")]],
        ),
        (
            [
                Path("y/2001/COTAHIST_A2001.TXT"),
                Path("y/2000/COTAHIST_A2000.TXT"),
            ],
            [
                [
                    Path("y/2000/COTAHIST_A2000.TXT"),
                    Path("y/2001/COTAHIST_A2001.TXT"),
                ],
            ],
        ),
        (
            [
                Path("y/2002/COTAHIST_A2002.TXT"),
                Path("y/2000/COTAHIST_A2000.TXT"),
                Path("y/2001/COTAHIST_A2001.TXT"),
            ],
            [
                [
                    Path("y/2000/COTAHIST_A2000.TXT"),
                    Path("y/2001/COTAHIST_A2001.TXT"),
                ],
                [Path("y/2002/COTAHIST_A2002.TXT")],
            ],
        ),
        (
            [
                Path("b/2000/COTAHIST_A2000.TXT"),
                Path("a/2003/COTAHIST_A2003.TXT"),
                Path("c/2002/COTAHIST_A2002.TXT"),
                Path("d/2001/COTAHIST_A2001.TXT"),
            ],
            [
                [
                    Path("b/2000/COTAHIST_A2000.TXT"),
                    Path("d/2001/COTAHIST_A2001.TXT"),
                ],
                [
                    Path("c/2002/COTAHIST_A2002.TXT"),
                    Path("a/2003/COTAHIST_A2003.TXT"),
                ],
            ],
        ),
    ],
)
def test_group_paths_into_two_year_windows(paths: list[Path], expected_windows: list[list[Path]]) -> None:
    assert group_paths_into_two_year_windows(paths) == expected_windows


def test_cotahist_txt_glob_sort_key_orders_by_year_then_path() -> None:
    p86 = Path("z/COTAHIST_A1986.TXT")
    p90 = Path("a/COTAHIST_A1990.TXT")
    assert cotahist_txt_glob_sort_key(p86) < cotahist_txt_glob_sort_key(p90)


def test_cotahist_txt_glob_sort_key_non_canonical_after_canonical() -> None:
    canonical = Path("COTAHIST_A2000.TXT")
    other = Path("other.TXT")
    assert cotahist_txt_glob_sort_key(canonical)[0] == 0
    assert cotahist_txt_glob_sort_key(other)[0] == 1


def test_group_paths_two_files_same_year_same_window() -> None:
    a = Path("x/2000/COTAHIST_A2000.TXT")
    b = Path("y/2000/COTAHIST_A2000.TXT")
    assert group_paths_into_two_year_windows([b, a]) == [[a, b]]
