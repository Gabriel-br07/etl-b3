from __future__ import annotations

import pytest

from app.etl.orchestration.cotahist_adaptive_strategy import resolve_strategy_for_bucket


def test_standard_bucket_selects_standard_strategy() -> None:
    assert resolve_strategy_for_bucket("standard") == "standard"


def test_medium_bucket_selects_medium_strategy() -> None:
    assert resolve_strategy_for_bucket("medium") == "medium"


def test_large_bucket_selects_large_strategy() -> None:
    assert resolve_strategy_for_bucket("large") == "large"


def test_unknown_bucket_is_rejected() -> None:
    with pytest.raises(ValueError, match="bucket"):
        resolve_strategy_for_bucket("unexpected")
