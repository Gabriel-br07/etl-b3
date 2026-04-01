from __future__ import annotations

from app.etl.orchestration.cotahist_adaptive_strategy import (
    choose_standard_execution_path,
    is_standard_fast_path_eligible,
)


def test_fast_path_is_eligible_when_fact_empty_and_no_dupes() -> None:
    assert (
        is_standard_fast_path_eligible(
            fact_empty_at_start=True,
            has_dupes=False,
        )
        is True
    )


def test_fast_path_not_eligible_when_fact_not_empty() -> None:
    assert (
        is_standard_fast_path_eligible(
            fact_empty_at_start=False,
            has_dupes=False,
        )
        is False
    )


def test_fast_path_not_eligible_when_has_dupes() -> None:
    assert (
        is_standard_fast_path_eligible(
            fact_empty_at_start=True,
            has_dupes=True,
        )
        is False
    )


def test_fast_path_not_eligible_when_both_conditions_unfavorable() -> None:
    assert (
        is_standard_fast_path_eligible(
            fact_empty_at_start=False,
            has_dupes=True,
        )
        is False
    )


def test_standard_path_resolves_explicit_fallback_when_ineligible() -> None:
    assert (
        choose_standard_execution_path(
            fact_empty_at_start=False,
            has_dupes=False,
        )
        == "fallback"
    )
