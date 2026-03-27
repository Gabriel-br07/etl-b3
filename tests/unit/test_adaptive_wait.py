from __future__ import annotations

import pytest

from app.scraping.common.adaptive_wait import (
    adaptive_timeout_ms,
    is_transient_playwright_error,
    run_with_adaptive_wait,
)


def test_adaptive_timeout_ms_grows_by_30_percent():
    assert adaptive_timeout_ms(1000, 1) == 1000
    assert adaptive_timeout_ms(1000, 2) == 1300
    assert adaptive_timeout_ms(1000, 3) == 1690


def test_is_transient_playwright_error_true_for_timeout():
    exc = RuntimeError("Timeout 30000ms exceeded while waiting for element")
    assert is_transient_playwright_error(exc) is True


def test_run_with_adaptive_wait_retries_then_succeeds():
    calls: list[int] = []

    def action(timeout_ms: int) -> str:
        calls.append(timeout_ms)
        if len(calls) < 3:
            raise RuntimeError("Timeout exceeded")
        return "ok"

    result = run_with_adaptive_wait(
        action_label="click:test",
        action=action,
        base_timeout_ms=1000,
        max_attempts=3,
        scraper_name="b3",
    )
    assert result == "ok"
    assert calls == [1000, 1300, 1690]


def test_run_with_adaptive_wait_non_transient_fails_immediately():
    calls = 0

    def action(timeout_ms: int) -> str:
        nonlocal calls
        calls += 1
        raise RuntimeError("schema changed permanently")

    with pytest.raises(RuntimeError):
        run_with_adaptive_wait(
            action_label="click:test",
            action=action,
            base_timeout_ms=1000,
            max_attempts=3,
            scraper_name="b3",
        )
    assert calls == 1
