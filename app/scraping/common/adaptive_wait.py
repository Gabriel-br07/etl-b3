"""Centralized adaptive wait/backoff for transient Playwright interactions."""

from __future__ import annotations

from typing import Any, Callable

from app.core.logging import get_logger

logger = get_logger(__name__)

_TRANSIENT_KEYWORDS = (
    "timeout",
    "not attached",
    "detached",
    "not visible",
    "not enabled",
    "intercepted",
    "actionability",
)


def is_transient_playwright_error(exc: Exception) -> bool:
    """Classify whether an exception looks transient/actionability-related."""
    msg = str(exc).lower()
    return any(k in msg for k in _TRANSIENT_KEYWORDS)


def adaptive_timeout_ms(base_timeout_ms: int, attempt: int) -> int:
    """Return timeout increased by 30% per attempt (attempt starts at 1)."""
    return int(round(base_timeout_ms * (1.3 ** max(0, attempt - 1))))


def run_with_adaptive_wait(
    *,
    action_label: str,
    action: Callable[[int], Any],
    base_timeout_ms: int,
    max_attempts: int = 3,
    scraper_name: str = "unknown",
) -> Any:
    """Run an action with adaptive timeout only for transient failures."""
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        timeout_ms = adaptive_timeout_ms(base_timeout_ms, attempt)
        logger.info(
            "[adaptive_wait] scraper=%s action=%s attempt=%d/%d timeout_ms=%d",
            scraper_name,
            action_label,
            attempt,
            max_attempts,
            timeout_ms,
        )
        try:
            return action(timeout_ms)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if (not is_transient_playwright_error(exc)) or attempt >= max_attempts:
                raise
            logger.warning(
                "[adaptive_wait] transient failure scraper=%s action=%s attempt=%d err=%s",
                scraper_name,
                action_label,
                attempt,
                exc,
            )
    if last_exc is not None:
        raise last_exc
