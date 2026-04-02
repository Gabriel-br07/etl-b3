"""Helpers for handling cookie banners in Playwright pages."""

from __future__ import annotations

from typing import Any

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from app.core.logging import get_logger

logger = get_logger(__name__)

_COOKIE_REJECT_LABEL = "REJEITAR TODOS OS COOKIES"


def _build_context_suffix(
    page: Any,
    *,
    scraper_name: str | None,
    step: str | None,
) -> str:
    parts: list[str] = []
    if scraper_name:
        parts.append(f"scraper={scraper_name}")
    if step:
        parts.append(f"step={step}")
    page_url = getattr(page, "url", None)
    if page_url:
        parts.append(f"url={page_url}")
    return f" [{', '.join(parts)}]" if parts else ""


def _is_expected_banner_absence_error(exc: PlaywrightError) -> bool:
    text = str(exc).lower()
    expected_tokens = (
        "timeout",
        "not visible",
        "not attached",
        "detached from dom",
        "outside of the viewport",
        "receives pointer events",
        "intercepts pointer events",
    )
    return any(token in text for token in expected_tokens)


def dismiss_cookie_banner_if_present(
    page: Any,
    *,
    timeout_ms: int = 2_000,
    scraper_name: str | None = None,
    step: str | None = None,
) -> bool:
    """Dismiss the cookie banner if the reject button is available.

    Returns ``True`` when the banner button is clicked, otherwise ``False``.
    This helper never raises solely because the banner is absent.
    """
    context_suffix = _build_context_suffix(page, scraper_name=scraper_name, step=step)
    try:
        button = page.get_by_role("button", name=_COOKIE_REJECT_LABEL)
        button.wait_for(state="visible", timeout=timeout_ms)
        button.click(timeout=timeout_ms)
        logger.info("Cookie banner dismissed via reject button%s", context_suffix)
        return True
    except PlaywrightTimeoutError as exc:
        logger.debug(
            "Cookie banner not dismissed (timeout/absent)%s: %s",
            context_suffix,
            exc,
        )
        return False
    except PlaywrightError as exc:
        if _is_expected_banner_absence_error(exc):
            logger.debug(
                "Cookie banner not dismissed (unavailable/actionability)%s: %s",
                context_suffix,
                exc,
            )
            return False
        logger.warning("Unexpected cookie banner failure%s: %s", context_suffix, exc)
        raise
    except Exception:
        logger.warning(
            "Non-Playwright failure while dismissing cookie banner%s",
            context_suffix,
            exc_info=True,
        )
        raise
