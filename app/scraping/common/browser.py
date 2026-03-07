"""Playwright browser / context factory.

Usage (sync)::

    from app.scraping.common.browser import build_browser_context

    with build_browser_context(playwright) as context:
        page = context.new_page()
        ...

The factory reads ``ScraperSettings`` from ``app.core.config.settings``
so all launch options (headless, slow_mo, downloads dir, etc.) are
controlled via environment variables.
"""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Generator

from playwright.sync_api import (
    BrowserContext,
    BrowserType,
    Playwright,
    ViewportSize,
)

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)


def _ensure_dir(path: str) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


@contextlib.contextmanager
def build_browser_context(
    playwright: Playwright,
    *,
    headless: bool | None = None,
    slow_mo: int | None = None,
    downloads_dir: str | None = None,
) -> Generator[BrowserContext, None, None]:
    """Launch a Chromium browser and yield a ``BrowserContext`` with download support.

    Parameters fall back to ``settings`` values when *None*.

    Args:
        playwright: The active Playwright instance.
        headless: Override headless mode. ``None`` uses ``PLAYWRIGHT_HEADLESS``.
        slow_mo: Override slow-motion delay in ms. ``None`` uses ``PLAYWRIGHT_SLOW_MO``.
        downloads_dir: Override downloads directory. ``None`` uses ``PLAYWRIGHT_DOWNLOADS_DIR``.

    Yields:
        A :class:`playwright.sync_api.BrowserContext` ready for navigation.
    """
    _headless = headless if headless is not None else settings.playwright_headless
    _slow_mo = slow_mo if slow_mo is not None else settings.playwright_slow_mo
    _dl_dir = _ensure_dir(downloads_dir or settings.playwright_downloads_dir)

    logger.info(
        "Launching Chromium — headless=%s  slow_mo=%s ms  downloads=%s",
        _headless,
        _slow_mo,
        _dl_dir,
    )

    browser_type: BrowserType = playwright.chromium
    browser = browser_type.launch(headless=_headless, slow_mo=float(_slow_mo))
    try:
        # `downloads_path` is NOT a valid parameter for new_context() in the
        # Playwright Python API. Download destinations are set explicitly via
        # download.save_as() in the scraper after capturing the download event.
        context = browser.new_context(
            accept_downloads=True,
            viewport=ViewportSize(width=1280, height=900),
        )

        context.set_default_timeout(settings.playwright_timeout_ms)
        try:
            yield context
        finally:
            context.close()
    finally:
        browser.close()
        logger.info("Browser closed.")
