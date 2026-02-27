"""Custom exception hierarchy for the scraping module."""

from __future__ import annotations

from pathlib import Path


class ScraperError(RuntimeError):
    """Base class for all scraper errors."""


class ElementNotFoundError(ScraperError):
    """Raised when a required DOM element cannot be located.

    Attributes:
        selector: The CSS/text selector that failed.
        screenshot_path: Optional path to a failure screenshot.
    """

    def __init__(
        self,
        message: str,
        selector: str = "",
        screenshot_path: Path | None = None,
    ) -> None:
        detail = message
        if selector:
            detail += f" (selector: {selector!r})"
        if screenshot_path:
            detail += f" — screenshot saved to {screenshot_path}"
        super().__init__(detail)
        self.selector = selector
        self.screenshot_path = screenshot_path


class DownloadError(ScraperError):
    """Raised when a file download fails or times out."""


class NavigationError(ScraperError):
    """Raised when page navigation fails (wrong URL, timeout, etc.)."""
