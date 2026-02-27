"""Abstract base class for all scraper implementations.

To add a new site scraper:
  1. Subclass ``BaseScraper``.
  2. Implement ``scrape(target_date)``.
  3. Return a list of ``ScrapeResult`` objects.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path


@dataclass
class ScrapeResult:
    """Metadata returned after a successful download."""

    source_url: str
    file_path: Path
    suggested_filename: str
    saved_filename: str
    original_file_path: Path | None = None
    downloaded_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    target_date: date | None = None
    # Conversion status: True when post-download CSV normalization succeeded.
    conversion_succeeded: bool = False
    # Optional conversion error message when conversion_succeeded is False.
    conversion_error: str | None = None

    def __str__(self) -> str:
        return (
            f"ScrapeResult(file={self.file_path.name}, "
            f"date={self.target_date}, url={self.source_url})"
        )


class BaseScraper(ABC):
    """Generic browser-based scraper interface.

    Each concrete implementation represents one data source / site.
    """

    #: Human-readable name for logging and identification.
    site_name: str = "unknown"

    @abstractmethod
    def scrape(self, target_date: date) -> list[ScrapeResult]:
        """Run the full scraping flow for *target_date*.

        Args:
            target_date: The business date for which data should be fetched.

        Returns:
            A list of :class:`ScrapeResult` – one per downloaded file.
        """
        ...
