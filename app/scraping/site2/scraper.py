"""Site 2 scraper – TODO placeholder.

Replace this file content once the second site URL and action steps
are provided.  Follow the same pattern as :mod:`app.scraping.b3.scraper`:

  1. Subclass :class:`~app.scraping.common.base.BaseScraper`.
  2. Implement ``scrape(target_date) -> list[ScrapeResult]``.
  3. Extract all selectors to a ``selectors.py`` companion module.
  4. Extract download logic to a ``downloader.py`` companion module.
"""

from __future__ import annotations

from datetime import date

from app.scraping.common.base import BaseScraper, ScrapeResult


class Site2Scraper(BaseScraper):
    """Placeholder scraper for site 2.

    TODO: Implement full scraping flow.
    """

    site_name = "site2"

    def scrape(self, target_date: date) -> list[ScrapeResult]:
        """TODO: implement site 2 scraping for *target_date*."""
        raise NotImplementedError(
            "Site 2 scraper is not yet implemented. "
            "Provide the site URL and interaction steps to complete the implementation."
        )
