"""Scraping module – browser-based data acquisition for B3 and future sites.

Re-exports key symbols for convenience.
"""

from app.scraping.b3.scraper import BoletimDiarioScraper
from app.scraping.common.base import BaseScraper, ScrapeResult

__all__ = ["BaseScraper", "ScrapeResult", "BoletimDiarioScraper"]
