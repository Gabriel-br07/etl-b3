"""Remote source adapter – discovers and downloads B3 Boletim Diário files.

MVP STATUS: Partial implementation.
Remote discovery from the B3 HTML page is fragile because B3 does not expose
a stable machine-readable API.  This adapter provides:
  - A configurable entrypoint URL (B3_BULLETIN_ENTRYPOINT_URL)
  - Optional direct URL templates (B3_INSTRUMENTS_URL_TEMPLATE / B3_TRADES_URL_TEMPLATE)
  - Download-to-local-temp logic with tenacity retries

TODO (post-MVP):
  - Implement full HTML scraping of the B3 Boletim Diário page to discover
    daily file links dynamically.
  - Parse the page to find the correct links for each date.
"""

from __future__ import annotations

import tempfile
from datetime import date
from pathlib import Path

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from app.core.config import settings
from app.core.constants import SourceName
from app.core.logging import get_logger
from app.etl.ingestion.base import B3PublicDataSource, SourceFile

logger = get_logger(__name__)


def _build_url(template: str | None, name: str, target_date: date) -> str | None:
    if not template:
        return None
    return template.format(
        name=name,
        date=target_date.strftime("%Y-%m-%d"),
        date_nodash=target_date.strftime("%Y%m%d"),
        year=target_date.year,
        month=f"{target_date.month:02d}",
        day=f"{target_date.day:02d}",
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def _download(url: str, dest: Path) -> None:
    logger.info("Downloading %s -> %s", url, dest)
    with httpx.stream("GET", url, follow_redirects=True, timeout=60) as resp:
        resp.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in resp.iter_bytes(chunk_size=65536):
                fh.write(chunk)


class RemoteAdapter(B3PublicDataSource):
    """Download B3 files from remote URLs (requires URL templates configured)."""

    def __init__(self) -> None:
        self._tmp_dir = Path(tempfile.mkdtemp(prefix="etlb3_"))

    def get_instruments_file(self, target_date: date) -> SourceFile:
        url = _build_url(
            settings.b3_instruments_url_template, SourceName.INSTRUMENTS, target_date
        )
        if not url:
            raise NotImplementedError(
                "B3_INSTRUMENTS_URL_TEMPLATE is not configured. "
                "Set the env var or use LocalFileAdapter. "
                "TODO: implement HTML scraping of B3_BULLETIN_ENTRYPOINT_URL."
            )
        dest = self._tmp_dir / f"{SourceName.INSTRUMENTS}_{target_date}.csv"
        _download(url, dest)
        return SourceFile(
            path=dest,
            source_name=SourceName.INSTRUMENTS,
            source_url=url,
            file_date=target_date,
        )

    def get_trades_file(self, target_date: date) -> SourceFile:
        url = _build_url(
            settings.b3_trades_url_template, SourceName.TRADES, target_date
        )
        if not url:
            raise NotImplementedError(
                "B3_TRADES_URL_TEMPLATE is not configured. "
                "Set the env var or use LocalFileAdapter. "
                "TODO: implement HTML scraping of B3_BULLETIN_ENTRYPOINT_URL."
            )
        dest = self._tmp_dir / f"{SourceName.TRADES}_{target_date}.zip"
        _download(url, dest)
        return SourceFile(
            path=dest,
            source_name=SourceName.TRADES,
            source_url=url,
            file_date=target_date,
        )
