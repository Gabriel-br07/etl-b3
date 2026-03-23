"""HTTP download and ZIP extraction for B3 annual COTAHIST_A{year}.zip files."""

from __future__ import annotations

import re
import shutil
import time
import zipfile
from pathlib import Path

import httpx

from app.core.config import settings
from app.core.logging import get_logger
from app.integrations.b3.constants import DEFAULT_HEADERS

logger = get_logger(__name__)

_YEAR_FROM_ZIP_STEM = re.compile(r"^COTAHIST_A(\d{4})$", re.I)
_YEAR_FROM_MEMBER = re.compile(r"^COTAHIST[._]A(\d{4})(?:\.[Tt][Xx][Tt])?$", re.I)
_LEGACY_MEMBER_RE = re.compile(r"^COTAHIST[._]A\d{4}$", re.I)


def _zip_member_basename(member: str) -> str:
    return member.replace("\\", "/").rstrip("/").split("/")[-1]


def _select_cotahist_archive_member(namelist: list[str]) -> str:
    """Pick the historical quotations file inside a COTAHIST ZIP.

    Preference order:
    1. Any member whose basename ends in ``.TXT`` / ``.txt`` (newer B3 layout).
    2. Legacy names ``COTAHIST.AYYYY`` or ``COTAHIST_AYYYY`` without extension
       (typical for years before 2002).
    3. If the archive has exactly one file member, use it (logged).
    """
    files = [n for n in namelist if n and not n.endswith("/")]
    if not files:
        raise ValueError("ZIP archive has no file members")

    txt_members = sorted(n for n in files if _zip_member_basename(n).upper().endswith(".TXT"))
    if txt_members:
        return txt_members[0]

    legacy = sorted(n for n in files if _LEGACY_MEMBER_RE.match(_zip_member_basename(n)))
    if legacy:
        return legacy[0]

    if len(files) == 1:
        logger.info("[cotahist] using sole ZIP member as COTAHIST data: %s", files[0])
        return files[0]

    raise ValueError(f"No COTAHIST data file found in archive (members={namelist!r})")


def _canonical_extracted_txt_path(
    *,
    zip_path: Path | None,
    dest_dir: Path,
    member: str,
) -> Path:
    """Target path for extracted bytes: ``COTAHIST_A{year}.TXT`` when year is known."""
    year: int | None = None
    if zip_path is not None:
        m = _YEAR_FROM_ZIP_STEM.match(zip_path.stem)
        if m:
            year = int(m.group(1))
    if year is None:
        mm = _YEAR_FROM_MEMBER.match(_zip_member_basename(member))
        if mm:
            year = int(mm.group(1))
    if year is not None:
        return dest_dir / f"COTAHIST_A{year}.TXT"

    base = _zip_member_basename(member)
    if base.upper().endswith(".TXT"):
        return dest_dir / base
    return dest_dir / f"{base}.TXT"


def cotahist_annual_zip_url(year: int, *, base_url: str | None = None) -> str:
    """Return the SerHist URL for the annual file of *year*."""
    base = (base_url or settings.b3_cotahist_base_url).rstrip("/")
    return f"{base}/COTAHIST_A{year}.zip"


def download_cotahist_zip(
    year: int,
    dest_zip: Path,
    *,
    base_url: str | None = None,
    timeout: float | None = None,
    max_retries: int | None = None,
) -> Path:
    """Download ZIP to *dest_zip* (parent dirs created). Raises on failure after retries."""
    url = cotahist_annual_zip_url(year, base_url=base_url)
    timeout = timeout if timeout is not None else settings.b3_cotahist_timeout
    max_retries = max_retries if max_retries is not None else settings.b3_cotahist_max_retries
    if max_retries < 1:
        raise ValueError(f"max_retries must be >= 1 (got {max_retries!r})")
    if timeout is not None and timeout <= 0:
        raise ValueError(f"timeout must be a positive number (got {timeout!r})")

    headers = {
        **DEFAULT_HEADERS,
        "Accept": "application/zip, application/octet-stream, */*",
    }
    dest_zip.parent.mkdir(parents=True, exist_ok=True)

    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            with httpx.Client(headers=headers, timeout=timeout, follow_redirects=True) as client:
                with client.stream("GET", url) as resp:
                    if resp.status_code == 404:
                        raise FileNotFoundError(f"COTAHIST ZIP not found (404): {url}")
                    resp.raise_for_status()
                    with open(dest_zip, "wb") as out:
                        for chunk in resp.iter_bytes():
                            out.write(chunk)
            logger.info(
                "[cotahist] downloaded year=%s bytes=%s path=%s",
                year,
                dest_zip.stat().st_size,
                dest_zip,
            )
            return dest_zip
        except FileNotFoundError:
            # Definitive missing object (e.g. 404 for unpublished years) — do not retry.
            raise
        except Exception as exc:
            last_exc = exc
            dest_zip.unlink(missing_ok=True)
            logger.warning(
                "[cotahist] download attempt %s/%s failed year=%s url=%s err=%s",
                attempt,
                max_retries,
                year,
                url,
                exc,
            )
            if attempt < max_retries:
                time.sleep(2 ** (attempt - 1))
    assert last_exc is not None
    raise last_exc


def extract_cotahist_txt(zip_path: Path, dest_dir: Path) -> Path:
    """Extract the COTAHIST fixed-width file from *zip_path* into *dest_dir*.

    Newer archives use inner names like ``COTAHIST_A2002.TXT``. Older archives use
    ``COTAHIST.A1986`` or ``COTAHIST_A2001`` without a ``.txt`` suffix; those are
    still detected. Output is normalized to ``COTAHIST_A{year}.TXT`` when the
    year can be inferred from the ZIP name or inner member.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    zip_path = Path(zip_path)
    with zipfile.ZipFile(zip_path, "r") as zf:
        member = _select_cotahist_archive_member(zf.namelist())
        out = _canonical_extracted_txt_path(zip_path=zip_path, dest_dir=dest_dir, member=member)
        with zf.open(member, "r") as src, open(out, "wb") as dst:
            shutil.copyfileobj(src, dst)
    logger.info("[cotahist] extracted member=%s from %s -> %s", member, zip_path.name, out)
    return out
