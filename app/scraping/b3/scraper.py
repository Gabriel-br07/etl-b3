"""B3 Boletim Diário scraper – orchestrates the full browser flow.

Flow:
  1. Open the B3 bulletin page.
  2. Click the "Renda variável" tab.
  3. Click "Resumo de ações" from the dropdown.
  4. Select "Cadastro de instrumentos" in ``#selectTabelas``.
  5. Click the CSV export button and capture the download.
  6. Save to ``data/raw/b3/boletim_diario/<YYYY-MM-DD>/``.

Debug / visual execution::

    # Headed browser (visible), slow interactions
    PLAYWRIGHT_HEADLESS=false PLAYWRIGHT_SLOW_MO=500 python scripts/run_b3_scraper.py --date 2024-06-14

    # Playwright Inspector
    PWDEBUG=1 python scripts/run_b3_scraper.py --date 2024-06-14
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
import time

from playwright.sync_api import sync_playwright

from app.core.config import settings
from app.core.logging import get_logger
from app.scraping.b3.downloader import (
    TABLE_CONFIG,
    trigger_csv_export_and_save,
)
from app.scraping.b3.selectors import B3Selectors
from app.scraping.common.base import BaseScraper, ScrapeResult
from app.scraping.common.browser import build_browser_context
from app.scraping.common.adaptive_wait import run_with_adaptive_wait
from app.scraping.common.cookie_banner import dismiss_cookie_banner_if_present
from app.scraping.common.exceptions import ElementNotFoundError, NavigationError
from app.scraping.common.storage import daily_output_dir, screenshots_dir

logger = get_logger(__name__)

#: CSS option value for "Cadastro de instrumentos"
_INSTRUMENTS_VALUE = TABLE_CONFIG["cadastro_instrumentos"][0]
_EXPORT_READY_PAUSE_MS = 2_000


class BoletimDiarioScraper(BaseScraper):
    """Playwright scraper for the B3 Boletim Diário do Mercado.

    Downloads the *Cadastro de Instrumentos* CSV for a given trading date.

    Args:
        headless: Override headless mode (``None`` → read from settings).
        slow_mo: Override slow-motion in ms (``None`` → read from settings).
        capture_screenshots: If ``True``, take a screenshot after each major step.
        capture_traces: If ``True``, record a Playwright trace for the session.
    """

    site_name = "b3"

    def __init__(
        self,
        *,
        headless: bool | None = None,
        slow_mo: int | None = None,
        capture_screenshots: bool = False,
        capture_traces: bool = False,
    ) -> None:
        self._headless = headless
        self._slow_mo = slow_mo
        self._capture_screenshots = capture_screenshots
        self._capture_traces = capture_traces

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scrape(self, target_date: date) -> list[ScrapeResult]:
        """Run the full B3 bulletin scraping flow for *target_date*.

        Args:
            target_date: Business date to download data for.

        Returns:
            A list with one :class:`~app.scraping.common.base.ScrapeResult`
            for the downloaded Cadastro de Instrumentos CSV.

        Raises:
            :class:`~app.scraping.common.exceptions.NavigationError`: If the
                page cannot be loaded.
            :class:`~app.scraping.common.exceptions.ElementNotFoundError`: If a
                required DOM element is missing.
            :class:`~app.scraping.common.exceptions.DownloadError`: If the file
                download fails.
        """
        output_dir = daily_output_dir(settings.b3_output_dir, self.site_name, target_date)
        ss_dir = screenshots_dir(settings.b3_screenshots_dir, self.site_name)

        logger.info(
            "Starting B3 Boletim Diário scrape — date=%s  output=%s",
            target_date,
            output_dir,
        )

        with sync_playwright() as playwright:
            with build_browser_context(
                playwright,
                headless=self._headless,
                slow_mo=self._slow_mo,
            ) as context:
                # Optionally start trace recording
                if self._capture_traces:
                    traces_path = Path(settings.b3_trace_dir)
                    traces_path.mkdir(parents=True, exist_ok=True)
                    context.tracing.start(screenshots=True, snapshots=True, sources=True)

                page = context.new_page()
                try:
                    result = self._run_flow(page, target_date, output_dir, ss_dir)
                except Exception:
                    if self._capture_screenshots:
                        self._screenshot(page, ss_dir, "failure")
                    if self._capture_traces:
                        self._stop_trace(context, target_date, "failure")
                    raise
                else:
                    if self._capture_traces:
                        self._stop_trace(context, target_date, "success")

        return [result]

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _run_flow(
        self,
        page,
        target_date: date,
        output_dir: Path,
        ss_dir: Path,
    ) -> ScrapeResult:
        sel = B3Selectors(page)

        # Step 1 – Open the B3 bulletin page
        url = settings.b3_bulletin_entrypoint_url
        logger.info("Opening B3 page: %s", url)
        response = page.goto(url, wait_until="domcontentloaded")
        if response is None or not response.ok:
            raise NavigationError(
                f"Failed to load B3 page: {url} "
                f"(status={getattr(response, 'status', 'unknown')})"
            )
        # Allow dynamic content to render (configurable pause)
        pause_after_open = int(settings.playwright_pause_after_open_ms or 0)
        if pause_after_open > 0:
            logger.debug("Pausing %sms after opening page to allow content render", pause_after_open)
            page.wait_for_timeout(pause_after_open)
        self._maybe_screenshot(page, ss_dir, "01_page_loaded")
        dismiss_cookie_banner_if_present(
            page,
            scraper_name=self.site_name,
            step="after_initial_page_load",
        )

        # Step 2 – Click "Renda variável" tab
        logger.info("Clicking 'Renda variável' tab …")
        # Optional pause before clicking Renda variável (page may still be settling)
        before_renda = int(settings.playwright_pause_before_renda_variavel_ms or 0)
        if before_renda > 0:
            logger.debug("Pausing %sms before clicking Renda variável", before_renda)
            page.wait_for_timeout(2000)
        self._safe_click(page, sel.tab_renda_variavel(), "Renda variável tab", ss_dir)
        # Small pause after clicking tab to allow dropdown/rendering
        between = int(settings.playwright_pause_between_actions_ms or 0)
        if between > 0:
            logger.debug("Pausing %sms after clicking Renda variável", between)
            page.wait_for_timeout(between)
        self._maybe_screenshot(page, ss_dir, "02_renda_variavel_clicked")

        # Step 3 – Click "Resumo de ações" dropdown item
        logger.info("Clicking 'Resumo de ações' …")
        self._safe_click(
            page, sel.dropdown_resumo_acoes(), "Resumo de ações dropdown item", ss_dir
        )
        if between > 0:
            logger.debug("Pausing %sms after clicking Resumo de ações", between)
            page.wait_for_timeout(between)
        self._maybe_screenshot(page, ss_dir, "03_resumo_acoes_clicked")

        # Interact with controls inside the embedded iframe
        logger.info("Interacting with iframe controls (Tabela select + Exportar CSV) …")
        iframe_el = page.query_selector("#bvmf_iframe")
        if iframe_el is None:
            ss_path = self._screenshot(page, ss_dir, "error_no_iframe")
            raise ElementNotFoundError(
                "Iframe #bvmf_iframe not found on page", screenshot_path=ss_path
            )

        frame = iframe_el.content_frame()
        if frame is None:
            ss_path = self._screenshot(page, ss_dir, "error_no_frame")
            raise ElementNotFoundError(
                "Iframe #bvmf_iframe has no content frame", screenshot_path=ss_path
            )

        # Click the labeled control 'Tabela' inside the frame to focus/open the table selector
        tabela_label = frame.get_by_label("Tabela")
        self._assert_visible(tabela_label, "Tabela label (iframe)", page, ss_dir)
        tabela_label.click()
        if between > 0:
            page.wait_for_timeout(between)

        # Select "Cadastro de instrumentos" from the select inside the frame
        select = frame.locator("#selectTabelas")
        self._assert_visible(select, "#selectTabelas (iframe)", page, ss_dir)
        select.select_option(_INSTRUMENTS_VALUE)
        if between > 0:
            page.wait_for_timeout(between)
        self._maybe_screenshot(page, ss_dir, "04_cadastro_selected")

        # Keep the same iframe-settle delay contract across bulletin scrapers.
        page.wait_for_timeout(_EXPORT_READY_PAUSE_MS)

        # Click the Export CSV button inside the frame and capture the download
        logger.info("Clicking Exportar CSV button inside iframe …")
        csv_btn = frame.get_by_role("button", name="Exportar CSV")
        self._assert_visible(csv_btn, "Exportar CSV button (iframe)", page, ss_dir)

        # Wait 1 second to ensure the UI inside the iframe has settled before clicking
        page.wait_for_timeout(1000)

        # Give the iframe UI a short moment to settle, then perform robust checks
        # before triggering the download. In fast/headless runs the button may
        # be visible but not yet enabled or the internal javascript may still be
        # preparing the file; these checks reduce race conditions that cause
        # expect_download to timeout.
        page.wait_for_timeout(1000)

        run_with_adaptive_wait(
            action_label="wait_visible:Exportar CSV button (iframe)",
            action=lambda timeout_ms: csv_btn.wait_for(state="visible", timeout=timeout_ms),
            base_timeout_ms=int(settings.playwright_timeout_ms),
            max_attempts=3,
            scraper_name=self.site_name,
        )

        # Wait until the button reports enabled, with a bounded loop.
        start = time.time()
        # Convert ms timeout to integer seconds for loop bound
        timeout_s = max(5, int(settings.playwright_timeout_ms) // 1000)
        while not csv_btn.is_enabled():
            if (time.time() - start) > timeout_s:
                ss_path = self._screenshot(page, ss_dir, "error_export_button_disabled")
                raise ElementNotFoundError(
                    "Exportar CSV button not enabled within timeout",
                    screenshot_path=ss_path,
                )
            time.sleep(0.1)

        # Give the page a chance to reach a quiet network state before the click
        try:
            page.wait_for_load_state("networkidle", timeout=int(settings.playwright_timeout_ms))
        except Exception:
            # networkidle is best-effort; continue even if it times out
            logger.debug("page.wait_for_load_state('networkidle') timed out; continuing")

        # Trigger the download; increase the expect_download timeout to be more
        # tolerant for slower server responses.
        result = trigger_csv_export_and_save(
            page=page,
            csv_button_locator=csv_btn,
            output_dir=output_dir,
            target_date=target_date,
            timeout_ms=max(int(settings.playwright_timeout_ms), 60_000),
        )
        self._maybe_screenshot(page, ss_dir, "05_download_complete")
        logger.info("Scrape complete → %s", result.file_path)
        return result

    def _safe_click(self, page, locator, description: str, ss_dir: Path) -> None:
        """Click *locator*, raising :class:`ElementNotFoundError` if not found."""
        self._assert_visible(locator, description, page, ss_dir)
        run_with_adaptive_wait(
            action_label=f"click:{description}",
            action=lambda timeout_ms: locator.click(timeout=timeout_ms),
            base_timeout_ms=int(settings.playwright_timeout_ms),
            max_attempts=3,
            scraper_name=self.site_name,
        )

    def _assert_visible(self, locator, description: str, page, ss_dir: Path) -> None:
        """Assert *locator* is visible; capture screenshot and raise if not."""
        if not locator.is_visible():
            ss_path: Path | None = None
            # Always capture a screenshot on error to aid debugging. This
            # clarifies the previous intent (the `or True` was redundant).
            ss_path = self._screenshot(page, ss_dir, f"error_{description[:30]}")
            raise ElementNotFoundError(
                f"Element not found or not visible: {description}",
                screenshot_path=ss_path,
            )

    def _maybe_screenshot(self, page, ss_dir: Path, step: str) -> None:
        """Take a screenshot only when ``capture_screenshots`` is enabled."""
        if self._capture_screenshots:
            self._screenshot(page, ss_dir, step)

    @staticmethod
    def _screenshot(page, ss_dir: Path, step: str) -> Path:
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
        path = ss_dir / f"{ts}_{step}.png"
        page.screenshot(path=str(path), full_page=True)
        logger.info("Screenshot saved: %s", path)
        return path

    @staticmethod
    def _stop_trace(context, target_date: date, suffix: str) -> None:
        from app.core.config import settings

        traces_path = Path(settings.b3_trace_dir)
        traces_path.mkdir(parents=True, exist_ok=True)
        trace_file = traces_path / f"trace_{target_date}_{suffix}.zip"
        context.tracing.stop(path=str(trace_file))
        logger.info("Trace saved: %s", trace_file)
