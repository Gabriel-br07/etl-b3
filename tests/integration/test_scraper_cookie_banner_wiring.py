from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.scraping.b3.scraper import BoletimDiarioScraper
from app.scraping.b3.scraper_negocios import NegociosConsolidadosScraper


pytestmark = pytest.mark.integration


def _ok_page() -> MagicMock:
    page = MagicMock()
    response = MagicMock()
    response.ok = True
    response.status = 200
    page.goto.return_value = response
    return page


def test_boletim_flow_calls_cookie_helper_after_initial_page_load() -> None:
    scraper = BoletimDiarioScraper()
    page = _ok_page()
    selectors = MagicMock()
    selectors.tab_renda_variavel.return_value = MagicMock()

    with (
        patch("app.scraping.b3.scraper.settings") as mock_settings,
        patch("app.scraping.b3.scraper.B3Selectors", return_value=selectors),
        patch("app.scraping.b3.scraper.dismiss_cookie_banner_if_present") as dismiss_cookie,
        patch.object(scraper, "_safe_click", side_effect=RuntimeError("stop_after_cookie")),
    ):
        mock_settings.b3_bulletin_entrypoint_url = "https://example.test/b3"
        mock_settings.playwright_pause_after_open_ms = 0
        mock_settings.playwright_pause_before_renda_variavel_ms = 0
        mock_settings.playwright_pause_between_actions_ms = 0

        with pytest.raises(RuntimeError, match="stop_after_cookie"):
            scraper._run_flow(page, date(2024, 6, 14), Path("out"), Path("ss"))

    dismiss_cookie.assert_called_once_with(
        page,
        scraper_name="b3",
        step="after_initial_page_load",
    )


def test_negocios_flow_calls_cookie_helper_after_initial_page_load() -> None:
    scraper = NegociosConsolidadosScraper()
    page = _ok_page()
    selectors = MagicMock()
    selectors.tab_renda_variavel.return_value = MagicMock()

    with (
        patch("app.scraping.b3.scraper_negocios.settings") as mock_settings,
        patch("app.scraping.b3.scraper_negocios.B3Selectors", return_value=selectors),
        patch("app.scraping.b3.scraper_negocios.dismiss_cookie_banner_if_present") as dismiss_cookie,
        patch.object(scraper, "_safe_click", side_effect=RuntimeError("stop_after_cookie")),
    ):
        mock_settings.b3_bulletin_entrypoint_url = "https://example.test/b3"
        mock_settings.playwright_pause_after_open_ms = 0
        mock_settings.playwright_pause_between_actions_ms = 0

        with pytest.raises(RuntimeError, match="stop_after_cookie"):
            scraper._run_flow(page, date(2024, 6, 14), Path("out"), Path("ss"))

    dismiss_cookie.assert_called_once_with(
        page,
        scraper_name="b3",
        step="after_initial_page_load",
    )
