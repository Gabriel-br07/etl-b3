from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from app.scraping.common.cookie_banner import dismiss_cookie_banner_if_present


def test_dismiss_cookie_banner_clicks_when_button_is_visible() -> None:
    page = MagicMock()
    page.url = "https://b3.example/page"
    button = MagicMock()
    page.get_by_role.return_value = button

    out = dismiss_cookie_banner_if_present(
        page,
        scraper_name="b3",
        step="after_initial_page_load",
    )

    assert out is True
    page.get_by_role.assert_called_once_with("button", name="REJEITAR TODOS OS COOKIES")
    button.wait_for.assert_called_once_with(state="visible", timeout=2_000)
    button.click.assert_called_once_with(timeout=2_000)


def test_dismiss_cookie_banner_is_noop_on_playwright_timeout() -> None:
    page = MagicMock()
    button = MagicMock()
    page.get_by_role.return_value = button
    button.wait_for.side_effect = PlaywrightTimeoutError("Timeout 2000ms exceeded")

    out = dismiss_cookie_banner_if_present(page)

    assert out is False
    page.get_by_role.assert_called_once_with("button", name="REJEITAR TODOS OS COOKIES")
    button.click.assert_not_called()


def test_dismiss_cookie_banner_is_noop_on_expected_actionability_error() -> None:
    page = MagicMock()
    button = MagicMock()
    page.get_by_role.return_value = button
    button.click.side_effect = PlaywrightError("Element is not visible")

    out = dismiss_cookie_banner_if_present(page)

    assert out is False


def test_dismiss_cookie_banner_reraises_unexpected_playwright_error_with_warning() -> None:
    page = MagicMock()
    page.url = "https://b3.example/page"
    button = MagicMock()
    page.get_by_role.return_value = button
    button.click.side_effect = PlaywrightError("Target page, context or browser has been closed")

    with patch("app.scraping.common.cookie_banner.logger") as mock_logger:
        with pytest.raises(PlaywrightError, match="closed"):
            dismiss_cookie_banner_if_present(
                page,
                scraper_name="b3",
                step="after_initial_page_load",
            )

    mock_logger.warning.assert_called_once()
    warning_text = mock_logger.warning.call_args.args[0]
    assert "Unexpected cookie banner failure" in warning_text


def test_dismiss_cookie_banner_reraises_non_playwright_error_with_warning() -> None:
    page = MagicMock()
    button = MagicMock()
    page.get_by_role.return_value = button
    button.wait_for.side_effect = RuntimeError("boom")

    with patch("app.scraping.common.cookie_banner.logger") as mock_logger:
        with pytest.raises(RuntimeError, match="boom"):
            dismiss_cookie_banner_if_present(page, scraper_name="b3")

    mock_logger.warning.assert_called_once()
