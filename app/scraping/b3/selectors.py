"""B3 page selectors and locator helpers.

Centralising all selectors here means that when B3 redesigns its
front-end, there is exactly one file to update.

Every method returns a Playwright ``Locator`` so callers never hardcode
CSS strings.  Resilience order of preference:
  1. ``get_by_role`` / ``get_by_text``
  2. ``locator("[data-*]")``
  3. CSS class (last resort)
"""

from __future__ import annotations

from playwright.sync_api import Page, Locator
import re


class B3Selectors:
    """Locator factory for the B3 Boletim Diário do Mercado page.

    Args:
        page: The Playwright :class:`~playwright.sync_api.Page` to scope locators to.
    """

    def __init__(self, page: Page) -> None:
        self._page = page

    # ------------------------------------------------------------------
    # Tab navigation
    # ------------------------------------------------------------------

    def tab_renda_variavel(self) -> Locator:
        """'Renda variável' primary navigation tab."""
        # Use a regex to avoid mismatches from invisible glyphs or spacing
        name_re = re.compile(r"Renda\s*variável", re.IGNORECASE)

        # 1) Try inside the known iframe (more robust if the nav is embedded)
        try:
            frame_loc = self._page.frame_locator("#bvmf_iframe")
            # Try button role first (some implementations use a button)
            btn = frame_loc.get_by_role("button", name=name_re)
            if btn.count() > 0:
                return btn
            # Then try link role
            link = frame_loc.get_by_role("link", name=name_re)
            if link.count() > 0:
                return link
        except Exception:
            # If frame_locator isn't available or fails, fall back to page-level locators
            pass

        # 2) Page-level role-based locator
        primary = self._page.get_by_role("link", name=name_re)
        if primary.count() > 0:
            return primary

        # 3) Fallback: text content (handles case-variation)
        return self._page.get_by_text("Renda variável", exact=False).first

    def dropdown_resumo_acoes(self) -> Locator:
        """'Resumo de ações' dropdown menu item."""
        name_re = re.compile(r"Resumo\s*de\s*Ações|Resumo\s*de\s*ações", re.IGNORECASE)

        # 1) Try inside iframe first
        try:
            frame_loc = self._page.frame_locator("#bvmf_iframe")
            # Some implementations use a button inside the iframe
            btn = frame_loc.get_by_role("button", name=name_re)
            if btn.count() > 0:
                return btn
            # Otherwise try link role
            link = frame_loc.get_by_role("link", name=name_re)
            if link.count() > 0:
                return link
        except Exception:
            pass

        # 2) Page-level role-based locator
        primary = self._page.get_by_role("link", name=name_re)
        if primary.count() > 0:
            return primary

        # 3) Fallback to generic anchor with partial text
        return self._page.locator("a.dropdown-item", has_text="Resumo de ações").first

    # ------------------------------------------------------------------
    # Table selector
    # ------------------------------------------------------------------

    def select_tabelas(self) -> Locator:
        """The ``#selectTabelas`` <select> element."""
        return self._page.locator("#selectTabelas")

    # ------------------------------------------------------------------
    # Export button
    # ------------------------------------------------------------------

    def csv_export_button(self) -> Locator:
        """The CSV download/export icon button.

        Tries multiple selectors in order of resilience.
        """
        # Prefer aria label first
        by_aria = self._page.get_by_role("button", name="CSV")
        if by_aria.count() > 0:
            return by_aria
        # Icon span wrapped in a clickable ancestor
        by_icon = self._page.locator("span.b3__ico--csv").first
        return by_icon

    # ------------------------------------------------------------------
    # Page readiness
    # ------------------------------------------------------------------

    def main_content_area(self) -> Locator:
        """Any landmark that signals the page has fully loaded."""
        return self._page.locator("main, #main-content, .container-fluid").first
