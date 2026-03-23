"""Settings normalization for B3 COTAHIST annual directory."""

from __future__ import annotations

from app.core.config import B3_COTAHIST_ANNUAL_DIR_DEFAULT, Settings


def test_b3_cotahist_annual_dir_empty_coerces_to_default() -> None:
    assert Settings(b3_cotahist_annual_dir="").b3_cotahist_annual_dir == B3_COTAHIST_ANNUAL_DIR_DEFAULT


def test_b3_cotahist_annual_dir_whitespace_coerces_to_default() -> None:
    assert (
        Settings(b3_cotahist_annual_dir="  \t  ").b3_cotahist_annual_dir
        == B3_COTAHIST_ANNUAL_DIR_DEFAULT
    )


def test_b3_cotahist_annual_dir_strips_whitespace() -> None:
    assert Settings(b3_cotahist_annual_dir="  data/cotahist  ").b3_cotahist_annual_dir == "data/cotahist"
