"""Unit tests for B3 locale numeric string parsing."""

from __future__ import annotations

import pytest

from app.etl.validation.b3_locale_numbers import parse_b3_locale_number


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("12769,99", 12769.99),
        ("1000", 1000.0),
        ("1.234,56", 1234.56),
        ("0,5", 0.5),
        ("  42  ", 42.0),
    ],
)
def test_parse_b3_locale_number_decimal_comma_and_integers(raw: str, expected: float) -> None:
    assert parse_b3_locale_number(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("", None),
        ("   ", None),
        ("-", None),
        (None, None),
    ],
)
def test_parse_b3_locale_number_null_like(raw: str | None, expected: None) -> None:
    assert parse_b3_locale_number(raw) is expected


def test_parse_b3_locale_number_dot_decimal_regression() -> None:
    """Fixture-style prices (dot only) must still parse."""
    assert parse_b3_locale_number("38.45") == 38.45
    assert parse_b3_locale_number("592940.00") == 592940.0


def test_parse_b3_locale_number_dot_as_thousands_no_comma() -> None:
    """Quantities like ``1.500`` (pt-BR thousands) must not become ``1.5``."""
    assert parse_b3_locale_number("1.500") == 1500.0
    assert parse_b3_locale_number("100.000") == 100_000.0
    assert parse_b3_locale_number("-1.500") == -1500.0
    assert parse_b3_locale_number("-100.000") == -100_000.0
    assert parse_b3_locale_number("+1.500") == 1500.0


def test_parse_b3_locale_number_invalid_returns_none() -> None:
    assert parse_b3_locale_number("abc") is None
    assert parse_b3_locale_number("12..34") is None


def test_parse_b3_locale_number_percent_suffix() -> None:
    assert parse_b3_locale_number("1,23%") == pytest.approx(1.23)
    assert parse_b3_locale_number("-0,5%") == pytest.approx(-0.5)
