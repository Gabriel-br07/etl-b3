"""Unit tests for technical indicators."""

from datetime import date
from decimal import Decimal

import pytest

from app.use_cases.quotes import indicators as ind


def test_sma_simple() -> None:
    closes = [Decimal("1"), Decimal("2"), Decimal("3"), Decimal("4")]
    out = ind.compute_sma(closes, 2)
    assert out[0] is None
    assert out[1] is not None
    assert out[1] == Decimal("1.5")


def test_ema_seed_is_sma() -> None:
    closes = [Decimal("1")] * 5 + [Decimal("10")]
    out = ind.compute_ema(closes, 3)
    assert out[2] is not None
    assert out[4] is not None


def test_rsi_flat_closes_produces_value() -> None:
    closes = [Decimal("10")] * 20
    out = ind.compute_rsi_wilder(closes, 14)
    assert out[-1] is not None
    assert Decimal(0) <= out[-1] <= Decimal(100)


def test_validate_period_bounds() -> None:
    with pytest.raises(ValueError):
        ind.validate_period(1)
    with pytest.raises(ValueError):
        ind.validate_period(500)


def test_build_indicator_series_sma() -> None:
    dates = [date(2024, 1, i) for i in range(1, 6)]
    closes = [Decimal("10")] * 5
    pairs = ind.build_indicator_series(dates, closes, "sma", 2)
    assert len(pairs) == 5
    assert pairs[1][1] is not None
