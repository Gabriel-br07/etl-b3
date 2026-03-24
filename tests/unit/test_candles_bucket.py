"""Unit tests for candle bucketing."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from app.use_cases.quotes import candles as c


def test_validate_interval_rejects_unknown() -> None:
    with pytest.raises(ValueError, match="interval"):
        c.validate_interval("2h")


def test_intraday_buckets_utc() -> None:
    # Two points in same 5m bucket [14:00,14:05), one in next
    t0 = datetime(2024, 6, 14, 14, 1, 0, tzinfo=timezone.utc)
    t1 = datetime(2024, 6, 14, 14, 3, 0, tzinfo=timezone.utc)
    t2 = datetime(2024, 6, 14, 14, 7, 0, tzinfo=timezone.utc)
    pts = [
        (t0, Decimal("10")),
        (t1, Decimal("12")),
        (t2, Decimal("11")),
    ]
    out = c.intraday_candles_from_points(pts, "5m", limit=None)
    assert len(out) == 2
    assert out[0]["open"] == Decimal("10")
    assert out[0]["close"] == Decimal("12")
    assert out[0]["high"] == Decimal("12")
    assert out[0]["low"] == Decimal("10")
    assert out[0]["point_count"] == 2
    assert out[1]["close"] == Decimal("11")


def test_clip_candles_respects_limit() -> None:
    rows = [{"interval": "1d", "i": i} for i in range(5)]
    clipped = c.clip_candles_chronological(rows, 2)
    assert len(clipped) == 2
    assert clipped[0]["i"] == 3


def test_daily_candles_from_quotes_stub() -> None:
    class Q:
        trade_date = __import__("datetime").date(2024, 1, 2)
        last_price = Decimal("5")
        max_price = Decimal("6")
        min_price = Decimal("4")
        financial_volume = Decimal("100")

    out = c.daily_candles_from_quotes([Q()])
    assert len(out) == 1
    assert out[0]["interval"] == "1d"
    assert out[0]["close"] == Decimal("5")
    assert out[0]["volume"] == Decimal("100")
