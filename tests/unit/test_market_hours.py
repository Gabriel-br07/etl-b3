"""Unit tests for B3 quote window helpers (America/Sao_Paulo semantics)."""

from __future__ import annotations

from datetime import datetime, time
from zoneinfo import ZoneInfo

import pytest

from app.etl.orchestration.market_hours import (
    B3QuoteWindowConfig,
    active_quote_window_bounds,
    is_within_b3_quote_window,
)


def test_active_quote_window_bounds_default_padding():
    cfg = B3QuoteWindowConfig(
        session_open=time(10, 0),
        session_close=time(17, 0),
        padding_before_minutes=10,
        padding_after_minutes=10,
        timezone="America/Sao_Paulo",
    )
    start, end = active_quote_window_bounds(cfg)
    assert start == time(9, 50)
    assert end == time(17, 10)


def test_is_within_window_mid_session_brt():
    tz = ZoneInfo("America/Sao_Paulo")
    cfg = B3QuoteWindowConfig(
        session_open=time(10, 0),
        session_close=time(17, 0),
        padding_before_minutes=10,
        padding_after_minutes=10,
        timezone="America/Sao_Paulo",
    )
    now = datetime(2026, 4, 3, 12, 0, 0, tzinfo=tz)
    assert is_within_b3_quote_window(now, cfg=cfg) is True


def test_is_within_window_ten_minutes_before_open_inclusive():
    tz = ZoneInfo("America/Sao_Paulo")
    cfg = B3QuoteWindowConfig(
        session_open=time(10, 0),
        session_close=time(17, 0),
        padding_before_minutes=10,
        padding_after_minutes=10,
        timezone="America/Sao_Paulo",
    )
    now = datetime(2026, 4, 3, 9, 50, 0, tzinfo=tz)
    assert is_within_b3_quote_window(now, cfg=cfg) is True


def test_is_within_window_one_second_before_window_starts():
    tz = ZoneInfo("America/Sao_Paulo")
    cfg = B3QuoteWindowConfig(
        session_open=time(10, 0),
        session_close=time(17, 0),
        padding_before_minutes=10,
        padding_after_minutes=10,
        timezone="America/Sao_Paulo",
    )
    now = datetime(2026, 4, 3, 9, 49, 59, tzinfo=tz)
    assert is_within_b3_quote_window(now, cfg=cfg) is False


def test_is_within_window_ten_minutes_after_close_inclusive():
    tz = ZoneInfo("America/Sao_Paulo")
    cfg = B3QuoteWindowConfig(
        session_open=time(10, 0),
        session_close=time(17, 0),
        padding_before_minutes=10,
        padding_after_minutes=10,
        timezone="America/Sao_Paulo",
    )
    now = datetime(2026, 4, 3, 17, 10, 0, tzinfo=tz)
    assert is_within_b3_quote_window(now, cfg=cfg) is True


def test_is_within_window_one_second_after_window_ends():
    tz = ZoneInfo("America/Sao_Paulo")
    cfg = B3QuoteWindowConfig(
        session_open=time(10, 0),
        session_close=time(17, 0),
        padding_before_minutes=10,
        padding_after_minutes=10,
        timezone="America/Sao_Paulo",
    )
    now = datetime(2026, 4, 3, 17, 10, 1, tzinfo=tz)
    assert is_within_b3_quote_window(now, cfg=cfg) is False


def test_saturday_still_uses_same_clock_bounds():
    """No holiday calendar: Saturday noon is "in window" if time matches; product no-ops elsewhere."""
    tz = ZoneInfo("America/Sao_Paulo")
    cfg = B3QuoteWindowConfig(
        session_open=time(10, 0),
        session_close=time(17, 0),
        padding_before_minutes=10,
        padding_after_minutes=10,
        timezone="America/Sao_Paulo",
    )
    # 2026-04-04 is Saturday
    now = datetime(2026, 4, 4, 12, 0, 0, tzinfo=tz)
    assert is_within_b3_quote_window(now, cfg=cfg) is True


def test_naive_datetime_assumed_brt():
    cfg = B3QuoteWindowConfig(
        session_open=time(10, 0),
        session_close=time(17, 0),
        padding_before_minutes=10,
        padding_after_minutes=10,
        timezone="America/Sao_Paulo",
    )
    now = datetime(2026, 4, 3, 12, 0, 0)
    assert is_within_b3_quote_window(now, cfg=cfg) is True


def test_invalid_window_raises():
    cfg = B3QuoteWindowConfig(
        session_open=time(17, 0),
        session_close=time(10, 0),
        padding_before_minutes=0,
        padding_after_minutes=0,
        timezone="America/Sao_Paulo",
    )
    tz = ZoneInfo("America/Sao_Paulo")
    with pytest.raises(ValueError, match="Quote window end"):
        is_within_b3_quote_window(datetime(2026, 4, 3, 12, 0, 0, tzinfo=tz), cfg=cfg)
