"""Configurable B3 equities session window for intraday quote scheduling.

Official B3 hours change via circulars; keep session open/close in env/config
and update when B3 publishes new timetables — do not treat defaults as permanent.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

DEFAULT_TZ = "America/Sao_Paulo"


def _parse_hhmm(value: str) -> time:
    parts = value.strip().split(":")
    if len(parts) == 1:
        return time(int(parts[0], 10), 0)
    return time(int(parts[0], 10), int(parts[1], 10))


@dataclass(frozen=True)
class B3QuoteWindowConfig:
    """Continuous session bounds (local exchange time, not UTC)."""

    session_open: time
    session_close: time
    padding_before_minutes: int
    padding_after_minutes: int
    timezone: str

    @classmethod
    def from_env(cls) -> B3QuoteWindowConfig:
        open_s = os.environ.get("B3_EQUITIES_SESSION_OPEN", "10:00")
        close_s = os.environ.get("B3_EQUITIES_SESSION_CLOSE", "17:00")
        pad_before = int(os.environ.get("B3_QUOTE_WINDOW_PADDING_BEFORE_MINUTES", "10"))
        pad_after = int(os.environ.get("B3_QUOTE_WINDOW_PADDING_AFTER_MINUTES", "10"))
        tz = os.environ.get("B3_MARKET_TIMEZONE", DEFAULT_TZ)
        return cls(
            session_open=_parse_hhmm(open_s),
            session_close=_parse_hhmm(close_s),
            padding_before_minutes=pad_before,
            padding_after_minutes=pad_after,
            timezone=tz,
        )


def active_quote_window_bounds(cfg: B3QuoteWindowConfig) -> tuple[time, time]:
    """Return (start_time, end_time) on a reference day for the active quote window.

    Uses session open/close in local time with symmetric padding by default
    (separate before/after minutes in :class:`B3QuoteWindowConfig`).
    """
    open_dt = datetime.combine(datetime.min.date(), cfg.session_open)
    close_dt = datetime.combine(datetime.min.date(), cfg.session_close)
    start = (open_dt - timedelta(minutes=cfg.padding_before_minutes)).time()
    end = (close_dt + timedelta(minutes=cfg.padding_after_minutes)).time()
    return start, end


def is_within_b3_quote_window(
    now: datetime | None = None,
    *,
    cfg: B3QuoteWindowConfig | None = None,
) -> bool:
    """True if *now* falls inside the configured active quote window (local session day).

    Weekend and exchange holidays are not modeled: the schedule may still fire
    (e.g. Prefect interval) and callers should no-op when this returns False
    outside the intraday window on any calendar day.
    """
    cfg = cfg or B3QuoteWindowConfig.from_env()
    tz = ZoneInfo(cfg.timezone)
    if now is None:
        now = datetime.now(tz=tz)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=tz)
    else:
        now = now.astimezone(tz)

    local_date = now.date()
    start_t, end_t = active_quote_window_bounds(cfg)

    start_dt = datetime.combine(local_date, start_t, tzinfo=tz)
    end_dt = datetime.combine(local_date, end_t, tzinfo=tz)

    if end_dt <= start_dt:
        raise ValueError(
            "Quote window end must be after start (check session times and padding)."
        )

    return start_dt <= now <= end_dt
