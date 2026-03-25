"""Build OHLC candles from daily quotes or intraday fact_quotes points."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from app.core.market_data_constants import CANDLE_INTERVALS

INTERVAL_TO_MINUTES: dict[str, int] = {"5m": 5, "15m": 15, "1h": 60}


def validate_interval(interval: str) -> None:
    if interval not in CANDLE_INTERVALS:
        raise ValueError(f"interval must be one of {sorted(CANDLE_INTERVALS)}")


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _bucket_start(dt: datetime, step_seconds: int) -> datetime:
    dt = _to_utc(dt)
    ts = int(dt.timestamp())
    bucket = (ts // step_seconds) * step_seconds
    return datetime.fromtimestamp(bucket, tz=timezone.utc)


def daily_candles_from_quotes(rows: list[Any]) -> list[dict[str, Any]]:
    """One row per ``FactDailyQuote`` in chronological order; interval ``1d``."""
    out: list[dict[str, Any]] = []
    for q in rows:
        start = datetime(
            q.trade_date.year,
            q.trade_date.month,
            q.trade_date.day,
            tzinfo=timezone.utc,
        )
        end = start + timedelta(days=1)
        close = q.last_price
        hi = q.max_price if q.max_price is not None else close
        lo = q.min_price if q.min_price is not None else close
        out.append(
            {
                "interval": "1d",
                "start_time": start,
                "end_time": end,
                "open": close,
                "high": hi,
                "low": lo,
                "close": close,
                "volume": q.financial_volume,
                "point_count": None,
            }
        )
    return out


def intraday_candles_from_points(
    points: list[tuple[datetime, Decimal | None]],
    interval_code: str,
    *,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Aggregate ordered (quoted_at, close_price) into OHLC buckets."""
    if interval_code not in INTERVAL_TO_MINUTES:
        raise ValueError(f"Not an intraday interval: {interval_code}")
    step = INTERVAL_TO_MINUTES[interval_code] * 60
    buckets: dict[int, list[tuple[datetime, Decimal]]] = {}
    for ts_, price in points:
        if price is None:
            continue
        b0 = _bucket_start(ts_, step)
        key = int(b0.timestamp())
        buckets.setdefault(key, []).append((ts_, price))
    out: list[dict[str, Any]] = []
    for key in sorted(buckets):
        pairs = sorted(buckets[key], key=lambda x: x[0])
        prices = [p for _, p in pairs]
        start = datetime.fromtimestamp(key, tz=timezone.utc)
        end = start + timedelta(seconds=step)
        out.append(
            {
                "interval": interval_code,
                "start_time": start,
                "end_time": end,
                "open": prices[0],
                "high": max(prices),
                "low": min(prices),
                "close": prices[-1],
                "volume": None,
                "point_count": len(prices),
            }
        )
    if limit is not None and len(out) > limit:
        out = out[-limit:]
    return out


def clip_candles_chronological(candles: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    """Keep last ``limit`` candles (assumes chronological order)."""
    if len(candles) <= limit:
        return candles
    return candles[-limit:]
