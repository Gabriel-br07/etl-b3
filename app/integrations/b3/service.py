"""B3 quote service – orchestrates client + parser + cache.

Responsibilities:
- Build and own a ``B3QuoteClient`` instance.
- Call the parser to convert raw payloads to typed domain models.
- Apply an in-memory TTL cache (per ticker, keyed by response type) to
  avoid hammering the API.
- Surface only domain exceptions upward; never leak ``httpx`` internals.

Public methods:
- ``get_quote_snapshot(ticker)`` → ``NormalizedQuote``  (legacy compat)
- ``get_latest_snapshot(ticker)`` → ``LatestSnapshotQuote``
- ``get_intraday_series(ticker)`` → ``IntradaySeriesQuote``
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Generic, TypeVar

from app.core.config import settings
from app.integrations.b3.client import B3QuoteClient
from app.integrations.b3.models import IntradaySeriesQuote, LatestSnapshotQuote, NormalizedQuote
from app.integrations.b3.parser import (
    parse_daily_fluctuation,
    parse_intraday_series,
    parse_latest_snapshot,
)

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


# ---------------------------------------------------------------------------
# Simple thread-safe in-memory cache
# ---------------------------------------------------------------------------


class _CacheEntry(Generic[_T]):
    __slots__ = ("value", "expires_at")

    def __init__(self, value: _T, ttl_seconds: int) -> None:
        self.value = value
        self.expires_at: float = time.monotonic() + ttl_seconds


class _QuoteCache(Generic[_T]):
    """Minimal thread-safe TTL cache keyed by ticker symbol."""

    def __init__(self) -> None:
        self._store: dict[str, _CacheEntry[_T]] = {}
        self._lock = threading.Lock()

    def get(self, ticker: str) -> _T | None:
        with self._lock:
            entry = self._store.get(ticker)
            if entry is None:
                return None
            if time.monotonic() > entry.expires_at:
                del self._store[ticker]
                return None
            return entry.value

    def set(self, ticker: str, value: _T, ttl_seconds: int) -> None:
        with self._lock:
            self._store[ticker] = _CacheEntry(value, ttl_seconds)

    def invalidate(self, ticker: str) -> None:
        with self._lock:
            self._store.pop(ticker, None)


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class B3QuoteService:
    """Orchestrate fetching and normalising B3 live quote data.

    Intended to be used as a singleton (or short-lived per-request) by
    the use-case and API layers.

    Args:
        client: Optional pre-built ``B3QuoteClient`` (useful in tests).
        cache_ttl: Per-ticker cache TTL in seconds.  Pass ``0`` to disable
            caching entirely.
    """

    def __init__(
        self,
        *,
        client: B3QuoteClient | None = None,
        cache_ttl: int | None = None,
    ) -> None:
        self._client: B3QuoteClient = client or B3QuoteClient()
        self._cache_ttl: int = (
            cache_ttl if cache_ttl is not None else settings.b3_quote_cache_ttl
        )
        # Separate caches for each response type to avoid cross-type collisions.
        self._cache: _QuoteCache[NormalizedQuote] = _QuoteCache()
        self._intraday_cache: _QuoteCache[IntradaySeriesQuote] = _QuoteCache()
        self._snapshot_cache: _QuoteCache[LatestSnapshotQuote] = _QuoteCache()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_quote_snapshot(self, ticker: str) -> NormalizedQuote:
        """Return a legacy normalised snapshot for *ticker* (backward-compat).

        Results are cached per-ticker for ``cache_ttl`` seconds.

        Raises:
            B3TickerNotFoundError: Ticker does not exist on B3.
            B3TemporaryBlockError: B3 is rate-limiting us.
            B3UnexpectedResponseError: Parse error or unexpected HTTP response.
        """
        ticker = ticker.strip().upper()

        if self._cache_ttl > 0:
            cached = self._cache.get(ticker)
            if cached is not None:
                logger.debug("Cache hit (NormalizedQuote) for ticker '%s'.", ticker)
                return cached

        logger.info("Cache miss (NormalizedQuote) for ticker '%s'; fetching from B3.", ticker)
        raw_payload: dict[str, Any] = self._client.get_daily_fluctuation_history(ticker)
        quote = parse_daily_fluctuation(raw_payload, requested_ticker=ticker)

        if self._cache_ttl > 0:
            self._cache.set(ticker, quote, self._cache_ttl)

        return quote

    def get_latest_snapshot(self, ticker: str) -> LatestSnapshotQuote:
        """Return the latest intraday snapshot for *ticker*.

        Derived from the *last* item in ``lstQtn`` of the real B3 payload.
        Results are cached per-ticker for ``cache_ttl`` seconds.

        Raises:
            B3TickerNotFoundError: Ticker does not exist on B3.
            B3TemporaryBlockError: B3 is rate-limiting us.
            B3UnexpectedResponseError: Parse error or unexpected HTTP response.
        """
        ticker = ticker.strip().upper()

        if self._cache_ttl > 0:
            cached = self._snapshot_cache.get(ticker)
            if cached is not None:
                logger.debug("Cache hit (LatestSnapshotQuote) for ticker '%s'.", ticker)
                return cached

        logger.info(
            "Cache miss (LatestSnapshotQuote) for ticker '%s'; fetching from B3.", ticker
        )
        raw_payload: dict[str, Any] = self._client.get_daily_fluctuation_history(ticker)
        snapshot = parse_latest_snapshot(raw_payload, requested_ticker=ticker)

        if self._cache_ttl > 0:
            self._snapshot_cache.set(ticker, snapshot, self._cache_ttl)

        return snapshot

    def get_intraday_series(self, ticker: str) -> IntradaySeriesQuote:
        """Return the full intraday series for *ticker*.

        Contains all minute-level ``lstQtn`` points from the real B3 payload.
        Results are cached per-ticker for ``cache_ttl`` seconds.

        Raises:
            B3TickerNotFoundError: Ticker does not exist on B3.
            B3TemporaryBlockError: B3 is rate-limiting us.
            B3UnexpectedResponseError: Parse error or unexpected HTTP response.
        """
        ticker = ticker.strip().upper()

        if self._cache_ttl > 0:
            cached = self._intraday_cache.get(ticker)
            if cached is not None:
                logger.debug("Cache hit (IntradaySeriesQuote) for ticker '%s'.", ticker)
                return cached

        logger.info(
            "Cache miss (IntradaySeriesQuote) for ticker '%s'; fetching from B3.", ticker
        )
        raw_payload: dict[str, Any] = self._client.get_daily_fluctuation_history(ticker)
        series = parse_intraday_series(raw_payload, requested_ticker=ticker)

        if self._cache_ttl > 0:
            self._intraday_cache.set(ticker, series, self._cache_ttl)

        return series

    def invalidate_cache(self, ticker: str) -> None:
        """Remove *ticker* from all caches, forcing a fresh fetch next time."""
        key = ticker.strip().upper()
        self._cache.invalidate(key)
        self._snapshot_cache.invalidate(key)
        self._intraday_cache.invalidate(key)

    def close(self) -> None:
        """Release underlying HTTP client resources."""
        self._client.close()

    # Context-manager support so callers can use ``with B3QuoteService() as svc:``
    def __enter__(self) -> "B3QuoteService":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


