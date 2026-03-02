"""Use case: get the current daily-fluctuation snapshot for a B3 ticker.

This layer is intentionally thin.  Its only job is to receive a ticker,
delegate to the service, and return the result.  No HTTP knowledge, no
parsing, no caching logic lives here.

Having a dedicated use-case module makes it easy to:
- swap the underlying service in tests.
- add cross-cutting concerns (auth checks, audit logs) without touching
  the service or the API route.
"""

from __future__ import annotations

import logging

from app.integrations.b3.models import NormalizedQuote
from app.integrations.b3.service import B3QuoteService

logger = logging.getLogger(__name__)

# Module-level default service instance (lazy singleton).
# The API route uses this directly; tests can inject a custom service.
_default_service: B3QuoteService | None = None


def _get_default_service() -> B3QuoteService:
    global _default_service
    if _default_service is None:
        _default_service = B3QuoteService()
    return _default_service


def get_daily_fluctuation(
    ticker: str,
    *,
    service: B3QuoteService | None = None,
) -> NormalizedQuote:
    """Return the latest daily fluctuation snapshot for *ticker*.

    Args:
        ticker: B3 ticker symbol (e.g. ``"PETR4"``).  Case-insensitive.
        service: Optional service override (useful in tests).

    Returns:
        A validated :class:`~app.integrations.b3.models.NormalizedQuote`.

    Raises:
        B3ClientError: (or any subclass) when the upstream call fails.
    """
    svc = service or _get_default_service()
    ticker = ticker.strip().upper()
    logger.info("Use case: get_daily_fluctuation ticker='%s'", ticker)
    return svc.get_quote_snapshot(ticker)
