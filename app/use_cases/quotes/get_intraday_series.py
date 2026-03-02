"""Use case: get the full intraday series for a B3 ticker.

Returns all minute-level ``lstQtn`` points from the real B3
DailyFluctuationHistory payload, ordered oldest-first.

This layer is intentionally thin.  Its only job is to receive a ticker,
delegate to the service, and return the result.
"""

from __future__ import annotations

import logging

from app.integrations.b3.models import IntradaySeriesQuote
from app.integrations.b3.service import B3QuoteService

logger = logging.getLogger(__name__)

_default_service: B3QuoteService | None = None


def _get_default_service() -> B3QuoteService:
    global _default_service
    if _default_service is None:
        _default_service = B3QuoteService()
    return _default_service


def get_intraday_series(
    ticker: str,
    *,
    service: B3QuoteService | None = None,
) -> IntradaySeriesQuote:
    """Return the full intraday series for *ticker*.

    Args:
        ticker: B3 ticker symbol (e.g. ``"PETR4"``).  Case-insensitive.
        service: Optional service override (useful in tests).

    Returns:
        A validated :class:`~app.integrations.b3.models.IntradaySeriesQuote`
        containing all minute-level quote points for today's session.

    Raises:
        B3ClientError: (or any subclass) when the upstream call fails.
    """
    svc = service or _get_default_service()
    ticker = ticker.strip().upper()
    logger.info("Use case: get_intraday_series ticker='%s'", ticker)
    return svc.get_intraday_series(ticker)
