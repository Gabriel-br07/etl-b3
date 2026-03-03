"""Use case: get the latest intraday snapshot for a B3 ticker.

Derives the snapshot from the *last* item in ``lstQtn`` of the real B3
DailyFluctuationHistory payload.

This layer is intentionally thin.  Its only job is to receive a ticker,
delegate to the service, and return the result.
"""

from __future__ import annotations

import logging

from app.integrations.b3.models import LatestSnapshotQuote
from app.integrations.b3.service import B3QuoteService

logger = logging.getLogger(__name__)

_default_service: B3QuoteService | None = None


def _get_default_service() -> B3QuoteService:
    global _default_service
    if _default_service is None:
        _default_service = B3QuoteService()
    return _default_service


def get_latest_snapshot(
    ticker: str,
    *,
    service: B3QuoteService | None = None,
) -> LatestSnapshotQuote:
    """Return the latest intraday snapshot (last ``lstQtn`` point) for *ticker*.

    Args:
        ticker: B3 ticker symbol (e.g. ``"PETR4"``).  Case-insensitive.
        service: Optional service override (useful in tests).

    Returns:
        A validated :class:`~app.integrations.b3.models.LatestSnapshotQuote`.

    Raises:
        B3ClientError: (or any subclass) when the upstream call fails.
    """
    svc = service or _get_default_service()
    ticker = ticker.strip().upper()
    logger.info("Use case: get_latest_snapshot ticker='%s'", ticker)
    return svc.get_latest_snapshot(ticker)
