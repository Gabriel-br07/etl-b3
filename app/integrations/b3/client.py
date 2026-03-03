"""B3 HTTP client for the DailyFluctuationHistory public endpoint.

Responsibilities:
- Manage an ``httpx.Client`` session with appropriate browser-like headers.
- Warm the session by visiting the public B3 quote page so that the server
  sets any required cookies naturally (no hardcoded cookies).
- Fetch daily fluctuation data for a given ticker.
- Retry once after a 403 / 429 by re-warming the session.
- Raise domain-specific exceptions; never swallow errors silently.

This module knows ONLY about HTTP transport.  Parsing and business logic
live elsewhere.
"""

from __future__ import annotations

import logging
from types import TracebackType
from typing import Any

import httpx

from app.core.config import settings
from app.integrations.b3.constants import (
    DAILY_FLUCTUATION_PATH,
    DEFAULT_HEADERS,
)
from app.integrations.b3.exceptions import (
    B3TemporaryBlockError,
    B3TickerNotFoundError,
    B3UnexpectedResponseError,
)

logger = logging.getLogger(__name__)

# HTTP status codes that indicate a temporary block / rate-limit.
_BLOCK_STATUSES: frozenset[int] = frozenset({403, 429})


class B3QuoteClient:
    """Synchronous HTTP client for the B3 public market-data API.

    Usage::

        with B3QuoteClient() as client:
            data = client.get_daily_fluctuation_history("PETR4")

    Or manage the lifecycle manually::

        client = B3QuoteClient()
        client.warm_session()
        data = client.get_daily_fluctuation_history("PETR4")
        client.close()
    """

    def __init__(
        self,
        *,
        base_url: str | None = None,
        warm_url: str | None = None,
        timeout: float | None = None,
        http2: bool | None = None,
    ) -> None:
        self._base_url = base_url or settings.b3_quote_base_url
        self._warm_url = warm_url or settings.b3_quote_warm_session_url
        self._timeout = timeout if timeout is not None else settings.b3_quote_timeout
        self._use_http2 = http2 if http2 is not None else settings.b3_quote_http2

        self._client = httpx.Client(
            headers=DEFAULT_HEADERS,
            timeout=self._timeout,
            follow_redirects=True,
            http2=self._use_http2,
        )
        self._session_warmed: bool = False

    # ------------------------------------------------------------------
    # Context-manager support
    # ------------------------------------------------------------------

    def __enter__(self) -> "B3QuoteClient":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying HTTP client and release resources."""
        self._client.close()

    # ------------------------------------------------------------------
    # Session warm-up
    # ------------------------------------------------------------------

    def warm_session(self) -> None:
        """Visit the public B3 quote page to obtain a valid session cookie.

        B3's infrastructure sometimes requires a browser-like cookie before
        allowing access to the internal API endpoints.  Visiting the public
        page triggers cookie assignment without any Playwright or browser
        automation.

        This method is idempotent; calling it multiple times is safe.
        """
        logger.info("Warming B3 session via %s", self._warm_url)
        try:
            resp = self._client.get(self._warm_url)
            logger.debug(
                "Session warm-up response: status=%d url=%s",
                resp.status_code,
                resp.url,
            )
            self._session_warmed = True
        except httpx.HTTPError as exc:
            # Warm-up failure is non-fatal; log and proceed.
            logger.warning("B3 session warm-up failed: %s", exc)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_daily_fluctuation_history(self, ticker: str) -> dict[str, Any]:
        """Fetch the DailyFluctuationHistory payload for *ticker*.

        Args:
            ticker: B3 ticker symbol (case-insensitive; will be uppercased).

        Returns:
            The JSON-decoded response body as a plain ``dict``.

        Raises:
            B3TickerNotFoundError: HTTP 404.
            B3TemporaryBlockError: HTTP 403 or 429 that persists after retry.
            B3UnexpectedResponseError: Any other non-2xx status code, or a
                response body that cannot be decoded as JSON.
        """
        ticker = ticker.strip().upper()
        url = self._base_url + DAILY_FLUCTUATION_PATH.format(ticker=ticker)

        logger.info("Fetching B3 daily fluctuation for ticker '%s' url=%s", ticker, url)

        response = self._request_with_retry(url=url, ticker=ticker)
        return self._decode_json(response, ticker=ticker)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _request_with_retry(self, *, url: str, ticker: str) -> httpx.Response:
        """Perform the GET request, retrying after a warm-up on 403/429."""
        # Default to 1 retry to preserve existing behavior if the setting is absent.
        max_retries = getattr(settings, "b3_quote_max_retries", 1)

        response = self._get(url)

        retries = 0
        while response.status_code in _BLOCK_STATUSES and retries < max_retries:
            logger.warning(
                "B3 returned %d for ticker '%s' – warming session and retrying "
                "(attempt %d of %d).",
                response.status_code,
                ticker,
                retries + 1,
                max_retries,
            )
            self.warm_session()
            response = self._get(url)
            retries += 1

        if response.status_code in _BLOCK_STATUSES:
            logger.error(
                "B3 still returning %d for ticker '%s' after %d retry attempt(s).",
                response.status_code,
                ticker,
                retries,
            )
            raise B3TemporaryBlockError(status_code=response.status_code, ticker=ticker)

        if response.status_code == 404:
            raise B3TickerNotFoundError(ticker=ticker)

        if not (200 <= response.status_code < 300):
            snippet = response.text[:300]
            raise B3UnexpectedResponseError(
                f"B3 returned unexpected HTTP {response.status_code} for ticker '{ticker}'.",
                status_code=response.status_code,
                raw_body=snippet,
            )

        logger.info(
            "B3 responded with status=%d for ticker '%s'.",
            response.status_code,
            ticker,
        )
        return response

    def _get(self, url: str) -> httpx.Response:
        """Execute a GET request and surface transport errors clearly."""
        try:
            return self._client.get(url)
        except httpx.TimeoutException as exc:
            raise B3UnexpectedResponseError(
                f"Request to B3 timed out: {url}",
            ) from exc
        except httpx.HTTPError as exc:
            raise B3UnexpectedResponseError(
                f"HTTP error while requesting B3: {exc}",
            ) from exc

    def _decode_json(self, response: httpx.Response, *, ticker: str) -> dict[str, Any]:
        """Decode the JSON body, raising a domain error on failure."""
        try:
            return response.json()
        except Exception as exc:
            raise B3UnexpectedResponseError(
                f"B3 response for ticker '{ticker}' is not valid JSON.",
                status_code=response.status_code,
                raw_body=response.text[:300],
            ) from exc
