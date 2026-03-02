"""Domain-specific exceptions for the B3 integration.

Callers should catch these instead of raw ``httpx`` exceptions so that
the rest of the application stays decoupled from the HTTP transport.
"""


class B3ClientError(Exception):
    """Base class for all B3 integration errors."""


class B3TemporaryBlockError(B3ClientError):
    """Raised when B3 returns 403 or 429, even after a retry.

    This indicates a rate-limit or access block that is expected to be
    transient.  The caller should surface a 503 or back off and retry later.
    """

    def __init__(self, status_code: int, ticker: str) -> None:
        self.status_code = status_code
        self.ticker = ticker
        super().__init__(
            f"B3 blocked the request for ticker '{ticker}' "
            f"with HTTP {status_code} (temporary block / rate limit)."
        )


class B3TickerNotFoundError(B3ClientError):
    """Raised when the API returns 404 for a given ticker."""

    def __init__(self, ticker: str) -> None:
        self.ticker = ticker
        super().__init__(f"Ticker '{ticker}' was not found on B3 (HTTP 404).")


class B3UnexpectedResponseError(B3ClientError):
    """Raised when the API responds with an unexpected status code or body.

    Carries the raw status code and a snippet of the raw body to make
    debugging straightforward without leaking PII.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        raw_body: str | None = None,
    ) -> None:
        self.status_code = status_code
        self.raw_body = raw_body
        super().__init__(message)
