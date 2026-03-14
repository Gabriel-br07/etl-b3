"""Quotes router.

DB-backed endpoints: GET /quotes/latest, GET /quotes/{ticker}/history.
Live B3 endpoints: GET /quotes/{ticker}, GET /quotes/{ticker}/snapshot, GET /quotes/{ticker}/intraday.
"""

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db.engine import get_db
from app.integrations.b3.exceptions import (
    B3TemporaryBlockError,
    B3TickerNotFoundError,
    B3UnexpectedResponseError,
)
from app.repositories.quote_repository import QuoteRepository
from app.schemas import DailyQuoteRead, PaginatedResponse
from app.use_cases.quotes.get_daily_fluctuation import get_daily_fluctuation
from app.use_cases.quotes.get_intraday_series import get_intraday_series
from app.use_cases.quotes.get_latest_snapshot import get_latest_snapshot

router = APIRouter(prefix="/quotes", tags=["Quotes"])


def _parse_ticker_list(tickers: str | None) -> list[str] | None:
    if not tickers:
        return None
    result = [t for t in (t.strip().upper() for t in tickers.split(",")) if t]
    return result or None


@router.get(
    "/latest",
    response_model=PaginatedResponse[DailyQuoteRead],
    summary="Latest quotes (DB)",
    description=(
        "Return the most recent available daily quote per ticker from the database (fact_daily_quotes). "
        "Each item is the latest trade_date for that ticker. "
        "Optionally filter by a comma-separated list of tickers. "
        "For live delayed data from B3 use GET /quotes/{ticker} or /quotes/{ticker}/snapshot."
    ),
    responses={
        200: {"description": "Paginated list of latest quotes per ticker."},
    },
)
def get_latest_quotes(
    tickers: str | None = Query(
        None,
        description="Comma-separated list of tickers (e.g. PETR4,VALE3). Omit for all tickers.",
    ),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of tickers to return."),
    offset: int = Query(0, ge=0, description="Number of items to skip."),
    db: Session = Depends(get_db),
) -> PaginatedResponse[DailyQuoteRead]:
    ticker_list = _parse_ticker_list(tickers)
    repo = QuoteRepository(db)
    items, total = repo.get_latest_per_ticker(tickers=ticker_list, limit=limit, offset=offset)
    return PaginatedResponse(
        total=total,
        limit=limit,
        offset=offset,
        items=[DailyQuoteRead.model_validate(q) for q in items],
    )


@router.get(
    "/{ticker}/history",
    response_model=list[DailyQuoteRead],
    summary="Quote history for a ticker (DB)",
    description=(
        "Return historical daily quotes for a specific ticker from the database. "
        "Optionally filter by start_date and end_date. "
        "Ordered by trade_date descending. "
        "Returns 404 if no quotes found for the ticker in the given range."
    ),
    responses={
        200: {"description": "List of daily quotes for the ticker."},
        404: {"description": "No quotes found for the ticker."},
    },
)
def get_quote_history(
    ticker: str,
    start_date: date | None = Query(
        None,
        description="Start of date range (YYYY-MM-DD).",
    ),
    end_date: date | None = Query(
        None,
        description="End of date range (YYYY-MM-DD).",
    ),
    limit: int = Query(365, ge=1, le=1000, description="Maximum number of days to return."),
    db: Session = Depends(get_db),
) -> list[DailyQuoteRead]:
    if start_date is not None and end_date is not None and end_date < start_date:
        raise HTTPException(
            status_code=400,
            detail="end_date must be greater than or equal to start_date.",
        )
    repo = QuoteRepository(db)
    items = repo.get_history(
        ticker=ticker.upper(),
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )
    if not items:
        raise HTTPException(status_code=404, detail=f"No quotes found for ticker '{ticker}'.")
    return [DailyQuoteRead.model_validate(q) for q in items]


# ---------------------------------------------------------------------------
# Live snapshot – B3 public internal endpoint (no DB)
# ---------------------------------------------------------------------------


@router.get(
    "/{ticker}/snapshot",
    response_model=dict,
    summary="Live delayed quote snapshot (B3 public API)",
    description=(
        "Fetch the latest public delayed quote snapshot for the ticker directly "
        "from the B3 internal market-data endpoint. Data is always delayed (not real-time). "
        "Response is cached in-memory per ticker (configurable TTL, default 5 min)."
    ),
    responses={
        200: {"description": "Delayed quote snapshot from B3."},
        404: {"description": "Ticker not found on B3."},
        502: {"description": "Unexpected upstream response from B3."},
        503: {"description": "B3 temporarily blocking requests (rate-limit)."},
    },
)
def get_quote_snapshot(ticker: str) -> dict:
    try:
        quote = get_daily_fluctuation(ticker)
    except B3TickerNotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "ticker_not_found",
                "message": str(exc),
                "ticker": getattr(exc, "ticker", ticker),
            },
        ) from exc
    except B3TemporaryBlockError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "upstream_temporary_block",
                "message": str(exc),
                "ticker": exc.ticker,
                "http_status_from_b3": exc.status_code,
            },
        ) from exc
    except B3UnexpectedResponseError as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "upstream_unexpected_response",
                "message": str(exc),
                "b3_status_code": exc.status_code,
            },
        ) from exc

    return {
        "ticker": quote.ticker,
        "trade_date": quote.trade_date.isoformat(),
        "last_price": str(quote.last_price) if quote.last_price is not None else None,
        "min_price": str(quote.min_price) if quote.min_price is not None else None,
        "max_price": str(quote.max_price) if quote.max_price is not None else None,
        "average_price": str(quote.average_price) if quote.average_price is not None else None,
        "oscillation_pct": str(quote.oscillation_pct) if quote.oscillation_pct is not None else None,
        "source": quote.source,
        "delayed": quote.delayed,
        "fetched_at": quote.fetched_at.isoformat(),
    }


@router.get(
    "/{ticker}",
    response_model=dict,
    summary="Latest intraday quote snapshot (B3 public API)",
    description=(
        "Fetch the latest intraday quote snapshot for the ticker from the B3 public market-data endpoint. "
        "Derived from the last item in the B3 DailyFluctuationHistory response. "
        "Data is always delayed. Cached in-memory per ticker. "
        "For the full minute-level series use GET /quotes/{ticker}/intraday."
    ),
    responses={
        200: {"description": "Latest intraday snapshot from B3."},
        404: {"description": "Ticker not found on B3."},
        502: {"description": "Unexpected upstream response."},
        503: {"description": "B3 temporarily blocking requests."},
    },
)
def get_ticker_latest_snapshot(ticker: str) -> dict:
    try:
        snapshot = get_latest_snapshot(ticker)
    except B3TickerNotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "ticker_not_found",
                "message": str(exc),
                "ticker": getattr(exc, "ticker", ticker),
            },
        ) from exc
    except B3TemporaryBlockError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "upstream_temporary_block",
                "message": str(exc),
                "ticker": exc.ticker,
                "http_status_from_b3": exc.status_code,
            },
        ) from exc
    except B3UnexpectedResponseError as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "upstream_unexpected_response",
                "message": str(exc),
                "b3_status_code": exc.status_code,
            },
        ) from exc

    return {
        "ticker": snapshot.ticker,
        "trade_date": snapshot.trade_date.isoformat(),
        "message_datetime": snapshot.message_datetime,
        "latest_time": snapshot.latest_time,
        "latest_close_price": (
            str(snapshot.latest_close_price)
            if snapshot.latest_close_price is not None
            else None
        ),
        "latest_price_fluctuation_pct": (
            str(snapshot.latest_price_fluctuation_pct)
            if snapshot.latest_price_fluctuation_pct is not None
            else None
        ),
        "source": snapshot.source,
        "delayed": snapshot.delayed,
        "fetched_at": snapshot.fetched_at.isoformat(),
    }


@router.get(
    "/{ticker}/intraday",
    response_model=dict,
    summary="Full intraday series (B3 public API)",
    description=(
        "Fetch the full minute-level intraday series for the ticker from the B3 public endpoint. "
        "Each point in points[] corresponds to one item in the B3 DailyFluctuationHistory response. "
        "Data is always delayed. Cached in-memory per ticker. "
        "For just the latest snapshot use GET /quotes/{ticker}."
    ),
    responses={
        200: {"description": "Full intraday series (points may be empty if market closed)."},
        404: {"description": "Ticker not found on B3."},
        502: {"description": "Unexpected upstream response."},
        503: {"description": "B3 temporarily blocking requests."},
    },
)
def get_ticker_intraday_series(ticker: str) -> dict:
    try:
        series = get_intraday_series(ticker)
    except B3TickerNotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "ticker_not_found",
                "message": str(exc),
                "ticker": exc.ticker if hasattr(exc, "ticker") else ticker,
            },
        ) from exc
    except B3TemporaryBlockError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "upstream_temporary_block",
                "message": str(exc),
                "ticker": exc.ticker,
                "http_status_from_b3": exc.status_code,
            },
        ) from exc
    except B3UnexpectedResponseError as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "upstream_unexpected_response",
                "message": str(exc),
                "b3_status_code": exc.status_code,
            },
        ) from exc

    return {
        "ticker": series.ticker,
        "trade_date": series.trade_date.isoformat(),
        "message_datetime": series.message_datetime,
        "point_count": len(series.points),
        "points": [
            {
                "time": p.time,
                "close_price": str(p.close_price) if p.close_price is not None else None,
                "price_fluctuation_pct": (
                    str(p.price_fluctuation_pct)
                    if p.price_fluctuation_pct is not None
                    else None
                ),
            }
            for p in series.points
        ],
        "source": series.source,
        "delayed": series.delayed,
        "fetched_at": series.fetched_at.isoformat(),
    }
