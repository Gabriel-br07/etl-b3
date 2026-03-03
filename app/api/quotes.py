"""Quotes router.

Exposes five endpoints:
- GET /quotes/latest          – latest DB-persisted quote per ticker.
- GET /quotes/{ticker}/history – historical DB-persisted quotes for a ticker.
- GET /quotes/{ticker}/snapshot – live delayed snapshot (legacy, NormalizedQuote).
- GET /quotes/{ticker}          – live latest snapshot (LatestSnapshotQuote).
- GET /quotes/{ticker}/intraday – live full intraday series (IntradaySeriesQuote).

The last two endpoints use the real B3 payload shape (BizSts/Msg/TradgFlr/lstQtn)
and return data sourced from ``b3_public_internal_endpoint``.  Data is always
delayed (not real-time).
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
from app.schemas import DailyQuoteRead
from app.use_cases.quotes.get_daily_fluctuation import get_daily_fluctuation
from app.use_cases.quotes.get_intraday_series import get_intraday_series
from app.use_cases.quotes.get_latest_snapshot import get_latest_snapshot

router = APIRouter(prefix="/quotes", tags=["Quotes"])


@router.get(
    "/latest",
    response_model=dict,
    summary="Latest quotes",
    description="Return the most recent available daily quote per ticker.",
)
def get_latest_quotes(
    tickers: str | None = Query(
        None,
        description="Comma-separated list of tickers, e.g. PETR4,VALE3",
    ),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
) -> dict:
    ticker_list = [t.strip().upper() for t in tickers.split(",")] if tickers else None
    repo = QuoteRepository(db)
    items, total = repo.get_latest_per_ticker(tickers=ticker_list, limit=limit, offset=offset)
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": [DailyQuoteRead.model_validate(q) for q in items],
    }


@router.get(
    "/{ticker}/history",
    response_model=list[DailyQuoteRead],
    summary="Quote history for a ticker",
    description="Return historical daily quotes for a specific ticker.",
)
def get_quote_history(
    ticker: str,
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    limit: int = Query(365, ge=1, le=1000),
    db: Session = Depends(get_db),
) -> list[DailyQuoteRead]:
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
        "Fetch the latest public delayed quote snapshot for *ticker* directly "
        "from the B3 internal market-data endpoint.  Data is **always delayed** "
        "(not real-time) and is sourced from `b3_public_internal_endpoint`.\n\n"
        "The response is cached in-memory per ticker (configurable TTL, default 5 min)."
    ),
)
def get_quote_snapshot(ticker: str) -> dict:
    """Return the latest B3 public delayed snapshot for the given ticker.

    HTTP status codes:
    - 200: success.
    - 404: ticker not found on B3.
    - 503: B3 is temporarily blocking requests (rate-limit / 403 / 429).
    - 502: unexpected upstream error (parse failure, bad JSON, etc.).
    """
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


# ---------------------------------------------------------------------------
# Live latest snapshot – real B3 payload shape (LatestSnapshotQuote)
# ---------------------------------------------------------------------------


@router.get(
    "/{ticker}",
    response_model=dict,
    summary="Latest intraday quote snapshot (B3 public API)",
    description=(
        "Fetch the **latest** intraday quote snapshot for *ticker* directly "
        "from the B3 public market-data endpoint.  The snapshot is derived "
        "from the **last item** in `TradgFlr.scty.lstQtn` of the real "
        "B3 DailyFluctuationHistory response.\n\n"
        "Data is **always delayed** (not real-time) and is sourced from "
        "`b3_public_internal_endpoint`.  The response is cached in-memory "
        "per ticker (configurable TTL, default 5 min).\n\n"
        "For the full minute-level series use `GET /quotes/{ticker}/intraday`."
    ),
)
def get_ticker_latest_snapshot(ticker: str) -> dict:
    """Return the latest B3 intraday snapshot for the given ticker.

    HTTP status codes:
    - 200: success.
    - 404: ticker not found on B3.
    - 503: B3 is temporarily blocking requests.
    - 502: unexpected upstream error.
    """
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


# ---------------------------------------------------------------------------
# Live intraday series – full lstQtn list (IntradaySeriesQuote)
# ---------------------------------------------------------------------------


@router.get(
    "/{ticker}/intraday",
    response_model=dict,
    summary="Full intraday series (B3 public API)",
    description=(
        "Fetch the **full minute-level intraday series** for *ticker* directly "
        "from the B3 public market-data endpoint.  Each point in `points[]` "
        "corresponds to one item in `TradgFlr.scty.lstQtn` of the real B3 "
        "DailyFluctuationHistory response.\n\n"
        "Data is **always delayed** (not real-time).  The response is cached "
        "in-memory per ticker (configurable TTL, default 5 min).\n\n"
        "For just the latest snapshot use `GET /quotes/{ticker}`."
    ),
)
def get_ticker_intraday_series(ticker: str) -> dict:
    """Return the full intraday series for the given ticker.

    HTTP status codes:
    - 200: success (``points`` may be an empty list if market is closed).
    - 404: ticker not found on B3.
    - 503: B3 is temporarily blocking requests.
    - 502: unexpected upstream error.
    """
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

