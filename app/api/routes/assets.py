"""Assets router.

Exposes B3 listed instruments (dim_assets) with pagination and search.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db.engine import get_db
from app.repositories.asset_repository import AssetRepository
from app.schemas import AssetCoverageRead, AssetOverviewRead, AssetRead, PaginatedResponse
from app.use_cases.assets.asset_detail import build_asset_coverage, build_asset_overview

router = APIRouter(prefix="/assets", tags=["Assets"])


@router.get(
    "",
    response_model=PaginatedResponse[AssetRead],
    summary="List assets",
    description=(
        "Return a paginated list of B3 listed assets from the database. "
        "Optionally search by ticker or asset name using the `q` parameter (case-insensitive partial match). "
        "Ordered by ticker."
    ),
    responses={
        200: {"description": "Paginated list of assets."},
    },
)
def list_assets(
    q: str | None = Query(
        None,
        description="Search by ticker or asset name (partial match, case-insensitive).",
    ),
    limit: int = Query(50, ge=1, le=500, description="Maximum number of items to return."),
    offset: int = Query(0, ge=0, description="Number of items to skip."),
    db: Session = Depends(get_db),
) -> PaginatedResponse[AssetRead]:
    repo = AssetRepository(db)
    items, total = repo.list_assets(q=q, limit=limit, offset=offset)
    return PaginatedResponse(
        total=total,
        limit=limit,
        offset=offset,
        items=[AssetRead.model_validate(a) for a in items],
    )


@router.get(
    "/{ticker}/coverage",
    response_model=AssetCoverageRead,
    summary="Data coverage for a ticker",
    description=(
        "Shows whether the ticker exists in ``dim_assets`` and the min/max dates available "
        "in daily quotes, daily trades, intraday ``fact_quotes``, and COTAHIST. "
        "``live_delayed_b3`` indicates the app can call B3 delayed endpoints (best-effort)."
    ),
    responses={200: {"description": "Coverage summary."}},
)
def get_asset_coverage(ticker: str, db: Session = Depends(get_db)) -> AssetCoverageRead:
    return build_asset_coverage(db, ticker)


@router.get(
    "/{ticker}/overview",
    response_model=AssetOverviewRead,
    summary="Consolidated asset overview",
    description=(
        "Single payload combining master data, latest DB quotes/trades/intraday, optional delayed B3 snapshot, "
        "historical date ranges, and section flags. If B3 is unreachable, ``live_snapshot`` is null and "
        "``sections.live_b3.has_data`` is false."
    ),
    responses={200: {"description": "Overview payload."}},
)
def get_asset_overview(ticker: str, db: Session = Depends(get_db)) -> AssetOverviewRead:
    return build_asset_overview(db, ticker)


@router.get(
    "/{ticker}",
    response_model=AssetRead,
    summary="Get asset by ticker",
    description=(
        "Return a single asset by its ticker symbol. "
        "Ticker is matched case-insensitively (stored in uppercase)."
    ),
    responses={
        200: {"description": "The asset record."},
        404: {"description": "Asset not found for the given ticker."},
    },
)
def get_asset(ticker: str, db: Session = Depends(get_db)) -> AssetRead:
    repo = AssetRepository(db)
    asset = repo.get_by_ticker(ticker.upper())
    if not asset:
        raise HTTPException(status_code=404, detail=f"Asset '{ticker}' not found.")
    return AssetRead.model_validate(asset)
