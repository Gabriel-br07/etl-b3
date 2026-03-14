"""Assets router.

Exposes B3 listed instruments (dim_assets) with pagination and search.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db.engine import get_db
from app.repositories.asset_repository import AssetRepository
from app.schemas import AssetRead, PaginatedResponse

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
