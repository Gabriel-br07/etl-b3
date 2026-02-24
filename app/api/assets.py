"""Assets router."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db.engine import get_db
from app.repositories.asset_repository import AssetRepository
from app.schemas import AssetRead

router = APIRouter(prefix="/assets", tags=["Assets"])


@router.get(
    "",
    response_model=dict,
    summary="List assets",
    description="Return a paginated list of B3 listed assets.",
)
def list_assets(
    q: str | None = Query(None, description="Search by ticker or name"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
) -> dict:
    repo = AssetRepository(db)
    items, total = repo.list_assets(q=q, limit=limit, offset=offset)
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": [AssetRead.model_validate(a) for a in items],
    }


@router.get(
    "/{ticker}",
    response_model=AssetRead,
    summary="Get asset by ticker",
    description="Return a single asset by its ticker symbol.",
)
def get_asset(ticker: str, db: Session = Depends(get_db)) -> AssetRead:
    repo = AssetRepository(db)
    asset = repo.get_by_ticker(ticker.upper())
    if not asset:
        raise HTTPException(status_code=404, detail=f"Asset '{ticker}' not found.")
    return AssetRead.model_validate(asset)
