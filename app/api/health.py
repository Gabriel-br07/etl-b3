"""Health check router."""

from fastapi import APIRouter

from app.core.config import settings
from app.schemas import HealthResponse

router = APIRouter(tags=["Health"])


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check",
    description="Returns the application health status.",
)
def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        version=settings.api_version,
        environment=settings.app_env,
    )
