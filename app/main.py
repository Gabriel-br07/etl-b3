"""FastAPI application factory."""

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from scalar_fastapi import get_scalar_api_reference

from app.api.assets import router as assets_router
from app.api.etl_router import router as etl_router
from app.api.health import router as health_router
from app.api.quotes import router as quotes_router
from app.core.config import settings
from app.core.logging import configure_logging

configure_logging()

app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    description=(
        "ETL pipeline and data API for B3 public daily market data (Brazil stock exchange). "
        "Provides access to listed instruments and consolidated daily quotes."
    ),
    docs_url=None,   # Disable default Swagger UI
    redoc_url=None,  # Disable default ReDoc UI
    openapi_tags=[
        {"name": "Health", "description": "Application health check."},
        {"name": "Assets", "description": "B3 listed instruments / assets."},
        {"name": "Quotes", "description": "Daily consolidated quotes."},
        {"name": "ETL", "description": "ETL pipeline triggers."},
    ],
)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

app.include_router(health_router)
app.include_router(assets_router)
app.include_router(quotes_router)
app.include_router(etl_router)


# ---------------------------------------------------------------------------
# Scalar documentation (replaces Swagger/ReDoc)
# ---------------------------------------------------------------------------


@app.get("/scalar", include_in_schema=False)
def scalar_docs() -> HTMLResponse:
    """Serve Scalar API reference documentation."""
    return get_scalar_api_reference(
        openapi_url=app.openapi_url,
        title=settings.api_title,
    )
