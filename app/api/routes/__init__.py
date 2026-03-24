"""API route modules by domain."""

from app.api.routes.assets import router as assets_router
from app.api.routes.cotahist import router as cotahist_router
from app.api.routes.etl import router as etl_router
from app.api.routes.fact_quotes import router as fact_quotes_router
from app.api.routes.health import router as health_router
from app.api.routes.market import router as market_router
from app.api.routes.quotes import router as quotes_router
from app.api.routes.trades import router as trades_router

__all__ = [
    "assets_router",
    "cotahist_router",
    "etl_router",
    "fact_quotes_router",
    "health_router",
    "market_router",
    "quotes_router",
    "trades_router",
]
