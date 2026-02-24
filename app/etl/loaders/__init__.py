"""app/etl/loaders package."""

from app.etl.loaders.db_loader import load_assets, load_quotes

__all__ = ["load_assets", "load_quotes"]
