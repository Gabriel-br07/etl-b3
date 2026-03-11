import importlib
from unittest.mock import patch

from app.core.config import settings
import app.db.engine as engine_module


def test_engine_applies_pool_recycle():
    """Engine should be created with pool_recycle equal to settings.db_pool_recycle."""
    # Patch sqlalchemy.create_engine so we can inspect how the engine is constructed
    with patch("sqlalchemy.create_engine") as mock_create_engine:
        # Reload the engine module so that its engine is (re)created using the patched create_engine
        importlib.reload(engine_module)

    # Ensure create_engine was called and inspect the arguments used to create the engine
    assert mock_create_engine.call_count >= 1, "create_engine was not called when initializing the engine"
    _, kwargs = mock_create_engine.call_args
    assert kwargs.get("pool_recycle") == settings.db_pool_recycle
