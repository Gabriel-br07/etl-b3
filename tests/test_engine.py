from app.core.config import settings
from app.db.engine import engine


def test_engine_applies_pool_recycle():
    """Engine should be created with pool_recycle equal to settings.db_pool_recycle."""
    # SQLAlchemy QueuePool stores the recycle value on the pool as _recycle
    pool = getattr(engine, "pool", None)
    assert pool is not None, "Engine has no pool attribute"
    assert hasattr(pool, "_recycle"), "Pool implementation does not expose _recycle"
    assert pool._recycle == settings.db_pool_recycle

