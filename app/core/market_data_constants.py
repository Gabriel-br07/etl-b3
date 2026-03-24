"""Constants for market-data APIs (candles, indicators).

Contract decisions:
- COTAHIST API field ``close_price`` maps from DB ``fact_cotahist_daily.last_price``.
- ETL list uses ``run_id`` (= ``etl_runs.id``); no ``created_at`` column — use ``started_at``.
- ``1d`` candles use ``fact_daily_quotes``; sub-daily intervals use ``fact_quotes`` only.
- Market overview: rankings from ``fact_daily_quotes`` (variation) and ``fact_daily_trades`` (volume/count).
"""

from typing import Final

# Accepted GET /quotes/{ticker}/candles and /fact-quotes/.../candles
CANDLE_INTERVALS: Final[frozenset[str]] = frozenset({"1d", "5m", "15m", "1h"})

# Accepted GET /quotes/{ticker}/indicators
INDICATOR_NAMES: Final[frozenset[str]] = frozenset({"SMA", "EMA", "RSI"})

# Sensible bounds for indicator period (inclusive)
INDICATOR_PERIOD_MIN: Final[int] = 2
INDICATOR_PERIOD_MAX: Final[int] = 200
