"""Serve Prefect deployments for Docker/local unified orchestration.

Registers **lightweight** recurring flows only:

- ``daily-registry`` — cadastro + negócios + registry DB loads (default 08:00
  America/Sao_Paulo).
- ``intraday-quotes`` — quote batch + intraday load every N minutes; the flow
  no-ops outside the configured B3 quote window.

Heavy annual COTAHIST and other full-stack ingestion stay in Compose profile
``full`` (dedicated worker), not in this serve process.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from typing import cast
from zoneinfo import ZoneInfo

from prefect import serve
from prefect.deployments.runner import RunnerDeployment
from prefect.schedules import Cron, Interval, Schedule

from app.etl.orchestration.prefect.flows.daily_scraping_flow import (
    daily_registry_flow,
    intraday_quotes_flow,
)

log = logging.getLogger(__name__)
DEFAULT_SCHEDULE_TZ = "America/Sao_Paulo"
_DEFAULT_INTRADAY_MINUTES = 30
# Fixed anchor so interval ticks align to wall clock after process restarts (e.g. :00 / :30).
_INTRADAY_INTERVAL_ANCHOR = datetime(2020, 1, 1, 0, 0, tzinfo=ZoneInfo(DEFAULT_SCHEDULE_TZ))


def _daily_registry_cron() -> str:
    return os.environ.get("PREFECT_DAILY_REGISTRY_CRON", "0 8 * * *")


def _intraday_interval_minutes() -> int:
    raw = os.environ.get("PREFECT_INTRADAY_INTERVAL_MINUTES")
    if raw is None or not str(raw).strip():
        return _DEFAULT_INTRADAY_MINUTES
    try:
        n = int(str(raw).strip(), 10)
    except ValueError:
        log.warning(
            "invalid PREFECT_INTRADAY_INTERVAL_MINUTES=%r; using default %s",
            raw,
            _DEFAULT_INTRADAY_MINUTES,
        )
        return _DEFAULT_INTRADAY_MINUTES
    return max(1, n)


def build_daily_registry_schedule() -> Schedule:
    expr = _daily_registry_cron()
    return Cron(expr, timezone=DEFAULT_SCHEDULE_TZ)


def build_intraday_interval_schedule() -> Schedule:
    minutes = _intraday_interval_minutes()
    return Interval(
        timedelta(minutes=minutes),
        anchor_date=_INTRADAY_INTERVAL_ANCHOR,
        timezone=DEFAULT_SCHEDULE_TZ,
    )


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        force=True,
    )
    try:
        daily_schedule = build_daily_registry_schedule()
        intraday_schedule = build_intraday_interval_schedule()
    except ValueError as exc:
        log.error("invalid Prefect schedule configuration: %s", exc)
        sys.exit(1)

    daily_deployment = cast(
        RunnerDeployment,
        daily_registry_flow.to_deployment(
            name="daily-registry",
            schedule=daily_schedule,
            parameters={},
        ),
    )
    deployments: list[RunnerDeployment] = [daily_deployment]

    if os.environ.get("PREFECT_SERVE_INTRADAY", "true").lower() in ("1", "true", "yes"):
        intraday_deployment = cast(
            RunnerDeployment,
            intraday_quotes_flow.to_deployment(
                name="intraday-quotes",
                schedule=intraday_schedule,
                parameters={},
            ),
        )
        deployments.append(intraday_deployment)

    serve(*deployments)


if __name__ == "__main__":
    main()
