"""Serve Prefect deployments for Docker/local unified orchestration."""

from __future__ import annotations

import os
from typing import cast

from prefect import serve
from prefect.deployments.runner import RunnerDeployment

from app.etl.orchestration.prefect.flows.cotahist_flow import cotahist_flow
from app.etl.orchestration.prefect.flows.daily_scraping_flow import (
    daily_scraping_flow,
    default_daily_parameters,
)


def build_daily_cron() -> str:
    hour = int(os.environ.get("DAILY_RUN_HOUR", "20"))
    minute = int(os.environ.get("DAILY_RUN_MINUTE", "0"))
    return f"{minute} {hour} * * *"


def main() -> None:
    # to_deployment is @async_dispatch; static types are RunnerDeployment | Coroutine[..., ...].
    daily_deployment = cast(
        RunnerDeployment,
        daily_scraping_flow.to_deployment(
            name="daily-scraping",
            cron=build_daily_cron(),
            parameters=default_daily_parameters(),
        ),
    )
    cotahist_deployment = cast(
        RunnerDeployment,
        cotahist_flow.to_deployment(
            name="cotahist-on-demand",
            schedule=None,
            parameters={"paths": []},
        ),
    )
    serve(daily_deployment, cotahist_deployment)


if __name__ == "__main__":
    main()
