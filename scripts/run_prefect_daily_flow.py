#!/usr/bin/env python3
"""Run Prefect ETL flows locally (manual / debugging).

Default: full **lightweight** chain (same as bootstrap: cadastro, negócios,
registry loads, intraday batch + intraday load). COTAHIST is **opt-in** via
``--cotahist`` (aligns with Docker ``PREFECT_RUN_COTAHIST`` default off).

Other modes:
  ``--registry-only`` — daily registry flow only (08:00-style path).
  ``--combined`` — legacy single flow with optional intraday and cotahist flags.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.etl.orchestration.prefect.flows.daily_scraping_flow import (
    daily_registry_flow,
    daily_scraping_flow,
    lightweight_bootstrap_flow,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run Prefect scraping / ETL flows locally.")
    p.add_argument("--date", type=date.fromisoformat, default=None, help="Target date YYYY-MM-DD")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument(
        "--registry-only",
        action="store_true",
        help="Only cadastro + negócios + registry DB loads (no intraday).",
    )
    mode.add_argument(
        "--combined",
        action="store_true",
        help="Use the legacy combined daily-scraping-flow (all steps in one flow).",
    )
    p.add_argument("--no-intraday", action="store_true", default=False, help="Combined mode only.")
    p.add_argument(
        "--cotahist",
        action="store_true",
        default=False,
        help="Combined mode only: run annual COTAHIST download/extract in-flow.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    td = args.date
    if args.combined:
        result = daily_scraping_flow(
            target_date=td,
            run_intraday=not args.no_intraday,
            run_cotahist=args.cotahist,
        )
    elif args.registry_only:
        result = daily_registry_flow(target_date=td)
    else:
        result = lightweight_bootstrap_flow(target_date=td)
    print(result)


if __name__ == "__main__":
    main()
