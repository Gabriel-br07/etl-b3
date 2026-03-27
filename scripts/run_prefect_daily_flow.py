#!/usr/bin/env python3
"""Run daily scraping Prefect flow locally."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.etl.orchestration.prefect.flows.daily_scraping_flow import daily_scraping_flow


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run the daily Prefect scraping flow.")
    p.add_argument("--date", type=date.fromisoformat, default=None, help="Target date YYYY-MM-DD")
    p.add_argument("--no-intraday", action="store_true", default=False)
    p.add_argument(
        "--no-cotahist",
        action="store_true",
        default=False,
        help="Skip annual COTAHIST download/extract for the target date's year",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    result = daily_scraping_flow(
        target_date=args.date,
        run_intraday=not args.no_intraday,
        run_cotahist=not args.no_cotahist,
    )
    print(result)


if __name__ == "__main__":
    main()
