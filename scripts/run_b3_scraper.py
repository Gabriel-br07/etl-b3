#!/usr/bin/env python3
"""CLI entry-point for the B3 Boletim Diário Playwright scraper.

Usage examples
--------------

Normal run (reads PLAYWRIGHT_HEADLESS from .env, defaults to headed):

    python scripts/run_b3_scraper.py

Run for a specific date:

    python scripts/run_b3_scraper.py --date 2024-06-14

Explicit headed (visual) debug run with slow interactions:

    python scripts/run_b3_scraper.py --no-headless --slow-mo 500 --screenshots

Full debug with trace recording:

    python scripts/run_b3_scraper.py --no-headless --slow-mo 800 --screenshots --traces

Playwright Inspector (step through actions manually):

    $env:PWDEBUG="1"; python scripts/run_b3_scraper.py --no-headless
    # Linux/macOS: PWDEBUG=1 python scripts/run_b3_scraper.py --no-headless

View a saved trace:

    playwright show-trace data/traces/b3/trace_2024-06-14_success.zip
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

# Ensure project root is on the path when running directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.core.config import settings
from app.core.logging import configure_logging, get_logger
from app.scraping.b3.scraper import BoletimDiarioScraper

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download B3 Boletim Diário CSV files using a Playwright browser.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        default=date.today().isoformat(),
        help="Target business date (default: today).",
    )
    headless_group = parser.add_mutually_exclusive_group()
    headless_group.add_argument(
        "--headless",
        dest="headless",
        action="store_true",
        default=None,
        help="Run browser in headless mode (overrides PLAYWRIGHT_HEADLESS).",
    )
    headless_group.add_argument(
        "--no-headless",
        dest="headless",
        action="store_false",
        help="Run browser in headed (visible) mode.",
    )
    parser.add_argument(
        "--slow-mo",
        type=int,
        default=None,
        metavar="MS",
        help="Slow-motion delay in ms between Playwright actions (overrides PLAYWRIGHT_SLOW_MO).",
    )
    parser.add_argument(
        "--screenshots",
        action="store_true",
        default=False,
        help="Capture a screenshot after each major step (saved to B3_SCREENSHOTS_DIR).",
    )
    parser.add_argument(
        "--traces",
        action="store_true",
        default=False,
        help="Record a Playwright trace (saved to B3_TRACE_DIR).",
    )
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()

    try:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    except ValueError:
        logger.error("Invalid date format: %r  (expected YYYY-MM-DD)", args.date)
        sys.exit(1)

    # Resolve headless: CLI flag → settings default
    headless = args.headless if args.headless is not None else settings.playwright_headless

    logger.info(
        "B3 scraper starting — date=%s  headless=%s  slow_mo=%s  "
        "screenshots=%s  traces=%s",
        target_date,
        headless,
        args.slow_mo,
        args.screenshots,
        args.traces,
    )

    scraper = BoletimDiarioScraper(
        headless=headless,
        slow_mo=args.slow_mo,
        capture_screenshots=args.screenshots,
        capture_traces=args.traces,
    )

    results = scraper.scrape(target_date)

    print("\n✅  Download complete:")
    for r in results:
        print(f"   file       : {r.file_path}")
        print(f"   filename   : {r.suggested_filename}")
        print(f"   source_url : {r.source_url}")
        print(f"   target_date: {r.target_date}")
        print(f"   saved_at   : {r.downloaded_at.isoformat()}")
        print(f"   conversion_succeeded: {r.conversion_succeeded}")
        if r.conversion_error:
            print(f"   conversion_error: {r.conversion_error}")

    logger.info("B3 scraper finished — %d file(s) downloaded.", len(results))


if __name__ == "__main__":
    main()
