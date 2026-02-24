#!/usr/bin/env python3
"""Run the daily B3 ETL from the command line.

Usage:
    python scripts/run_etl.py [--date YYYY-MM-DD] [--mode local|remote]
"""

import argparse
import sys
from datetime import date
from pathlib import Path

# Ensure project root is in sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.etl.orchestration.flow import run_daily_b3_etl


def main() -> None:
    parser = argparse.ArgumentParser(description="Run B3 daily ETL")
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        help="Target date (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--mode",
        choices=["local", "remote"],
        default="local",
        help="Source mode: 'local' reads from B3_DATA_DIR, 'remote' downloads from B3.",
    )
    args = parser.parse_args()

    result = run_daily_b3_etl(target_date=args.date, source_mode=args.mode)
    print("ETL completed:", result)


if __name__ == "__main__":
    main()
