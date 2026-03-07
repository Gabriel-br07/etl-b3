#!/usr/bin/env python3
"""Orchestrator for Diário Oficial daily run.

This wrapper delegates the data-collection work to the existing scripts in
`/app/scripts` so the scheduled job (run_job.sh diario_oficial) runs all
required sub-scripts in sequence.

It forwards the optional --date argument to each child script. If any child
script fails (non-zero exit code) the wrapper exits with a non-zero code and
logs the failure.
"""
from __future__ import annotations

import argparse
import sys
import subprocess
from datetime import datetime
from pathlib import Path

# Ensure project root is on the path when running directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.core.logging import configure_logging, get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Diário Oficial daily orchestrator")
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        default=datetime.today().strftime("%Y-%m-%d"),
        help="Target business date (default: today).",
    )
    return parser.parse_args()


def run_child(script_path: Path, date_arg: str) -> int:
    """Run a child script using the current Python interpreter.

    Streams stdout/stderr to the parent so logs appear in container logs.
    Returns the child's exit code.
    """
    if not script_path.exists():
        logger.error("Child script not found: %s", script_path)
        return 127

    cmd = [sys.executable, str(script_path), "--date", date_arg]
    logger.info("Running child script: %s", " ".join(cmd))

    try:
        # Let the child inherit stdout/stderr so logs go to docker logs
        result = subprocess.run(cmd)
        logger.info("Child finished: %s (exit=%d)", script_path.name, result.returncode)
        return result.returncode
    except Exception:
        logger.exception("Failed to run child script: %s", script_path)
        return 2


def main() -> None:
    configure_logging()
    args = parse_args()
    date_arg = args.date

    logger.info("Diário Oficial orchestrator starting — date=%s", date_arg)

    scripts_dir = Path(__file__).resolve().parent

    # Order: main site scraper, negocios scraper (trades), then quote batch ingestion
    to_run = [
        scripts_dir / "run_b3_scraper.py",
        scripts_dir / "run_b3_scraper_negocios.py",
        scripts_dir / "run_b3_quote_batch.py",
    ]

    failures: list[tuple[str, int]] = []

    for s in to_run:
        code = run_child(s, date_arg)
        if code != 0:
            failures.append((s.name, code))
            # Continue running the rest to collect full picture, do not short-circuit

    if failures:
        for name, code in failures:
            logger.error("Subtask failed: %s (exit=%d)", name, code)
        # Return first failing code if it's a conventional non-zero, else 1
        first_code = failures[0][1] or 1
        sys.exit(first_code)

    logger.info("Diário Oficial orchestrator finished successfully")


if __name__ == "__main__":
    main()
