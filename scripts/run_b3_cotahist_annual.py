#!/usr/bin/env python3
"""Stage 1 only: fetch B3 annual COTAHIST ZIPs and prepare fixed-width TXT on disk.

This script never connects to Postgres, never writes ``etl_runs``, and never loads
``fact_cotahist_daily``. To load prepared files into the database, use
``scripts/run_etl.py --run-cotahist-annual`` (see README).

Examples
--------
    uv run python scripts/run_b3_cotahist_annual.py --dry-run
    uv run python scripts/run_b3_cotahist_annual.py --year 2023
    uv run python scripts/run_b3_cotahist_annual.py --from-year 2020 --to-year 2022
    uv run python scripts/run_b3_cotahist_annual.py --download-only --year 2023
    uv run python scripts/run_b3_cotahist_annual.py --extract-only --year 2023
    uv run python scripts/run_b3_cotahist_annual.py --parse-only --year 2023

On-disk layout (default root from ``B3_COTAHIST_ANNUAL_DIR`` / ``--data-dir``):
``{root}/{year}/COTAHIST_A{year}.zip`` and extracted ``COTAHIST_A{year}.TXT``.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.core.config import settings
from app.core.logging import configure_logging, get_logger
from app.integrations.b3.cotahist_downloader import (
    cotahist_annual_zip_url,
    download_cotahist_zip,
    extract_cotahist_txt,
)
from app.use_cases.quotes.cotahist_annual_ingestion import parse_cotahist_txt_stats_only

logger = get_logger(__name__)


def _default_data_root() -> Path:
    return Path(settings.b3_cotahist_annual_dir)


def _resolve_years(args: argparse.Namespace) -> list[int]:
    if args.year is not None:
        return [args.year]
    if args.from_year is not None and args.to_year is not None:
        lo, hi = args.from_year, args.to_year
        if lo > hi:
            lo, hi = hi, lo
        return list(range(lo, hi + 1))
    return list(range(settings.b3_cotahist_year_start, settings.b3_cotahist_year_end + 1))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="B3 annual COTAHIST: download ZIPs, extract TXT, optional parse validation (no DB)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--year", type=int, help="Single calendar year (e.g. 2023)")
    p.add_argument("--from-year", type=int, help="Start year (inclusive, with --to-year)")
    p.add_argument("--to-year", type=int, help="End year (inclusive, with --from-year)")

    p.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help=f"Root for {{year}}/ subdirs (default: {settings.b3_cotahist_annual_dir})",
    )

    mode = p.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Log planned URLs and paths only",
    )
    mode.add_argument(
        "--download-only",
        action="store_true",
        help="Download ZIPs only (no extract, no parse)",
    )
    mode.add_argument(
        "--extract-only",
        action="store_true",
        help="Extract TXT from existing ZIP only (does not download)",
    )
    mode.add_argument(
        "--parse-only",
        action="store_true",
        help="Download if needed, extract, parse TXT for stats in logs (no DB)",
    )

    p.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop on first year error (default: continue other years)",
    )
    p.add_argument(
        "--track-in-file-duplicates",
        action="store_true",
        help=(
            "During parse validation, count duplicate natural keys in the TXT "
            "(extra memory on very large files; default off)"
        ),
    )
    return p.parse_args()


def _log_parse_stats(
    year: int, txt_path: Path, *, track_in_file_duplicates: bool
) -> None:
    summary = parse_cotahist_txt_stats_only(
        txt_path, track_in_file_duplicates=track_in_file_duplicates
    )
    logger.info(
        "[cotahist] parse-validation year=%s file=%s lines=%s valid=%s invalid=%s dup_keys=%s",
        year,
        txt_path.name,
        summary.lines_read,
        summary.normalized_valid,
        summary.normalized_invalid,
        summary.in_file_duplicate_keys,
    )


def main() -> None:
    configure_logging()
    args = parse_args()

    if args.from_year is not None or args.to_year is not None:
        if args.from_year is None or args.to_year is None:
            logger.error("--from-year and --to-year must be used together")
            sys.exit(2)
    if args.year is not None and (args.from_year is not None or args.to_year is not None):
        logger.error("Use either --year or --from-year/--to-year, not both")
        sys.exit(2)

    data_root = args.data_dir or _default_data_root()

    years = _resolve_years(args)
    if not years:
        logger.error("No years to process")
        sys.exit(2)

    failures: list[str] = []

    for year in years:
        url = cotahist_annual_zip_url(year)
        year_dir = data_root / str(year)
        zip_path = year_dir / f"COTAHIST_A{year}.zip"

        if args.dry_run:
            logger.info("[cotahist] dry-run year=%s url=%s zip=%s", year, url, zip_path)
            continue

        try:
            if args.extract_only:
                if not zip_path.is_file():
                    raise FileNotFoundError(
                        f"ZIP missing for extract-only (expected {zip_path}); "
                        "run without --extract-only or download first"
                    )
                txt_path = extract_cotahist_txt(zip_path, year_dir)
                logger.info("[cotahist] extract-only done year=%s txt=%s", year, txt_path)
                continue

            if args.download_only:
                download_cotahist_zip(year, zip_path)
                logger.info("[cotahist] download-only saved year=%s path=%s", year, zip_path)
                continue

            if args.parse_only:
                if not zip_path.is_file():
                    download_cotahist_zip(year, zip_path)
                txt_path = extract_cotahist_txt(zip_path, year_dir)
                _log_parse_stats(
                    year, txt_path, track_in_file_duplicates=args.track_in_file_duplicates
                )
                continue

            # Default: download + extract + parse validation (no DB)
            if settings.b3_cotahist_delay_between_years > 0 and year != years[0]:
                time.sleep(settings.b3_cotahist_delay_between_years)

            download_cotahist_zip(year, zip_path)
            txt_path = extract_cotahist_txt(zip_path, year_dir)
            _log_parse_stats(
                year, txt_path, track_in_file_duplicates=args.track_in_file_duplicates
            )
            logger.info("[cotahist] fetch+validate done year=%s txt=%s", year, txt_path)

        except Exception as exc:
            msg = f"year={year}: {exc}"
            failures.append(msg)
            logger.exception("[cotahist] failed %s", msg)
            if args.fail_fast:
                sys.exit(1)

    if args.dry_run:
        print(f"Dry-run complete for {len(years)} year(s).")
        return

    if failures:
        logger.error("[cotahist] completed with %d failure(s): %s", len(failures), failures)
        sys.exit(1)

    print("Done.")


if __name__ == "__main__":
    main()
