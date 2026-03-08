#!/usr/bin/env python3
"""Fetch B3 DailyFluctuationHistory intraday series for all tickers in a
normalized instruments CSV and write the results to a single JSONL file.

Each line in the JSONL file is a self-contained JSON object with the ticker
metadata and its full ``lstQtn`` price-history array.

A report CSV is also written at the end summarising every attempted ticker
(HTTP status code and data-point count).

Dual-source filter (default behaviour)
---------------------------------------
By default the script automatically looks for the companion
``negocios_consolidados_*.normalized.csv`` file in the **same folder** as
the instruments CSV and activates the dual-source ticker filter.

The filter reduces useless API requests by keeping only tickers that:
  1. Pass the structural master rules (CASH / EQUITY-CASH / SHARES, active date
     range) from ``cadastro_instrumentos``.
  2. Also appear as active in ``negocios_consolidados`` (trade count > 0 and
     non-null close price).

If the negocios sibling cannot be found the script logs a warning and falls
back to using all tickers from the instruments CSV (old behaviour).

Disable auto-discovery with ``--no-auto-trades`` or supply a custom path with
``--trades``.

Usage
-----
Default run – auto-discovers the negocios sibling, applies strict filter:

    uv run python scripts/run_b3_quote_batch.py

Explicit date:

    uv run python scripts/run_b3_quote_batch.py \\
        --instruments data/raw/b3/boletim_diario/2026-02-26/cadastro_instrumentos_20260226.normalized.csv \\
        --date 2026-02-26

Use the full master list (no negocios filter):

    uv run python scripts/run_b3_quote_batch.py --filter-mode fallback

Skip auto-discovery entirely:

    uv run python scripts/run_b3_quote_batch.py --no-auto-trades

Output
------
Single JSONL file (all tickers, one JSON object per line):
    data/raw/b3/daily_fluctuation_history/{date}/daily_fluctuation_{YYYYMMDDTHHMMSS}.jsonl

Report CSV (one row per attempted ticker):
    data/raw/b3/daily_fluctuation_history/{date}/report_{YYYYMMDDTHHMMSS}.csv
"""

from __future__ import annotations

import argparse
import sys
import re
from datetime import date
from pathlib import Path

# Ensure project root is in sys.path when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.core.logging import configure_logging, get_logger
from app.etl.orchestration.csv_resolver import CSVNotFoundError, resolve_instruments_csv
from app.use_cases.quotes.batch_ingestion import run_batch_quote_ingestion

logger = get_logger(__name__)

# None → auto-discovery via csv_resolver (today → yesterday fallback + retry).
# Pass an explicit path with --instruments to skip auto-discovery.
_DEFAULT_INSTRUMENTS_CSV: str | None = None

# ---------------------------------------------------------------------------
# Auto-discovery helper
# ---------------------------------------------------------------------------

def _find_negocios_sibling(instruments_path: Path) -> Path | None:
    """Look for the negocios_consolidados sibling in the same folder.

    Strategy:
      1. Extract the date suffix (YYYYMMDD) from the instruments filename.
      2. Look for ``negocios_consolidados_{YYYYMMDD}.normalized.csv`` in the
         same directory.
      3. If not found by exact name, fall back to any
         ``negocios_consolidados_*.normalized.csv`` in the same folder (picks
         the most recent by name, alphabetically last).

    Returns the path if found, else None.
    """
    folder = instruments_path.parent

    # Step 1 – try exact date-matched sibling
    m = re.search(r"(\d{8})", instruments_path.stem)
    if m:
        date_tag = m.group(1)
        candidate = folder / f"negocios_consolidados_{date_tag}.normalized.csv"
        if candidate.exists():
            return candidate

    # Step 2 – fallback: any negocios normalized CSV in the same folder
    siblings = sorted(folder.glob("negocios_consolidados_*.normalized.csv"))
    if siblings:
        return siblings[-1]  # alphabetically last == most recent date

    return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch B3 DailyFluctuationHistory intraday series for all tickers "
            "in a normalized instruments CSV.  By default the dual-source ticker "
            "filter is activated automatically using the companion negocios CSV "
            "found in the same folder."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--instruments",
        metavar="PATH",
        default=_DEFAULT_INSTRUMENTS_CSV,
        help=(
            "Path to the normalized instruments CSV "
            "(cadastro_instrumentos_*.normalized.csv). "
            "When omitted, auto-discovery runs: today → yesterday fallback with retry."
        ),
    )
    parser.add_argument(
        "--trades",
        metavar="PATH",
        default=None,
        help=(
            "Explicit path to a normalized negocios_consolidados CSV.  "
            "When omitted the script auto-discovers the sibling file in the "
            "same folder as --instruments."
        ),
    )
    parser.add_argument(
        "--no-auto-trades",
        action="store_true",
        default=False,
        help=(
            "Disable automatic negocios sibling discovery.  "
            "All tickers from --instruments will be queried without filtering."
        ),
    )
    parser.add_argument(
        "--filter-mode",
        metavar="MODE",
        choices=["strict", "fallback"],
        default="strict",
        help=(
            "Ticker filter mode. "
            "'strict' (default) = master ∩ negocios (fewer, higher-quality requests). "
            "'fallback' = full master list regardless of negocios."
        ),
    )
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        default=None,
        type=date.fromisoformat,
        help="Reference date label used for the output folder. Defaults to today.",
    )
    parser.add_argument(
        "--output-dir",
        metavar="DIR",
        default=None,
        help=(
            "Root directory for output files. "
            "Default: data/raw/b3/daily_fluctuation_history (relative to project root)."
        ),
    )
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()

    # ------------------------------------------------------------------
    # Resolve instruments CSV: explicit path > auto-discovery with retry
    # ------------------------------------------------------------------
    if args.instruments:
        instruments_path = Path(args.instruments)
        if not instruments_path.exists():
            logger.error("Instruments CSV not found: '%s'", instruments_path)
            sys.exit(1)
        logger.info("[etl_pipeline] Using explicit instruments CSV: '%s'", instruments_path)
    else:
        logger.info("[etl_pipeline] No --instruments supplied — running auto-discovery…")
        try:
            instruments_path = resolve_instruments_csv()
        except CSVNotFoundError as exc:
            logger.error("[etl_pipeline] %s", exc)
            sys.exit(1)

    # ------------------------------------------------------------------
    # Resolve trades CSV: explicit > auto-discovered > disabled
    # ------------------------------------------------------------------
    trades_path: Path | None = None

    if args.no_auto_trades:
        logger.info("Auto-discovery disabled (--no-auto-trades). No filter applied.")
    elif args.trades:
        trades_path = Path(args.trades)
        if not trades_path.exists():
            logger.error("Trades CSV not found: '%s'", trades_path)
            sys.exit(1)
        logger.info("Using explicitly supplied trades CSV: '%s'", trades_path)
    else:
        # Auto-discover the negocios sibling in the same folder
        trades_path = _find_negocios_sibling(instruments_path)
        if trades_path:
            logger.info(
                "Auto-discovered negocios sibling: '%s' — dual-source filter active.",
                trades_path,
            )
        else:
            logger.warning(
                "No negocios_consolidados sibling found next to '%s'. "
                "Falling back to unfiltered ticker list.",
                instruments_path,
            )

    report_path = run_batch_quote_ingestion(
        instruments_csv=instruments_path,
        trades_csv=trades_path,
        filter_mode=args.filter_mode,
        reference_date=args.date,
        output_dir=args.output_dir,
    )
    print(f"Done. Report written to: {report_path}")


if __name__ == "__main__":
    main()