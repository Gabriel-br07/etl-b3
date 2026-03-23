#!/usr/bin/env python3
"""Run the full B3 ETL from the command line.

This script is an orchestrator that can run one or more pipeline entry
points from `app.etl.orchestration.pipeline`:

- instruments (+ optional trades)  -> run_instruments_and_trades_pipeline
- daily quotes (normalized negocios CSV) -> run_daily_quotes_pipeline
- intraday quotes (JSONL daily_fluctuation_*.jsonl) -> run_intraday_quotes_pipeline
- annual COTAHIST (fixed-width ``COTAHIST_A*.TXT``) -> ``run_cotahist_annual_pipeline``
  (one file) or ``run_cotahist_historical_pipeline`` (multiple files: one ``etl_run``, 2-year windows)

Usage examples
--------------
# Run all (default): auto-discover under B3_DATA_DIR and annual COTAHIST under B3_COTAHIST_ANNUAL_DIR
python scripts/run_etl.py

# Explicit files and date
python scripts/run_etl.py \
    --instruments data/raw/b3/boletim_diario/2026-03-10/cadastro_instrumentos_20260310.normalized.csv \
    --trades     data/raw/b3/boletim_diario/2026-03-10/negocios_consolidados_20260310.normalized.csv \
    --quotes      data/raw/b3/intraday/daily_fluctuation_20260310T100000.jsonl \
    --date 2026-03-10

Notes
-----
- If a step is requested explicitly (path provided or run flag set) and the
  file is missing, the script exits with code 1.
- If a file is not provided, the script attempts auto-discovery under
  B3_DATA_DIR. Missing auto-discovered files only produce warnings and the
  corresponding step is skipped.
- Default / ``--run-all`` also auto-discovers annual COTAHIST under
  ``B3_COTAHIST_ANNUAL_DIR`` (every ``**/COTAHIST_A*.TXT``). If none are
  found, a warning is logged and that step is skipped (unlike
  ``--run-cotahist-annual`` alone, which requires inputs).
- Large annual trees can make a full run slow; Stage 1 fetch stays in
  ``scripts/run_b3_cotahist_annual.py``.
- The script uses lazy imports to avoid loading heavy modules until needed.
"""

from __future__ import annotations

import argparse
import sys
import re
import time
import json
from datetime import date
from pathlib import Path
from typing import Optional, Sequence

# Ensure project root is in sys.path when running as a plain script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.core.logging import configure_logging, get_logger
from app.core.config import B3_COTAHIST_ANNUAL_DIR_DEFAULT, settings
from app.etl.orchestration.cotahist_historical_planning import cotahist_txt_glob_sort_key

# Configure logging early so lazy-imported modules inherit configuration
configure_logging()
logger = get_logger(__name__)


def wait_for_stable_file(path: Path, attempts: int = 3, delay: float = 1.0) -> bool:
    """Return True if file size is stable across consecutive checks.

    This helps avoid picking a file that's still being written by the producer.
    If the file disappears during the checks, return False.

    The function checks the file size `attempts` times, sleeping `delay`
    seconds between checks. It returns True only if the final size equals
    the previous checked size (stability observed). This ensures the last
    comparison is performed rather than returning early on the first stable
    observation which might be transient.
    """
    prev_size = -1
    stable = False

    for _ in range(attempts):
        try:
            size = path.stat().st_size
        except Exception:
            # File missing / inaccessible
            return False
        # Stability is when current size equals the previous observed size.
        stable = (size == prev_size)
        prev_size = size
        # Only sleep between checks, not after the final measurement.
        # This avoids adding an unnecessary delay before returning.
        # If attempts <= 1 the loop executes once and no sleep occurs.
        # Use an indexed loop to detect the last iteration.

    # The previous implementation always slept after each stat call. To
    # preserve the same number of checks but avoid the trailing sleep, we
    # perform the sleeps between iterations. Reimplement the loop with
    # explicit index to control sleeping.

    prev_size = -1
    stable = False
    for i in range(attempts):
        try:
            size = path.stat().st_size
        except Exception:
            return False
        stable = (size == prev_size)
        prev_size = size
        if i < attempts - 1:
            time.sleep(delay)

    # After running the full loop, return whether stability was observed
    # between the last two measurements.
    return stable


def is_success(result: dict) -> bool:
    """Normalize pipeline result status to boolean success.

    Accepts Enum-like objects with .value or plain strings.
    """
    status = result.get("status")
    status_val = getattr(status, "value", status)
    try:
        return str(status_val).lower() == "success"
    except Exception:
        return False


def find_latest_jsonl(data_dir: Path) -> Optional[Path]:
    """Find the most recent daily_fluctuation_*.jsonl under data_dir.

    Scans recursively (glob **/daily_fluctuation_*.jsonl) and returns the
    newest file by filename (lexicographical) or by modification time if
    filenames don't sort sensibly.
    """
    intraday_glob = list(data_dir.glob("**/daily_fluctuation_*.jsonl"))
    if not intraday_glob:
        return None
    # Prefer newest by filename (timestamps are embedded) then fallback to mtime
    try:
        return sorted(intraday_glob, key=lambda p: p.name)[-1]
    except Exception:
        return sorted(intraday_glob, key=lambda p: p.stat().st_mtime)[-1]


def resolve_trades_sibling(instruments_csv: Path) -> Optional[Path]:
    """Given an instruments CSV, try to find a sibling negocios_consolidados CSV.

    Mirrors the previous behavior in this repository: try to extract YYYYMMDD
    from the instruments filename and prefer an exact match, otherwise pick
    the latest sibling file.
    """
    folder = instruments_csv.parent
    m = re.search(r"(\d{8})", instruments_csv.stem)
    if m:
        candidate = folder / f"negocios_consolidados_{m.group(1)}.normalized.csv"
        if candidate.exists() and candidate.is_file():
            return candidate
    siblings = sorted(folder.glob("negocios_consolidados_*.normalized.csv"))
    if siblings:
        # prefer files that are regular files
        for s in reversed(siblings):
            if s.is_file():
                return s
    return None


def _normalized_cotahist_settings_root(settings_root: str) -> str:
    s = str(settings_root).strip()
    return B3_COTAHIST_ANNUAL_DIR_DEFAULT if not s else s


def resolve_cotahist_txt_files(
    *,
    cotahist_txt: Sequence[Path] | None,
    cotahist_year: int | None,
    cotahist_from_year: int | None,
    cotahist_to_year: int | None,
    cotahist_dir: Path | None,
    cotahist_data_dir: Path | None,
    settings_root: str,
) -> list[Path]:
    """Resolve one or more COTAHIST TXT paths from CLI arguments.

    The returned list is de-duplicated then sorted with ``cotahist_txt_glob_sort_key``
    (same key as the ``cotahist_dir`` glob) so order does not depend on which flags
    appeared first on the CLI.
    """
    root = (
        Path(cotahist_data_dir)
        if cotahist_data_dir is not None
        else Path(_normalized_cotahist_settings_root(settings_root))
    )
    out: list[Path] = []

    if cotahist_txt:
        for p in cotahist_txt:
            out.append(p.resolve())

    if cotahist_year is not None:
        out.append((root / str(cotahist_year) / f"COTAHIST_A{cotahist_year}.TXT").resolve())

    if cotahist_from_year is not None and cotahist_to_year is not None:
        lo, hi = cotahist_from_year, cotahist_to_year
        if lo > hi:
            lo, hi = hi, lo
        for y in range(lo, hi + 1):
            out.append((root / str(y) / f"COTAHIST_A{y}.TXT").resolve())

    if cotahist_dir is not None:
        base = cotahist_dir.resolve()
        if base.is_dir():
            found = sorted(base.glob("**/COTAHIST_A*.TXT"), key=cotahist_txt_glob_sort_key)
            out.extend(found)
        else:
            logger.warning(
                "COTAHIST cotahist_dir is not a directory (skipping **/COTAHIST_A*.TXT glob): %s",
                base,
            )

    seen: set[Path] = set()
    unique: list[Path] = []
    for p in out:
        if p not in seen:
            seen.add(p)
            unique.append(p)
    return sorted(unique, key=cotahist_txt_glob_sort_key)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run B3 ETL pipelines (instruments, trades, daily quotes, intraday quotes, annual COTAHIST)"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--instruments",
        type=Path,
        default=None,
        help=(
            "Path to the normalized instruments CSV. "
            "If omitted, auto-discovered from B3_DATA_DIR using today/yesterday fallback."
        ),
    )
    parser.add_argument(
        "--trades",
        type=Path,
        default=None,
        help=(
            "Path to the normalized trades CSV/ZIP. "
            "If omitted, the pipeline looks for a sibling negocios_consolidados_*.normalized.csv "
            "in the same folder as the instruments CSV or under B3_DATA_DIR."
        ),
    )
    parser.add_argument(
        "--daily-quotes",
        type=Path,
        default=None,
        help=(
            "Path to the normalized daily quotes CSV (negocios_consolidados). "
            "If omitted, the script will use the --trades file when available."
        ),
    )
    parser.add_argument(
        "--quotes",
        type=Path,
        default=None,
        help=(
            "Path to the intraday JSONL (daily_fluctuation_*.jsonl). "
            "If omitted, auto-discovered under B3_DATA_DIR/intraday."
        ),
    )
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        help="Target date YYYY-MM-DD (used in the etl_runs audit row). Defaults to today.",
    )

    parser.add_argument(
        "--cotahist-txt",
        type=Path,
        action="append",
        default=None,
        help=(
            "Path to a COTAHIST_A*.TXT file (repeatable). "
            "Used in default/--run-all or with --run-cotahist-annual."
        ),
    )
    parser.add_argument(
        "--cotahist-year",
        type=int,
        default=None,
        help="Load canonical TXT under cotahist root: {root}/{year}/COTAHIST_A{year}.TXT",
    )
    parser.add_argument(
        "--cotahist-from-year",
        type=int,
        default=None,
        help="Inclusive start year (use with --cotahist-to-year)",
    )
    parser.add_argument(
        "--cotahist-to-year",
        type=int,
        default=None,
        help="Inclusive end year (use with --cotahist-from-year)",
    )
    parser.add_argument(
        "--cotahist-dir",
        type=Path,
        default=None,
        help="Directory: load every **/COTAHIST_A*.TXT (sorted by year from filename)",
    )
    parser.add_argument(
        "--cotahist-data-dir",
        type=Path,
        default=None,
        help=(
            "Root directory for annual COTAHIST data (default: B3_COTAHIST_ANNUAL_DIR). "
            "Used for year-based paths (--cotahist-year / --cotahist-from-year / --cotahist-to-year) "
            "and as the default scan root when no explicit --cotahist-* inputs are provided."
        ),
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--run-all", dest="mode_all", action="store_true", help="Run all pipelines (default)")
    group.add_argument("--run-instruments", dest="run_instruments", action="store_true", help="Run instruments pipeline only")
    group.add_argument("--run-trades", dest="run_trades", action="store_true", help="Run trades pipeline only (requires trades file)")
    group.add_argument("--run-daily-quotes", dest="run_daily_quotes", action="store_true", help="Run daily quotes pipeline only (CSV)")
    group.add_argument("--run-quotes", dest="run_quotes", action="store_true", help="Run intraday quotes pipeline only (JSONL)")
    group.add_argument(
        "--run-cotahist-annual",
        dest="run_cotahist_annual",
        action="store_true",
        help="Run annual COTAHIST pipeline only (fixed-width TXT → fact_cotahist_daily)",
    )

    args = parser.parse_args()

    if (args.cotahist_from_year is not None or args.cotahist_to_year is not None) and (
        args.cotahist_from_year is None or args.cotahist_to_year is None
    ):
        logger.error("--cotahist-from-year and --cotahist-to-year must be used together")
        sys.exit(2)
    if args.cotahist_year is not None and (
        args.cotahist_from_year is not None or args.cotahist_to_year is not None
    ):
        logger.error("Use either --cotahist-year or --cotahist-from-year/--cotahist-to-year, not both")
        sys.exit(2)

    target_date: date = args.date or date.today()

    any_single_mode = any(
        [
            args.run_instruments,
            args.run_trades,
            args.run_daily_quotes,
            args.run_quotes,
            args.run_cotahist_annual,
        ]
    )

    # Determine which steps to run. Default / --run-all: all pipelines including annual COTAHIST.
    if args.mode_all or not any_single_mode:
        run_instruments_step = True
        run_trades_step = True
        run_daily_quotes_step = True
        run_intraday_quotes_step = True
        run_cotahist_annual_step = True
    else:
        run_instruments_step = bool(args.run_instruments)
        run_trades_step = bool(args.run_trades)
        run_daily_quotes_step = bool(args.run_daily_quotes)
        run_intraday_quotes_step = bool(args.run_quotes)
        run_cotahist_annual_step = bool(args.run_cotahist_annual)

    cotahist_only_mode = bool(any_single_mode and args.run_cotahist_annual)

    summary: dict = {"date": str(target_date), "pipelines": {}, "success": True}

    requires_b3_data_dir = (
        run_instruments_step or run_trades_step or run_daily_quotes_step or run_intraday_quotes_step
    )
    data_dir = Path(settings.b3_data_dir or "")

    if requires_b3_data_dir:
        if not data_dir.exists() or not data_dir.is_dir():
            logger.error("Invalid b3_data_dir: %s", data_dir, extra={"stage": "config_validation"})
            sys.exit(1)
    elif not data_dir.exists():
        # Cotahist-only (or unused): avoid failing when B3_DATA_DIR is a placeholder.
        logger.info(
            "Skipping b3_data_dir validation (no instruments/trades/daily/intraday steps); "
            "configured path may be absent: %s",
            data_dir,
            extra={"stage": "config_validation"},
        )

    logger.info("Starting ETL orchestrator", extra={"date": str(target_date), "data_dir": str(data_dir)})


    def _validate_explicit_path(arg_path: Optional[Path], name: str) -> None:
        if arg_path is None:
            return
        if not arg_path.exists() or not arg_path.is_file():
            logger.error("Explicit path for %s is invalid: %s", name, arg_path, extra={"stage": "arg_validation", "arg": name, "path": str(arg_path)})
            sys.exit(1)

    _validate_explicit_path(args.instruments, "instruments")
    _validate_explicit_path(args.trades, "trades")
    _validate_explicit_path(args.daily_quotes, "daily_quotes")
    _validate_explicit_path(args.quotes, "quotes")
    if args.cotahist_txt:
        for cpath in args.cotahist_txt:
            _validate_explicit_path(cpath, "cotahist_txt")

    instruments_csv: Optional[Path] = None
    instruments_explicit = args.instruments is not None
    trades_file: Optional[Path] = None
    trades_explicit = args.trades is not None
    daily_quotes_file: Optional[Path] = None
    daily_quotes_explicit = args.daily_quotes is not None
    jsonl_file: Optional[Path] = None
    jsonl_explicit = args.quotes is not None

    if requires_b3_data_dir:
        if args.instruments:
            instruments_csv = args.instruments
            logger.info(
                "Using explicit instruments file",
                extra={"pipeline": "resolve", "source_file": str(instruments_csv)},
            )
        else:
            try:
                from app.etl.orchestration.csv_resolver import CSVNotFoundError, resolve_instruments_csv

                logger.info(
                    "Auto-discovering instruments CSV",
                    extra={"pipeline": "resolve", "data_dir": str(data_dir)},
                )
                try:
                    instruments_csv = resolve_instruments_csv(data_dir=data_dir)
                    if instruments_csv and not wait_for_stable_file(instruments_csv):
                        logger.warning(
                            "Instruments CSV is not stable (still being written): %s — skipping",
                            instruments_csv,
                            extra={"pipeline": "resolve", "source_file": str(instruments_csv)},
                        )
                        instruments_csv = None
                except CSVNotFoundError as exc:
                    logger.warning("Cannot resolve instruments CSV: %s", exc, extra={"pipeline": "resolve"})
                    instruments_csv = None
            except Exception as exc:
                logger.exception("Failed to import CSV resolver: %s", exc, extra={"stage": "resolve_import"})
                instruments_csv = None

        if instruments_csv is not None:
            logger.info(
                "Instruments file resolved",
                extra={"pipeline": "resolve", "source_file": str(instruments_csv)},
            )
        elif run_instruments_step and instruments_explicit:
            logger.error(
                "Requested instruments step but instruments file is missing.",
                extra={"pipeline": "resolve"},
            )
            sys.exit(1)
        else:
            logger.info(
                "No instruments file found — instruments step will be skipped.",
                extra={"pipeline": "resolve"},
            )

        if args.trades:
            trades_file = args.trades
            logger.info(
                "Using explicit trades file",
                extra={"pipeline": "resolve", "source_file": str(trades_file)},
            )
        else:
            if instruments_csv is not None:
                trades_file = resolve_trades_sibling(instruments_csv)
            if trades_file is None:
                candidates = list(data_dir.glob("**/negocios_consolidados_*.normalized.csv"))
                if candidates:
                    trades_file = sorted(candidates)[-1]
            if trades_file is not None and not trades_explicit:
                if not wait_for_stable_file(trades_file):
                    logger.warning(
                        "Trades CSV is not stable (still being written): %s — skipping",
                        trades_file,
                        extra={"pipeline": "resolve", "source_file": str(trades_file)},
                    )
                    trades_file = None

        if trades_file is not None:
            logger.info(
                "Trades file resolved",
                extra={"pipeline": "resolve", "source_file": str(trades_file)},
            )
        else:
            logger.info(
                "No trades file found — trades/daily-quotes steps may be skipped.",
                extra={"pipeline": "resolve"},
            )
            if trades_explicit and (run_trades_step or run_daily_quotes_step):
                logger.error(
                    "Requested trades/daily-quotes step but trades file is missing.",
                    extra={"pipeline": "resolve"},
                )
                sys.exit(1)

        if args.daily_quotes:
            daily_quotes_file = args.daily_quotes
            logger.info(
                "Using explicit daily quotes file",
                extra={"pipeline": "resolve", "source_file": str(daily_quotes_file)},
            )
        else:
            daily_quotes_file = trades_file

        if daily_quotes_file is not None:
            logger.info(
                "Daily quotes file resolved",
                extra={"pipeline": "resolve", "source_file": str(daily_quotes_file)},
            )
        elif run_daily_quotes_step and daily_quotes_explicit:
            logger.error(
                "Requested daily-quotes step but daily quotes file is missing.",
                extra={"pipeline": "resolve"},
            )
            sys.exit(1)

        if args.quotes:
            jsonl_file = args.quotes
            logger.info(
                "Using explicit JSONL file",
                extra={"pipeline": "resolve", "source_file": str(jsonl_file)},
            )
        else:
            jsonl_file = find_latest_jsonl(data_dir)
            if jsonl_file is not None and not wait_for_stable_file(jsonl_file):
                logger.warning(
                    "JSONL file is not stable (still being written): %s — skipping",
                    jsonl_file,
                    extra={"pipeline": "resolve", "source_file": str(jsonl_file)},
                )
                jsonl_file = None

        if jsonl_file is not None:
            logger.info(
                "Intraday JSONL resolved",
                extra={"pipeline": "resolve", "source_file": str(jsonl_file)},
            )
        else:
            logger.info(
                "No intraday JSONL found — intraday quotes step will be skipped.",
                extra={"pipeline": "resolve"},
            )
            if jsonl_explicit and run_intraday_quotes_step:
                logger.error(
                    "Requested intraday-quotes step but JSONL file is missing.",
                    extra={"pipeline": "resolve"},
                )
                sys.exit(1)
    else:
        logger.info(
            "Skipping instruments/trades/daily-quotes/JSONL resolution "
            "(only annual COTAHIST step selected)",
            extra={"pipeline": "resolve"},
        )

    # ------------------------------------------------------------------
    # Resolve annual COTAHIST TXT files
    # ------------------------------------------------------------------
    cotahist_files: list[Path] = []
    if run_cotahist_annual_step:
        annual_root = (
            args.cotahist_data_dir.resolve()
            if args.cotahist_data_dir is not None
            else Path(settings.b3_cotahist_annual_dir).resolve()
        )
        has_explicit_cotahist_inputs = bool(
            args.cotahist_txt
            or args.cotahist_year is not None
            or (
                args.cotahist_from_year is not None
                and args.cotahist_to_year is not None
            )
            or args.cotahist_dir is not None
        )
        cotahist_dir_for_resolve = args.cotahist_dir
        if not has_explicit_cotahist_inputs:
            cotahist_dir_for_resolve = annual_root

        cotahist_files = resolve_cotahist_txt_files(
            cotahist_txt=args.cotahist_txt or (),
            cotahist_year=args.cotahist_year,
            cotahist_from_year=args.cotahist_from_year,
            cotahist_to_year=args.cotahist_to_year,
            cotahist_dir=cotahist_dir_for_resolve,
            cotahist_data_dir=args.cotahist_data_dir,
            settings_root=settings.b3_cotahist_annual_dir,
        )
        if not cotahist_files:
            if cotahist_only_mode:
                logger.error(
                    "COTAHIST annual step requested but no inputs: use --cotahist-txt, "
                    "--cotahist-year, --cotahist-from-year/--cotahist-to-year, and/or --cotahist-dir, "
                    "or place COTAHIST_A*.TXT under %s",
                    annual_root,
                    extra={"pipeline": "cotahist_resolve"},
                )
                sys.exit(2)
            logger.warning(
                "No annual COTAHIST TXT files found under %s (glob **/COTAHIST_A*.TXT). "
                "Skipping COTAHIST load; fetch/extract with scripts/run_b3_cotahist_annual.py if needed.",
                annual_root,
                extra={"pipeline": "cotahist_resolve"},
            )
        if cotahist_files:
            for cp in cotahist_files:
                if not cp.is_file():
                    logger.error("COTAHIST file missing: %s", cp, extra={"pipeline": "cotahist_resolve"})
                    sys.exit(1)
            logger.info(
                "COTAHIST files resolved",
                extra={
                    "pipeline": "cotahist_resolve",
                    "count": len(cotahist_files),
                    "files": [str(x) for x in cotahist_files],
                },
            )

    # ------------------------------------------------------------------
    # Execute pipelines in the required order, using lazy imports
    #
    # Each step uses record_audit=True so ``etl_runs`` captures RUNNING/SUCCESS/FAILED
    # (source file, row counts, errors). The final stdout JSON and logs remain the
    # CLI batch summary on top of that.
    # ------------------------------------------------------------------
    overall_success = True

    # 1) instruments (+ optional trades)
    if run_instruments_step:
        if instruments_csv is None:
            logger.warning("Skipping instruments pipeline: no instruments CSV available.", extra={"pipeline": "instruments_and_trades"})
        else:
            try:
                from app.etl.orchestration.pipeline import run_instruments_and_trades_pipeline

                logger.info("Starting instruments+trades pipeline", extra={"pipeline": "instruments_and_trades", "source_file": str(instruments_csv), "trades_file": str(trades_file) if trades_file else None, "date": str(target_date)})
                start = time.perf_counter()
                result = run_instruments_and_trades_pipeline(
                    instruments_csv, trades_file, target_date, record_audit=True
                )
                duration = time.perf_counter() - start

                summary["pipelines"]["instruments_and_trades"] = result

                if not is_success(result):
                    logger.error("instruments+trades pipeline finished with non-success status", extra={"pipeline": "instruments_and_trades", "status": result.get("status"), "duration": duration})
                    overall_success = False
                    summary["success"] = False
                else:
                    logger.info("instruments+trades pipeline completed successfully", extra={"pipeline": "instruments_and_trades", "duration": duration})
            except Exception as exc:
                logger.exception("instruments+trades pipeline raised an exception: %s", exc, extra={"pipeline": "instruments_and_trades"})
                overall_success = False
                summary["success"] = False

    # 2) trades-only (run only if instruments step did NOT run)
    if run_trades_step and not run_instruments_step:
        # Only run trades pipeline if the pipeline's required inputs are present.
        if trades_file is None:
            logger.warning("Skipping trades pipeline: no trades CSV available.", extra={"pipeline": "trades"})
        else:
            # The existing run_instruments_and_trades_pipeline requires an instruments Path.
            # To avoid passing None into it, skip running if instruments CSV is absent.
            if instruments_csv is None:
                logger.warning("Skipping trades pipeline: instruments CSV is missing and pipeline requires it; provide --instruments or run instruments first.", extra={"pipeline": "trades"})
            else:
                try:
                    from app.etl.orchestration.pipeline import run_instruments_and_trades_pipeline

                    logger.info("Starting trades-only pipeline (via instruments_and_trades entry)", extra={"pipeline": "trades", "trades_file": str(trades_file), "date": str(target_date)})
                    start = time.perf_counter()
                    result = run_instruments_and_trades_pipeline(
                        instruments_csv, trades_file, target_date, record_audit=True
                    )
                    duration = time.perf_counter() - start

                    summary["pipelines"]["trades"] = result

                    if not is_success(result):
                        logger.error("trades pipeline finished with non-success status", extra={"pipeline": "trades", "status": result.get("status"), "duration": duration})
                        overall_success = False
                        summary["success"] = False
                    else:
                        logger.info("trades pipeline completed successfully", extra={"pipeline": "trades", "duration": duration})
                except Exception as exc:
                    logger.exception("trades pipeline raised an exception: %s", exc, extra={"pipeline": "trades"})
                    overall_success = False
                    summary["success"] = False

    # 3) daily quotes (CSV)
    if run_daily_quotes_step:
        if daily_quotes_file is None:
            logger.warning("Skipping daily quotes pipeline: no daily quotes CSV available.", extra={"pipeline": "daily_quotes"})
        else:
            try:
                from app.etl.orchestration.pipeline import run_daily_quotes_pipeline

                logger.info("Starting daily quotes pipeline", extra={"pipeline": "daily_quotes", "source_file": str(daily_quotes_file), "date": str(target_date)})
                start = time.perf_counter()
                result = run_daily_quotes_pipeline(
                    daily_quotes_file, target_date, record_audit=True
                )
                duration = time.perf_counter() - start

                summary["pipelines"]["daily_quotes"] = result

                if not is_success(result):
                    logger.error("daily quotes pipeline finished with non-success status", extra={"pipeline": "daily_quotes", "status": result.get("status"), "duration": duration})
                    overall_success = False
                    summary["success"] = False
                else:
                    logger.info("daily quotes pipeline completed successfully", extra={"pipeline": "daily_quotes", "duration": duration})
            except Exception as exc:
                logger.exception("daily quotes pipeline raised an exception: %s", exc, extra={"pipeline": "daily_quotes"})
                overall_success = False
                summary["success"] = False

    # 4) intraday quotes (JSONL)
    if run_intraday_quotes_step:
        if jsonl_file is None:
            logger.warning("Skipping intraday quotes pipeline: no JSONL available.", extra={"pipeline": "intraday_quotes"})
        else:
            try:
                from app.etl.orchestration.pipeline import run_intraday_quotes_pipeline

                logger.info("Starting intraday quotes pipeline", extra={"pipeline": "intraday_quotes", "source_file": str(jsonl_file)})
                start = time.perf_counter()
                result = run_intraday_quotes_pipeline(jsonl_file, record_audit=True)
                duration = time.perf_counter() - start

                summary["pipelines"]["intraday_quotes"] = result

                if not is_success(result):
                    logger.error("intraday quotes pipeline finished with non-success status", extra={"pipeline": "intraday_quotes", "status": result.get("status"), "duration": duration})
                    overall_success = False
                    summary["success"] = False
                else:
                    logger.info("intraday quotes pipeline completed successfully", extra={"pipeline": "intraday_quotes", "duration": duration})
            except Exception as exc:
                logger.exception("intraday quotes pipeline raised an exception: %s", exc, extra={"pipeline": "intraday_quotes"})
                overall_success = False
                summary["success"] = False

    # 5) annual COTAHIST (fixed-width TXT)
    if run_cotahist_annual_step:
        if not cotahist_files:
            logger.warning("Skipping COTAHIST annual pipeline: no TXT files resolved.", extra={"pipeline": "cotahist_annual"})
        else:
            try:
                from app.etl.orchestration.pipeline import (
                    run_cotahist_annual_pipeline,
                    run_cotahist_historical_pipeline,
                )

                if len(cotahist_files) > 1:
                    results_hist: list[dict] = []
                    logger.info(
                        "Starting COTAHIST historical pipeline (multi-file)",
                        extra={
                            "pipeline": "cotahist_historical",
                            "file_count": len(cotahist_files),
                        },
                    )
                    start = time.perf_counter()
                    result = run_cotahist_historical_pipeline(
                        cotahist_files,
                        record_audit=True,
                        track_in_file_duplicates=False,
                    )
                    duration = time.perf_counter() - start
                    results_hist.append(
                        {
                            "historical_batch": True,
                            "files": [str(p) for p in cotahist_files],
                            "result": result,
                            "duration": duration,
                        }
                    )
                    if not is_success(result):
                        logger.error(
                            "COTAHIST historical pipeline non-success",
                            extra={
                                "pipeline": "cotahist_historical",
                                "status": result.get("status"),
                                "duration": duration,
                            },
                        )
                        overall_success = False
                        summary["success"] = False
                    else:
                        logger.info(
                            "COTAHIST historical pipeline completed",
                            extra={"pipeline": "cotahist_historical", "duration": duration},
                        )
                    summary["pipelines"]["cotahist_historical"] = results_hist
                else:
                    results_annual: list[dict] = []
                    for txt_path in cotahist_files:
                        logger.info(
                            "Starting COTAHIST annual pipeline",
                            extra={"pipeline": "cotahist_annual", "source_file": str(txt_path)},
                        )
                        start = time.perf_counter()
                        result = run_cotahist_annual_pipeline(
                            txt_path,
                            record_audit=True,
                            track_in_file_duplicates=False,
                        )
                        duration = time.perf_counter() - start
                        results_annual.append({"file": str(txt_path), "result": result, "duration": duration})
                        if not is_success(result):
                            logger.error(
                                "COTAHIST annual pipeline non-success",
                                extra={
                                    "pipeline": "cotahist_annual",
                                    "source_file": str(txt_path),
                                    "status": result.get("status"),
                                    "duration": duration,
                                },
                            )
                            overall_success = False
                            summary["success"] = False
                        else:
                            logger.info(
                                "COTAHIST annual pipeline completed",
                                extra={
                                    "pipeline": "cotahist_annual",
                                    "source_file": str(txt_path),
                                    "duration": duration,
                                },
                            )

                    summary["pipelines"]["cotahist_annual"] = results_annual
            except Exception as exc:
                logger.exception(
                    "COTAHIST load step raised an exception: %s",
                    exc,
                    extra={"pipeline": "cotahist_load"},
                )
                overall_success = False
                summary["success"] = False

    # Final summary and exit
    summary["success"] = overall_success

    cotahist_hist = summary["pipelines"].get("cotahist_historical")
    if cotahist_hist:
        hist_result = cotahist_hist[0]["result"]
        if is_success(hist_result):
            logger.info(
                "ETL certification: COTAHIST historical load succeeded",
                extra={
                    "pipeline": "cotahist_historical",
                    "rows_upsert_ops": hist_result.get("rows_upsert_ops"),
                    "files": hist_result.get("files"),
                    "windows": hist_result.get("windows"),
                },
            )
        else:
            logger.error(
                "ETL certification: COTAHIST historical load failed",
                extra={
                    "pipeline": "cotahist_historical",
                    "status": hist_result.get("status"),
                    "error": hist_result.get("error"),
                },
            )

    logger.info("ETL summary", extra={"summary": summary})
    print(json.dumps(summary, default=str))

    if overall_success:
        logger.info("ETL finished: SUCCESS", extra={"summary": summary})
        sys.exit(0)
    else:
        logger.error("ETL finished: FAILURE", extra={"summary": summary})
        sys.exit(1)


if __name__ == "__main__":
    main()
