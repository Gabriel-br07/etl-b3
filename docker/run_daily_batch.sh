#!/bin/sh
# =============================================================================
# docker/run_daily_batch.sh — daily Playwright scraper runner
# =============================================================================
#
# Runs the two B3 daily scrapers sequentially:
#   1. run_b3_scraper.py          (BoletimDiarioScraper — cadastro de instrumentos)
#   2. run_b3_scraper_negocios.py (NegociosConsolidadosScraper — negócios consolidados)
#
# Exits 0 only if both scrapers succeeded.
#
# Usage:
#   /app/docker/run_daily_batch.sh [--date YYYY-MM-DD]
#
# Debug single scraper:
#   docker compose exec scheduler /app/.venv/bin/python \
#       /app/scripts/run_b3_scraper.py --date 2024-06-14
# =============================================================================
set -eu

PYTHON=/app/.venv/bin/python

# ---------------------------------------------------------------------------
# Parse --date argument
# ---------------------------------------------------------------------------
_DATE_ARG=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --date)
            _DATE_ARG="$2"
            shift 2
            ;;
        *)
            echo "[daily_batch] unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Headless flag: map PLAYWRIGHT_HEADLESS env → --headless / --no-headless
# ---------------------------------------------------------------------------
case "$(echo "${PLAYWRIGHT_HEADLESS:-true}" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')" in
    0|false|f|no|n|off) _HEADLESS="--no-headless" ;;
    *)                   _HEADLESS="--headless"    ;;
esac

TS() { date '+%Y-%m-%dT%H:%M:%S%z'; }

# ---------------------------------------------------------------------------
# run_scraper NAME SCRIPT
# All arguments are properly quoted — no word-splitting risk.
# ---------------------------------------------------------------------------
run_scraper() {
    _name="$1"
    _script="$2"
    _start=$(date +%s)
    echo "[$(TS)] [daily_batch] starting: ${_name}"
    _exit=0

    if [ -n "${_DATE_ARG}" ]; then
        "${PYTHON}" "${_script}" "${_HEADLESS}" --date "${_DATE_ARG}" || _exit=$?
    else
        "${PYTHON}" "${_script}" "${_HEADLESS}" || _exit=$?
    fi

    _elapsed=$(( $(date +%s) - _start ))
    if [ "${_exit}" -eq 0 ]; then
        echo "[$(TS)] [daily_batch] PASS: ${_name}  elapsed=${_elapsed}s"
    else
        echo "[$(TS)] [daily_batch] FAIL: ${_name}  elapsed=${_elapsed}s  exit=${_exit}"
    fi
    return "${_exit}"
}

echo "[$(TS)] [daily_batch] === batch start  date=${_DATE_ARG:-today}  headless=${_HEADLESS} ==="

EXIT1=0; EXIT2=0
run_scraper "b3_boletim_diario"        /app/scripts/run_b3_scraper.py         || EXIT1=$?
run_scraper "b3_negocios_consolidados" /app/scripts/run_b3_scraper_negocios.py || EXIT2=$?

echo "[$(TS)] [daily_batch] === batch end  boletim=${EXIT1}  negocios=${EXIT2} ==="

[ "${EXIT1}" -eq 0 ] && [ "${EXIT2}" -eq 0 ]
