#!/usr/bin/env bash
# =============================================================================
# docker/run_daily_batch.sh — daily Playwright scraper runner (simplified)
# =============================================================================
# Responsibilities:
#  - Run the two daily Playwright scrapers sequentially
#  - Emit clear logs and return non-zero exit code if any scraper fails
#  - Retry each scraper individually on transient failures
#
# NOTE: ETL invocation (discovery + load) is not in this script. Default
# production orchestration is Prefect (docs/etl_canonical_runtime.md). Legacy
# `scripts/legacy_scheduler.py` may invoke this script; see docs/legacy_scheduler.md.
# =============================================================================
set -euo pipefail
IFS=$'\n\t'

# Use python from the PATH (Dockerfile sets /app/.venv/bin on PATH)
PYTHON=${PYTHON:-python}
# Retry configuration (can be overridden in environment)
SCRAPER_RETRY_ATTEMPTS=${SCRAPER_RETRY_ATTEMPTS:-${RETRY_COUNT:-3}}
SCRAPER_RETRY_DELAY_SECONDS=${SCRAPER_RETRY_DELAY_SECONDS:-${RETRY_DELAY_SECONDS:-15}}

TS() { date '+%Y-%m-%dT%H:%M:%S%z'; }

echo "[$(TS)] [daily_batch] starting scrapers  PYTHON=${PYTHON}  shell=$(ps -p $$ -o comm=)"

# ---------------------------------------------------------------------------
# Parse --date argument
# ---------------------------------------------------------------------------
_DATE_ARG=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --date)
            if [ "$#" -lt 2 ]; then
                echo "Usage: $0 [--date YYYY-MM-DD]" >&2
                exit 2
            fi
            _DATE_ARG="$2"
            shift 2
            ;;
        *)
            echo "[daily_batch] unknown argument: $1" >&2
            exit 2
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

run_scraper_once() {
    _name="$1"
    _script="$2"
    _start=$(date +%s)

    if [ -n "${_DATE_ARG}" ]; then
        "${PYTHON}" "${_script}" "${_HEADLESS}" --date "${_DATE_ARG}"
    else
        "${PYTHON}" "${_script}" "${_HEADLESS}"
    fi

    return $?
}

run_scraper_with_retry() {
    _name="$1"
    _script="$2"

    _attempt=1
    _final_exit=0
    while [ ${_attempt} -le ${SCRAPER_RETRY_ATTEMPTS} ]; do
        echo "[$(TS)] [daily_batch] starting: ${_name} attempt=${_attempt}/${SCRAPER_RETRY_ATTEMPTS}"
        _start=$(date +%s)
        if run_scraper_once "${_name}" "${_script}"; then
            _elapsed=$(( $(date +%s) - _start ))
            echo "[$(TS)] [daily_batch] PASS: ${_name}  attempt=${_attempt}/${SCRAPER_RETRY_ATTEMPTS} elapsed=${_elapsed}s"
            _final_exit=0
            break
        else
            _exit=$?
            _elapsed=$(( $(date +%s) - _start ))
            echo "[$(TS)] [daily_batch] FAIL: ${_name}  attempt=${_attempt}/${SCRAPER_RETRY_ATTEMPTS} elapsed=${_elapsed}s exit=${_exit}"
            _final_exit=${_exit}
            if [ ${_attempt} -lt ${SCRAPER_RETRY_ATTEMPTS} ]; then
                # exponential backoff: base * 2^(attempt-1)
                _backoff=$(( SCRAPER_RETRY_DELAY_SECONDS * (2 ** (_attempt - 1)) ))
                echo "[$(TS)] [daily_batch] scraper=${_name} retrying in ${_backoff}s"
                sleep ${_backoff}
            else
                echo "[$(TS)] [daily_batch] scraper=${_name} exhausted attempts (${_attempt}/${SCRAPER_RETRY_ATTEMPTS})"
            fi
        fi
        _attempt=$(( _attempt + 1 ))
    done

    return ${_final_exit}
}

EXIT1=0; EXIT2=0
run_scraper_with_retry "b3_boletim_diario"        /app/scripts/run_b3_scraper.py         || EXIT1=$?
run_scraper_with_retry "b3_negocios_consolidados" /app/scripts/run_b3_scraper_negocios.py || EXIT2=$?

echo "[$(TS)] [daily_batch] scrapers finished  boletim=${EXIT1}  negocios=${EXIT2}"

# Final exit: prefer non-zero exit from scrapers; 0 means all succeeded
FINAL_EXIT=0
if [ "${EXIT1}" -ne 0 ]; then
    FINAL_EXIT=${EXIT1}
elif [ "${EXIT2}" -ne 0 ]; then
    FINAL_EXIT=${EXIT2}
fi

exit ${FINAL_EXIT}
