#!/bin/sh
# =============================================================================
# docker/entrypoint_cotahist.sh — one-shot heavy-stack COTAHIST worker (runs as root)
# Started via Compose profile `full` or `cotahist` (see compose.yaml).
# =============================================================================
# Responsibilities:
# 1. Ensure required data/home directories exist with scraper ownership
# 2. Export HOME/PREFECT_HOME/PYTHONPATH expected by runtime
# 3. Drop privileges and exec one-shot worker command
#
# IMPORTANT: this entrypoint intentionally does NOT run Alembic migrations.
# =============================================================================
set -e

DATA_DIR="${B3_DATA_DIR:-/app/data/raw}"
DATA_DIR=${DATA_DIR%/}
case "$DATA_DIR" in
    */raw)
        VOLUME_ROOT=$(dirname "$DATA_DIR")
        ;;
    *)
        echo "[entrypoint_cotahist] ERROR: B3_DATA_DIR ('${DATA_DIR}') must point to a '.../raw' directory." >&2
        exit 1
        ;;
esac

SCRAPER_HOME="/home/scraper"
PREFECT_DIR="${SCRAPER_HOME}/.prefect"

mkdir -p \
    "${DATA_DIR}/b3/cotahist_annual" \
    "${PREFECT_DIR}"

current_owner="$(stat -c '%u:%g' "${VOLUME_ROOT}" 2>/dev/null || echo '')"
if [ "${current_owner}" != "1001:1001" ]; then
    chown 1001:1001 "${VOLUME_ROOT}"
fi

paths_to_fix="
${DATA_DIR}
${DATA_DIR}/b3
${DATA_DIR}/b3/cotahist_annual
${SCRAPER_HOME}
${PREFECT_DIR}
"

for p in $paths_to_fix; do
    if [ -e "$p" ]; then
        dir_owner="$(stat -c '%u:%g' "$p" 2>/dev/null || echo '')"
        if [ "$dir_owner" != "1001:1001" ]; then
            chown -R 1001:1001 "$p"
        fi
    fi
done

export HOME="${SCRAPER_HOME}"
export PREFECT_HOME="${PREFECT_DIR}"
export PREFECT_PROFILES_PATH="${PREFECT_DIR}/profiles.toml"
export PYTHONPATH="/app"

if [ "$#" -eq 0 ]; then
    echo "[entrypoint_cotahist] ERROR: no command specified to execute." >&2
    exit 1
fi

echo "[entrypoint_cotahist] starting: $*"
exec setpriv --reuid=1001 --regid=1001 --clear-groups -- "$@"
