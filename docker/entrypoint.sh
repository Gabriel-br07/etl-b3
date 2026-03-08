#!/bin/sh
# =============================================================================
# docker/entrypoint.sh — container bootstrap  (runs as root)
# =============================================================================
#
# 1. Creates /app/data tree and /home/scraper/.prefect, fixes ownership
# 2. Exports HOME / PREFECT_HOME so Prefect never touches /root/.prefect
# 3. Prints startup diagnostics (user, home, prefect dirs)
# 4. exec's CMD as scraper (uid 1001) via setpriv
#
# Why HOME must be set explicitly
# --------------------------------
# `useradd -r` (system account) sets HOME=/root by default.  setpriv carries
# the current environment — if HOME is still /root, Prefect will attempt to
# read /root/.prefect/profiles.toml and raise PermissionError.
# Setting HOME here (and in the Dockerfile ENV) ensures every exec'd child
# process sees the correct home directory.
# =============================================================================
set -e

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR="${B3_DATA_DIR:-/app/data/raw}"
# Normalize DATA_DIR (strip trailing slash) and ensure it points to a /raw directory
DATA_DIR=${DATA_DIR%/}
case "$DATA_DIR" in
    */raw)
        VOLUME_ROOT=$(dirname "$DATA_DIR")  # e.g. /app/data
        ;;
    *)
        echo "[entrypoint] ERROR: B3_DATA_DIR ('${DATA_DIR}') must point to a '.../raw' directory." >&2
        exit 1
        ;;
esac
SCRAPER_HOME="/home/scraper"
PREFECT_DIR="${SCRAPER_HOME}/.prefect"

# ---------------------------------------------------------------------------
# Data volume tree
# ---------------------------------------------------------------------------
echo "[entrypoint] ensuring ${VOLUME_ROOT} tree owned by scraper(1001)"
mkdir -p \
    "${DATA_DIR}/b3/boletim_diario" \
    "${DATA_DIR}/b3/daily_fluctuation_history" \
    "${VOLUME_ROOT}/screenshots/b3" \
    "${VOLUME_ROOT}/traces/e2e"
current_owner="$(stat -c '%u:%g' "${VOLUME_ROOT}" 2>/dev/null || echo '')"
if [ "${current_owner}" != "1001:1001" ]; then
    echo "[entrypoint] fixing ownership for ${VOLUME_ROOT} (current ${current_owner:-unknown} != 1001:1001)"
    chown -R 1001:1001 "${VOLUME_ROOT}"
fi

# ---------------------------------------------------------------------------
# Prefect home directory
# ---------------------------------------------------------------------------
echo "[entrypoint] ensuring ${PREFECT_DIR} owned by scraper(1001)"
mkdir -p "${PREFECT_DIR}"
chown -R 1001:1001 "${SCRAPER_HOME}"

# ---------------------------------------------------------------------------
# Environment — must be exported so setpriv inherits them
# ---------------------------------------------------------------------------
export HOME="${SCRAPER_HOME}"
export PREFECT_HOME="${PREFECT_DIR}"
export PREFECT_PROFILES_PATH="${PREFECT_DIR}/profiles.toml"

# ---------------------------------------------------------------------------
# Startup diagnostics
# ---------------------------------------------------------------------------
echo "[entrypoint] --- startup diagnostics ---"
echo "[entrypoint] running as : $(id)"
echo "[entrypoint] HOME       : ${HOME}"
echo "[entrypoint] PREFECT_HOME: ${PREFECT_HOME}"
echo "[entrypoint] PREFECT_PROFILES_PATH: ${PREFECT_PROFILES_PATH}"
echo "[entrypoint] /home/scraper     : $(ls -la /home/scraper 2>&1 | head -5)"
echo "[entrypoint] /home/scraper/.prefect: $(ls -la /home/scraper/.prefect 2>&1 | head -3)"
echo "[entrypoint] /root/.prefect    : $(ls -la /root/.prefect 2>/dev/null || echo '(does not exist — correct)')"
echo "[entrypoint] --- end diagnostics ---"

# ---------------------------------------------------------------------------
# Drop to scraper and exec CMD
# ---------------------------------------------------------------------------
echo "[entrypoint] exec as scraper(1001): $*"
exec setpriv --reuid=1001 --regid=1001 --init-groups -- "$@"
