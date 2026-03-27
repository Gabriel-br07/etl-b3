#!/bin/sh
# =============================================================================
# docker/entrypoint.sh — container bootstrap  (runs as root)
# =============================================================================
#
# Responsibilities:
# 1. Create /app/data tree and /home/scraper/.prefect, fix ownership
# 2. Export HOME / PREFECT_HOME so Prefect never touches /root/.prefect
# 3. Print startup diagnostics (user, home, prefect dirs)
# 4. Exec CMD as scraper (uid 1001) via setpriv
#
# NOTE: Default runtime is Prefect serve (see Dockerfile CMD). Before starting
# the runtime, this entrypoint runs `alembic upgrade head` against the ready DB.
# Compose enforces DB health readiness before this container starts.
# =============================================================================
set -e

# Debug: list /app/docker to verify entrypoint is present at runtime
echo "[entrypoint] ls -lah /app/docker:"; ls -lah /app/docker || true

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
    echo "[entrypoint] fixing ownership for ${VOLUME_ROOT} itself (current ${current_owner:-unknown} != 1001:1001)"
    chown 1001:1001 "${VOLUME_ROOT}"
fi

# Only recursively fix ownership on known subpaths we create/use to avoid
# O(volume_size) work on every container start.
paths_to_fix="
${DATA_DIR}
${DATA_DIR}/b3
${DATA_DIR}/b3/boletim_diario
${DATA_DIR}/b3/daily_fluctuation_history
${VOLUME_ROOT}/screenshots
${VOLUME_ROOT}/screenshots/b3
${VOLUME_ROOT}/traces
${VOLUME_ROOT}/traces/e2e
"

for p in $paths_to_fix; do
    if [ -e "$p" ]; then
        dir_owner="$(stat -c '%u:%g' "$p" 2>/dev/null || echo '')"
        if [ "$dir_owner" != "1001:1001" ]; then
            echo "[entrypoint] fixing ownership for $p (current ${dir_owner:-unknown} != 1001:1001)"
            chown -R 1001:1001 "$p"
        fi
    fi
done
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
# Ensure Python can import the top-level package `app` when processes run
export PYTHONPATH="/app"

# ---------------------------------------------------------------------------
# Startup diagnostics
# ---------------------------------------------------------------------------
echo "[entrypoint] --- startup diagnostics ---"
echo "[entrypoint] running as : $(id)"
echo "[entrypoint] HOME       : ${HOME}"
echo "[entrypoint] PREFECT_HOME: ${PREFECT_HOME}"
echo "[entrypoint] PREFECT_PROFILES_PATH: ${PREFECT_PROFILES_PATH}"
echo "[entrypoint] PYTHONPATH : ${PYTHONPATH}"
echo "[entrypoint] /home/scraper     : $(ls -la /home/scraper 2>&1 | head -5)"
echo "[entrypoint] /home/scraper/.prefect: $(ls -la /home/scraper/.prefect 2>&1 | head -3)"
echo "[entrypoint] /root/.prefect    : $(ls -la /root/.prefect 2>/dev/null || echo '(does not exist — correct)')"
echo "[entrypoint] --- end diagnostics ---"

# ---------------------------------------------------------------------------
# Run migrations before starting the main runtime process.
# Database readiness is enforced by compose `depends_on: condition: service_healthy`.
# ---------------------------------------------------------------------------
echo "[entrypoint] running migrations: alembic upgrade head"
if ! /app/.venv/bin/alembic upgrade head; then
    echo "[entrypoint] ERROR: migration step failed; refusing to start runtime." >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Drop to scraper and exec CMD
# ---------------------------------------------------------------------------
# Ensure a command was passed from Docker (via CMD or override). If none,
# fail early with a clear error (prevents `setpriv: No program specified`).
if [ "$#" -eq 0 ]; then
    echo "[entrypoint] ERROR: no command specified to execute. Provide a CMD in the Dockerfile or pass a command to 'docker run'." >&2
    exit 1
fi

echo "[entrypoint] starting: $*"
# Use --clear-groups to avoid inheriting root groups; exec so signals propagate
exec setpriv --reuid=1001 --regid=1001 --clear-groups -- "$@"
