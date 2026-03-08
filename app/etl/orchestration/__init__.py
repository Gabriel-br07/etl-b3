"""app/etl/orchestration package.

Prefect-based flows are imported lazily to avoid triggering Prefect
initialization (which reads ~/.prefect/profiles.toml) when only lightweight
helpers such as csv_resolver are needed.

To use the Prefect flow, import it directly:
    from app.etl.orchestration.flow import run_daily_b3_etl
"""
# Intentionally empty — do NOT add eager imports of flow.py here.
# Any module that imports app.etl.orchestration.csv_resolver would otherwise
# trigger `from prefect import ...` and crash in non-root containers where
# HOME=/root is not writable.
