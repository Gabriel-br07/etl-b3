"""app/etl/orchestration package.

Prefect-based flows live under ``app.etl.orchestration.prefect`` and are imported
lazily from there (not from this package root) to avoid triggering Prefect
initialization when only lightweight helpers such as ``csv_resolver`` are needed.

Canonical runtime::

    from app.etl.orchestration.prefect.flows.daily_scraping_flow import daily_registry_flow

See ``docs/etl_canonical_runtime.md``.
"""
# Intentionally empty — do NOT add eager imports of Prefect modules here.
# Any module that imports app.etl.orchestration.csv_resolver would otherwise
# trigger `from prefect import ...` and crash in non-root containers where
# HOME=/root is not writable.
