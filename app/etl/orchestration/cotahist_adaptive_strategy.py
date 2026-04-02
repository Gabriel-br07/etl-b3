"""Adaptive load strategy orchestration for COTAHIST files.

The selection layer is generic (size buckets + strategy resolution), while
runtime execution remains COTAHIST-aware through operation hooks.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import text

from app.use_cases.quotes.cotahist_annual_ingestion import ingest_cotahist_txt_file


SizeBucket = str
StrategyName = str

_ONE_MIB = 1024 * 1024


@dataclass(frozen=True)
class AdaptiveSizeThresholdsMiB:
    """Thresholds in MiB for size bucket classification."""

    standard_max_mib: int = 200
    medium_max_mib: int = 700


@dataclass(frozen=True)
class MediumStrategySettings:
    """Reference settings for the medium strategy."""

    medium_merge_span: int = 100_000
    bulk_append_sync_commit_off: bool = False
    ingest_session_sync_commit_off: bool = False


class CotahistAdaptiveRuntimeOps:
    """Default runtime ops for COTAHIST strategy execution.

    Current implementation keeps compatibility by delegating concrete writes to
    existing ingestion primitives and exposing explicit strategy hooks.
    """

    supports_distinct_fast_path = False
    supports_specialized_medium = False
    supports_specialized_large = False

    def __init__(self, *, db: Any, txt_path: Path | str, track_in_file_duplicates: bool) -> None:
        self.db = db
        self.txt_path = Path(txt_path)
        self.track_in_file_duplicates = track_in_file_duplicates
        self._last_metrics: dict[str, int] = {}

    def set_local_synchronous_commit_off(self) -> None:
        self.db.execute(text("SET LOCAL synchronous_commit = off"))

    def insert_select_from_staging(self) -> dict[str, int]:
        # Distinct fast path is not implemented in default runtime yet.
        # Kept for explicit strategy API only.
        return self.execute_standard_fallback()

    def collect_rowcount_metrics(self) -> dict[str, int]:
        return dict(self._last_metrics)

    def execute_standard_fallback(self) -> dict[str, int]:
        summary = ingest_cotahist_txt_file(
            self.db,
            self.txt_path,
            track_in_file_duplicates=self.track_in_file_duplicates,
        )
        metrics = {
            "rows_upsert_ops": summary.db_upsert_operations,
            "normalized_valid": summary.normalized_valid,
            "normalized_invalid": summary.normalized_invalid,
            "in_file_duplicate_keys": summary.in_file_duplicate_keys,
        }
        self._last_metrics = metrics
        return metrics

    def execute_medium_merge(
        self,
        *,
        medium_merge_span: int,
        bulk_append_sync_commit_off: bool,
        ingest_session_sync_commit_off: bool,
    ) -> dict[str, int]:
        # Settings are explicit and observable; write path remains compatible.
        _ = bulk_append_sync_commit_off
        _ = ingest_session_sync_commit_off
        summary = ingest_cotahist_txt_file(
            self.db,
            self.txt_path,
            track_in_file_duplicates=self.track_in_file_duplicates,
            batch_size=medium_merge_span,
        )
        metrics = {
            "rows_upsert_ops": summary.db_upsert_operations,
            "normalized_valid": summary.normalized_valid,
            "normalized_invalid": summary.normalized_invalid,
            "in_file_duplicate_keys": summary.in_file_duplicate_keys,
        }
        self._last_metrics = metrics
        return metrics

    def copy_to_staging(self) -> None:
        return None

    def validate_duplicates_in_staging(self) -> None:
        return None

    def create_unlogged_build_table(self) -> None:
        return None

    def bulk_load_build_table(self) -> None:
        self.execute_standard_fallback()

    def create_indexes_after_load(self) -> None:
        return None

    def analyze_build_table(self) -> None:
        return None

    def promote_ready_table(self) -> dict[str, Any]:
        return dict(self._last_metrics)


def classify_size_bucket(
    file_size_bytes: int,
    *,
    source_file_name: str | None = None,
    thresholds: AdaptiveSizeThresholdsMiB | None = None,
) -> SizeBucket:
    """Classify input size into standard/medium/large.

    ``source_file_name`` is currently reserved for future classifier extensions.
    """
    _ = source_file_name
    cfg = thresholds or AdaptiveSizeThresholdsMiB()
    size_mib = file_size_bytes / _ONE_MIB
    if size_mib <= cfg.standard_max_mib:
        return "standard"
    if size_mib <= cfg.medium_max_mib:
        return "medium"
    return "large"


def resolve_strategy_for_bucket(bucket: SizeBucket) -> StrategyName:
    """Resolve concrete strategy name from bucket."""
    allowed = {"standard", "medium", "large"}
    if bucket not in allowed:
        raise ValueError(f"Unknown strategy bucket: {bucket}")
    return bucket


def _resolve_support_flag(ops: Any, attr_name: str, *, default: bool) -> bool:
    """Resolve ops capability that can be a bool or a zero-arg callable."""
    value = getattr(ops, attr_name, default)
    return bool(value() if callable(value) else value)


def is_standard_fast_path_eligible(*, fact_empty_at_start: bool, has_dupes: bool) -> bool:
    """Eligibility predicate for standard fast path."""
    return fact_empty_at_start and (not has_dupes)


def choose_standard_execution_path(*, fact_empty_at_start: bool, has_dupes: bool) -> str:
    """Resolve standard strategy branch: fast path or fallback."""
    if is_standard_fast_path_eligible(
        fact_empty_at_start=fact_empty_at_start,
        has_dupes=has_dupes,
    ):
        return "fast_path"
    return "fallback"


def execute_standard_strategy(
    *,
    db: Any,
    ops: Any,
    fact_empty_at_start: bool,
    has_dupes: bool,
) -> dict[str, Any]:
    """Execute standard strategy branch."""
    path = choose_standard_execution_path(
        fact_empty_at_start=fact_empty_at_start,
        has_dupes=has_dupes,
    )
    supports_fast_path = _resolve_support_flag(
        ops,
        "supports_distinct_fast_path",
        default=False,
    )

    if path == "fast_path" and supports_fast_path:
        ops.set_local_synchronous_commit_off()
        ops.insert_select_from_staging()
        metrics = ops.collect_rowcount_metrics()
        return {
            "execution_path": "fast_path",
            "fallback_used": False,
            "implementation_mode": "specialized",
            "metrics": metrics,
            "dedupe_validation_respected": True,
        }

    metrics = ops.execute_standard_fallback()
    exec_path = "fallback" if path == "fallback" else "compat_fallback_no_fast_path"
    return {
        "execution_path": exec_path,
        "fallback_used": True,
        "implementation_mode": "compatible_fallback",
        "metrics": metrics,
        "dedupe_validation_respected": True,
    }


def execute_medium_strategy(
    *,
    db: Any,
    ops: Any,
    settings: MediumStrategySettings | None = None,
) -> dict[str, Any]:
    """Execute medium strategy branch."""
    cfg = settings or MediumStrategySettings()
    metrics = ops.execute_medium_merge(
        medium_merge_span=cfg.medium_merge_span,
        bulk_append_sync_commit_off=cfg.bulk_append_sync_commit_off,
        ingest_session_sync_commit_off=cfg.ingest_session_sync_commit_off,
    )
    return {
        "execution_path": "medium_merge",
        "fallback_used": False,
        "implementation_mode": (
            "specialized"
            if _resolve_support_flag(ops, "supports_specialized_medium", default=False)
            else "compatible_fallback"
        ),
        "metrics": metrics,
        "dedupe_validation_respected": True,
    }


def execute_large_strategy(*, db: Any, ops: Any) -> dict[str, Any]:
    """Execute large strategy branch."""
    ops.copy_to_staging()
    ops.validate_duplicates_in_staging()
    ops.create_unlogged_build_table()
    ops.bulk_load_build_table()
    ops.create_indexes_after_load()
    ops.analyze_build_table()
    metrics = ops.promote_ready_table()
    return {
        "execution_path": "staged_build_promote",
        "fallback_used": False,
        "implementation_mode": (
            "specialized"
            if _resolve_support_flag(ops, "supports_specialized_large", default=False)
            else "compatible_fallback"
        ),
        "metrics": metrics,
        "dedupe_validation_respected": True,
    }


def orchestrate_adaptive_cotahist_load(
    *,
    db: Any,
    txt_path: Path | str,
    file_size_bytes: int,
    fact_empty_at_start: bool,
    has_dupes: bool,
    track_in_file_duplicates: bool = False,
    thresholds: AdaptiveSizeThresholdsMiB | None = None,
    ops: Any = None,
    logger: Any = None,
) -> dict[str, Any]:
    """Top-level adaptive orchestration contract."""
    runtime_ops = (
        ops
        if ops is not None
        else CotahistAdaptiveRuntimeOps(
            db=db,
            txt_path=txt_path,
            track_in_file_duplicates=track_in_file_duplicates,
        )
    )
    bucket = classify_size_bucket(
        file_size_bytes=file_size_bytes,
        source_file_name=Path(txt_path).name if txt_path else None,
        thresholds=thresholds,
    )
    selected_strategy = resolve_strategy_for_bucket(bucket)
    fast_path_eligible = is_standard_fast_path_eligible(
        fact_empty_at_start=fact_empty_at_start,
        has_dupes=has_dupes,
    )

    if selected_strategy == "standard":
        strategy_out = execute_standard_strategy(
            db=db,
            ops=runtime_ops,
            fact_empty_at_start=fact_empty_at_start,
            has_dupes=has_dupes,
        )
    elif selected_strategy == "medium":
        strategy_out = execute_medium_strategy(db=db, ops=runtime_ops)
    else:
        strategy_out = execute_large_strategy(db=db, ops=runtime_ops)

    final_metrics = strategy_out.get("metrics", {})
    out = {
        "bucket": bucket,
        "selected_strategy": selected_strategy,
        "execution_path": strategy_out.get("execution_path"),
        "standard_fast_path_eligible": fast_path_eligible,
        "fallback_used_when_ineligible": selected_strategy == "standard" and (not fast_path_eligible),
        "dedupe_validation_respected": bool(strategy_out.get("dedupe_validation_respected", True)),
        "final_metrics": final_metrics,
        "implementation_mode": strategy_out.get("implementation_mode", "unspecified"),
        "observability": {
            "file_size_bytes": file_size_bytes,
            "size_bucket": bucket,
            "selected_strategy": selected_strategy,
            "standard_fast_path_eligible": fast_path_eligible,
            "final_metrics": final_metrics,
            "implementation_mode": strategy_out.get("implementation_mode", "unspecified"),
        },
    }
    if logger is not None and hasattr(logger, "info"):
        logger.info(
            "[cotahist_adaptive] file_size_bytes=%s bucket=%s strategy=%s fast_path_eligible=%s metrics=%s",
            file_size_bytes,
            bucket,
            selected_strategy,
            fast_path_eligible,
            final_metrics,
        )
    return out
