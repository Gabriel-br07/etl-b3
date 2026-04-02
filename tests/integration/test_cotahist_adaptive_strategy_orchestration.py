"""Integration-style collaboration tests for adaptive COTAHIST strategy."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.etl.orchestration.cotahist_adaptive_strategy import (
    CotahistAdaptiveRuntimeOps,
    MediumStrategySettings,
    execute_large_strategy,
    execute_medium_strategy,
    execute_standard_strategy,
    orchestrate_adaptive_cotahist_load,
)
from app.use_cases.quotes.cotahist_annual_ingestion import CotahistIngestSummary


pytestmark = pytest.mark.integration


def test_standard_fast_path_uses_insert_select_single_shot_and_rowcount_metrics() -> None:
    ops = MagicMock()

    execute_standard_strategy(
        db=MagicMock(),
        ops=ops,
        fact_empty_at_start=True,
        has_dupes=False,
    )

    ops.insert_select_from_staging.assert_called_once()
    ops.batch_by_line_seq.assert_not_called()
    ops.create_temp_line_seq_index.assert_not_called()
    ops.upsert_from_staging.assert_not_called()
    ops.insert_with_returning.assert_not_called()
    ops.metrics_via_duplicate_counting_cte.assert_not_called()
    ops.collect_rowcount_metrics.assert_called_once()
    ops.set_local_synchronous_commit_off.assert_called_once()


def test_standard_strategy_fallback_when_fast_path_is_not_eligible() -> None:
    ops = MagicMock()
    execute_standard_strategy(
        db=MagicMock(),
        ops=ops,
        fact_empty_at_start=False,
        has_dupes=True,
    )
    ops.insert_select_from_staging.assert_not_called()
    ops.set_local_synchronous_commit_off.assert_not_called()
    ops.execute_standard_fallback.assert_called_once()


def test_medium_strategy_uses_benchmark_derived_reference_settings() -> None:
    ops = MagicMock()
    execute_medium_strategy(
        db=MagicMock(),
        ops=ops,
        settings=MediumStrategySettings(),
    )

    ops.execute_medium_merge.assert_called_once_with(
        medium_merge_span=100_000,
        bulk_append_sync_commit_off=False,
        ingest_session_sync_commit_off=False,
    )


def test_large_strategy_follows_staged_build_promote_order() -> None:
    calls: list[str] = []

    class Ops:
        def copy_to_staging(self) -> None:
            calls.append("copy_to_staging")

        def validate_duplicates_in_staging(self) -> None:
            calls.append("validate_duplicates_in_staging")

        def create_unlogged_build_table(self) -> None:
            calls.append("create_unlogged_build_table")

        def bulk_load_build_table(self) -> None:
            calls.append("bulk_load_build_table")

        def create_indexes_after_load(self) -> None:
            calls.append("create_indexes_after_load")

        def analyze_build_table(self) -> None:
            calls.append("analyze_build_table")

        def promote_ready_table(self) -> None:
            calls.append("promote_ready_table")

    execute_large_strategy(db=MagicMock(), ops=Ops())

    assert calls == [
        "copy_to_staging",
        "validate_duplicates_in_staging",
        "create_unlogged_build_table",
        "bulk_load_build_table",
        "create_indexes_after_load",
        "analyze_build_table",
        "promote_ready_table",
    ]


def test_orchestrator_safety_and_observability_contract() -> None:
    logger = MagicMock()
    out = orchestrate_adaptive_cotahist_load(
        db=MagicMock(),
        txt_path="arquivo_qualquer_sem_padrao.txt",
        file_size_bytes=120 * 1024 * 1024,
        fact_empty_at_start=True,
        has_dupes=True,
        logger=logger,
        ops=MagicMock(),
    )

    assert out["bucket"] == "standard"
    assert out["selected_strategy"] == "standard"
    assert out["dedupe_validation_respected"] is True
    assert out["standard_fast_path_eligible"] is False
    assert out["fallback_used_when_ineligible"] is True
    assert logger.info.called


def test_default_runtime_reports_compatible_mode_when_fast_path_not_supported(tmp_path) -> None:
    txt_path = tmp_path / "COTAHIST_A2024.TXT"
    txt_path.write_text("dummy", encoding="utf-8")
    db = MagicMock()
    ops = CotahistAdaptiveRuntimeOps(
        db=db,
        txt_path=txt_path,
        track_in_file_duplicates=False,
    )

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.etl.orchestration.cotahist_adaptive_strategy.ingest_cotahist_txt_file",
            lambda *_args, **_kwargs: CotahistIngestSummary(
                db_upsert_operations=5,
                normalized_valid=5,
                normalized_invalid=0,
                in_file_duplicate_keys=0,
            ),
        )
        out = execute_standard_strategy(
            db=db,
            ops=ops,
            fact_empty_at_start=True,
            has_dupes=False,
        )

    assert out["execution_path"] == "compat_fallback_no_fast_path"
    assert out["implementation_mode"] == "compatible_fallback"


def test_medium_strategy_support_flag_callable_is_resolved() -> None:
    class Ops:
        def execute_medium_merge(self, **_kwargs):
            return {"rows_upsert_ops": 1}

        def supports_specialized_medium(self) -> bool:
            return True

    out = execute_medium_strategy(
        db=MagicMock(),
        ops=Ops(),
        settings=MediumStrategySettings(),
    )
    assert out["implementation_mode"] == "specialized"


def test_large_strategy_support_flag_callable_is_resolved() -> None:
    class Ops:
        def copy_to_staging(self) -> None:
            return None

        def validate_duplicates_in_staging(self) -> None:
            return None

        def create_unlogged_build_table(self) -> None:
            return None

        def bulk_load_build_table(self) -> None:
            return None

        def create_indexes_after_load(self) -> None:
            return None

        def analyze_build_table(self) -> None:
            return None

        def promote_ready_table(self):
            return {"rows_upsert_ops": 1}

        def supports_specialized_large(self) -> bool:
            return True

    out = execute_large_strategy(db=MagicMock(), ops=Ops())
    assert out["implementation_mode"] == "specialized"


def test_standard_without_support_flag_defaults_to_safe_fallback() -> None:
    class Ops:
        def __init__(self) -> None:
            self.fallback_calls = 0

        def execute_standard_fallback(self):
            self.fallback_calls += 1
            return {"rows_upsert_ops": 1}

    ops = Ops()
    out = execute_standard_strategy(
        db=MagicMock(),
        ops=ops,
        fact_empty_at_start=True,
        has_dupes=False,
    )

    assert ops.fallback_calls == 1
    assert out["execution_path"] == "compat_fallback_no_fast_path"
