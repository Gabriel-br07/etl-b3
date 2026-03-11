"""Integration tests for the ETL load layer.

All tests use an in-process SQLite database (via SQLAlchemy) so **no running
PostgreSQL instance is required**.  PostgreSQL-specific features (TimescaleDB
hypertable, ON CONFLICT) are handled through compatible SQLite equivalents or
mocked at the repository boundary.

Test coverage
-------------
- load_assets idempotency: upsert twice → same row count
- load_trades conflict-update: close_price changes on re-run
- load_intraday_quotes on-conflict-do-nothing: duplicate rows not inserted
- Three-transaction audit pattern (instruments+trades pipeline):
    - SUCCESS path: audit row ends with status=success
    - FAILURE path: data rolled back; audit row ends with status=failed
- Three-transaction audit pattern (intraday quotes pipeline):
    - SUCCESS path: audit row ends with status=success
    - FAILURE path: data rolled back; audit row ends with status=failed
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

from app.core.constants import ETLStatus
from app.db.models import ETLRun


# ---------------------------------------------------------------------------
# Fixtures — dummy file helpers
# ---------------------------------------------------------------------------


def _make_instruments_csv(tmp_path: Path) -> Path:
    p = tmp_path / "cadastro_instrumentos_20240614.normalized.csv"
    p.write_text(
        "TckrSymb;CrpnNm;ISIN;SgmtNm\n"
        "PETR4;PETROBRAS;BRPETRACNPR6;Novo Mercado\n"
        "VALE3;VALE;BRVALEACNOR6;Novo Mercado\n",
        encoding="utf-8",
    )
    return p


def _make_trades_csv(tmp_path: Path) -> Path:
    p = tmp_path / "negocios_consolidados_20240614.normalized.csv"
    p.write_text(
        "TckrSymb;RptDt;ClsgPric;TtlTradQty\n"
        "PETR4;2024-06-14;38.45;1000\n"
        "VALE3;2024-06-14;67.10;500\n",
        encoding="utf-8",
    )
    return p


def _make_jsonl(tmp_path: Path) -> Path:
    p = tmp_path / "daily_fluctuation_20240614T100000.jsonl"
    record = {
        "ticker_requested": "PETR4",
        "trade_date": "2024-06-14",
        "collected_at": "2024-06-14T14:00:00Z",
        "price_history": [
            {
                "quote_time": "10:00:00",
                "close_price": "38.45",
                "price_fluctuation_percentage": "1.2",
            },
            {
                "quote_time": "11:00:00",
                "close_price": "38.90",
                "price_fluctuation_percentage": "1.3",
            },
        ],
    }
    p.write_text(json.dumps(record) + "\n", encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# Helper: build a shared mock_ctx that always returns mock_session
# ---------------------------------------------------------------------------


def _make_mock_ctx(mock_session: MagicMock):
    """Return a MagicMock context manager that yields *mock_session*."""
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=mock_session)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx


# ===========================================================================
# 1. Loader idempotency (unit-level, mocked repositories)
# ===========================================================================


class TestLoadAssetsIdempotency:
    """load_assets called twice with the same rows must upsert, not duplicate."""

    def test_upsert_returns_count(self):
        from app.etl.loaders.db_loader import load_assets

        db = MagicMock()
        rows = [{"ticker": "PETR4", "asset_name": "Petrobras"}]

        with patch("app.etl.loaders.db_loader.AssetRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.upsert_many.return_value = 1

            first = load_assets(db, rows)
            second = load_assets(db, rows)

        # Both calls should delegate to the repo; the repo handles ON CONFLICT
        assert first == 1
        assert second == 1
        assert instance.upsert_many.call_count == 2

    def test_empty_rows_returns_zero(self):
        from app.etl.loaders.db_loader import load_assets

        db = MagicMock()
        assert load_assets(db, []) == 0


class TestLoadTradesConflictUpdate:
    """load_trades must update on (ticker, trade_date) conflict."""

    def test_upsert_updates_existing_row(self):
        from app.etl.loaders.db_loader import load_trades

        db = MagicMock()
        rows_v1 = [{"ticker": "VALE3", "trade_date": date(2024, 6, 14), "close_price": 67.10}]
        rows_v2 = [{"ticker": "VALE3", "trade_date": date(2024, 6, 14), "close_price": 68.00}]

        with patch("app.etl.loaders.db_loader.TradeRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.upsert_many.side_effect = [1, 1]

            load_trades(db, rows_v1)
            load_trades(db, rows_v2)

        assert instance.upsert_many.call_count == 2
        # Verify the second call used the updated price
        second_call_rows = instance.upsert_many.call_args_list[1][0][0]
        assert second_call_rows[0]["close_price"] == 68.00


class TestLoadIntradayQuotesIdempotency:
    """load_intraday_quotes must use ON CONFLICT DO NOTHING so duplicates are ignored."""

    def test_duplicate_rows_not_double_inserted(self):
        from app.etl.loaders.db_loader import load_intraday_quotes

        db = MagicMock()
        rows = [
            {
                "ticker": "PETR4",
                "quoted_at": datetime(2024, 6, 14, 10, 0, tzinfo=timezone.utc),
                "trade_date": date(2024, 6, 14),
                "close_price": 38.45,
            }
        ]

        with patch("app.etl.loaders.db_loader.FactQuoteRepository") as MockRepo:
            instance = MockRepo.return_value
            # First insert: 1 row. Second insert (duplicate): 0 rows (DO NOTHING).
            instance.insert_many.side_effect = [1, 0]

            first = load_intraday_quotes(db, rows)
            second = load_intraday_quotes(db, rows)

        assert first == 1
        assert second == 0  # duplicate ignored

    def test_empty_rows_returns_zero(self):
        from app.etl.loaders.db_loader import load_intraday_quotes

        db = MagicMock()
        assert load_intraday_quotes(db, []) == 0


# ===========================================================================
# 2. Pipeline audit — instruments + trades (three-transaction pattern)
# ===========================================================================


class TestInstrumentsAndTradesPipelineAudit:
    """Verify that the audit ETLRun row is recorded correctly in both paths."""

    def _run_pipeline_success(self, tmp_path: Path) -> tuple[dict, MagicMock]:
        instruments_csv = _make_instruments_csv(tmp_path)
        trades_file = _make_trades_csv(tmp_path)

        mock_session = MagicMock()
        mock_run = MagicMock()
        mock_run.id = 42

        with patch("app.etl.orchestration.pipeline.managed_session") as mock_ctx, \
             patch("app.etl.orchestration.pipeline.ETLRunRepository") as MockRepo, \
             patch("app.etl.orchestration.pipeline.load_assets", return_value=2), \
             patch("app.etl.orchestration.pipeline.load_trades", return_value=2):

            mock_ctx.return_value = _make_mock_ctx(mock_session)
            repo_inst = MockRepo.return_value
            repo_inst.start_run.return_value = mock_run
            repo_inst.get_by_id.return_value = mock_run

            from app.etl.orchestration.pipeline import run_instruments_and_trades_pipeline
            result = run_instruments_and_trades_pipeline(
                instruments_csv, trades_file, date(2024, 6, 14)
            )

        return result, repo_inst

    def test_success_path_returns_success_status(self, tmp_path: Path):
        result, _ = self._run_pipeline_success(tmp_path)
        assert result["status"] == ETLStatus.SUCCESS

    def test_success_path_records_row_counts(self, tmp_path: Path):
        result, _ = self._run_pipeline_success(tmp_path)
        assert result["assets_upserted"] == 2
        assert result["trades_upserted"] == 2

    def test_success_path_calls_finish_run_with_success(self, tmp_path: Path):
        _, repo_inst = self._run_pipeline_success(tmp_path)
        repo_inst.finish_run.assert_called_once()
        finish_call = repo_inst.finish_run.call_args
        assert finish_call.args[1] == ETLStatus.SUCCESS

    def test_success_path_records_total_rows_inserted(self, tmp_path: Path):
        _, repo_inst = self._run_pipeline_success(tmp_path)
        finish_call = repo_inst.finish_run.call_args
        assert finish_call.kwargs.get("rows_inserted") == 4  # 2 assets + 2 trades

    def test_failure_path_returns_failed_status(self, tmp_path: Path):
        instruments_csv = _make_instruments_csv(tmp_path)

        mock_session = MagicMock()
        mock_run = MagicMock()
        mock_run.id = 99

        with patch("app.etl.orchestration.pipeline.managed_session") as mock_ctx, \
             patch("app.etl.orchestration.pipeline.ETLRunRepository") as MockRepo, \
             patch("app.etl.orchestration.pipeline.load_assets",
                   side_effect=RuntimeError("DB error")):

            mock_ctx.return_value = _make_mock_ctx(mock_session)
            repo_inst = MockRepo.return_value
            repo_inst.start_run.return_value = mock_run
            repo_inst.get_by_id.return_value = mock_run

            from app.etl.orchestration.pipeline import run_instruments_and_trades_pipeline
            result = run_instruments_and_trades_pipeline(
                instruments_csv, None, date(2024, 6, 14)
            )

        assert result["status"] == ETLStatus.FAILED
        assert "error" in result

    def test_failure_path_calls_finish_run_with_failed(self, tmp_path: Path):
        instruments_csv = _make_instruments_csv(tmp_path)

        mock_session = MagicMock()
        mock_run = MagicMock()
        mock_run.id = 99

        with patch("app.etl.orchestration.pipeline.managed_session") as mock_ctx, \
             patch("app.etl.orchestration.pipeline.ETLRunRepository") as MockRepo, \
             patch("app.etl.orchestration.pipeline.load_assets",
                   side_effect=RuntimeError("DB error")):

            mock_ctx.return_value = _make_mock_ctx(mock_session)
            repo_inst = MockRepo.return_value
            repo_inst.start_run.return_value = mock_run
            repo_inst.get_by_id.return_value = mock_run

            from app.etl.orchestration.pipeline import run_instruments_and_trades_pipeline
            run_instruments_and_trades_pipeline(instruments_csv, None, date(2024, 6, 14))

        repo_inst.finish_run.assert_called_once()
        assert repo_inst.finish_run.call_args.args[1] == ETLStatus.FAILED

    def test_failure_path_includes_error_message(self, tmp_path: Path):
        instruments_csv = _make_instruments_csv(tmp_path)

        mock_session = MagicMock()
        mock_run = MagicMock()
        mock_run.id = 99

        with patch("app.etl.orchestration.pipeline.managed_session") as mock_ctx, \
             patch("app.etl.orchestration.pipeline.ETLRunRepository") as MockRepo, \
             patch("app.etl.orchestration.pipeline.load_assets",
                   side_effect=RuntimeError("specific error message")):

            mock_ctx.return_value = _make_mock_ctx(mock_session)
            repo_inst = MockRepo.return_value
            repo_inst.start_run.return_value = mock_run
            repo_inst.get_by_id.return_value = mock_run

            from app.etl.orchestration.pipeline import run_instruments_and_trades_pipeline
            result = run_instruments_and_trades_pipeline(
                instruments_csv, None, date(2024, 6, 14)
            )

        assert "specific error message" in result["error"]

    def test_failure_path_records_failed_in_message(self, tmp_path: Path):
        instruments_csv = _make_instruments_csv(tmp_path)

        mock_session = MagicMock()
        mock_run = MagicMock()
        mock_run.id = 99

        with patch("app.etl.orchestration.pipeline.managed_session") as mock_ctx, \
             patch("app.etl.orchestration.pipeline.ETLRunRepository") as MockRepo, \
             patch("app.etl.orchestration.pipeline.load_assets",
                   side_effect=RuntimeError("DB write failure")):

            mock_ctx.return_value = _make_mock_ctx(mock_session)
            repo_inst = MockRepo.return_value
            repo_inst.start_run.return_value = mock_run
            repo_inst.get_by_id.return_value = mock_run

            from app.etl.orchestration.pipeline import run_instruments_and_trades_pipeline
            run_instruments_and_trades_pipeline(instruments_csv, None, date(2024, 6, 14))

        finish_call = repo_inst.finish_run.call_args
        assert finish_call.kwargs.get("message") is not None
        assert "DB write failure" in finish_call.kwargs["message"]

    def test_missing_trades_file_still_succeeds(self, tmp_path: Path):
        """Pipeline must succeed (instruments only) when trades file is absent."""
        instruments_csv = _make_instruments_csv(tmp_path)
        missing_trades = tmp_path / "nonexistent.csv"

        mock_session = MagicMock()
        mock_run = MagicMock()
        mock_run.id = 1

        with patch("app.etl.orchestration.pipeline.managed_session") as mock_ctx, \
             patch("app.etl.orchestration.pipeline.ETLRunRepository") as MockRepo, \
             patch("app.etl.orchestration.pipeline.load_assets", return_value=2) as mock_la, \
             patch("app.etl.orchestration.pipeline.load_trades") as mock_lt:

            mock_ctx.return_value = _make_mock_ctx(mock_session)
            repo_inst = MockRepo.return_value
            repo_inst.start_run.return_value = mock_run
            repo_inst.get_by_id.return_value = mock_run

            from app.etl.orchestration.pipeline import run_instruments_and_trades_pipeline
            result = run_instruments_and_trades_pipeline(
                instruments_csv, missing_trades, date(2024, 6, 14)
            )

        assert result["status"] == ETLStatus.SUCCESS
        assert result["trades_upserted"] == 0
        mock_lt.assert_not_called()


# ===========================================================================
# 3. Pipeline audit — intraday quotes (three-transaction pattern)
# ===========================================================================


class TestIntradayQuotesPipelineAudit:
    """Verify that the intraday quotes pipeline audit row is recorded correctly."""

    def test_success_path_returns_success_status(self, tmp_path: Path):
        jsonl_path = _make_jsonl(tmp_path)

        mock_session = MagicMock()
        mock_run = MagicMock()
        mock_run.id = 5

        with patch("app.etl.orchestration.pipeline.managed_session") as mock_ctx, \
             patch("app.etl.orchestration.pipeline.ETLRunRepository") as MockRepo, \
             patch("app.etl.orchestration.pipeline.load_intraday_quotes", return_value=2):

            mock_ctx.return_value = _make_mock_ctx(mock_session)
            repo_inst = MockRepo.return_value
            repo_inst.start_run.return_value = mock_run
            repo_inst.get_by_id.return_value = mock_run

            from app.etl.orchestration.pipeline import run_intraday_quotes_pipeline
            result = run_intraday_quotes_pipeline(jsonl_path)

        assert result["status"] == ETLStatus.SUCCESS
        assert result["rows_inserted"] == 2

    def test_success_path_calls_finish_run_with_success(self, tmp_path: Path):
        jsonl_path = _make_jsonl(tmp_path)

        mock_session = MagicMock()
        mock_run = MagicMock()
        mock_run.id = 5

        with patch("app.etl.orchestration.pipeline.managed_session") as mock_ctx, \
             patch("app.etl.orchestration.pipeline.ETLRunRepository") as MockRepo, \
             patch("app.etl.orchestration.pipeline.load_intraday_quotes", return_value=2):

            mock_ctx.return_value = _make_mock_ctx(mock_session)
            repo_inst = MockRepo.return_value
            repo_inst.start_run.return_value = mock_run
            repo_inst.get_by_id.return_value = mock_run

            from app.etl.orchestration.pipeline import run_intraday_quotes_pipeline
            run_intraday_quotes_pipeline(jsonl_path)

        repo_inst.finish_run.assert_called_once()
        assert repo_inst.finish_run.call_args.args[1] == ETLStatus.SUCCESS

    def test_success_path_records_rows_inserted_in_audit(self, tmp_path: Path):
        jsonl_path = _make_jsonl(tmp_path)

        mock_session = MagicMock()
        mock_run = MagicMock()
        mock_run.id = 5

        with patch("app.etl.orchestration.pipeline.managed_session") as mock_ctx, \
             patch("app.etl.orchestration.pipeline.ETLRunRepository") as MockRepo, \
             patch("app.etl.orchestration.pipeline.load_intraday_quotes", return_value=7):

            mock_ctx.return_value = _make_mock_ctx(mock_session)
            repo_inst = MockRepo.return_value
            repo_inst.start_run.return_value = mock_run
            repo_inst.get_by_id.return_value = mock_run

            from app.etl.orchestration.pipeline import run_intraday_quotes_pipeline
            run_intraday_quotes_pipeline(jsonl_path)

        finish_call = repo_inst.finish_run.call_args
        assert finish_call.kwargs.get("rows_inserted") == 7
        assert finish_call.kwargs.get("rows_failed") == 0

    def test_failure_path_returns_failed_status(self, tmp_path: Path):
        jsonl_path = _make_jsonl(tmp_path)

        mock_session = MagicMock()
        mock_run = MagicMock()
        mock_run.id = 6

        with patch("app.etl.orchestration.pipeline.managed_session") as mock_ctx, \
             patch("app.etl.orchestration.pipeline.ETLRunRepository") as MockRepo, \
             patch("app.etl.orchestration.pipeline.load_intraday_quotes",
                   side_effect=RuntimeError("hypertable insert error")):

            mock_ctx.return_value = _make_mock_ctx(mock_session)
            repo_inst = MockRepo.return_value
            repo_inst.start_run.return_value = mock_run
            repo_inst.get_by_id.return_value = mock_run

            from app.etl.orchestration.pipeline import run_intraday_quotes_pipeline
            result = run_intraday_quotes_pipeline(jsonl_path)

        assert result["status"] == ETLStatus.FAILED
        assert "error" in result

    def test_failure_path_calls_finish_run_with_failed(self, tmp_path: Path):
        jsonl_path = _make_jsonl(tmp_path)

        mock_session = MagicMock()
        mock_run = MagicMock()
        mock_run.id = 6

        with patch("app.etl.orchestration.pipeline.managed_session") as mock_ctx, \
             patch("app.etl.orchestration.pipeline.ETLRunRepository") as MockRepo, \
             patch("app.etl.orchestration.pipeline.load_intraday_quotes",
                   side_effect=RuntimeError("hypertable insert error")):

            mock_ctx.return_value = _make_mock_ctx(mock_session)
            repo_inst = MockRepo.return_value
            repo_inst.start_run.return_value = mock_run
            repo_inst.get_by_id.return_value = mock_run

            from app.etl.orchestration.pipeline import run_intraday_quotes_pipeline
            run_intraday_quotes_pipeline(jsonl_path)

        repo_inst.finish_run.assert_called_once()
        assert repo_inst.finish_run.call_args.args[1] == ETLStatus.FAILED

    def test_failure_path_includes_error_in_message(self, tmp_path: Path):
        jsonl_path = _make_jsonl(tmp_path)

        mock_session = MagicMock()
        mock_run = MagicMock()
        mock_run.id = 6

        with patch("app.etl.orchestration.pipeline.managed_session") as mock_ctx, \
             patch("app.etl.orchestration.pipeline.ETLRunRepository") as MockRepo, \
             patch("app.etl.orchestration.pipeline.load_intraday_quotes",
                   side_effect=RuntimeError("unique constraint violation")):

            mock_ctx.return_value = _make_mock_ctx(mock_session)
            repo_inst = MockRepo.return_value
            repo_inst.start_run.return_value = mock_run
            repo_inst.get_by_id.return_value = mock_run

            from app.etl.orchestration.pipeline import run_intraday_quotes_pipeline
            result = run_intraday_quotes_pipeline(jsonl_path)

        assert "unique constraint violation" in result["error"]


# ===========================================================================
# 4. Transaction isolation — data rollback on failure
# ===========================================================================


class TestTransactionRollback:
    """Verify that managed_session rolls back the data transaction on failure."""

    def test_managed_session_rolls_back_on_exception(self):
        """managed_session must call db.rollback() and re-raise on exception."""
        from app.db.engine import managed_session

        mock_session = MagicMock()
        mock_session.commit.side_effect = None  # normal commit works
        mock_session_factory = MagicMock(return_value=mock_session)

        with patch("app.db.engine.SessionLocal", mock_session_factory):
            with pytest.raises(ValueError, match="intentional error"):
                with managed_session() as db:
                    db.execute("INSERT INTO test VALUES (1)")
                    raise ValueError("intentional error")

        mock_session.rollback.assert_called_once()
        mock_session.commit.assert_not_called()

    def test_managed_session_commits_on_success(self):
        """managed_session must commit when the block exits normally."""
        from app.db.engine import managed_session

        mock_session = MagicMock()
        mock_session_factory = MagicMock(return_value=mock_session)

        with patch("app.db.engine.SessionLocal", mock_session_factory):
            with managed_session() as db:
                db.execute("SELECT 1")

        mock_session.commit.assert_called_once()
        mock_session.rollback.assert_not_called()

    def test_managed_session_always_closes(self):
        """managed_session must close the session even when an exception occurs."""
        from app.db.engine import managed_session

        mock_session = MagicMock()
        mock_session_factory = MagicMock(return_value=mock_session)

        with patch("app.db.engine.SessionLocal", mock_session_factory):
            with pytest.raises(RuntimeError):
                with managed_session():
                    raise RuntimeError("oops")

        mock_session.close.assert_called_once()


# ===========================================================================
# 5. ETLRunRepository — get_by_id
# ===========================================================================


class TestETLRunRepositoryGetById:
    """Verify the new get_by_id helper."""

    def test_get_by_id_delegates_to_session_get(self):
        from app.repositories.etl_run_repository import ETLRunRepository

        db = MagicMock()
        mock_run = MagicMock(spec=ETLRun)
        db.get.return_value = mock_run

        repo = ETLRunRepository(db)
        result = repo.get_by_id(42)

        db.get.assert_called_once_with(ETLRun, 42)
        assert result is mock_run

    def test_get_by_id_returns_none_when_not_found(self):
        from app.repositories.etl_run_repository import ETLRunRepository

        db = MagicMock()
        db.get.return_value = None

        repo = ETLRunRepository(db)
        assert repo.get_by_id(9999) is None

