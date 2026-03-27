"""Entrypoint tests for docker/scheduler.py (legacy stack; non-default CMD).

Canonical runtime is Prefect — see docs/legacy_scheduler.md. These tests keep
contract coverage for the optional scheduler + run_daily_batch.sh path.

Tests focus on the scheduler orchestrator and its helpers with full mocking:
no real DB, subprocess, or filesystem (except where using tmp_path).
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Load scheduler module by path (docker is not a package)
ROOT = Path(__file__).resolve().parent.parent.parent
SCHEDULER_PATH = ROOT / "docker" / "scheduler.py"


def _load_scheduler():
    import importlib.util
    spec = importlib.util.spec_from_file_location("scheduler", SCHEDULER_PATH)
    mod = importlib.util.module_from_spec(spec)
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    # Register so patches like patch("scheduler.time") apply to this module
    sys.modules["scheduler"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def scheduler():
    """Load the scheduler module once per test (imports app.*)."""
    return _load_scheduler()


# ---------------------------------------------------------------------------
# next_daily_run(now)
# ---------------------------------------------------------------------------


def test_next_daily_run_returns_today_when_before_target_time(scheduler):
    # Default DAILY_RUN_HOUR=20, DAILY_RUN_MINUTE=0
    with patch.object(scheduler, "DAILY_RUN_HOUR", 20), patch.object(
        scheduler, "DAILY_RUN_MINUTE", 0
    ):
        now = datetime(2026, 3, 18, 10, 0, 0)
        result = scheduler.next_daily_run(now=now)
        assert result == datetime(2026, 3, 18, 20, 0, 0)


def test_next_daily_run_returns_tomorrow_when_after_target_time(scheduler):
    with patch.object(scheduler, "DAILY_RUN_HOUR", 20), patch.object(
        scheduler, "DAILY_RUN_MINUTE", 0
    ):
        now = datetime(2026, 3, 18, 20, 30, 0)
        result = scheduler.next_daily_run(now=now)
        assert result == datetime(2026, 3, 19, 20, 0, 0)


def test_next_daily_run_returns_tomorrow_when_exactly_at_target_time(scheduler):
    with patch.object(scheduler, "DAILY_RUN_HOUR", 20), patch.object(
        scheduler, "DAILY_RUN_MINUTE", 0
    ):
        now = datetime(2026, 3, 18, 20, 0, 0)
        result = scheduler.next_daily_run(now=now)
        assert result == datetime(2026, 3, 19, 20, 0, 0)


def test_next_daily_run_uses_datetime_now_when_now_is_none(scheduler):
    with patch.object(scheduler, "DAILY_RUN_HOUR", 9), patch.object(
        scheduler, "DAILY_RUN_MINUTE", 30
    ), patch.object(scheduler, "datetime") as mod_dt:
        mod_dt.now.return_value = datetime(2026, 3, 18, 8, 0, 0)
        # next_daily_run(now=None) uses datetime.now() from the module
        result = scheduler.next_daily_run(now=None)
    assert result == datetime(2026, 3, 18, 9, 30, 0)


# ---------------------------------------------------------------------------
# run_with_retry
# ---------------------------------------------------------------------------


def test_run_with_retry_succeeds_on_first_attempt(scheduler):
    func = MagicMock(return_value=True)
    with patch("scheduler.time") as mock_time:
        mock_time.monotonic.side_effect = [0, 1]
        result = scheduler.run_with_retry(func, name="test_scraper", attempts=3, delay_seconds=1)
    assert result is True
    assert func.call_count == 1
    mock_time.sleep.assert_not_called()


def test_run_with_retry_succeeds_on_second_attempt(scheduler):
    func = MagicMock(side_effect=[False, True])
    with patch("scheduler.time") as mock_time:
        mock_time.monotonic.side_effect = [0, 1, 2, 3]
        result = scheduler.run_with_retry(func, name="test", attempts=3, delay_seconds=2)
    assert result is True
    assert func.call_count == 2
    mock_time.sleep.assert_called_once_with(2)


def test_run_with_retry_fails_after_all_attempts(scheduler):
    func = MagicMock(return_value=False)
    with patch("scheduler.time") as mock_time:
        mock_time.monotonic.side_effect = list(range(10))
        result = scheduler.run_with_retry(func, name="test", attempts=3, delay_seconds=1)
    assert result is False
    assert func.call_count == 3
    assert mock_time.sleep.call_count == 2


def test_run_with_retry_exception_then_success(scheduler):
    func = MagicMock(side_effect=[ValueError("transient"), True])
    with patch("scheduler.time") as mock_time:
        mock_time.monotonic.side_effect = [0, 1, 2, 3]
        result = scheduler.run_with_retry(func, name="test", attempts=3, delay_seconds=1)
    assert result is True
    assert func.call_count == 2
    mock_time.sleep.assert_called_once_with(1)


# ---------------------------------------------------------------------------
# run_migrations
# ---------------------------------------------------------------------------


def test_run_migrations_success(scheduler):
    with patch("scheduler.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")
        result = scheduler.run_migrations()
    assert result is True
    assert mock_run.call_count >= 1
    # First call may be "which alembic", then alembic upgrade head
    upgrade_calls = [c for c in mock_run.call_args_list if len(c[0][0]) and c[0][0][0] != "which"]
    assert len(upgrade_calls) >= 1
    assert upgrade_calls[-1][0][0][-2:] == ["upgrade", "head"]


def test_run_migrations_failure(scheduler):
    with patch("scheduler.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error")
        result = scheduler.run_migrations()
    assert result is False


def test_run_migrations_file_not_found(scheduler):
    with patch("scheduler.subprocess.run", side_effect=FileNotFoundError()):
        result = scheduler.run_migrations()
    assert result is False


# ---------------------------------------------------------------------------
# wait_for_db
# ---------------------------------------------------------------------------


def test_wait_for_db_success(scheduler):
    with patch("app.db.engine.wait_for_db", MagicMock()) as mock_wait:
        result = scheduler.wait_for_db(retries=1, delay=0.1)
    assert result is True
    mock_wait.assert_called_once_with(retries=1, delay=0.1)


def test_wait_for_db_failure(scheduler):
    with patch("app.db.engine.wait_for_db", side_effect=RuntimeError("connection refused")):
        result = scheduler.wait_for_db(retries=1, delay=0.1)
    assert result is False


# ---------------------------------------------------------------------------
# run_daily_scrapers
# ---------------------------------------------------------------------------


def test_run_daily_scrapers_success(scheduler, tmp_path):
    batch_script = tmp_path / "run_daily_batch.sh"
    batch_script.write_text("#!/bin/bash\ntrue")
    batch_script.chmod(0o755)
    with patch.object(scheduler, "DAILY_BATCH", str(batch_script)):
        with patch("scheduler.Path") as mock_path_cls:
            mock_path = MagicMock()
            mock_path.exists.return_value = True
            mock_path.stat.return_value = MagicMock(st_uid=0, st_gid=0, st_mode=0o755)
            mock_path.__str__ = lambda s: str(batch_script)
            mock_path_cls.return_value = mock_path
            with patch("scheduler.os.access", return_value=True), patch(
                "scheduler.subprocess.run"
            ) as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stdout="", stderr=None)
                with patch("scheduler.time.monotonic", side_effect=[0, 5]):
                    result = scheduler.run_daily_scrapers()
    assert result is True
    mock_run.assert_called_once()
    assert mock_run.call_args[0][0] == [str(batch_script)]


def test_run_daily_scrapers_script_not_found(scheduler):
    with patch.object(scheduler, "DAILY_BATCH", "/nonexistent/batch.sh"):
        with patch("scheduler.Path") as mock_path:
            mock_path.return_value = MagicMock(exists=MagicMock(return_value=False))
            result = scheduler.run_daily_scrapers()
    assert result is False


def test_run_daily_scrapers_script_not_executable(scheduler, tmp_path):
    batch_script = tmp_path / "run_daily_batch.sh"
    batch_script.write_text("")
    with patch.object(scheduler, "DAILY_BATCH", str(batch_script)):
        with patch("scheduler.Path") as mock_path:
            mock_path.return_value = MagicMock(
                exists=MagicMock(return_value=True),
                stat=MagicMock(return_value=MagicMock(st_uid=0, st_gid=0, st_mode=0o644)),
            )
            with patch("scheduler.os.access", return_value=False):
                result = scheduler.run_daily_scrapers()
    assert result is False


# ---------------------------------------------------------------------------
# load_instruments_and_trades
# ---------------------------------------------------------------------------


def test_load_instruments_and_trades_success(scheduler, tmp_path):
    csv_path = tmp_path / "cadastro_instrumentos_20260318.normalized.csv"
    csv_path.write_text("Instrumento financeiro;Ativo\nPETR4;PETR", encoding="utf-8")
    # Sibling trades file so scheduler runs run_daily_quotes_pipeline
    trades_path = tmp_path / "negocios_consolidados_20260318.normalized.csv"
    trades_path.write_text("ticker;trade_date\nPETR4;2026-03-18", encoding="utf-8")
    with patch("scheduler._log_csv_diagnostics"):
        with patch(
            "app.etl.orchestration.pipeline.run_instruments_and_trades_pipeline"
        ) as mock_pipeline:
            mock_pipeline.return_value = {
                "status": scheduler.ETLStatus.SUCCESS,
                "assets_upserted": 10,
                "trades_upserted": 5,
            }
            with patch(
                "app.etl.orchestration.pipeline.run_daily_quotes_pipeline"
            ) as mock_dq:
                mock_dq.return_value = {
                    "status": scheduler.ETLStatus.SUCCESS,
                    "quotes_upserted": 100,
                }
                result = scheduler.load_instruments_and_trades(csv_path)
    assert result is True
    mock_pipeline.assert_called_once()
    assert mock_pipeline.call_args[0][0] == csv_path
    mock_dq.assert_called_once()


def test_load_instruments_and_trades_no_trades_sibling_still_succeeds(scheduler, tmp_path):
    csv_path = tmp_path / "cadastro_20260318.normalized.csv"
    csv_path.write_text("Instrumento financeiro;Ativo\nPETR4;PETR", encoding="utf-8")
    with patch("scheduler._log_csv_diagnostics"):
        with patch(
            "app.etl.orchestration.pipeline.run_instruments_and_trades_pipeline"
        ) as mock_pipeline:
            mock_pipeline.return_value = {
                "status": scheduler.ETLStatus.SUCCESS,
                "assets_upserted": 5,
                "trades_upserted": 0,
            }
            result = scheduler.load_instruments_and_trades(csv_path)
    assert result is True
    mock_pipeline.assert_called_once()
    assert mock_pipeline.call_args[0][1] is None  # trades_path


def test_load_instruments_and_trades_failure_zero_assets(scheduler, tmp_path):
    csv_path = tmp_path / "cadastro_20260318.normalized.csv"
    csv_path.write_text("Instrumento financeiro;Ativo\n", encoding="utf-8")
    with patch("scheduler._log_csv_diagnostics"):
        with patch(
            "app.etl.orchestration.pipeline.run_instruments_and_trades_pipeline"
        ) as mock_pipeline:
            mock_pipeline.return_value = {
                "status": scheduler.ETLStatus.SUCCESS,
                "assets_upserted": 0,
                "trades_upserted": 0,
            }
            result = scheduler.load_instruments_and_trades(csv_path)
    assert result is False


# ---------------------------------------------------------------------------
# load_latest_jsonl
# ---------------------------------------------------------------------------


def test_load_latest_jsonl_no_dir_returns_false(scheduler, tmp_path):
    with patch.object(scheduler, "B3_DATA_DIR", tmp_path):
        ref = date(2026, 3, 18)
        jsonl_dir = tmp_path / "b3" / "daily_fluctuation_history" / ref.isoformat()
        jsonl_dir.mkdir(parents=True, exist_ok=True)
        # Empty dir - no jsonl files
        result = scheduler.load_latest_jsonl(ref)
    assert result is False


def test_load_latest_jsonl_success(scheduler, tmp_path):
    ref = date(2026, 3, 18)
    jsonl_dir = tmp_path / "b3" / "daily_fluctuation_history" / ref.isoformat()
    jsonl_dir.mkdir(parents=True, exist_ok=True)
    (jsonl_dir / "daily_fluctuation_20260318T120000.jsonl").write_text("{}")
    with patch.object(scheduler, "B3_DATA_DIR", tmp_path):
        with patch(
            "app.etl.orchestration.pipeline.run_intraday_quotes_pipeline"
        ) as mock_pipeline:
            mock_pipeline.return_value = {
                "status": scheduler.ETLStatus.SUCCESS,
                "rows_inserted": 50,
                "source_file": "daily_fluctuation_20260318T120000.jsonl",
            }
            result = scheduler.load_latest_jsonl(ref)
    assert result is True
    mock_pipeline.assert_called_once()
    assert "daily_fluctuation" in str(mock_pipeline.call_args[0][0])


# ---------------------------------------------------------------------------
# run_quote_batch
# ---------------------------------------------------------------------------


def test_run_quote_batch_success(scheduler, tmp_path):
    instruments_csv = tmp_path / "instruments.csv"
    instruments_csv.write_text("ticker\nPETR4", encoding="utf-8")
    # time.monotonic: run_with_retry (start, end), _invoke (start, end) = 4 calls
    with patch("scheduler.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr=None)
        with patch("scheduler.time.monotonic", side_effect=[0, 1, 2, 3]):
            with patch.object(scheduler, "PYTHON", "python"), patch.object(
                scheduler, "QUOTE_SCRIPT", "/app/scripts/run_b3_quote_batch.py"
            ):
                result = scheduler.run_quote_batch(instruments_csv)
    assert result is True
    mock_run.assert_called()
    call_args = mock_run.call_args[0][0]
    assert call_args[0] == "python"
    assert "run_b3_quote_batch.py" in call_args[1]
    assert "--instruments" in call_args
    assert str(instruments_csv) in call_args


def test_run_quote_batch_failure_after_retry(scheduler, tmp_path):
    instruments_csv = tmp_path / "instruments.csv"
    instruments_csv.write_text("ticker\nPETR4", encoding="utf-8")
    with patch("scheduler.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="err")
        with patch("scheduler.time.monotonic", side_effect=iter(range(20))), patch(
            "scheduler.time.sleep"
        ):  # avoid real 15s+30s sleeps from run_with_retry backoff
            with patch.object(scheduler, "PYTHON", "python"), patch.object(
                scheduler, "QUOTE_SCRIPT", "/app/scripts/run_b3_quote_batch.py"
            ):
                result = scheduler.run_quote_batch(instruments_csv)
    assert result is False
    assert mock_run.call_count >= 1


# ---------------------------------------------------------------------------
# main() startup sequence (no infinite loop)
# ---------------------------------------------------------------------------


def test_main_waits_for_db_then_exits_on_failure(scheduler, tmp_path):
    with patch.object(scheduler, "B3_DATA_DIR", tmp_path), patch.object(
        scheduler, "RUN_MIGRATIONS", False
    ), patch("scheduler.resolver_ensure_data_dirs"), patch(
        "scheduler.wait_for_db", return_value=False
    ), patch("scheduler.sys.exit") as mock_exit:
        mock_exit.side_effect = SystemExit  # so main() actually stops; otherwise it continues into the loop and hangs
        with pytest.raises(SystemExit):
            scheduler.main()
    mock_exit.assert_called_once_with(1)


def test_main_runs_migrations_then_exits_on_migration_failure(scheduler, tmp_path):
    with patch.object(scheduler, "B3_DATA_DIR", tmp_path), patch.object(
        scheduler, "RUN_MIGRATIONS", True
    ), patch("scheduler.resolver_ensure_data_dirs"), patch(
        "scheduler.wait_for_db", return_value=True
    ), patch("scheduler.run_migrations", return_value=False), patch(
        "scheduler.sys.exit"
    ) as mock_exit:
        mock_exit.side_effect = SystemExit  # so main() actually stops; otherwise it continues into the loop and hangs
        with pytest.raises(SystemExit):
            scheduler.main()
    mock_exit.assert_called_once_with(2)


def test_main_startup_sequence_calls_resolve_then_load_then_intraday(scheduler, tmp_path):
    """Assert main() calls ensure_dirs, wait_for_db, run_daily_scrapers_once, resolve_instruments_csv, load_instruments_and_trades, run_intraday_quotes_once, then enters loop and sleeps."""
    (tmp_path / "instruments.csv").write_text("x", encoding="utf-8")
    # Use fixed times: 10:00 so next_daily is today 20:00, next_intraday is 10:30; loop now=10:00 so we go to sleep once, then break
    t10 = datetime(2026, 3, 18, 10, 0, 0)
    with patch.object(scheduler, "B3_DATA_DIR", tmp_path), patch.object(
        scheduler, "RUN_MIGRATIONS", False
    ), patch("scheduler.resolver_ensure_data_dirs"), patch(
        "scheduler.wait_for_db", return_value=True
    ), patch("scheduler.run_daily_scrapers_once"), patch(
        "scheduler.resolve_instruments_csv", return_value=tmp_path / "instruments.csv"
    ), patch("scheduler.load_instruments_and_trades") as mock_load, patch(
        "scheduler.run_intraday_quotes_once"
    ) as mock_intraday, patch("scheduler.next_daily_run", return_value=datetime(2026, 3, 18, 20, 0, 0)), patch(
        "scheduler.datetime"
    ) as mock_dt, patch("scheduler.timedelta", wraps=timedelta):
        mock_dt.now.side_effect = [t10, t10, t10]  # next_daily_run(now), next_intraday calc, loop
        with patch("scheduler.time.sleep", side_effect=KeyboardInterrupt("stop")):
            try:
                scheduler.main()
            except KeyboardInterrupt:
                pass
    mock_load.assert_called_once()
    mock_intraday.assert_called()
