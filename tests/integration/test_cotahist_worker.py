from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import InterfaceError, OperationalError

ROOT = Path(__file__).resolve().parent.parent.parent
WORKER_PATH = ROOT / "docker" / "cotahist_worker.py"


def _load_worker_module():
    spec = importlib.util.spec_from_file_location("cotahist_worker", WORKER_PATH)
    mod = importlib.util.module_from_spec(spec)
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    sys.modules["cotahist_worker"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_main_exits_75_when_advisory_lock_is_unavailable(monkeypatch):
    worker = _load_worker_module()

    fake_config = ModuleType("app.core.config")
    fake_config.settings = SimpleNamespace(
        b3_cotahist_annual_dir="/tmp/cotahist",
        b3_cotahist_year_start=2000,
        b3_cotahist_year_end=2001,
    )

    fake_lock_result = SimpleNamespace(scalar=lambda: False)
    fake_conn = MagicMock()
    fake_conn.execute.return_value = fake_lock_result
    fake_connect_cm = MagicMock()
    fake_connect_cm.__enter__.return_value = fake_conn
    fake_connect_cm.__exit__.return_value = False
    fake_engine = SimpleNamespace(connect=MagicMock(return_value=fake_connect_cm))

    fake_engine_module = ModuleType("app.db.engine")
    fake_engine_module.engine = fake_engine
    fake_engine_module.wait_for_db = MagicMock()

    monkeypatch.setitem(sys.modules, "app.core.config", fake_config)
    monkeypatch.setitem(sys.modules, "app.db.engine", fake_engine_module)
    monkeypatch.setenv("COTAHIST_YEAR", "2024")

    with patch.object(worker, "wait_for_cotahist_table", return_value=None), patch.object(
        worker.subprocess, "run"
    ) as run_mock:
        with pytest.raises(SystemExit) as exc:
            worker.main()

    assert exc.value.code == 75
    run_mock.assert_not_called()


def test_main_commits_after_lock_acquisition(monkeypatch):
    worker = _load_worker_module()

    fake_config = ModuleType("app.core.config")
    fake_config.settings = SimpleNamespace(
        b3_cotahist_annual_dir="/tmp/cotahist",
        b3_cotahist_year_start=2000,
        b3_cotahist_year_end=2001,
    )

    fake_conn = MagicMock()
    fake_conn.execute.side_effect = [
        SimpleNamespace(scalar=lambda: True),   # pg_try_advisory_lock
        SimpleNamespace(scalar=lambda: True),   # pg_advisory_unlock
    ]
    fake_connect_cm = MagicMock()
    fake_connect_cm.__enter__.return_value = fake_conn
    fake_connect_cm.__exit__.return_value = False
    fake_engine = SimpleNamespace(connect=MagicMock(return_value=fake_connect_cm))

    fake_engine_module = ModuleType("app.db.engine")
    fake_engine_module.engine = fake_engine
    fake_engine_module.wait_for_db = MagicMock()

    monkeypatch.setitem(sys.modules, "app.core.config", fake_config)
    monkeypatch.setitem(sys.modules, "app.db.engine", fake_engine_module)
    monkeypatch.setenv("COTAHIST_YEAR", "2024")
    monkeypatch.setenv("COTAHIST_SKIP_DOWNLOAD", "1")

    fake_completed = SimpleNamespace(returncode=0)
    with patch.object(worker, "wait_for_cotahist_table", return_value=None), patch.object(
        worker.subprocess, "run", return_value=fake_completed
    ), patch.object(worker, "start_scraper_audit", return_value=9001) as start_audit, patch.object(
        worker, "finish_scraper_audit"
    ) as finish_audit:
        worker.main()

    fake_conn.commit.assert_called_once()
    start_audit.assert_called_once()
    finish_audit.assert_called_once()
    assert finish_audit.call_args.kwargs["status"] == "skipped"
    assert finish_audit.call_args.kwargs["audit_id"] == 9001


def test_fetch_stage_calls_start_audit_before_subprocess(monkeypatch):
    worker = _load_worker_module()

    fake_config = ModuleType("app.core.config")
    fake_config.settings = SimpleNamespace(
        b3_cotahist_annual_dir="/tmp/cotahist",
        b3_cotahist_year_start=2000,
        b3_cotahist_year_end=2001,
    )

    fake_conn = MagicMock()
    fake_conn.execute.side_effect = [
        SimpleNamespace(scalar=lambda: True),
        SimpleNamespace(scalar=lambda: True),
    ]
    fake_connect_cm = MagicMock()
    fake_connect_cm.__enter__.return_value = fake_conn
    fake_connect_cm.__exit__.return_value = False
    fake_engine = SimpleNamespace(connect=MagicMock(return_value=fake_connect_cm))

    fake_engine_module = ModuleType("app.db.engine")
    fake_engine_module.engine = fake_engine
    fake_engine_module.wait_for_db = MagicMock()

    monkeypatch.setitem(sys.modules, "app.core.config", fake_config)
    monkeypatch.setitem(sys.modules, "app.db.engine", fake_engine_module)
    monkeypatch.setenv("COTAHIST_YEAR", "2024")
    monkeypatch.delenv("COTAHIST_SKIP_DOWNLOAD", raising=False)

    calls: list[str] = []

    def fake_start(**kwargs):
        calls.append("start")
        return 42

    def run_side_effect(*args, **kwargs):
        calls.append("subprocess")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    with patch.object(worker, "wait_for_cotahist_table", return_value=None), patch.object(
        worker, "start_scraper_audit", side_effect=fake_start
    ), patch.object(worker, "finish_scraper_audit") as finish_audit, patch.object(
        worker.subprocess, "run", side_effect=run_side_effect
    ):
        worker.main()

    assert calls == ["start", "subprocess", "subprocess"]
    finish_audit.assert_called_once()
    assert finish_audit.call_args.kwargs["status"] == "success"
    assert finish_audit.call_args.kwargs["audit_id"] == 42


def test_fetch_stage_finishes_failed_on_nonzero_exit(monkeypatch):
    worker = _load_worker_module()

    fake_config = ModuleType("app.core.config")
    fake_config.settings = SimpleNamespace(
        b3_cotahist_annual_dir="/tmp/cotahist",
        b3_cotahist_year_start=2000,
        b3_cotahist_year_end=2001,
    )

    fake_conn = MagicMock()
    fake_conn.execute.side_effect = [
        SimpleNamespace(scalar=lambda: True),
        SimpleNamespace(scalar=lambda: True),
    ]
    fake_connect_cm = MagicMock()
    fake_connect_cm.__enter__.return_value = fake_conn
    fake_connect_cm.__exit__.return_value = False
    fake_engine = SimpleNamespace(connect=MagicMock(return_value=fake_connect_cm))

    fake_engine_module = ModuleType("app.db.engine")
    fake_engine_module.engine = fake_engine
    fake_engine_module.wait_for_db = MagicMock()

    monkeypatch.setitem(sys.modules, "app.core.config", fake_config)
    monkeypatch.setitem(sys.modules, "app.db.engine", fake_engine_module)
    monkeypatch.setenv("COTAHIST_YEAR", "2024")
    monkeypatch.delenv("COTAHIST_SKIP_DOWNLOAD", raising=False)

    bad_fetch = SimpleNamespace(returncode=7, stdout="out", stderr="err")

    with patch.object(worker, "wait_for_cotahist_table", return_value=None), patch.object(
        worker, "start_scraper_audit", return_value=99
    ), patch.object(worker, "finish_scraper_audit") as finish_audit, patch.object(
        worker.subprocess, "run", return_value=bad_fetch
    ):
        with pytest.raises(SystemExit) as exc:
            worker.main()

    assert exc.value.code == 7
    finish_audit.assert_called_once()
    assert finish_audit.call_args.kwargs["status"] == "failed"
    assert finish_audit.call_args.kwargs["audit_id"] == 99
    assert "7" in finish_audit.call_args.kwargs["error_message"]


def test_fetch_stage_proceeds_without_audit_when_db_unavailable(monkeypatch):
    worker = _load_worker_module()

    fake_config = ModuleType("app.core.config")
    fake_config.settings = SimpleNamespace(
        b3_cotahist_annual_dir="/tmp/cotahist",
        b3_cotahist_year_start=2000,
        b3_cotahist_year_end=2001,
    )

    fake_conn = MagicMock()
    fake_conn.execute.side_effect = [
        SimpleNamespace(scalar=lambda: True),
        SimpleNamespace(scalar=lambda: True),
    ]
    fake_connect_cm = MagicMock()
    fake_connect_cm.__enter__.return_value = fake_conn
    fake_connect_cm.__exit__.return_value = False
    fake_engine = SimpleNamespace(connect=MagicMock(return_value=fake_connect_cm))

    fake_engine_module = ModuleType("app.db.engine")
    fake_engine_module.engine = fake_engine
    fake_engine_module.wait_for_db = MagicMock()

    monkeypatch.setitem(sys.modules, "app.core.config", fake_config)
    monkeypatch.setitem(sys.modules, "app.db.engine", fake_engine_module)
    monkeypatch.setenv("COTAHIST_YEAR", "2024")
    monkeypatch.delenv("COTAHIST_SKIP_DOWNLOAD", raising=False)

    run_results = [
        SimpleNamespace(returncode=0, stdout="", stderr=""),
        SimpleNamespace(returncode=0),
    ]

    with patch.object(worker, "wait_for_cotahist_table", return_value=None), patch.object(
        worker, "start_scraper_audit", side_effect=OperationalError("stmt", {}, None)
    ), patch.object(worker, "finish_scraper_audit") as finish_audit, patch.object(
        worker.subprocess, "run", side_effect=run_results
    ) as run_mock:
        worker.main()

    finish_audit.assert_not_called()
    assert run_mock.call_count == 2


def test_skip_download_graceful_when_audit_db_unavailable(monkeypatch):
    worker = _load_worker_module()

    fake_config = ModuleType("app.core.config")
    fake_config.settings = SimpleNamespace(
        b3_cotahist_annual_dir="/tmp/cotahist",
        b3_cotahist_year_start=2000,
        b3_cotahist_year_end=2001,
    )

    fake_conn = MagicMock()
    fake_conn.execute.side_effect = [
        SimpleNamespace(scalar=lambda: True),
        SimpleNamespace(scalar=lambda: True),
    ]
    fake_connect_cm = MagicMock()
    fake_connect_cm.__enter__.return_value = fake_conn
    fake_connect_cm.__exit__.return_value = False
    fake_engine = SimpleNamespace(connect=MagicMock(return_value=fake_connect_cm))

    fake_engine_module = ModuleType("app.db.engine")
    fake_engine_module.engine = fake_engine
    fake_engine_module.wait_for_db = MagicMock()

    monkeypatch.setitem(sys.modules, "app.core.config", fake_config)
    monkeypatch.setitem(sys.modules, "app.db.engine", fake_engine_module)
    monkeypatch.setenv("COTAHIST_YEAR", "2024")
    monkeypatch.setenv("COTAHIST_SKIP_DOWNLOAD", "1")

    fake_completed = SimpleNamespace(returncode=0)
    with patch.object(worker, "wait_for_cotahist_table", return_value=None), patch.object(
        worker.subprocess, "run", return_value=fake_completed
    ), patch.object(worker, "start_scraper_audit", side_effect=OperationalError("stmt", {}, None)), patch.object(
        worker, "finish_scraper_audit"
    ) as finish_audit:
        worker.main()

    finish_audit.assert_not_called()


def test_fetch_stage_proceeds_without_audit_when_interface_error_on_start(monkeypatch):
    worker = _load_worker_module()

    fake_config = ModuleType("app.core.config")
    fake_config.settings = SimpleNamespace(
        b3_cotahist_annual_dir="/tmp/cotahist",
        b3_cotahist_year_start=2000,
        b3_cotahist_year_end=2001,
    )

    fake_conn = MagicMock()
    fake_conn.execute.side_effect = [
        SimpleNamespace(scalar=lambda: True),
        SimpleNamespace(scalar=lambda: True),
    ]
    fake_connect_cm = MagicMock()
    fake_connect_cm.__enter__.return_value = fake_conn
    fake_connect_cm.__exit__.return_value = False
    fake_engine = SimpleNamespace(connect=MagicMock(return_value=fake_connect_cm))

    fake_engine_module = ModuleType("app.db.engine")
    fake_engine_module.engine = fake_engine
    fake_engine_module.wait_for_db = MagicMock()

    monkeypatch.setitem(sys.modules, "app.core.config", fake_config)
    monkeypatch.setitem(sys.modules, "app.db.engine", fake_engine_module)
    monkeypatch.setenv("COTAHIST_YEAR", "2024")
    monkeypatch.delenv("COTAHIST_SKIP_DOWNLOAD", raising=False)

    run_results = [
        SimpleNamespace(returncode=0, stdout="", stderr=""),
        SimpleNamespace(returncode=0),
    ]

    with patch.object(worker, "wait_for_cotahist_table", return_value=None), patch.object(
        worker, "start_scraper_audit", side_effect=InterfaceError("stmt", {}, None)
    ), patch.object(worker, "finish_scraper_audit") as finish_audit, patch.object(
        worker.subprocess, "run", side_effect=run_results
    ) as run_mock:
        worker.main()

    finish_audit.assert_not_called()
    assert run_mock.call_count == 2


def test_skip_download_graceful_when_interface_error_on_audit(monkeypatch):
    worker = _load_worker_module()

    fake_config = ModuleType("app.core.config")
    fake_config.settings = SimpleNamespace(
        b3_cotahist_annual_dir="/tmp/cotahist",
        b3_cotahist_year_start=2000,
        b3_cotahist_year_end=2001,
    )

    fake_conn = MagicMock()
    fake_conn.execute.side_effect = [
        SimpleNamespace(scalar=lambda: True),
        SimpleNamespace(scalar=lambda: True),
    ]
    fake_connect_cm = MagicMock()
    fake_connect_cm.__enter__.return_value = fake_conn
    fake_connect_cm.__exit__.return_value = False
    fake_engine = SimpleNamespace(connect=MagicMock(return_value=fake_connect_cm))

    fake_engine_module = ModuleType("app.db.engine")
    fake_engine_module.engine = fake_engine
    fake_engine_module.wait_for_db = MagicMock()

    monkeypatch.setitem(sys.modules, "app.core.config", fake_config)
    monkeypatch.setitem(sys.modules, "app.db.engine", fake_engine_module)
    monkeypatch.setenv("COTAHIST_YEAR", "2024")
    monkeypatch.setenv("COTAHIST_SKIP_DOWNLOAD", "1")

    fake_completed = SimpleNamespace(returncode=0)
    with patch.object(worker, "wait_for_cotahist_table", return_value=None), patch.object(
        worker.subprocess, "run", return_value=fake_completed
    ), patch.object(worker, "start_scraper_audit", side_effect=InterfaceError("stmt", {}, None)), patch.object(
        worker, "finish_scraper_audit"
    ) as finish_audit:
        worker.main()

    finish_audit.assert_not_called()


def test_fetch_failure_still_exits_with_return_code_when_finish_audit_raises(monkeypatch):
    worker = _load_worker_module()

    fake_config = ModuleType("app.core.config")
    fake_config.settings = SimpleNamespace(
        b3_cotahist_annual_dir="/tmp/cotahist",
        b3_cotahist_year_start=2000,
        b3_cotahist_year_end=2001,
    )

    fake_conn = MagicMock()
    fake_conn.execute.side_effect = [
        SimpleNamespace(scalar=lambda: True),
        SimpleNamespace(scalar=lambda: True),
    ]
    fake_connect_cm = MagicMock()
    fake_connect_cm.__enter__.return_value = fake_conn
    fake_connect_cm.__exit__.return_value = False
    fake_engine = SimpleNamespace(connect=MagicMock(return_value=fake_connect_cm))

    fake_engine_module = ModuleType("app.db.engine")
    fake_engine_module.engine = fake_engine
    fake_engine_module.wait_for_db = MagicMock()

    monkeypatch.setitem(sys.modules, "app.core.config", fake_config)
    monkeypatch.setitem(sys.modules, "app.db.engine", fake_engine_module)
    monkeypatch.setenv("COTAHIST_YEAR", "2024")
    monkeypatch.delenv("COTAHIST_SKIP_DOWNLOAD", raising=False)

    bad_fetch = SimpleNamespace(returncode=7, stdout="out", stderr="err")

    with patch.object(worker, "wait_for_cotahist_table", return_value=None), patch.object(
        worker, "start_scraper_audit", return_value=99
    ), patch.object(
        worker, "finish_scraper_audit", side_effect=RuntimeError("finish blew up")
    ), patch.object(worker.subprocess, "run", return_value=bad_fetch):
        with pytest.raises(SystemExit) as exc:
            worker.main()

    assert exc.value.code == 7


def test_success_path_completes_etl_when_finish_audit_raises(monkeypatch, capsys):
    worker = _load_worker_module()

    fake_config = ModuleType("app.core.config")
    fake_config.settings = SimpleNamespace(
        b3_cotahist_annual_dir="/tmp/cotahist",
        b3_cotahist_year_start=2000,
        b3_cotahist_year_end=2001,
    )

    fake_conn = MagicMock()
    fake_conn.execute.side_effect = [
        SimpleNamespace(scalar=lambda: True),
        SimpleNamespace(scalar=lambda: True),
    ]
    fake_connect_cm = MagicMock()
    fake_connect_cm.__enter__.return_value = fake_conn
    fake_connect_cm.__exit__.return_value = False
    fake_engine = SimpleNamespace(connect=MagicMock(return_value=fake_connect_cm))

    fake_engine_module = ModuleType("app.db.engine")
    fake_engine_module.engine = fake_engine
    fake_engine_module.wait_for_db = MagicMock()

    monkeypatch.setitem(sys.modules, "app.core.config", fake_config)
    monkeypatch.setitem(sys.modules, "app.db.engine", fake_engine_module)
    monkeypatch.setenv("COTAHIST_YEAR", "2024")
    monkeypatch.delenv("COTAHIST_SKIP_DOWNLOAD", raising=False)

    def run_side_effect(*args, **kwargs):
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    with patch.object(worker, "wait_for_cotahist_table", return_value=None), patch.object(
        worker, "start_scraper_audit", return_value=42
    ), patch.object(
        worker, "finish_scraper_audit", side_effect=RuntimeError("finish blew up")
    ), patch.object(worker.subprocess, "run", side_effect=run_side_effect):
        worker.main()

    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert "finish_scraper_audit failed" in combined
    assert "status=success" in combined
