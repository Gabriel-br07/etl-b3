from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

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
    ):
        worker.main()

    fake_conn.commit.assert_called_once()
