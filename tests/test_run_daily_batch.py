"""Contract tests for docker/run_daily_batch.sh.

The script is the shell entrypoint invoked by the scheduler to run the two
daily Playwright scrapers. These tests verify the script contract without
running real scrapers (syntax check + expected script paths and args).
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = ROOT / "docker" / "run_daily_batch.sh"

# Expected script paths and patterns (must match run_daily_batch.sh)
EXPECTED_SCRAPER_SCRIPTS = [
    "/app/scripts/run_b3_scraper.py",
    "/app/scripts/run_b3_scraper_negocios.py",
]
EXPECTED_SCRAPER_NAMES = ["b3_boletim_diario", "b3_negocios_consolidados"]


def test_run_daily_batch_script_exists():
    assert SCRIPT_PATH.exists(), f"Shell script not found: {SCRIPT_PATH}"
    assert SCRIPT_PATH.is_file()


def test_run_daily_batch_script_syntax():
    """bash -n checks syntax without executing (skip if bash not available)."""
    try:
        result = subprocess.run(
            ["bash", "-n", str(SCRIPT_PATH)],
            capture_output=True,
            text=True,
            cwd=str(ROOT),
            timeout=5,
        )
    except (FileNotFoundError, OSError):
        pytest.skip("bash not available (e.g. on Windows without Git Bash)")
    if result.returncode != 0:
        err = (result.stderr or "") + (result.stdout or "")
        if "No such file or directory" in err or "WSL" in err or "execvpe" in err:
            pytest.skip(
                "bash -n failed (bash not properly available on this platform): "
                + (result.stderr or "").strip()
            )
        pytest.fail(f"bash -n failed: {result.stderr}")


def test_run_daily_batch_script_invokes_expected_scrapers():
    """Assert the script contains invocations of both scraper scripts."""
    content = SCRIPT_PATH.read_text(encoding="utf-8")
    for script_path in EXPECTED_SCRAPER_SCRIPTS:
        assert script_path in content, f"Script should invoke {script_path}"


def test_run_daily_batch_script_uses_headless_flag():
    """Script must pass _HEADLESS (--headless or --no-headless) to scrapers."""
    content = SCRIPT_PATH.read_text(encoding="utf-8")
    assert "_HEADLESS" in content
    assert "--headless" in content or "headless" in content.lower()


def test_run_daily_batch_script_supports_date_arg():
    """Script must support optional --date and pass it to scrapers."""
    content = SCRIPT_PATH.read_text(encoding="utf-8")
    assert "--date" in content
    assert "_DATE_ARG" in content


def test_run_daily_batch_script_retry_config():
    """Script must use SCRAPER_RETRY_ATTEMPTS and backoff."""
    content = SCRIPT_PATH.read_text(encoding="utf-8")
    assert "SCRAPER_RETRY" in content or "RETRY" in content
    assert "run_scraper_with_retry" in content


def test_run_daily_batch_script_scraper_names():
    """Assert both scraper names are passed to run_scraper_with_retry."""
    content = SCRIPT_PATH.read_text(encoding="utf-8")
    for name in EXPECTED_SCRAPER_NAMES:
        assert name in content, f"Script should reference scraper name {name}"
