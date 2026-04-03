"""Unit tests for Prefect schedule builders (BRT timezone)."""

from __future__ import annotations

import datetime
import logging

import pytest

from app.etl.orchestration.prefect.serve import (
    build_daily_registry_schedule,
    build_intraday_interval_schedule,
    _daily_registry_cron,
    _intraday_interval_minutes,
)


def test_daily_registry_schedule_uses_brt_timezone(monkeypatch):
    monkeypatch.delenv("PREFECT_DAILY_REGISTRY_CRON", raising=False)
    s = build_daily_registry_schedule()
    assert s.cron == "0 8 * * *"
    assert s.timezone == "America/Sao_Paulo"


def test_daily_registry_cron_overridable(monkeypatch):
    monkeypatch.setenv("PREFECT_DAILY_REGISTRY_CRON", "30 7 * * *")
    assert _daily_registry_cron() == "30 7 * * *"
    s = build_daily_registry_schedule()
    assert s.cron == "30 7 * * *"
    assert s.timezone == "America/Sao_Paulo"


def test_intraday_interval_defaults_30_minutes_brt(monkeypatch):
    monkeypatch.delenv("PREFECT_INTRADAY_INTERVAL_MINUTES", raising=False)
    s = build_intraday_interval_schedule()
    assert s.interval == datetime.timedelta(minutes=30)
    assert s.timezone == "America/Sao_Paulo"
    assert s.anchor_date.year == 2020 and s.anchor_date.month == 1 and s.anchor_date.day == 1


def test_intraday_interval_minutes_overridable(monkeypatch):
    monkeypatch.setenv("PREFECT_INTRADAY_INTERVAL_MINUTES", "15")
    assert _intraday_interval_minutes() == 15
    s = build_intraday_interval_schedule()
    assert s.interval.total_seconds() == 15 * 60


def test_intraday_interval_invalid_env_falls_back_to_30(monkeypatch, caplog):
    monkeypatch.setenv("PREFECT_INTRADAY_INTERVAL_MINUTES", "not-a-number")
    with caplog.at_level(logging.WARNING, logger="app.etl.orchestration.prefect.serve"):
        assert _intraday_interval_minutes() == 30
    assert "invalid PREFECT_INTRADAY_INTERVAL_MINUTES" in caplog.text


def test_daily_registry_invalid_cron_raises(monkeypatch):
    monkeypatch.setenv("PREFECT_DAILY_REGISTRY_CRON", "not a cron")
    with pytest.raises(ValueError):
        build_daily_registry_schedule()
