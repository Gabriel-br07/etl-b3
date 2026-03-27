from __future__ import annotations

from datetime import date
from unittest.mock import patch

from app.core.config import settings
from app.etl.orchestration.prefect.flows.daily_scraping_flow import (
    daily_scraping_flow,
    default_daily_parameters,
)
from app.etl.orchestration.prefect.tasks.scraping_tasks import scrape_cotahist_task


def test_daily_scraping_flow_happy_path(tmp_path):
    cadastro = tmp_path / "cadastro.csv"
    negocios = tmp_path / "negocios.csv"
    cadastro.write_text("ticker\nPETR4", encoding="utf-8")
    negocios.write_text("ticker;trade_date\nPETR4;2026-03-26", encoding="utf-8")

    with patch(
        "app.etl.orchestration.prefect.flows.daily_scraping_flow.scrape_cadastro_task"
    ) as mock_cad, patch(
        "app.etl.orchestration.prefect.flows.daily_scraping_flow.scrape_negocios_task"
    ) as mock_neg, patch(
        "app.etl.orchestration.prefect.flows.daily_scraping_flow.scrape_cotahist_task"
    ) as mock_cot, patch(
        "app.etl.orchestration.prefect.flows.daily_scraping_flow.validate_outputs_task"
    ) as mock_val, patch(
        "app.etl.orchestration.prefect.flows.daily_scraping_flow.run_intraday_quote_batch_task"
    ) as mock_intra, patch(
        "app.etl.orchestration.prefect.flows.daily_scraping_flow.trigger_transform_load_task"
    ) as mock_handoff:
        mock_cad.return_value = cadastro
        mock_neg.return_value = negocios
        mock_cot.return_value = [tmp_path / "2026" / "COTAHIST_A2026.TXT"]
        mock_val.return_value = {"ok": True}
        mock_intra.return_value = tmp_path / "report.csv"
        mock_handoff.return_value = {"status": "success"}

        result = daily_scraping_flow(target_date=date(2026, 3, 26), run_intraday=True)

    assert result["target_date"] == "2026-03-26"
    assert result["validation"]["ok"] is True
    assert result["handoff"]["status"] == "success"
    mock_handoff.assert_called_once()
    assert mock_handoff.call_args.kwargs["cotahist_txt_paths"] == [tmp_path / "2026" / "COTAHIST_A2026.TXT"]


def test_daily_scraping_flow_without_intraday(tmp_path):
    cadastro = tmp_path / "cadastro.csv"
    negocios = tmp_path / "negocios.csv"
    cadastro.write_text("ticker\nPETR4", encoding="utf-8")
    negocios.write_text("ticker;trade_date\nPETR4;2026-03-26", encoding="utf-8")

    with patch(
        "app.etl.orchestration.prefect.flows.daily_scraping_flow.scrape_cadastro_task",
        return_value=cadastro,
    ), patch(
        "app.etl.orchestration.prefect.flows.daily_scraping_flow.scrape_negocios_task",
        return_value=negocios,
    ), patch(
        "app.etl.orchestration.prefect.flows.daily_scraping_flow.scrape_cotahist_task",
        return_value=None,
    ), patch(
        "app.etl.orchestration.prefect.flows.daily_scraping_flow.validate_outputs_task",
        return_value={"ok": True},
    ), patch(
        "app.etl.orchestration.prefect.flows.daily_scraping_flow.run_intraday_quote_batch_task"
    ) as mock_intra, patch(
        "app.etl.orchestration.prefect.flows.daily_scraping_flow.trigger_transform_load_task",
        return_value={"status": "success"},
    ):
        daily_scraping_flow(target_date=date(2026, 3, 26), run_intraday=False)

    mock_intra.assert_not_called()


def test_scrape_cotahist_task_fn_runs_download_when_enabled(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "b3_cotahist_annual_dir", str(tmp_path))
    monkeypatch.setattr(settings, "b3_cotahist_year_start", 2023)
    monkeypatch.setattr(settings, "b3_cotahist_year_end", 2023)
    txt = tmp_path / "2023" / "COTAHIST_A2023.TXT"
    txt.parent.mkdir(parents=True, exist_ok=True)
    txt.write_text("00header\n", encoding="utf-8")
    with patch(
        "app.etl.orchestration.prefect.tasks.scraping_tasks.download_cotahist_zip"
    ) as mock_dl, patch(
        "app.etl.orchestration.prefect.tasks.scraping_tasks.extract_cotahist_txt",
        return_value=txt,
    ), patch(
        "app.etl.orchestration.prefect.tasks.scraping_tasks.start_scraper_audit",
        return_value=1,
    ), patch(
        "app.etl.orchestration.prefect.tasks.scraping_tasks.finish_scraper_audit",
    ):
        out = scrape_cotahist_task.fn(target_date=date(2023, 6, 1), enabled=True)
    assert out == [txt]
    mock_dl.assert_called_once()
    args, _kwargs = mock_dl.call_args
    y, zip_path = args
    assert y == 2023
    assert zip_path.name == "COTAHIST_A2023.zip"


def test_default_daily_parameters_disable_cotahist_by_default(monkeypatch):
    monkeypatch.delenv("PREFECT_RUN_COTAHIST", raising=False)
    params = default_daily_parameters()
    assert params["run_cotahist"] is False


def test_default_daily_parameters_enable_cotahist_when_explicit(monkeypatch):
    monkeypatch.setenv("PREFECT_RUN_COTAHIST", "true")
    params = default_daily_parameters()
    assert params["run_cotahist"] is True


def test_daily_scraping_flow_default_path_passes_run_cotahist_false(tmp_path):
    cadastro = tmp_path / "cadastro.csv"
    negocios = tmp_path / "negocios.csv"
    cadastro.write_text("ticker\nPETR4", encoding="utf-8")
    negocios.write_text("ticker;trade_date\nPETR4;2026-03-26", encoding="utf-8")

    with patch(
        "app.etl.orchestration.prefect.flows.daily_scraping_flow.scrape_cadastro_task",
        return_value=cadastro,
    ), patch(
        "app.etl.orchestration.prefect.flows.daily_scraping_flow.scrape_negocios_task",
        return_value=negocios,
    ), patch(
        "app.etl.orchestration.prefect.flows.daily_scraping_flow.scrape_cotahist_task",
        return_value=None,
    ) as mock_cot, patch(
        "app.etl.orchestration.prefect.flows.daily_scraping_flow.validate_outputs_task",
        return_value={"ok": True},
    ), patch(
        "app.etl.orchestration.prefect.flows.daily_scraping_flow.run_intraday_quote_batch_task"
    ), patch(
        "app.etl.orchestration.prefect.flows.daily_scraping_flow.trigger_transform_load_task",
        return_value={"status": "success"},
    ):
        daily_scraping_flow(target_date=date(2026, 3, 26))

    assert mock_cot.call_args.kwargs["enabled"] is False
