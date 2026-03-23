"""Entrypoint tests for CLI scripts.

Tests main() / CLI dispatch for run_b3_quote_batch, run_etl, and scraper scripts.
Scrapers are tested by mocking the scraper class (no real Playwright).
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.core.config import B3_COTAHIST_ANNUAL_DIR_DEFAULT

ROOT = Path(__file__).resolve().parent.parent.parent


# ---------------------------------------------------------------------------
# run_b3_quote_batch.main()
# ---------------------------------------------------------------------------


def _load_quote_batch_module():
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "run_b3_quote_batch",
        ROOT / "scripts" / "run_b3_quote_batch.py",
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["run_b3_quote_batch"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_run_b3_quote_batch_main_explicit_instruments_calls_ingestion(tmp_path):
    instruments_csv = tmp_path / "instruments.csv"
    instruments_csv.write_text("Instrumento financeiro;Ativo\nPETR4;PETR", encoding="utf-8")
    report_path = tmp_path / "report.csv"
    mod = _load_quote_batch_module()
    mock_ingest = MagicMock(return_value=report_path)
    with patch.object(mod, "run_batch_quote_ingestion", mock_ingest):
        with patch("sys.argv", ["run_b3_quote_batch.py", "--instruments", str(instruments_csv)]):
            with patch("sys.exit") as mock_exit:
                mod.main()
    mock_ingest.assert_called_once()
    call_kw = mock_ingest.call_args[1]
    assert call_kw["instruments_csv"] == instruments_csv
    assert call_kw["filter_mode"] == "strict"
    assert call_kw["trades_csv"] is None or call_kw["trades_csv"] == Path(call_kw["trades_csv"])
    mock_exit.assert_not_called()


def test_run_b3_quote_batch_main_filter_mode_fallback(tmp_path):
    instruments_csv = tmp_path / "instruments.csv"
    instruments_csv.write_text("Instrumento financeiro;Ativo\nPETR4;PETR", encoding="utf-8")
    mod = _load_quote_batch_module()
    mock_ingest = MagicMock(return_value=tmp_path / "report.csv")
    with patch.object(mod, "run_batch_quote_ingestion", mock_ingest):
        with patch(
            "sys.argv",
            ["run_b3_quote_batch.py", "--instruments", str(instruments_csv), "--filter-mode", "fallback"],
        ), patch("sys.exit"):
            mod.main()
    call_kw = mock_ingest.call_args[1]
    assert call_kw["filter_mode"] == "fallback"


def test_run_b3_quote_batch_main_exits_when_instruments_not_found(tmp_path):
    # Use a path that does not exist (avoid Windows path quirks with /nonexistent)
    missing = tmp_path / "does_not_exist.csv"
    assert not missing.exists()
    mod = _load_quote_batch_module()
    with patch("sys.argv", ["run_b3_quote_batch.py", "--instruments", str(missing)]):
        with patch("sys.exit") as mock_exit:
            mock_exit.side_effect = SystemExit
            with pytest.raises(SystemExit):
                mod.main()
    mock_exit.assert_called_once_with(1)


# ---------------------------------------------------------------------------
# run_etl.main()
# ---------------------------------------------------------------------------


def _load_run_etl_module():
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "run_etl",
        ROOT / "scripts" / "run_etl.py",
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["run_etl"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_run_etl_main_calls_run_instruments_and_trades_pipeline(tmp_path):
    instruments_csv = tmp_path / "instruments.csv"
    trades_csv = tmp_path / "trades.csv"
    instruments_csv.write_text("x", encoding="utf-8")
    trades_csv.write_text("y", encoding="utf-8")
    cotahist_root = tmp_path / "cotahist_empty"
    cotahist_root.mkdir()
    success_result = {"status": MagicMock(value="success"), "assets_upserted": 1}
    with patch("app.etl.orchestration.pipeline.run_instruments_and_trades_pipeline") as mock_pipeline, patch(
        "app.etl.orchestration.pipeline.run_daily_quotes_pipeline", return_value=success_result
    ), patch("app.etl.orchestration.pipeline.run_intraday_quotes_pipeline", return_value=success_result), patch(
        "app.etl.orchestration.pipeline.run_cotahist_annual_pipeline"
    ) as mock_cot:
        mock_pipeline.return_value = success_result
        with patch("app.core.config.settings") as mock_settings:
            mock_settings.b3_data_dir = str(tmp_path)
            mock_settings.b3_cotahist_annual_dir = str(cotahist_root)
            with patch(
                "sys.argv",
                [
                    "run_etl.py",
                    "--instruments", str(instruments_csv),
                    "--trades", str(trades_csv),
                    "--date", "2026-03-18",
                ],
            ), patch("sys.exit") as mock_exit:
                mod = _load_run_etl_module()
                mod.main()
    mock_pipeline.assert_called()
    mock_cot.assert_not_called()
    mock_exit.assert_called_once_with(0)


def test_run_etl_main_run_daily_quotes_only_calls_daily_quotes_pipeline(tmp_path):
    daily_csv = tmp_path / "negocios.csv"
    daily_csv.write_text("ticker;close\nPETR4;10", encoding="utf-8")
    with patch("app.etl.orchestration.pipeline.run_daily_quotes_pipeline") as mock_dq:
        mock_dq.return_value = {"status": MagicMock(value="success"), "quotes_upserted": 5}
        with patch("app.etl.orchestration.csv_resolver.resolve_instruments_csv", return_value=None):
            with patch("app.core.config.settings") as mock_settings:
                mock_settings.b3_data_dir = str(tmp_path)
                with patch(
                    "sys.argv",
                    ["run_etl.py", "--run-daily-quotes", "--daily-quotes", str(daily_csv), "--date", "2026-03-18"],
                ), patch("sys.exit"):
                    if "run_etl" in sys.modules:
                        del sys.modules["run_etl"]
                    mod = _load_run_etl_module()
                    mod.main()
    mock_dq.assert_called_once()
    assert mock_dq.call_args[0][1] == date(2026, 3, 18)


def test_run_etl_main_run_quotes_only_calls_intraday_quotes_pipeline(tmp_path):
    jsonl = tmp_path / "daily_fluctuation_20260318.jsonl"
    jsonl.write_text("{}", encoding="utf-8")
    with patch("app.etl.orchestration.pipeline.run_intraday_quotes_pipeline") as mock_intraday:
        mock_intraday.return_value = {"status": MagicMock(value="success"), "rows_inserted": 10}
        with patch("app.etl.orchestration.csv_resolver.resolve_instruments_csv", return_value=None):
            with patch("app.core.config.settings") as mock_settings:
                mock_settings.b3_data_dir = str(tmp_path)
                with patch(
                    "sys.argv",
                    ["run_etl.py", "--run-quotes", "--quotes", str(jsonl)],
                ), patch("sys.exit"):
                    if "run_etl" in sys.modules:
                        del sys.modules["run_etl"]
                    mod = _load_run_etl_module()
                    mod.main()
    mock_intraday.assert_called_once()
    assert mock_intraday.call_args[0][0] == jsonl


# ---------------------------------------------------------------------------
# run_b3_scraper.py main() — mock BoletimDiarioScraper
# ---------------------------------------------------------------------------


def test_run_b3_scraper_main_invokes_scraper_with_date():
    with patch("app.scraping.b3.scraper.BoletimDiarioScraper") as mock_scraper_cls:
        mock_instance = MagicMock()
        mock_instance.scrape.return_value = []
        mock_scraper_cls.return_value = mock_instance
        with patch("sys.argv", ["run_b3_scraper.py", "--headless", "--date", "2024-06-14"]):
            if str(ROOT) not in sys.path:
                sys.path.insert(0, str(ROOT))
            if "run_b3_scraper" in sys.modules:
                del sys.modules["run_b3_scraper"]
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "run_b3_scraper",
                ROOT / "scripts" / "run_b3_scraper.py",
            )
            mod = importlib.util.module_from_spec(spec)
            sys.modules["run_b3_scraper"] = mod
            spec.loader.exec_module(mod)
            mod.main()
    mock_scraper_cls.assert_called_once()
    mock_instance.scrape.assert_called_once()
    assert mock_instance.scrape.call_args[0][0] == date(2024, 6, 14)


# ---------------------------------------------------------------------------
# run_b3_scraper_negocios.py main() — mock NegociosConsolidadosScraper
# ---------------------------------------------------------------------------


def test_run_b3_scraper_negocios_main_invokes_scraper_with_date():
    with patch("app.scraping.b3.scraper_negocios.NegociosConsolidadosScraper") as mock_scraper_cls:
        mock_instance = MagicMock()
        mock_instance.scrape.return_value = []
        mock_scraper_cls.return_value = mock_instance
        with patch("sys.argv", ["run_b3_scraper_negocios.py", "--headless", "--date", "2024-06-14"]):
            if str(ROOT) not in sys.path:
                sys.path.insert(0, str(ROOT))
            if "run_b3_scraper_negocios" in sys.modules:
                del sys.modules["run_b3_scraper_negocios"]
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "run_b3_scraper_negocios",
                ROOT / "scripts" / "run_b3_scraper_negocios.py",
            )
            mod = importlib.util.module_from_spec(spec)
            sys.modules["run_b3_scraper_negocios"] = mod
            spec.loader.exec_module(mod)
            mod.main()
    mock_scraper_cls.assert_called_once()
    mock_instance.scrape.assert_called_once()
    assert mock_instance.scrape.call_args[0][0] == date(2024, 6, 14)


# ---------------------------------------------------------------------------
# run_b3_cotahist_annual.main() — acquisition only (no DB / pipeline)
# ---------------------------------------------------------------------------


def _load_run_b3_cotahist_annual_module():
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "run_b3_cotahist_annual",
        ROOT / "scripts" / "run_b3_cotahist_annual.py",
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["run_b3_cotahist_annual"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_run_b3_cotahist_annual_main_dry_run_never_touches_db_or_pipeline():
    mod = _load_run_b3_cotahist_annual_module()
    with patch("app.etl.orchestration.pipeline.run_cotahist_annual_pipeline") as mock_pipeline, patch(
        "app.db.engine.managed_session"
    ) as mock_session, patch.object(mod, "download_cotahist_zip") as mock_dl, patch.object(
        mod, "extract_cotahist_txt"
    ) as mock_ex, patch.object(mod, "parse_cotahist_txt_stats_only") as mock_parse:
        with patch("sys.argv", ["run_b3_cotahist_annual.py", "--dry-run", "--year", "2020"]), patch(
            "sys.exit"
        ):
            mod.main()
    mock_pipeline.assert_not_called()
    mock_session.assert_not_called()
    mock_dl.assert_not_called()
    mock_ex.assert_not_called()
    mock_parse.assert_not_called()


def test_run_b3_cotahist_annual_main_default_parses_without_pipeline(tmp_path):
    mod = _load_run_b3_cotahist_annual_module()
    txt = tmp_path / "2020" / "COTAHIST_A2020.TXT"
    txt.parent.mkdir(parents=True, exist_ok=True)
    txt.write_text("", encoding="utf-8")
    with patch("app.etl.orchestration.pipeline.run_cotahist_annual_pipeline") as mock_pipeline, patch(
        "app.db.engine.managed_session"
    ) as mock_session, patch.object(mod, "download_cotahist_zip"), patch.object(
        mod, "extract_cotahist_txt", return_value=txt
    ), patch.object(mod, "parse_cotahist_txt_stats_only") as mock_parse:
        with patch(
            "sys.argv",
            ["run_b3_cotahist_annual.py", "--year", "2020", "--data-dir", str(tmp_path)],
        ), patch("sys.exit"):
            mod.main()
    mock_pipeline.assert_not_called()
    mock_session.assert_not_called()
    mock_parse.assert_called_once()


# ---------------------------------------------------------------------------
# run_etl.main() — annual COTAHIST
# ---------------------------------------------------------------------------


def test_run_etl_main_run_cotahist_annual_calls_pipeline(tmp_path):
    txt = tmp_path / "COTAHIST_A2020.TXT"
    txt.write_text("x", encoding="utf-8")
    success = {"status": MagicMock(value="success"), "rows_upsert_ops": 1}
    with patch("app.etl.orchestration.pipeline.run_cotahist_annual_pipeline") as mock_cot:
        mock_cot.return_value = success
        with patch("app.core.config.settings") as mock_settings:
            mock_settings.b3_data_dir = str(tmp_path / "missing_b3_dir")
            mock_settings.b3_cotahist_annual_dir = str(tmp_path)
            with patch(
                "sys.argv",
                ["run_etl.py", "--run-cotahist-annual", "--cotahist-txt", str(txt)],
            ), patch("sys.exit"):
                if "run_etl" in sys.modules:
                    del sys.modules["run_etl"]
                mod = _load_run_etl_module()
                mod.main()
    mock_cot.assert_called_once()
    assert mock_cot.call_args[0][0] == txt


def test_run_etl_main_default_runs_cotahist_when_txt_under_annual_root(tmp_path):
    instruments_csv = tmp_path / "instruments.csv"
    trades_csv = tmp_path / "trades.csv"
    instruments_csv.write_text("x", encoding="utf-8")
    trades_csv.write_text("y", encoding="utf-8")
    cotahist_root = tmp_path / "cotahist"
    ydir = cotahist_root / "2020"
    ydir.mkdir(parents=True)
    txt = ydir / "COTAHIST_A2020.TXT"
    txt.write_text("x", encoding="utf-8")
    success_result = {"status": MagicMock(value="success"), "assets_upserted": 1}
    cot_success = {"status": MagicMock(value="success"), "rows_upsert_ops": 1}
    with patch("app.etl.orchestration.pipeline.run_instruments_and_trades_pipeline") as mock_pipeline, patch(
        "app.etl.orchestration.pipeline.run_daily_quotes_pipeline", return_value=success_result
    ), patch("app.etl.orchestration.pipeline.run_intraday_quotes_pipeline", return_value=success_result), patch(
        "app.etl.orchestration.pipeline.run_cotahist_annual_pipeline", return_value=cot_success
    ) as mock_cot:
        mock_pipeline.return_value = success_result
        with patch("app.core.config.settings") as mock_settings:
            mock_settings.b3_data_dir = str(tmp_path)
            mock_settings.b3_cotahist_annual_dir = str(cotahist_root)
            with patch(
                "sys.argv",
                [
                    "run_etl.py",
                    "--instruments",
                    str(instruments_csv),
                    "--trades",
                    str(trades_csv),
                    "--date",
                    "2026-03-18",
                ],
            ), patch("sys.exit") as mock_exit:
                if "run_etl" in sys.modules:
                    del sys.modules["run_etl"]
                mod = _load_run_etl_module()
                mod.main()
    mock_cot.assert_called_once()
    assert mock_cot.call_args[0][0] == txt.resolve()
    mock_exit.assert_called_once_with(0)


def test_run_etl_main_cotahist_only_exits_when_no_txts(tmp_path):
    empty = tmp_path / "cotahist"
    empty.mkdir()
    with patch("app.etl.orchestration.pipeline.run_cotahist_annual_pipeline") as mock_cot:
        with patch("app.core.config.settings") as mock_settings:
            mock_settings.b3_data_dir = str(tmp_path / "no_b3_data")
            mock_settings.b3_cotahist_annual_dir = str(empty)
            with patch("sys.argv", ["run_etl.py", "--run-cotahist-annual"]), patch("sys.exit") as mock_exit:
                mock_exit.side_effect = SystemExit
                if "run_etl" in sys.modules:
                    del sys.modules["run_etl"]
                mod = _load_run_etl_module()
                with pytest.raises(SystemExit):
                    mod.main()
    mock_cot.assert_not_called()
    mock_exit.assert_called_once_with(2)


def test_resolve_cotahist_txt_files_year_range(tmp_path):
    mod = _load_run_etl_module()
    ydir = tmp_path / "2021"
    ydir.mkdir()
    f = ydir / "COTAHIST_A2021.TXT"
    f.write_text("", encoding="utf-8")
    paths = mod.resolve_cotahist_txt_files(
        cotahist_txt=(),
        cotahist_year=None,
        cotahist_from_year=2021,
        cotahist_to_year=2021,
        cotahist_dir=None,
        cotahist_data_dir=tmp_path,
        settings_root="unused",
    )
    assert paths == [f.resolve()]


def test_resolve_cotahist_txt_files_glob_sorted(tmp_path):
    mod = _load_run_etl_module()
    (tmp_path / "a").mkdir()
    f1986 = tmp_path / "a" / "COTAHIST_A1986.TXT"
    f1990 = tmp_path / "a" / "COTAHIST_A1990.TXT"
    f1986.write_text("", encoding="utf-8")
    f1990.write_text("", encoding="utf-8")
    paths = mod.resolve_cotahist_txt_files(
        cotahist_txt=(),
        cotahist_year=None,
        cotahist_from_year=None,
        cotahist_to_year=None,
        cotahist_dir=tmp_path,
        cotahist_data_dir=None,
        settings_root="unused",
    )
    assert paths == [f1986.resolve(), f1990.resolve()]


def test_resolve_cotahist_txt_files_skips_glob_when_not_directory(tmp_path):
    mod = _load_run_etl_module()
    not_a_dir = tmp_path / "not_a_dir.txt"
    not_a_dir.write_text("x", encoding="utf-8")
    paths = mod.resolve_cotahist_txt_files(
        cotahist_txt=(),
        cotahist_year=None,
        cotahist_from_year=None,
        cotahist_to_year=None,
        cotahist_dir=not_a_dir,
        cotahist_data_dir=None,
        settings_root="unused",
    )
    assert paths == []


def test_normalized_cotahist_settings_root_empty_uses_default():
    mod = _load_run_etl_module()
    assert mod._normalized_cotahist_settings_root("") == B3_COTAHIST_ANNUAL_DIR_DEFAULT
    assert mod._normalized_cotahist_settings_root("   ") == B3_COTAHIST_ANNUAL_DIR_DEFAULT
