"""Tests for standalone ETL pipeline functions.
Uses mocking so no real DB connection is needed.
"""
from __future__ import annotations
import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch
from app.core.constants import ETLStatus
# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_instruments_csv(tmp_path: Path) -> Path:
    p = tmp_path / "cadastro_instrumentos_20240614.normalized.csv"
    p.write_text(
        "TckrSymb;CrpnNm;ISIN;SgmtNm\nPETR4;PETROBRAS;BRPETRACNPR6;Novo Mercado\n",
        encoding="utf-8",
    )
    return p
def _make_trades_csv(tmp_path: Path) -> Path:
    p = tmp_path / "negocios_consolidados_20240614.normalized.csv"
    p.write_text(
        "TckrSymb;RptDt;ClsgPric;TtlTradQty\nPETR4;2024-06-14;38.45;1000\n",
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
            {"quote_time": "10:00:00", "close_price": "38.45", "price_fluctuation_percentage": "1.2"}
        ],
    }
    p.write_text(json.dumps(record) + "\n", encoding="utf-8")
    return p
# ---------------------------------------------------------------------------
# run_instruments_and_trades_pipeline
# ---------------------------------------------------------------------------
def test_instruments_and_trades_pipeline_success(tmp_path):
    instruments_csv = _make_instruments_csv(tmp_path)
    trades_file = _make_trades_csv(tmp_path)
    mock_session = MagicMock()
    mock_run = MagicMock(id=1)
    with patch("app.etl.orchestration.pipeline.managed_session") as mock_ctx, \
         patch("app.etl.orchestration.pipeline.ETLRunRepository") as MockRepo, \
         patch("app.etl.orchestration.pipeline.load_assets", return_value=1) as mock_la, \
         patch("app.etl.orchestration.pipeline.load_trades", return_value=1) as mock_lt:
        mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
        repo_inst = MockRepo.return_value
        repo_inst.start_run.return_value = mock_run
        from app.etl.orchestration.pipeline import run_instruments_and_trades_pipeline
        result = run_instruments_and_trades_pipeline(instruments_csv, trades_file, date(2024, 6, 14))
    assert result["status"] == ETLStatus.SUCCESS
    assert result["assets_upserted"] == 1
    assert result["trades_upserted"] == 1
    mock_la.assert_called_once()
    mock_lt.assert_called_once()
def test_instruments_and_trades_pipeline_skips_missing_trades(tmp_path):
    instruments_csv = _make_instruments_csv(tmp_path)
    missing_trades = tmp_path / "nonexistent.csv"
    mock_session = MagicMock()
    mock_run = MagicMock(id=1)
    with patch("app.etl.orchestration.pipeline.managed_session") as mock_ctx, \
         patch("app.etl.orchestration.pipeline.ETLRunRepository") as MockRepo, \
         patch("app.etl.orchestration.pipeline.load_assets", return_value=5) as mock_la, \
         patch("app.etl.orchestration.pipeline.load_trades") as mock_lt:
        mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
        MockRepo.return_value.start_run.return_value = mock_run
        from app.etl.orchestration.pipeline import run_instruments_and_trades_pipeline
        result = run_instruments_and_trades_pipeline(instruments_csv, missing_trades, date(2024, 6, 14))
    assert result["status"] == ETLStatus.SUCCESS
    assert result["assets_upserted"] == 5
    assert result["trades_upserted"] == 0
    mock_lt.assert_not_called()
def test_instruments_and_trades_pipeline_records_failure(tmp_path):
    instruments_csv = _make_instruments_csv(tmp_path)
    mock_session = MagicMock()
    mock_run = MagicMock(id=1)
    with patch("app.etl.orchestration.pipeline.managed_session") as mock_ctx, \
         patch("app.etl.orchestration.pipeline.ETLRunRepository") as MockRepo, \
         patch("app.etl.orchestration.pipeline.load_assets", side_effect=RuntimeError("DB error")):
        mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
        repo_inst = MockRepo.return_value
        repo_inst.start_run.return_value = mock_run
        from app.etl.orchestration.pipeline import run_instruments_and_trades_pipeline
        result = run_instruments_and_trades_pipeline(instruments_csv, None, date(2024, 6, 14))
    assert result["status"] == ETLStatus.FAILED
    assert "error" in result
    repo_inst.finish_run.assert_called_once()
    finish_args = repo_inst.finish_run.call_args
    assert finish_args.args[1] == ETLStatus.FAILED
# ---------------------------------------------------------------------------
# run_intraday_quotes_pipeline
# ---------------------------------------------------------------------------
def test_intraday_quotes_pipeline_success(tmp_path):
    jsonl_path = _make_jsonl(tmp_path)
    mock_session = MagicMock()
    mock_run = MagicMock(id=2)
    from datetime import datetime, timezone
    from decimal import Decimal
    with patch("app.etl.orchestration.pipeline.managed_session") as mock_ctx, \
         patch("app.etl.orchestration.pipeline.ETLRunRepository") as MockRepo, \
         patch(
             "app.etl.orchestration.pipeline.transform_jsonl_quotes",
             return_value=[
                 {
                     "ticker": "PETR4",
                     "quoted_at": datetime(2024, 6, 14, 10, 0, 0, tzinfo=timezone.utc),
                     "trade_date": date(2024, 6, 14),
                     "close_price": Decimal("38.45"),
                     "price_fluctuation_pct": Decimal("1.2"),
                     "source_jsonl": '{"ticker_requested": "PETR4"}',
                 }
             ],
         ) as mock_tjq, \
         patch("app.etl.orchestration.pipeline.load_intraday_quotes", return_value=1) as mock_liq:
        mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
        MockRepo.return_value.start_run.return_value = mock_run
        from app.etl.orchestration.pipeline import run_intraday_quotes_pipeline
        result = run_intraday_quotes_pipeline(jsonl_path)
    assert result["status"] == ETLStatus.SUCCESS
    assert result["rows_inserted"] == 1
    assert result["source_file"] == jsonl_path.name
    mock_tjq.assert_called_once()
    mock_liq.assert_called_once()


def test_intraday_quotes_pipeline_records_failure(tmp_path):
    jsonl_path = _make_jsonl(tmp_path)
    mock_session = MagicMock()
    mock_run = MagicMock(id=3)
    with patch("app.etl.orchestration.pipeline.managed_session") as mock_ctx, \
         patch("app.etl.orchestration.pipeline.ETLRunRepository") as MockRepo, \
         patch("app.etl.orchestration.pipeline.transform_jsonl_quotes", return_value=[{"ticker": "PETR4"}]), \
         patch("app.etl.orchestration.pipeline.load_intraday_quotes", side_effect=RuntimeError("insert fail")):
        mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
        repo_inst = MockRepo.return_value
        repo_inst.start_run.return_value = mock_run
        from app.etl.orchestration.pipeline import run_intraday_quotes_pipeline
        result = run_intraday_quotes_pipeline(jsonl_path)
    assert result["status"] == ETLStatus.FAILED
    repo_inst.finish_run.assert_called_once()
    assert repo_inst.finish_run.call_args.args[1] == ETLStatus.FAILED

# ---------------------------------------------------------------------------
# run_daily_quotes_pipeline
# ---------------------------------------------------------------------------

def _make_quotes_csv(tmp_path: Path) -> Path:
    """Normalized negocios_consolidados CSV with internal column names."""
    p = tmp_path / "negocios_consolidados_20240614.normalized.csv"
    p.write_text(
        "ticker;last_price;min_price;max_price;avg_price;variation_pct;financial_volume;trade_count\n"
        "PETR4;38.45;37.80;39.10;38.00;1.5;1000000;200\n"
        "VALE3;62.30;61.50;63.20;62.00;-0.3;500000;150\n",
        encoding="utf-8",
    )
    return p


def test_daily_quotes_pipeline_success(tmp_path):
    quotes_csv = _make_quotes_csv(tmp_path)
    mock_session = MagicMock()
    mock_run = MagicMock(id=4)
    with patch("app.etl.orchestration.pipeline.managed_session") as mock_ctx, \
         patch("app.etl.orchestration.pipeline.ETLRunRepository") as MockRepo, \
         patch("app.etl.orchestration.pipeline.load_daily_quotes", return_value=2) as mock_ldq:
        mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
        repo_inst = MockRepo.return_value
        repo_inst.start_run.return_value = mock_run
        from app.etl.orchestration.pipeline import run_daily_quotes_pipeline
        result = run_daily_quotes_pipeline(quotes_csv, date(2024, 6, 14))
    assert result["status"] == ETLStatus.SUCCESS
    assert result["quotes_upserted"] == 2
    assert result["target_date"] == "2024-06-14"
    mock_ldq.assert_called_once()
    # Verify open_price is NOT in the rows passed to load_daily_quotes
    call_args = mock_ldq.call_args
    rows_arg = call_args.args[1]
    assert all("open_price" not in r for r in rows_arg), \
        "open_price must not be passed to load_daily_quotes (not in fact_daily_quotes schema)"


def test_daily_quotes_pipeline_records_failure(tmp_path):
    quotes_csv = _make_quotes_csv(tmp_path)
    mock_session = MagicMock()
    mock_run = MagicMock(id=5)
    with patch("app.etl.orchestration.pipeline.managed_session") as mock_ctx, \
         patch("app.etl.orchestration.pipeline.ETLRunRepository") as MockRepo, \
         patch("app.etl.orchestration.pipeline.load_daily_quotes",
               side_effect=RuntimeError("DB insert fail")):
        mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
        repo_inst = MockRepo.return_value
        repo_inst.start_run.return_value = mock_run
        from app.etl.orchestration.pipeline import run_daily_quotes_pipeline
        result = run_daily_quotes_pipeline(quotes_csv, date(2024, 6, 14))
    assert result["status"] == ETLStatus.FAILED
    assert "error" in result
    repo_inst.finish_run.assert_called_once()
    assert repo_inst.finish_run.call_args.args[1] == ETLStatus.FAILED


def test_daily_quotes_pipeline_derives_trade_date_from_target(tmp_path):
    """When no trade_date column in CSV, pipeline uses target_date."""
    # CSV with NO trade_date column – trade_date must come from target_date
    p = tmp_path / "no_date_quotes.csv"
    p.write_text(
        "ticker;last_price\nPETR4;38.45\n",
        encoding="utf-8",
    )
    mock_session = MagicMock()
    mock_run = MagicMock(id=6)
    captured_rows: list = []

    def capture_load(db, rows):
        captured_rows.extend(rows)
        return len(rows)

    with patch("app.etl.orchestration.pipeline.managed_session") as mock_ctx, \
         patch("app.etl.orchestration.pipeline.ETLRunRepository") as MockRepo, \
         patch("app.etl.orchestration.pipeline.load_daily_quotes",
               side_effect=capture_load):
        mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
        MockRepo.return_value.start_run.return_value = mock_run
        from app.etl.orchestration.pipeline import run_daily_quotes_pipeline
        result = run_daily_quotes_pipeline(p, date(2024, 6, 14))

    assert result["status"] == ETLStatus.SUCCESS
    assert len(captured_rows) == 1
    from datetime import date as date_type
    assert captured_rows[0]["trade_date"] == date_type(2024, 6, 14)

