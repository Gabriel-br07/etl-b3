"""Unit tests for asset overview/coverage orchestration (mocked B3)."""

from unittest.mock import MagicMock, patch

from app.use_cases.assets.asset_detail import build_asset_coverage, build_asset_overview


def test_build_asset_coverage_minimal() -> None:
    db = MagicMock()
    assets = MagicMock()
    assets.get_by_ticker.return_value = None
    quotes = MagicMock()
    quotes.min_max_trade_dates.return_value = (None, None)
    trades = MagicMock()
    trades.min_max_trade_dates.return_value = (None, None)
    facts = MagicMock()
    facts.min_max_quoted_at.return_value = (None, None)
    cot = MagicMock()
    cot.min_max_trade_dates.return_value = (None, None)

    with (
        patch("app.use_cases.assets.asset_detail.AssetRepository", return_value=assets),
        patch("app.use_cases.assets.asset_detail.QuoteRepository", return_value=quotes),
        patch("app.use_cases.assets.asset_detail.TradeRepository", return_value=trades),
        patch("app.use_cases.assets.asset_detail.FactQuoteRepository", return_value=facts),
        patch("app.use_cases.assets.asset_detail.CotahistReadRepository", return_value=cot),
    ):
        cov = build_asset_coverage(db, "X")
    assert cov.ticker == "X"
    assert cov.has_asset_master is False


def test_build_asset_overview_b3_failure_returns_null_live() -> None:
    db = MagicMock()
    assets = MagicMock()
    assets.get_by_ticker.return_value = None
    quotes = MagicMock()
    quotes.get_latest_for_ticker.return_value = None
    quotes.min_max_trade_dates.return_value = (None, None)
    trades = MagicMock()
    trades.get_latest_for_ticker.return_value = None
    trades.min_max_trade_dates.return_value = (None, None)
    facts = MagicMock()
    facts.get_latest_for_ticker.return_value = None
    facts.min_max_quoted_at.return_value = (None, None)
    cot = MagicMock()
    cot.min_max_trade_dates.return_value = (None, None)

    with (
        patch("app.use_cases.assets.asset_detail.AssetRepository", return_value=assets),
        patch("app.use_cases.assets.asset_detail.QuoteRepository", return_value=quotes),
        patch("app.use_cases.assets.asset_detail.TradeRepository", return_value=trades),
        patch("app.use_cases.assets.asset_detail.FactQuoteRepository", return_value=facts),
        patch("app.use_cases.assets.asset_detail.CotahistReadRepository", return_value=cot),
        patch(
            "app.use_cases.assets.asset_detail.get_latest_snapshot",
            side_effect=OSError("network"),
        ),
    ):
        ov = build_asset_overview(db, "ZZZZ")
    assert ov.live_snapshot is None
    assert ov.sections["live_b3"].has_data is False
