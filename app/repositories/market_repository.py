"""Aggregated market-day metrics (fact_daily_quotes + fact_daily_trades)."""

from __future__ import annotations

from datetime import date

from sqlalchemy import desc, func, nulls_last, select
from sqlalchemy.orm import Session

from app.db.models import FactDailyQuote, FactDailyTrade


class MarketRepository:
    """Rankings and counts for one ``trade_date``."""

    _rank_limit = 10

    def __init__(self, db: Session) -> None:
        self.db = db

    def overview_for_date(self, trade_date: date) -> dict:
        """Return structured metrics for API layer."""
        by_financial_volume = self._trades_rank(
            trade_date, nulls_last(desc(FactDailyTrade.financial_volume))
        )
        return {
            "trade_date": trade_date,
            "top_gainers": self._top_gainers(trade_date),
            "top_losers": self._top_losers(trade_date),
            # "Volume" = financial turnover (BRL). Same ordering as most_traded_by_financial_volume.
            "most_traded_by_volume": by_financial_volume,
            "most_traded_by_financial_volume": by_financial_volume,
            "highest_trade_count": self._trades_rank(
                trade_date, nulls_last(desc(FactDailyTrade.trade_count))
            ),
            "traded_assets_count": self._traded_assets_count(trade_date),
        }

    def _top_gainers(self, trade_date: date) -> list[dict]:
        stmt = (
            select(
                FactDailyQuote.ticker,
                FactDailyQuote.variation_pct,
                FactDailyQuote.last_price,
            )
            .where(
                FactDailyQuote.trade_date == trade_date,
                FactDailyQuote.variation_pct.isnot(None),
            )
            .order_by(
                desc(FactDailyQuote.variation_pct),
                FactDailyQuote.ticker.asc(),
            )
            .limit(self._rank_limit)
        )
        rows = self.db.execute(stmt).all()
        return [
            {"ticker": r[0], "variation_pct": r[1], "last_price": r[2]}
            for r in rows
        ]

    def _top_losers(self, trade_date: date) -> list[dict]:
        stmt = (
            select(
                FactDailyQuote.ticker,
                FactDailyQuote.variation_pct,
                FactDailyQuote.last_price,
            )
            .where(
                FactDailyQuote.trade_date == trade_date,
                FactDailyQuote.variation_pct.isnot(None),
            )
            .order_by(
                FactDailyQuote.variation_pct.asc(),
                FactDailyQuote.ticker.asc(),
            )
            .limit(self._rank_limit)
        )
        rows = self.db.execute(stmt).all()
        return [
            {"ticker": r[0], "variation_pct": r[1], "last_price": r[2]}
            for r in rows
        ]

    def _trades_rank(
        self,
        trade_date: date,
        order_clause,
    ) -> list[dict]:
        stmt = (
            select(
                FactDailyTrade.ticker,
                FactDailyTrade.trade_count,
                FactDailyTrade.financial_volume,
            )
            .where(FactDailyTrade.trade_date == trade_date)
            .order_by(order_clause, FactDailyTrade.ticker.asc())
            .limit(self._rank_limit)
        )
        rows = self.db.execute(stmt).all()
        return [
            {"ticker": r[0], "trade_count": r[1], "financial_volume": r[2]}
            for r in rows
        ]

    def _traded_assets_count(self, trade_date: date) -> int:
        stmt = select(func.count(func.distinct(FactDailyTrade.ticker))).where(
            FactDailyTrade.trade_date == trade_date
        )
        n = self.db.execute(stmt).scalar_one()
        return int(n or 0)
