from datetime import date, datetime, timezone
from decimal import Decimal

import polars as pl


def make_instruments_df():
    return pl.DataFrame(
        {
            "ticker": ["petr4", "VALE3", " ITUB4 ", ""],
            "asset_name": ["PETROBRAS PN", "VALE ON", "ITAU PN", "INVALID"],
            "isin": ["BR123", "BR456", "BR789", None],
            "segment": ["NM", "NM", "NM", None],
        }
    )


def make_trades_df():
    return pl.DataFrame(
        {
            "ticker": ["PETR4", "VALE3", "ITUB4"],
            "trade_date": ["2024-06-14", "2024-06-14", "2024-06-14"],
            "last_price": ["38,45", "62.30", "32.15"],
            "min_price": ["37.80", "61.50", "31.80"],
            "max_price": ["39.10", "63.20", "32.60"],
        }
    )


def make_parsed_rows():
    return [
        {
            "ticker": "PETR4",
            "quoted_at": datetime(2024, 6, 14, 10, 0, 0, tzinfo=timezone.utc),
            "trade_date": date(2024, 6, 14),
            "close_price": Decimal("38.45"),
            "price_fluctuation_pct": Decimal("1.2"),
            "source_jsonl": "daily_fluctuation_20240614T100000.jsonl",
        },
        {
            "ticker": "VALE3",
            "quoted_at": datetime(2024, 6, 14, 10, 0, 0, tzinfo=timezone.utc),
            "trade_date": date(2024, 6, 14),
            "close_price": Decimal("62.30"),
            "price_fluctuation_pct": Decimal("-0.35"),
            "source_jsonl": "daily_fluctuation_20240614T100000.jsonl",
        },
    ]


def make_quotes_df(**kwargs):
    data = {
        "ticker": ["PETR4", "VALE3"],
        "trade_date": ["2024-06-14", "2024-06-14"],
        "last_price": ["38.45", "62.30"],
        "min_price": ["37.80", "61.50"],
        "max_price": ["39.10", "63.20"],
        "avg_price": ["38.00", "62.00"],
        "variation_pct": ["1.5", "-0.3"],
        "financial_volume": ["1000000.00", "500000.00"],
        "trade_count": ["200", "150"],
    }
    data.update(kwargs)
    return pl.DataFrame(data)

