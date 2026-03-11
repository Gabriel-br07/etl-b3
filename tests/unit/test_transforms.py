from datetime import date, datetime, timezone
from decimal import Decimal

import polars as pl
import pytest

from app.etl.transforms.b3_transforms import (
    ESSENTIAL_TRADE_COLUMNS,
    OPTIONAL_TRADE_COLUMNS,
    REQUIRED_TRADE_COLUMNS,
    transform_daily_quotes,
    transform_instruments,
    transform_jsonl_quotes,
    transform_trades,
)

from tests.fixtures.polars_builders import (
    make_instruments_df,
    make_trades_df,
    make_parsed_rows,
    make_quotes_df,
)


def test_transform_instruments_uppercases_tickers():
    # Arrange
    df = make_instruments_df()
    # Act
    rows = transform_instruments(df, date(2024, 6, 14))
    # Assert
    tickers = [r["ticker"] for r in rows]
    assert all(t == t.upper() for t in tickers)


def test_transform_instruments_drops_empty_tickers():
    df = make_instruments_df()
    rows = transform_instruments(df, date(2024, 6, 14))
    tickers = [r["ticker"] for r in rows]
    assert "" not in tickers


def test_transform_instruments_deduplicates():
    df = pl.DataFrame({"ticker": ["PETR4", "PETR4"], "asset_name": ["A", "B"]})
    rows = transform_instruments(df, date(2024, 6, 14))
    assert len(rows) == 1


def test_transform_instruments_adds_file_date():
    df = pl.DataFrame({"ticker": ["PETR4"], "asset_name": ["PETROBRAS PN"]})
    rows = transform_instruments(df, date(2024, 6, 14))
    assert rows[0]["source_file_date"] == date(2024, 6, 14)


def test_transform_trades_parses_numeric():
    df = make_trades_df()
    rows = transform_trades(df, "NegociosConsolidados_2024-06-14.zip")
    petr = next(r for r in rows if r["ticker"] == "PETR4")
    assert petr["last_price"] == pytest.approx(38.45, abs=0.01)


def test_transform_trades_parses_dates():
    df = make_trades_df()
    rows = transform_trades(df, "test.zip")
    assert rows[0]["trade_date"] == date(2024, 6, 14)


def test_transform_trades_adds_source_file_name():
    df = make_trades_df()
    rows = transform_trades(df, "my_source_file.zip")
    assert rows[0]["source_file_name"] == "my_source_file.zip"


def test_transform_instruments_empty_df_returns_empty():
    df = pl.DataFrame({"ticker": [], "asset_name": []})
    rows = transform_instruments(df, date(2024, 6, 14))
    assert rows == []


# Robustness tests

def test_transform_trades_derives_trade_date_from_target_date():
    df = pl.DataFrame({"ticker": ["PETR4", "VALE3"], "last_price": ["38.45", "62.30"]})
    target = date(2024, 6, 14)
    rows = transform_trades(df, "no_date_in_name.csv", target_date=target)
    assert len(rows) == 2
    assert all(r["trade_date"] == target for r in rows)


def test_transform_trades_derives_trade_date_from_filename():
    df = pl.DataFrame({"ticker": ["PETR4"], "last_price": ["38.45"]})
    rows = transform_trades(df, "NegociosConsolidados_2024-06-14.csv")
    assert len(rows) == 1
    assert rows[0]["trade_date"] == date(2024, 6, 14)


def test_transform_trades_returns_empty_when_no_date_source():
    df = pl.DataFrame({"ticker": ["PETR4"], "last_price": ["38.45"]})
    rows = transform_trades(df, "no_date.csv")
    assert rows == []


def test_transform_trades_returns_empty_when_ticker_missing():
    df = pl.DataFrame({"trade_date": ["2024-06-14"], "last_price": ["38.45"]})
    rows = transform_trades(df, "trades.csv", target_date=date(2024, 6, 14))
    assert rows == []


def test_transform_trades_missing_optional_columns_do_not_abort():
    df = pl.DataFrame({"ticker": ["PETR4", "VALE3"], "trade_date": ["2024-06-14", "2024-06-14"]})
    rows = transform_trades(df, "trades.csv")
    assert len(rows) == 2
    assert "last_price" in rows[0]
    assert rows[0]["last_price"] is None


def test_transform_trades_partial_price_columns():
    df = pl.DataFrame({"ticker": ["PETR4"], "trade_date": ["2024-06-14"], "last_price": ["38.45"]})
    rows = transform_trades(df, "trades.csv")
    assert len(rows) == 1
    assert rows[0]["last_price"] == pytest.approx(38.45, abs=0.01)
    assert rows[0]["min_price"] is None
    assert rows[0]["max_price"] is None


def test_transform_trades_all_columns_present():
    df = pl.DataFrame({
        "ticker": ["PETR4"],
        "trade_date": ["2024-06-14"],
        "open_price": ["37.00"],
        "last_price": ["38.45"],
        "min_price": ["37.00"],
        "max_price": ["39.10"],
        "avg_price": ["38.00"],
        "variation_pct": ["1.5"],
        "financial_volume": ["1000000.0"],
        "trade_count": ["500"],
    })
    rows = transform_trades(df, "NegociosConsolidados_2024-06-14.csv")
    assert len(rows) == 1
    r = rows[0]
    assert r["ticker"] == "PETR4"
    assert r["trade_date"] == date(2024, 6, 14)
    assert r["open_price"] == pytest.approx(37.0, abs=0.01)
    assert r["financial_volume"] == pytest.approx(1_000_000.0)
    assert r["trade_count"] == pytest.approx(500)


def test_required_trade_columns_constant():
    assert "ticker" in ESSENTIAL_TRADE_COLUMNS
    assert "trade_date" in ESSENTIAL_TRADE_COLUMNS
    assert "last_price" in OPTIONAL_TRADE_COLUMNS
    assert ESSENTIAL_TRADE_COLUMNS | OPTIONAL_TRADE_COLUMNS == REQUIRED_TRADE_COLUMNS


# Daily quotes

def test_transform_daily_quotes_happy_path():
    df = make_quotes_df()
    rows = transform_daily_quotes(df, "negocios_consolidados_20240614.csv")
    assert len(rows) == 2
    r = next(r for r in rows if r["ticker"] == "PETR4")
    assert r["trade_date"] == date(2024, 6, 14)
    assert r["last_price"] == pytest.approx(38.45, abs=0.01)


def test_transform_daily_quotes_excludes_open_price():
    df = make_quotes_df()
    df = df.with_columns(pl.lit("37.00").alias("open_price"))
    rows = transform_daily_quotes(df, "negocios_consolidados_20240614.csv")
    assert len(rows) > 0
    for row in rows:
        assert "open_price" not in row


def test_transform_daily_quotes_no_extra_columns_pass_through():
    df = make_quotes_df()
    df = df.with_columns(pl.lit("extra").alias("some_unknown_column"))
    rows = transform_daily_quotes(df, "test.csv")
    assert len(rows) > 0
    for row in rows:
        assert "some_unknown_column" not in row


def test_transform_daily_quotes_derives_trade_date_from_target_date():
    df = pl.DataFrame({"ticker": ["PETR4"], "last_price": ["38.45"]})
    target = date(2024, 6, 14)
    rows = transform_daily_quotes(df, "no_date.csv", target_date=target)
    assert len(rows) == 1
    assert rows[0]["trade_date"] == target


def test_transform_daily_quotes_derives_trade_date_from_filename():
    df = pl.DataFrame({"ticker": ["PETR4"], "last_price": ["38.45"]})
    rows = transform_daily_quotes(df, "negocios_consolidados_20240614.csv")
    assert len(rows) == 1
    assert rows[0]["trade_date"] == date(2024, 6, 14)


def test_transform_daily_quotes_returns_empty_when_no_date():
    df = pl.DataFrame({"ticker": ["PETR4"], "last_price": ["38.45"]})
    rows = transform_daily_quotes(df, "no_date_at_all.csv")
    assert rows == []


def test_transform_daily_quotes_returns_empty_when_ticker_missing():
    df = pl.DataFrame({"trade_date": ["2024-06-14"], "last_price": ["38.45"]})
    rows = transform_daily_quotes(df, "test.csv", target_date=date(2024, 6, 14))
    assert rows == []


def test_transform_daily_quotes_missing_optional_cols_produce_nulls():
    df = pl.DataFrame({"ticker": ["PETR4"], "trade_date": ["2024-06-14"]})
    rows = transform_daily_quotes(df, "test.csv")
    assert len(rows) == 1
    assert rows[0]["last_price"] is None
    assert rows[0]["min_price"] is None


def test_transform_daily_quotes_handles_numeric_dtype_columns():
    df = pl.DataFrame({
        "ticker": ["PETR4"],
        "trade_date": ["2024-06-14"],
        "last_price": [38.45],
        "min_price": [37.80],
        "max_price": [39.10],
        "avg_price": [38.00],
        "variation_pct": [1.5],
        "financial_volume": [1_000_000.0],
        "trade_count": [200],
    })
    rows = transform_daily_quotes(df, "test.csv")
    assert len(rows) == 1
    assert rows[0]["last_price"] == pytest.approx(38.45, abs=0.01)


def test_transform_daily_quotes_comma_decimal_separator():
    df = pl.DataFrame({"ticker": ["PETR4"], "trade_date": ["2024-06-14"], "last_price": ["38,45"]})
    rows = transform_daily_quotes(df, "test.csv")
    assert len(rows) == 1
    assert rows[0]["last_price"] == pytest.approx(38.45, abs=0.01)


def test_transform_daily_quotes_uppercase_tickers():
    df = pl.DataFrame({"ticker": ["petr4", "vale3"], "trade_date": ["2024-06-14", "2024-06-14"]})
    rows = transform_daily_quotes(df, "test.csv")
    assert all(r["ticker"] == r["ticker"].upper() for r in rows)


def test_transform_daily_quotes_deduplicates():
    df = pl.DataFrame({"ticker": ["PETR4", "PETR4"], "trade_date": ["2024-06-14", "2024-06-14"], "last_price": ["38.45", "39.00"]})
    rows = transform_daily_quotes(df, "test.csv")
    assert len(rows) == 1


def test_transform_daily_quotes_source_file_name():
    df = make_quotes_df()
    rows = transform_daily_quotes(df, "my_source.csv")
    assert all(r["source_file_name"] == "my_source.csv" for r in rows)


def test_transform_daily_quotes_drops_null_tickers():
    df = pl.DataFrame({"ticker": ["PETR4", None, "", "VALE3"], "trade_date": ["2024-06-14", "2024-06-14", "2024-06-14", "2024-06-14"]})
    rows = transform_daily_quotes(df, "test.csv")
    assert len(rows) == 2
    assert {r["ticker"] for r in rows} == {"PETR4", "VALE3"}


# JSONL quotes

def test_transform_jsonl_quotes_passthrough_parse_jsonl_output():
    rows = make_parsed_rows()
    out = transform_jsonl_quotes(rows, source_name="test.jsonl")
    assert len(out) == 2
    r = next(r for r in out if r["ticker"] == "PETR4")
    assert r["quoted_at"] == datetime(2024, 6, 14, 10, 0, 0, tzinfo=timezone.utc)
    assert r["trade_date"] == date(2024, 6, 14)
    assert r["close_price"] == Decimal("38.45")
    assert r["price_fluctuation_pct"] == Decimal("1.2")


def test_transform_jsonl_quotes_maps_symbol_to_ticker():
    rows = [{
        "symbol": "itub4",
        "quoted_at": datetime(2024, 6, 14, 10, 0, tzinfo=timezone.utc),
        "trade_date": date(2024, 6, 14),
        "close_price": Decimal("32.00"),
        "price_fluctuation_pct": Decimal("0.5"),
    }]
    out = transform_jsonl_quotes(rows)
    assert len(out) == 1
    assert out[0]["ticker"] == "ITUB4"


def test_transform_jsonl_quotes_maps_timestamp_to_quoted_at():
    rows = [{
        "ticker": "BBAS3",
        "timestamp": "2024-06-14T11:30:00",
        "trade_date": date(2024, 6, 14),
        "close_price": Decimal("20.00"),
        "price_fluctuation_pct": None,
    }]
    out = transform_jsonl_quotes(rows)
    assert len(out) == 1
    assert out[0]["quoted_at"] == datetime(2024, 6, 14, 11, 30, 0, tzinfo=timezone.utc)


def test_transform_jsonl_quotes_maps_price_to_close_price():
    rows = [{
        "ticker": "MGLU3",
        "quoted_at": datetime(2024, 6, 14, 9, 0, tzinfo=timezone.utc),
        "trade_date": date(2024, 6, 14),
        "price": "10.50",
        "price_fluctuation_pct": None,
    }]
    out = transform_jsonl_quotes(rows)
    assert len(out) == 1
    assert out[0]["close_price"] == Decimal("10.50")


def test_transform_jsonl_quotes_maps_variation_pct_to_price_fluctuation():
    rows = [{
        "ticker": "WEGE3",
        "quoted_at": datetime(2024, 6, 14, 9, 0, tzinfo=timezone.utc),
        "trade_date": date(2024, 6, 14),
        "close_price": Decimal("40.00"),
        "variation_pct": "-1.2",
    }]
    out = transform_jsonl_quotes(rows)
    assert len(out) == 1
    assert out[0]["price_fluctuation_pct"] == Decimal("-1.2")


def test_transform_jsonl_quotes_derives_trade_date_from_quoted_at():
    rows = [{
        "ticker": "PETR4",
        "quoted_at": datetime(2024, 6, 14, 10, 0, tzinfo=timezone.utc),
        "close_price": Decimal("38.45"),
        "price_fluctuation_pct": None,
    }]
    out = transform_jsonl_quotes(rows)
    assert len(out) == 1
    assert out[0]["trade_date"] == date(2024, 6, 14)


def test_transform_jsonl_quotes_uppercases_ticker():
    rows = make_parsed_rows()
    rows[0]["ticker"] = "petr4"
    out = transform_jsonl_quotes(rows)
    assert out[0]["ticker"] == "PETR4"


def test_transform_jsonl_quotes_skips_missing_ticker():
    rows = [{
        "quoted_at": datetime(2024, 6, 14, 10, 0, tzinfo=timezone.utc),
        "trade_date": date(2024, 6, 14),
        "close_price": Decimal("38.45"),
        "price_fluctuation_pct": None,
    }]
    out = transform_jsonl_quotes(rows)
    assert out == []


def test_transform_jsonl_quotes_skips_missing_quoted_at():
    rows = [{
        "ticker": "PETR4",
        "trade_date": date(2024, 6, 14),
        "close_price": Decimal("38.45"),
        "price_fluctuation_pct": None,
    }]
    out = transform_jsonl_quotes(rows)
    assert out == []


def test_transform_jsonl_quotes_empty_input():
    out = transform_jsonl_quotes([])
    assert out == []


def test_transform_jsonl_quotes_output_schema():
    rows = make_parsed_rows()
    out = transform_jsonl_quotes(rows)
    required = {"ticker", "quoted_at", "trade_date", "close_price", "price_fluctuation_pct", "source_jsonl"}
    for row in out:
        assert required.issubset(row.keys()), f"Missing keys: {required - row.keys()}"


def test_transform_jsonl_quotes_naive_datetime_gets_utc():
    rows = [{
        "ticker": "PETR4",
        "quoted_at": datetime(2024, 6, 14, 10, 0, 0),
        "trade_date": date(2024, 6, 14),
        "close_price": Decimal("38.45"),
        "price_fluctuation_pct": None,
    }]
    out = transform_jsonl_quotes(rows)
    assert len(out) == 1
    assert out[0]["quoted_at"].tzinfo is not None
    assert out[0]["quoted_at"].tzinfo == timezone.utc


def test_transform_jsonl_quotes_comma_decimal_in_string_price():
    rows = [{
        "ticker": "VALE3",
        "quoted_at": datetime(2024, 6, 14, 10, 0, tzinfo=timezone.utc),
        "trade_date": date(2024, 6, 14),
        "close_price": "62,30",
        "price_fluctuation_pct": None,
    }]
    out = transform_jsonl_quotes(rows)
    assert len(out) == 1
    assert out[0]["close_price"] == Decimal("62.30")


# ---------------------------------------------------------------------------
# Regression tests for specific bug fixes
# ---------------------------------------------------------------------------


def test_parse_date_column_br_format():
    """_parse_date_column must fall back to %d/%m/%Y when dates are in BR format.

    Previously strict=False caused the first format (%Y-%m-%d) to return a
    series of null values without raising, so Brazilian-format dates like
    "14/06/2024" were silently lost.
    """
    from app.etl.transforms.b3_transforms import _parse_date_column
    import polars as pl

    series = pl.Series(["14/06/2024", "15/06/2024"])
    result = _parse_date_column(series)
    assert result[0] == date(2024, 6, 14)
    assert result[1] == date(2024, 6, 15)


def test_parse_date_column_yyyymmdd_format():
    """_parse_date_column must fall back to %Y%m%d format."""
    from app.etl.transforms.b3_transforms import _parse_date_column
    import polars as pl

    series = pl.Series(["20240614", "20240615"])
    result = _parse_date_column(series)
    assert result[0] == date(2024, 6, 14)
    assert result[1] == date(2024, 6, 15)


def test_transform_trades_parses_br_date_format():
    """transform_trades must correctly parse trade_date in dd/mm/yyyy format."""
    df = pl.DataFrame({
        "ticker": ["PETR4"],
        "trade_date": ["14/06/2024"],
        "last_price": ["38.45"],
    })
    rows = transform_trades(df, "NegociosConsolidados_14-06-2024.csv")
    assert len(rows) == 1
    assert rows[0]["trade_date"] == date(2024, 6, 14)


def test_transform_daily_quotes_parses_br_date_format():
    """transform_daily_quotes must correctly parse trade_date in dd/mm/yyyy format."""
    df = pl.DataFrame({
        "ticker": ["PETR4"],
        "trade_date": ["14/06/2024"],
        "last_price": ["38.45"],
    })
    rows = transform_daily_quotes(df, "negocios_20240614.csv")
    assert len(rows) == 1
    assert rows[0]["trade_date"] == date(2024, 6, 14)


def test_transform_jsonl_quotes_time_only_string_is_skipped():
    """transform_jsonl_quotes must not accept time-only strings as quoted_at.

    A time-only string like "10:30:00" produces datetime(1900, 1, 1, ...)
    which is a bogus trade date. Such rows must be skipped rather than
    written to the DB with an incorrect 1900-01-01 date.
    """
    rows = [{
        "ticker": "PETR4",
        "time": "10:30:00",  # time-only string – no date part
        "trade_date": date(2024, 6, 14),
        "close_price": Decimal("38.45"),
        "price_fluctuation_pct": None,
    }]
    out = transform_jsonl_quotes(rows)
    # Row must be skipped because quoted_at cannot be parsed from a time-only value.
    assert out == []

