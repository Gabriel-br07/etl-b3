"""Column mapping layer for B3 instrument (CadInstrumento) files.

B3 field names are abbreviated and may change between file versions.
This mapping normalises them to our internal snake_case names.

The mapping is intentionally conservative: only map what we need for the MVP.
Unknown columns are silently ignored.
"""

# Maps B3 column name (case-insensitive key) -> internal name
INSTRUMENT_COLUMN_MAP: dict[str, str] = {
    # Ticker / code
    "codNeg": "ticker",
    "TckrSymb": "ticker",
    "Tckr": "ticker",
    "ticker": "ticker",
    # Asset name
    "NmOfc": "asset_name",
    "SpecfctnCd": "asset_name",
    "asset_name": "asset_name",
    "nome": "asset_name",
    # ISIN
    "ISIN": "isin",
    "isin": "isin",
    # Segment / market
    "Sgmt": "segment",
    "segment": "segment",
    "MktNm": "segment",
    # Date
    "DtRfrn": "source_file_date",
    "source_file_date": "source_file_date",
}

# Maps B3 trade file columns -> internal names
TRADE_COLUMN_MAP: dict[str, str] = {
    # Ticker
    "TckrSymb": "ticker",
    "codNeg": "ticker",
    "ticker": "ticker",
    # Trade date
    "RptDt": "trade_date",
    "DtRfrn": "trade_date",
    "trade_date": "trade_date",
    # Prices
    "LastPric": "last_price",
    "ClsgPric": "last_price",
    "last_price": "last_price",
    "MinPric": "min_price",
    "min_price": "min_price",
    "MaxPric": "max_price",
    "max_price": "max_price",
    "TradAvrgPric": "avg_price",
    "avg_price": "avg_price",
    # Variation
    "OsctnPctg": "variation_pct",
    "variation_pct": "variation_pct",
    # Volume / trades
    "FinInstrmQty": "financial_volume",
    "NtlFinVol": "financial_volume",
    "financial_volume": "financial_volume",
    "TradQty": "trade_count",
    "trade_count": "trade_count",
}


def map_columns(df_columns: list[str], mapping: dict[str, str]) -> dict[str, str]:
    """Build a rename dict from actual DataFrame columns to internal names.

    Only columns present in the mapping are included.
    Matching is case-insensitive.
    """
    lower_map = {k.lower(): v for k, v in mapping.items()}
    result: dict[str, str] = {}
    for col in df_columns:
        internal = lower_map.get(col.lower())
        if internal:
            result[col] = internal
    return result
