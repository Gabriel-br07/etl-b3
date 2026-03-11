"""Column mapping layer for B3 instrument (CadInstrumento) files.

B3 field names are abbreviated and may change between file versions.
This mapping normalises them to our internal snake_case names.

The mapping is intentionally conservative: only map what we need for the MVP.
Unknown columns are silently ignored.

Note on header normalization collisions
--------------------------------------
When multiple mapping keys normalise to the same canonical form (for example
"Variação" and "Variacao"), the later key will overwrite the earlier one
in the internal lookup. A warning is logged when this happens so maintainers
are aware of potentially ambiguous headers. The overwrite is intentional to
keep the left-most (DataFrame) column selection logic in `map_columns` simple.
"""

from __future__ import annotations

import unicodedata
import re
import logging

# Restore human-readable mapping keys (accented where appropriate). Ensure no keys that
# normalize to the same canonical form are repeated (case/accent variants removed).
INSTRUMENT_COLUMN_MAP = {
    "codNeg": "ticker",
    "TckrSymb": "ticker",
    "Tckr": "ticker",
    "ticker": "ticker",
    "Instrumento financeiro": "ticker",
    "Ativo": "ticker",
    "NmOfc": "asset_name",
    "SpecfctnCd": "asset_name",
    "asset_name": "asset_name",
    "nome": "asset_name",
    "Descrição do ativo": "asset_name",
    "ISIN": "isin",
    "isin": "isin",
    "Código ISIN": "isin",
    "Sgmt": "segment",
    "segment": "segment",
    "MktNm": "segment",
    "Segmento": "segment",
    "Mercado": "segment",
    "DtRfrn": "source_file_date",
    "source_file_date": "source_file_date",
    "Data de expiração": "source_file_date",
}

# Maps B3 trade file columns -> internal names
TRADE_COLUMN_MAP = {
    "TckrSymb": "ticker",
    "codNeg": "ticker",
    "ticker": "ticker",
    "Instrumento financeiro": "ticker",
    "Ativo": "ticker",
    "RptDt": "trade_date",
    "DtRfrn": "trade_date",
    "trade_date": "trade_date",
    "Data início negócio": "trade_date",
    "Data": "trade_date",
    "DtNeg": "trade_date",
    "DtNegoc": "trade_date",
    "DtPregao": "trade_date",
    "DataPregao": "trade_date",
    "Data do negócio": "trade_date",
    "Dt": "trade_date",
    "Date": "trade_date",
    "TradeDate": "trade_date",
    "trade date": "trade_date",
    "LastPric": "last_price",
    "ClsgPric": "last_price",
    "last_price": "last_price",
    "MinPric": "min_price",
    "min_price": "min_price",
    "MaxPric": "max_price",
    "max_price": "max_price",
    "TradAvrgPric": "avg_price",
    "avg_price": "avg_price",
    "Preço de abertura": "open_price",
    "Preço mínimo": "min_price",
    "Preço máximo": "max_price",
    "Preço médio": "avg_price",
    "Preço de fechamento": "last_price",
    "Preo de abertura": "open_price",
    "Preo mnimo": "min_price",
    "Preo mximo": "max_price",
    "Preo mdio": "avg_price",
    "Preo de fechamento": "last_price",
    "Preo de referncia": "last_price",
    "Preco Minimo": "min_price",
    "Preco Maximo": "max_price",
    "Preco Medio": "avg_price",
    "Preco de Referencia": "last_price",
    "open_price": "open_price",
    "OsctnPctg": "variation_pct",
    "variation_pct": "variation_pct",
    "Oscilao": "variation_pct",
    "Variao": "variation_pct",
    "Oscilacao": "variation_pct",
    "Variação": "variation_pct",
    "FinInstrmQty": "financial_volume",
    "NtlFinVol": "financial_volume",
    "financial_volume": "financial_volume",
    "TradQty": "trade_count",
    "trade_count": "trade_count",
    "Quantidade de negócios": "trade_count",
    "Quantidade de negócio": "trade_count",
    "Quantidade de contratos": "trade_count",
    "Volume financeiro": "financial_volume",
    "Num. de Negocios": "trade_count",
    "Num de Negocios": "trade_count",
    "Número de Negócios": "trade_count",
    "Qtd. Negocios": "trade_count",
    "Qtd Negocios": "trade_count",
    "Vol. Financeiro": "financial_volume",
    "Volume Financeiro": "financial_volume",
    "Vlm Financeiro": "financial_volume",
    "TtlTradQty": "trade_count",
    "TtlFinVol": "financial_volume",
    "Quantidade de negcios": "trade_count",
}


def _normalize_name(name: str) -> str:
    """Normalize a header/mapping key for case-insensitive, accent-insensitive matching.

    Steps:
      - ensure str
      - strip leading/trailing whitespace
      - apply Unicode NFKD and remove combining marks (accents)
      - collapse whitespace to single spaces
      - remove non-word characters except spaces
      - lowercase

    Note: This function is used to normalize both DataFrame headers and mapping
    keys in the column mapping dictionaries. Collisions can occur if multiple
    different headers or mapping keys normalise to the same canonical form.
    In that case, later mappings will overwrite earlier ones in the internal
    lookup, and a warning is logged. The left-most DataFrame column still wins
    when producing the final rename map in `map_columns`.
    """
    if not isinstance(name, str):
        name = str(name or "")
    s = name.strip()
    # Normalize accents: decompose and remove combining marks
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    # Replace multiple whitespace/newlines/tabs with single space
    s = re.sub(r"\s+", " ", s)
    # Remove punctuation except spaces (keep letters, numbers and spaces)
    s = re.sub(r"[^\w\s]", "", s)
    return s.lower()


# Precompute normalized lookup maps for the builtin mappings to avoid
# rebuilding them on every call and to ensure collision messages are emitted
# only once at import time (and at DEBUG level to avoid spamming production
# logs). For any other mapping provided at runtime, we build a transient
# normalized map on demand but log collisions at DEBUG.
def _build_normalized_map(mapping: dict[str, str], mapping_name: str | None = None, log_level: int = logging.DEBUG) -> dict[str, str]:
    logger = logging.getLogger(__name__)
    norm: dict[str, str] = {}
    original_key: dict[str, str] = {}
    for k, v in mapping.items():
        nk = _normalize_name(k)
        if nk in norm:
            logger.log(
                log_level,
                "Header normalization collision in %s: '%s' (maps to %s) conflicts with '%s' (maps to %s) - overwriting with latest",
                mapping_name or "<mapping>",
                k,
                v,
                original_key.get(nk, "<unknown>"),
                norm.get(nk),
            )
        norm[nk] = v
        original_key[nk] = k
    return norm


# Precomputed normalized maps for the two built-in mapping dicts
INSTRUMENT_NORMALIZED_MAP = _build_normalized_map(INSTRUMENT_COLUMN_MAP, mapping_name="INSTRUMENT_COLUMN_MAP")
TRADE_NORMALIZED_MAP = _build_normalized_map(TRADE_COLUMN_MAP, mapping_name="TRADE_COLUMN_MAP")


def map_columns(df_columns: list[str], mapping: dict[str, str]) -> dict[str, str]:
    """Build a rename dict from actual DataFrame columns to internal names.

    Only columns present in the mapping are included.
    Matching is case-insensitive and accent-insensitive.

    Note: If multiple source columns map to the same internal name we prefer
    the first occurrence (left-most column in the DataFrame) and skip later
    ones to avoid creating duplicate column names when renaming.

    When building the internal normalized lookup from mapping keys, multiple
    mapping keys may normalise to the same canonical form. In that case a
    warning is logged showing the collision and the later mapping will
    overwrite the earlier one in the normalized lookup. The left-most
    DataFrame column still wins when producing the final rename map.
    """
    logger = logging.getLogger(__name__)

    # Use precomputed normalized maps for the built-in mappings to avoid
    # repeated work and repeated log noise. For other mappings, build a
    # transient normalized map and emit collisions at DEBUG level only.
    if mapping is INSTRUMENT_COLUMN_MAP:
        lower_map = INSTRUMENT_NORMALIZED_MAP
    elif mapping is TRADE_COLUMN_MAP:
        lower_map = TRADE_NORMALIZED_MAP
    else:
        # transient normalized map for custom mappings
        lower_map = _build_normalized_map(mapping, mapping_name=None, log_level=logging.DEBUG)

    result: dict[str, str] = {}
    used_internals: set[str] = set()
    for col in df_columns:
        ncol = _normalize_name(col)
        internal = lower_map.get(ncol)
        if internal:
            if internal in used_internals:
                # Skip this mapping to avoid duplicate internal column names.
                logger.debug(
                    "Skipping mapping for column %s -> %s because %s is already mapped",
                    col,
                    internal,
                    internal,
                )
                continue
            result[col] = internal
            used_internals.add(internal)
    return result


def extract_ticker(row: dict) -> str | None:
    """Extract a ticker string from *row* using INSTRUMENT_COLUMN_MAP heuristics.

    Tries the following (in order):
      - direct 'ticker' key in the row
      - any mapping key in INSTRUMENT_COLUMN_MAP that maps to 'ticker'
        matched against the normalized row keys.

    Returns an uppercased, stripped ticker string when found, otherwise None.
    """
    if not isinstance(row, dict):
        return None

    # Fast path: common keys
    direct = row.get("ticker") or row.get("TckrSymb") or row.get("codNeg")
    if direct is not None:
        try:
            t = str(direct).strip().upper()
        except Exception:
            return None
        return t if t != "" else None

    # Build normalized lookup of row keys
    norm_row = { _normalize_name(k): v for k, v in row.items() }
    # Use precomputed normalized mapping for instruments
    norm_map = INSTRUMENT_NORMALIZED_MAP

    for nk, internal in norm_map.items():
        if internal != "ticker":
            continue
        if nk in norm_row:
            val = norm_row.get(nk)
            if val is None:
                continue
            try:
                t = str(val).strip().upper()
            except Exception:
                continue
            if t:
                return t
    return None
