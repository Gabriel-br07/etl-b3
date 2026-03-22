"""B3 COTAHIST fixed-width layout (245 bytes per line).

Field positions follow the official historical quotations specification; slices
are **0-based [start:end)** Python indices equivalent to B3 1-based inclusive
positions documented in HistoricalQuotations / SeriesHistoricas layout PDF.

Reference implementation cross-check: diogolr/b3parser ``regex.py`` (regex_cotacao).
"""

from __future__ import annotations

RECORD_LEN: int = 245

# Record type codes (positions 1–2).
TIP_HEADER: str = "00"
TIP_QUOTE: str = "01"
TIP_TRAILER: str = "99"

# Type 01 — daily quotation row (slices on full 245-char line).
SLC_TIPREG: slice = slice(0, 2)
SLC_TRADE_DATE: slice = slice(2, 10)  # YYYYMMDD
SLC_COD_BDI: slice = slice(10, 12)
SLC_CODNEG: slice = slice(12, 24)
SLC_TP_MERC: slice = slice(24, 27)
SLC_NOMRES: slice = slice(27, 39)
SLC_ESPECI: slice = slice(39, 49)
SLC_PRAZOT: slice = slice(49, 52)
SLC_MODREF: slice = slice(52, 56)
SLC_PREABE: slice = slice(56, 69)
SLC_PREMAX: slice = slice(69, 82)
SLC_PREMIN: slice = slice(82, 95)
SLC_PREMED: slice = slice(95, 108)
SLC_PREULT: slice = slice(108, 121)
SLC_PREOFC: slice = slice(121, 134)
SLC_PREOFV: slice = slice(134, 147)
SLC_TOTNEG: slice = slice(147, 152)
SLC_QUATOT: slice = slice(152, 170)
SLC_VOLTOT: slice = slice(170, 188)
SLC_PREEXE: slice = slice(188, 201)
SLC_INDOPC: slice = slice(201, 202)
SLC_DATVEN: slice = slice(202, 210)
SLC_FATCOT: slice = slice(210, 217)
SLC_PTOEXE: slice = slice(217, 230)
SLC_CODISIN: slice = slice(230, 242)
SLC_DISMES: slice = slice(242, 245)
