"""Streaming fixed-width parser for B3 COTAHIST.TXT (245-byte records).

Ingests only type ``01`` rows as quote data; types ``00`` and ``99`` are parsed
for validation/logging and excluded from the quote row iterator.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, TextIO

from app.etl.parsers import cotahist_layout as L

_HEADER_RE = re.compile(
    r"^00COTAHIST\.(?P<year>\d{4})BOVESPA (?P<file_date>\d{8})"
)
_TRAILER_RE = re.compile(
    r"^99COTAHIST\.(?P<year>\d{4})BOVESPA (?P<file_date>\d{8})(?P<num_records>\d{11})"
)


@dataclass
class CotahistHeader:
    """Parsed type-00 header (best-effort)."""

    raw: str
    historic_year: int | None = None
    file_created_yyyymmdd: str | None = None


@dataclass
class CotahistTrailer:
    """Parsed type-99 trailer."""

    raw: str
    historic_year: int | None = None
    file_created_yyyymmdd: str | None = None
    num_data_records: int | None = None


@dataclass
class CotahistRawQuote:
    """Raw substrings from a type-01 line (trim at normalisation stage)."""

    line_no: int
    raw_line: str
    trade_date_yyyymmdd: str
    cod_bdi: str
    codneg: str
    tp_merc: str
    nomres: str
    especi: str
    prazot: str
    modref: str
    preabe: str
    premax: str
    premin: str
    premed: str
    preult: str
    preofc: str
    preofv: str
    totneg: str
    quatot: str
    voltot: str
    preexe: str
    indopc: str
    datven: str
    fatcot: str
    ptoexe: str
    codisin: str
    dismes: str


@dataclass
class CotahistParseStats:
    """Counters for a parse run."""

    lines_read: int = 0
    header_rows: int = 0
    trailer_rows: int = 0
    quote_rows: int = 0
    skipped_wrong_length: int = 0
    skipped_unknown_tip: int = 0
    skipped_malformed_quote: int = 0


def _parse_header(line: str) -> CotahistHeader:
    h = CotahistHeader(raw=line)
    m = _HEADER_RE.match(line.strip())
    if m:
        h.historic_year = int(m.group("year"))
        h.file_created_yyyymmdd = m.group("file_date")
    return h


def _parse_trailer(line: str) -> CotahistTrailer:
    t = CotahistTrailer(raw=line)
    m = _TRAILER_RE.match(line.strip())
    if m:
        t.historic_year = int(m.group("year"))
        t.file_created_yyyymmdd = m.group("file_date")
        t.num_data_records = int(m.group("num_records"))
    return t


def _split_quote(line: str, line_no: int) -> CotahistRawQuote | None:
    if line[L.SLC_TIPREG] != L.TIP_QUOTE:
        return None
    try:
        return CotahistRawQuote(
            line_no=line_no,
            raw_line=line,
            trade_date_yyyymmdd=line[L.SLC_TRADE_DATE],
            cod_bdi=line[L.SLC_COD_BDI],
            codneg=line[L.SLC_CODNEG],
            tp_merc=line[L.SLC_TP_MERC],
            nomres=line[L.SLC_NOMRES],
            especi=line[L.SLC_ESPECI],
            prazot=line[L.SLC_PRAZOT],
            modref=line[L.SLC_MODREF],
            preabe=line[L.SLC_PREABE],
            premax=line[L.SLC_PREMAX],
            premin=line[L.SLC_PREMIN],
            premed=line[L.SLC_PREMED],
            preult=line[L.SLC_PREULT],
            preofc=line[L.SLC_PREOFC],
            preofv=line[L.SLC_PREOFV],
            totneg=line[L.SLC_TOTNEG],
            quatot=line[L.SLC_QUATOT],
            voltot=line[L.SLC_VOLTOT],
            preexe=line[L.SLC_PREEXE],
            indopc=line[L.SLC_INDOPC],
            datven=line[L.SLC_DATVEN],
            fatcot=line[L.SLC_FATCOT],
            ptoexe=line[L.SLC_PTOEXE],
            codisin=line[L.SLC_CODISIN],
            dismes=line[L.SLC_DISMES],
        )
    except IndexError:
        return None


def iter_cotahist_quote_rows(
    path: Path | str,
    *,
    encoding: str = "latin-1",
) -> Iterator[tuple[CotahistRawQuote, CotahistParseStats]]:
    """Yield ``(raw_quote, mutable_stats)`` for each type-01 row.

    *stats* is the same object on every iteration; mutate it only externally
    after iteration if you need a final snapshot, or read fields from the
    last yield.
    """
    path = Path(path)
    stats = CotahistParseStats()
    with path.open(encoding=encoding, newline="") as fh:
        yield from _iter_from_file(fh, stats, source_label=str(path))


def _iter_from_file(fh: TextIO, stats: CotahistParseStats, source_label: str) -> Iterator[tuple[CotahistRawQuote, CotahistParseStats]]:
    _ = source_label
    for line_no, raw in enumerate(fh, start=1):
        line = raw.rstrip("\r\n")
        stats.lines_read += 1
        if len(line) != L.RECORD_LEN:
            stats.skipped_wrong_length += 1
            continue
        tip = line[L.SLC_TIPREG]
        if tip == L.TIP_HEADER:
            stats.header_rows += 1
            continue
        if tip == L.TIP_TRAILER:
            stats.trailer_rows += 1
            continue
        if tip != L.TIP_QUOTE:
            stats.skipped_unknown_tip += 1
            continue
        q = _split_quote(line, line_no)
        if q is None:
            stats.skipped_malformed_quote += 1
            continue
        stats.quote_rows += 1
        yield q, stats


def parse_cotahist_file_metadata(path: Path | str, *, encoding: str = "latin-1") -> tuple[CotahistHeader | None, CotahistTrailer | None, CotahistParseStats]:
    """Scan file once for first header, last trailer, and aggregate stats."""
    path = Path(path)
    stats = CotahistParseStats()
    header: CotahistHeader | None = None
    trailer: CotahistTrailer | None = None
    with path.open(encoding=encoding, newline="") as fh:
        for line_no, raw in enumerate(fh, start=1):
            line = raw.rstrip("\r\n")
            stats.lines_read += 1
            if len(line) != L.RECORD_LEN:
                stats.skipped_wrong_length += 1
                continue
            tip = line[L.SLC_TIPREG]
            if tip == L.TIP_HEADER:
                stats.header_rows += 1
                if header is None:
                    header = _parse_header(line)
            elif tip == L.TIP_TRAILER:
                stats.trailer_rows += 1
                trailer = _parse_trailer(line)
            elif tip == L.TIP_QUOTE:
                q = _split_quote(line, line_no)
                if q is None:
                    stats.skipped_malformed_quote += 1
                else:
                    stats.quote_rows += 1
            else:
                stats.skipped_unknown_tip += 1
    return header, trailer, stats
