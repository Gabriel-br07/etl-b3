"""Normalise raw COTAHIST type-01 rows into dicts for ``fact_cotahist_daily``.

Price fields use implied decimals (N(11)V99 → /100) per HistoricalQuotations.
``QUATOT`` is N(18) (plain integer). ``VOLTOT`` is N(16)V99 (18 digits, /100).

Coexistence with daily pipelines
--------------------------------
Rows are **only** written to ``fact_cotahist_daily``. They are **not** merged into
``fact_daily_quotes`` or ``fact_quotes``. See module docstring on
``FactCotahistDaily`` and README for source-of-truth rules.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from app.core.logging import get_logger
from app.etl.parsers.cotahist_parser import CotahistRawQuote

logger = get_logger(__name__)


def _trim(s: str) -> str:
    return (s or "").strip()


def _parse_yyyymmdd(raw: str) -> date | None:
    raw = _trim(raw)
    if len(raw) != 8 or not raw.isdigit():
        return None
    try:
        y, m, d = int(raw[:4]), int(raw[4:6]), int(raw[6:8])
        return date(y, m, d)
    except ValueError:
        return None


def _parse_int_field(raw: str) -> int | None:
    raw = _trim(raw)
    if not raw or not raw.isdigit():
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _parse_price_13(raw: str) -> Decimal | None:
    """N(11)V99 — 13 digit string, scale /100."""
    raw = _trim(raw)
    if not raw or not raw.isdigit():
        return None
    try:
        return Decimal(raw) / Decimal(100)
    except (InvalidOperation, ValueError):
        return None


def _parse_n18_integer(raw: str) -> Decimal | None:
    """QUATOT (positions 153–170): N(18) — whole units, no implied decimals."""
    raw = _trim(raw)
    if not raw or not raw.isdigit():
        return None
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError):
        return None


def _parse_voltot_n16v99(raw: str) -> Decimal | None:
    """VOLTOT (positions 171–188): N(16)V99 — 18 digit string, scale /100."""
    raw = _trim(raw)
    if not raw or not raw.isdigit():
        return None
    try:
        return Decimal(raw) / Decimal(100)
    except (InvalidOperation, ValueError):
        return None


def _parse_ptoexe_13(raw: str) -> Decimal | None:
    """N(7)V999999 — 13 digits, scale / 1_000_000."""
    raw = _trim(raw)
    if not raw or not raw.isdigit():
        return None
    try:
        return Decimal(raw) / Decimal(1_000_000)
    except (InvalidOperation, ValueError):
        return None


def _prazo_term_days(raw: str) -> int:
    raw = _trim(raw)
    if not raw or not raw.isdigit():
        return -1
    try:
        return int(raw)
    except ValueError:
        return -1


def _expiration_key(raw: str) -> str:
    raw = _trim(raw)
    if len(raw) != 8:
        return "00000000"
    if not raw.isdigit():
        return "00000000"
    if raw == "00000000":
        return "00000000"
    return raw


def _expiration_date(raw: str) -> date | None:
    key = _expiration_key(raw)
    if key == "00000000":
        return None
    return _parse_yyyymmdd(key)


def natural_key_tuple(row: dict[str, Any]) -> tuple[Any, ...]:
    """Business key used for idempotency and optional in-file duplicate detection."""
    return (
        row["trade_date"],
        row["codneg"],
        row["cod_bdi"],
        row["tp_merc"],
        row["especi"],
        row["prazo_term_days"],
        row["moeda_ref"],
        row["expiration_key"],
        row["isin"],
        row["distribution_num"],
        row["ind_opc"],
    )


def normalize_cotahist_quote(
    raw: CotahistRawQuote,
    *,
    source_file_name: str,
) -> dict[str, Any] | None:
    """Map a raw type-01 row to a DB row dict, or None if invalid."""
    trade_date = _parse_yyyymmdd(raw.trade_date_yyyymmdd)
    if trade_date is None:
        logger.debug(
            "cotahist line %s: invalid trade_date %r",
            raw.line_no,
            raw.trade_date_yyyymmdd,
        )
        return None

    codneg = _trim(raw.codneg).upper()
    if not codneg:
        return None

    cod_bdi = _trim(raw.cod_bdi)[:2].ljust(2)[:2]
    tp_merc = _trim(raw.tp_merc)[:3].ljust(3)[:3]
    especi = _trim(raw.especi)[:10]
    moeda_ref = _trim(raw.modref)[:4].ljust(4)[:4]
    prazo_term_days = _prazo_term_days(raw.prazot)
    expiration_key = _expiration_key(raw.datven)
    expiration_date = _expiration_date(raw.datven)
    isin = _trim(raw.codisin)[:12]
    distribution_num = (raw.dismes or "")[:3]
    if len(distribution_num) < 3:
        distribution_num = distribution_num.ljust(3)
    ind_opc = _trim(raw.indopc)[:1] or " "

    fatcot_raw = _parse_int_field(raw.fatcot)

    return {
        "trade_date": trade_date,
        "ticker": codneg[:20],
        "codneg": codneg[:12],
        "cod_bdi": cod_bdi if cod_bdi.strip() else "  ",
        "tp_merc": tp_merc if tp_merc.strip() else "000",
        "nomres": _trim(raw.nomres)[:12] or None,
        "especi": especi,
        "prazo_term_days": prazo_term_days,
        "moeda_ref": moeda_ref if moeda_ref.strip() else "    ",
        "open_price": _parse_price_13(raw.preabe),
        "max_price": _parse_price_13(raw.premax),
        "min_price": _parse_price_13(raw.premin),
        "avg_price": _parse_price_13(raw.premed),
        "last_price": _parse_price_13(raw.preult),
        "best_bid": _parse_price_13(raw.preofc),
        "best_ask": _parse_price_13(raw.preofv),
        "trade_count": _parse_int_field(raw.totneg),
        "quantity_total": _parse_n18_integer(raw.quatot),
        "volume_financial": _parse_voltot_n16v99(raw.voltot),
        "strike_price": _parse_price_13(raw.preexe),
        "ind_opc": ind_opc,
        "expiration_date": expiration_date,
        "expiration_key": expiration_key,
        "quotation_factor": fatcot_raw,
        "strike_points": _parse_ptoexe_13(raw.ptoexe),
        "isin": isin,
        "distribution_num": distribution_num,
        "source_file_name": source_file_name[:255],
    }
