"""Unit tests: COTAHIST normalisation."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal

from app.db.models import FactCotahistDaily
from app.etl.parsers.cotahist_parser import CotahistRawQuote
from app.etl.transforms.cotahist_transforms import (
    natural_key_tuple,
    normalize_cotahist_quote,
)


def _minimal_raw() -> CotahistRawQuote:
    return CotahistRawQuote(
        line_no=2,
        raw_line="",
        trade_date_yyyymmdd="20240102",
        cod_bdi="02",
        codneg="PETR4       ",
        tp_merc="010",
        nomres="PETROBRAS   ",
        especi="ON        ",
        prazot="000",
        modref="REAI",
        preabe="0000000001050",
        premax="0000000001050",
        premin="0000000001050",
        premed="0000000001050",
        preult="0000000001050",
        preofc="0000000001050",
        preofv="0000000001050",
        totneg="00150",
        quatot="000000000000010000",
        voltot="000000000000105000",
        preexe="0000000000000",
        indopc="0",
        datven="00000000",
        fatcot="0000001",
        ptoexe="0000000000000",
        codisin="BRPETRACNOR9",
        dismes="001",
    )


def test_normalize_price_scaled():
    row = normalize_cotahist_quote(_minimal_raw(), source_file_name="x.TXT")
    assert row is not None
    assert row["trade_date"] == date(2024, 1, 2)
    assert row["last_price"] == Decimal("10.50")
    # QUATOT is N(18) whole units (no implied decimals per B3 layout).
    assert row["quantity_total"] == Decimal("10000")
    assert row["volume_financial"] == Decimal("1050.00")
    assert row["ticker"] == "PETR4"
    assert row["expiration_date"] is None
    assert row["expiration_key"] == "00000000"


def test_natural_key_stable():
    row = normalize_cotahist_quote(_minimal_raw(), source_file_name="f")
    assert row is not None
    k1 = natural_key_tuple(row)
    k2 = natural_key_tuple(row)
    assert k1 == k2


def test_invalid_date_returns_none():
    raw = replace(_minimal_raw(), trade_date_yyyymmdd="abcdefgh")
    assert normalize_cotahist_quote(raw, source_file_name="f") is None


def test_normalize_payload_keys_match_fact_table_excluding_autogen():
    row = normalize_cotahist_quote(_minimal_raw(), source_file_name="f.TXT")
    assert row is not None
    table_cols = {c.name for c in FactCotahistDaily.__table__.columns}
    payload_keys = set(row)
    expected = table_cols - {"id", "ingested_at"}
    assert payload_keys == expected


def test_dedupe_cotahist_batch_last_row_wins_per_natural_key():
    from app.etl.loaders.db_loader import _dedupe_cotahist_batch

    row = normalize_cotahist_quote(_minimal_raw(), source_file_name="first.TXT")
    assert row is not None
    row2 = {**row, "last_price": row["last_price"] + 1 if row["last_price"] else None, "source_file_name": "second.TXT"}
    out = _dedupe_cotahist_batch([row, row2])
    assert len(out) == 1
    assert out[0]["source_file_name"] == "second.TXT"
