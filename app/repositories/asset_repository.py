"""Database repository for DimAsset."""

from sqlalchemy import func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
import logging

from app.db.models import DimAsset
from app.etl.parsers.column_mapping import extract_ticker


# Module-level logger to avoid recreating per-row
logger = logging.getLogger(__name__)

# Max lengths must match the ORM / DB schema defined in app.db.models
_TICKER_MAX = 20
_ISIN_MAX = 20
_ASSET_NAME_MAX = 200
_SEGMENT_MAX = 100
_INSTR_TYPE_MAX = 50


def _sanitize_row(row: dict) -> dict | None:
    """Return a sanitized copy of `row` suitable for DB insert or None to skip.

    Normalizations applied:
      - ticker: required, uppercased, stripped and truncated to _TICKER_MAX
      - isin: uppercased, stripped and truncated to _ISIN_MAX
      - asset_name, segment, instrument_type: stripped and truncated to their limits
      - source_file_date and other fields are left as-is

    If ticker is missing or empty after strip, returns None (row will be skipped).
    """
    if not isinstance(row, dict):
        return None
    r = dict(row)


    ticker = r.get("ticker")
    if ticker is None:
        # Try extracting via shared helper to centralize header variations
        ticker = extract_ticker(r)
    if ticker is None:
        logger.warning("Skipping asset row without ticker: %r", row)
        return None

    # Normalize ticker
    try:
        t = str(ticker).strip().upper()
    except Exception:
        t = ""
    if not t:
        logger.warning("Skipping asset row with empty ticker after normalization: %r", row)
        return None
    if len(t) > _TICKER_MAX:
        logger.warning(
            "Truncating ticker %r to %d chars (row: %r)",
            t,
            _TICKER_MAX,
            row,
        )
        t = t[:_TICKER_MAX]
    r["ticker"] = t

    # ISIN
    isin = r.get("isin")
    if isin is not None:
        try:
            i = str(isin).strip().upper()
        except Exception:
            i = ""
        if i == "":
            r["isin"] = None
        else:
            if len(i) > _ISIN_MAX:
                logger.warning(
                    "Truncating isin %r to %d chars (row: %r)", i, _ISIN_MAX, row
                )
                i = i[:_ISIN_MAX]
            r["isin"] = i

    # Asset name
    an = r.get("asset_name")
    if an is not None:
        try:
            a = str(an).strip()
        except Exception:
            a = ""
        if a == "":
            r["asset_name"] = None
        else:
            if len(a) > _ASSET_NAME_MAX:
                logger.info(
                    "Truncating asset_name for ticker %s to %d chars",
                    r["ticker"],
                    _ASSET_NAME_MAX,
                )
                a = a[:_ASSET_NAME_MAX]
            r["asset_name"] = a

    # Segment
    seg = r.get("segment")
    if seg is not None:
        try:
            s = str(seg).strip()
        except Exception:
            s = ""
        if s == "":
            r["segment"] = None
        else:
            if len(s) > _SEGMENT_MAX:
                logger.info(
                    "Truncating segment for ticker %s to %d chars",
                    r["ticker"],
                    _SEGMENT_MAX,
                )
                s = s[:_SEGMENT_MAX]
            r["segment"] = s

    # Instrument type
    it = r.get("instrument_type")
    if it is not None:
        try:
            itv = str(it).strip()
        except Exception:
            itv = ""
        if itv == "":
            r["instrument_type"] = None
        else:
            if len(itv) > _INSTR_TYPE_MAX:
                logger.info(
                    "Truncating instrument_type for ticker %s to %d chars",
                    r["ticker"],
                    _INSTR_TYPE_MAX,
                )
                itv = itv[:_INSTR_TYPE_MAX]
            r["instrument_type"] = itv

    return r


class AssetRepository:
    """CRUD and upsert operations for dim_assets."""

    def __init__(self, db: Session) -> None:
        self.db = db
        self._logger = logging.getLogger(__name__)

    def get_by_ticker(self, ticker: str) -> DimAsset | None:
        stmt = select(DimAsset).where(DimAsset.ticker == ticker.upper())
        return self.db.execute(stmt).scalar_one_or_none()

    def list_assets(
        self,
        q: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[DimAsset], int]:
        """Return a page of assets and the total count."""
        base = select(DimAsset)
        if q:
            pattern = f"%{q.upper()}%"
            base = base.where(
                or_(
                    func.upper(DimAsset.ticker).like(pattern),
                    func.upper(DimAsset.asset_name).like(pattern),
                )
            )
        total = self.db.execute(
            select(func.count()).select_from(base.subquery())
        ).scalar_one()
        items = self.db.execute(
            base.order_by(DimAsset.ticker).limit(limit).offset(offset)
        ).scalars().all()
        return list(items), total

    def upsert_many(self, rows: list[dict]) -> int:
        """Upsert a list of asset dicts by ticker. Returns affected row count."""
        if not rows:
            return 0

        # Sanitize and enforce max lengths to avoid DB insertion errors
        clean_rows: list[dict] = []
        skipped = 0
        for r in rows:
            cr = _sanitize_row(r)
            if cr is None:
                skipped += 1
                continue
            clean_rows.append(cr)

        if not clean_rows:
            self._logger.warning("All asset rows skipped during sanitization (skipped=%d)", skipped)
            return 0

        stmt = insert(DimAsset).values(clean_rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["ticker"],
            set_={
                "asset_name": stmt.excluded.asset_name,
                "isin": stmt.excluded.isin,
                "segment": stmt.excluded.segment,
                "source_file_date": stmt.excluded.source_file_date,
                "updated_at": func.now(),
            },
        )
        result = self.db.execute(stmt)
        # Commit is handled by the caller (managed_session) to provide atomicity.
        # SQLAlchemy's CursorResult always has `rowcount`; use a type ignore to
        # satisfy static type checkers without masking potential runtime errors.
        return result.rowcount  # type: ignore[attr-defined]
