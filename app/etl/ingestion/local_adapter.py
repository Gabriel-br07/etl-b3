"""Local-file fallback source adapter.

Reads files from a configurable local directory (B3_DATA_DIR).
File naming convention expected:
  - Instruments: CadInstrumento_<YYYY-MM-DD>.csv  (or CadInstrumento.csv for latest)
  - Trades:      NegociosConsolidados_<YYYY-MM-DD>.zip (or NegociosConsolidados.zip)

This adapter is the MVP default. Remote discovery is a separate adapter (TODO).
"""

from datetime import date
from pathlib import Path

from app.core.config import settings
from app.core.constants import SourceName
from app.core.logging import get_logger
from app.etl.ingestion.base import B3PublicDataSource, SourceFile

logger = get_logger(__name__)

_INSTRUMENT_PATTERNS = [
    "{name}_{date}.csv",
    "{name}_{date}.CSV",
    "{name}.csv",
    "{name}.CSV",
]

_TRADES_PATTERNS = [
    "{name}_{date}.zip",
    "{name}_{date}.ZIP",
    "{name}_{date}.csv",
    "{name}_{date}.CSV",
    "{name}.zip",
    "{name}.ZIP",
    "{name}.csv",
    "{name}.CSV",
]


def _resolve(base_dir: Path, name: str, target_date: date, patterns: list[str]) -> Path:
    date_str = target_date.strftime("%Y-%m-%d")
    for pattern in patterns:
        candidate = base_dir / pattern.format(name=name, date=date_str)
        if candidate.exists():
            logger.info("Resolved local file: %s", candidate)
            return candidate
    raise FileNotFoundError(
        f"No local file found for '{name}' on {date_str} in {base_dir}. "
        f"Tried patterns: {patterns}"
    )


class LocalFileAdapter(B3PublicDataSource):
    """Read B3 source files from a local directory (fallback / MVP mode)."""

    def __init__(self, data_dir: str | Path | None = None) -> None:
        self.data_dir = Path(data_dir or settings.b3_data_dir)

    def get_instruments_file(self, target_date: date) -> SourceFile:
        path = _resolve(self.data_dir, SourceName.INSTRUMENTS, target_date, _INSTRUMENT_PATTERNS)
        return SourceFile(
            path=path,
            source_name=SourceName.INSTRUMENTS,
            source_url=None,
            file_date=target_date,
        )

    def get_trades_file(self, target_date: date) -> SourceFile:
        path = _resolve(self.data_dir, SourceName.TRADES, target_date, _TRADES_PATTERNS)
        return SourceFile(
            path=path,
            source_name=SourceName.TRADES,
            source_url=None,
            file_date=target_date,
        )
