"""Shared constants and enumerations."""

from enum import StrEnum


class ETLStatus(StrEnum):
    """Status values for ETL run records."""

    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


class SourceName(StrEnum):
    """Known B3 source file names."""

    INSTRUMENTS = "CadInstrumento"
    TRADES = "NegociosConsolidados"


class SourceMode(StrEnum):
    """Source acquisition mode."""

    LOCAL = "local"
    REMOTE = "remote"
