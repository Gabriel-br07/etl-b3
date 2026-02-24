"""Abstract source adapter interface."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass
class SourceFile:
    """Resolved source file metadata."""

    path: Path
    source_name: str
    source_url: str | None
    file_date: date


class B3PublicDataSource(ABC):
    """Interface for B3 public data source adapters."""

    @abstractmethod
    def get_instruments_file(self, target_date: date) -> SourceFile:
        """Resolve and return the instruments (Cadastro) file for a given date."""

    @abstractmethod
    def get_trades_file(self, target_date: date) -> SourceFile:
        """Resolve and return the consolidated trades file for a given date."""
