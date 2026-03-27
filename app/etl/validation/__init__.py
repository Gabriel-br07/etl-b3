"""Validation package for scraping outputs."""

from app.etl.validation.scraping_output_validator import ValidationResult, validate_scraped_csv

__all__ = ["ValidationResult", "validate_scraped_csv"]
