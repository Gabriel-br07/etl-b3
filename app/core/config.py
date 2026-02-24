"""Application configuration and settings."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central settings loaded from environment variables / .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application
    app_env: str = "development"

    # Database
    database_url: str = "postgresql://etlb3:etlb3pass@localhost:5432/etlb3"

    # B3 data directory (local fallback mode)
    b3_data_dir: str = "data/sample"

    # B3 source URLs
    b3_bulletin_entrypoint_url: str = (
        "https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/"
        "market-data/consultas/boletim-diario/boletim-diario-do-mercado/"
    )
    b3_instruments_url_template: str | None = None
    b3_trades_url_template: str | None = None

    # Logging
    log_level: str = "INFO"

    # API metadata
    api_title: str = "ETL B3 API"
    api_version: str = "0.1.0"


settings = Settings()
