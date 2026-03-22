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

    # Database — override with DATABASE_URL env var in Docker
    database_url: str = "postgresql://etlb3:etlb3pass@localhost:5432/etlb3"
    # db_pool settings
    db_pool_size: int = 5
    db_max_overflow: int = 10
    # Recycle connections after this many seconds (prevents "server closed the connection" errors)
    db_pool_recycle: int = 1800  # 1800 seconds (30 minutes)

    # B3 data directory (local fallback mode)
    b3_data_dir: str = "data/sample"

    # B3 source URLs
    b3_bulletin_entrypoint_url: str = (
        "https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/"
        "market-data/consultas/boletim-diario/boletim-diario-do-mercado/"
    )
    b3_instruments_url_template: str | None = None
    b3_trades_url_template: str | None = None

    # B3 scraper output directories
    b3_output_dir: str = "data/raw"
    b3_screenshots_dir: str = "data/screenshots"
    b3_trace_dir: str = "data/traces"

    # Playwright browser settings
    playwright_headless: bool = False  # default False – show browser during development
    playwright_slow_mo: int = 0        # ms between Playwright actions (0 = off)
    playwright_timeout_ms: int = 30_000
    playwright_downloads_dir: str = "data/raw"

    # Playwright explicit pauses (ms)
    # Pause right after opening the entrypoint page to allow dynamic content to render
    playwright_pause_after_open_ms: int = 3000
    # Pause specifically before clicking the 'Renda variável' tab
    playwright_pause_before_renda_variavel_ms: int = 1500
    # Small pause between interactive actions (clicks/selects)
    playwright_pause_between_actions_ms: int = 800

    # ---------------------------------------------------------------------------
    # B3 live quote snapshot (DailyFluctuationHistory public endpoint)
    # ---------------------------------------------------------------------------

    #: Base URL for the B3 public market-data service (cotacao.b3.com.br).
    #: Full endpoint: {b3_quote_base_url}/DailyFluctuationHistory/{ticker}
    b3_quote_base_url: str = "https://cotacao.b3.com.br/mds/api/v1"

    #: Public page visited during session warm-up to obtain cookies naturally.
    b3_quote_warm_session_url: str = (
        "https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/"
        "market-data/consultas/mercado-a-vista/variacao-diaria/mercado-continuo/"
    )

    #: Request timeout in seconds for the quote HTTP client.
    b3_quote_timeout: float = 15.0


    #: Enable HTTP/2 for the quote client (reduces latency when supported).
    b3_quote_http2: bool = False

    #: Per-ticker in-memory cache TTL in seconds.  Set to 0 to disable cache.
    b3_quote_cache_ttl: int = 300  # 5 minutes

    #: Maximum number of retry attempts for quote requests.
    #: Env var: B3_QUOTE_MAX_RETRIES
    b3_quote_max_retries: int = 3

    # ---------------------------------------------------------------------------
    # B3 annual COTAHIST (SerHist ZIP / fixed-width TXT)
    # ---------------------------------------------------------------------------

    #: Base URL for annual files, e.g. {base}/COTAHIST_A1986.zip
    b3_cotahist_base_url: str = "https://bvmf.bmfbovespa.com.br/InstDados/SerHist"

    #: HTTP timeout (seconds) for COTAHIST downloads.
    b3_cotahist_timeout: float = 120.0

    #: Max retries per year (exponential backoff between attempts).
    b3_cotahist_max_retries: int = 4

    #: Optional pause (seconds) between successful year downloads.
    b3_cotahist_delay_between_years: float = 0.0

    #: Default inclusive year range for bulk download (overridable via CLI).
    b3_cotahist_year_start: int = 1986
    b3_cotahist_year_end: int = 2026

    #: Root for annual COTAHIST on disk: ``{this}/{year}/COTAHIST_A{year}.zip|.TXT``.
    #: Env var: B3_COTAHIST_ANNUAL_DIR
    b3_cotahist_annual_dir: str = "data/raw/b3/cotahist_annual"

    # Logging
    log_level: str = "INFO"

    # API metadata
    api_title: str = "ETL B3 API"
    api_version: str = "0.1.0"


settings = Settings()
