"""Constants for the B3 public market-data integration.

All magic strings and URL templates live here.
Never scatter these values across other modules.
"""

# ---------------------------------------------------------------------------
# Endpoint base URL
# ---------------------------------------------------------------------------

#: Root of the B3 public market-data service (cotacao.b3.com.br).
#: This is the stable, direct endpoint that does NOT require a proxy hop.
B3_MDS_BASE_URL: str = "https://cotacao.b3.com.br/mds/api/v1"

#: Path template for the DailyFluctuationHistory endpoint.
#: Accepts one positional placeholder: the uppercased ticker symbol.
#: Full URL: https://cotacao.b3.com.br/mds/api/v1/DailyFluctuationHistory/{ticker}
DAILY_FLUCTUATION_PATH: str = "/DailyFluctuationHistory/{ticker}"

# ---------------------------------------------------------------------------
# Session warm-up
# ---------------------------------------------------------------------------

#: Public B3 page that issues the cookies we need for the API.
B3_WARM_SESSION_URL: str = (
    "https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/"
    "market-data/consultas/mercado-a-vista/variacao-diaria/mercado-continuo/"
)


# ---------------------------------------------------------------------------
# HTTP headers – mimic a normal browser without hardcoding session cookies
# ---------------------------------------------------------------------------

DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Origin": "https://www.b3.com.br",
    "Referer": (
        "https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/"
        "market-data/consultas/mercado-a-vista/variacao-diaria/mercado-continuo/"
    ),
}

# ---------------------------------------------------------------------------
# Batch ingestion
# ---------------------------------------------------------------------------

#: Column name expected in the normalized instruments CSV.
TICKER_COLUMN: str = "Instrumento financeiro"

#: Maximum ticker length; values longer than this are noise/footer lines.
MAX_TICKER_LENGTH: int = 20

# ---------------------------------------------------------------------------
# Data metadata
# ---------------------------------------------------------------------------

#: Source identifier used in the normalised output model.
SOURCE_LABEL: str = "b3_public_internal_endpoint"
