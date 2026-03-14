# Tests for downloader CSV normalization and save_download

import sys
import types
from datetime import date
import importlib.util
from pathlib import Path
from types import ModuleType


def load_downloader_module():
    """Load app.scraping.b3.downloader from its file path without importing parent package init.

    Pre-load app.scraping.common.base into sys.modules and create minimal package modules
    for 'app', 'app.scraping', and 'app.scraping.common' so that downloader's
    "from app.scraping.common.base import ScrapeResult" works without running
    app/scraping/__init__.py.
    """
    repo_root = Path(__file__).resolve().parents[1]

    # Ensure minimal package modules in sys.modules
    app_pkg = sys.modules.get("app")
    if app_pkg is None:
        app_pkg = ModuleType("app")
        app_pkg.__path__ = [str(repo_root / "app")]
        sys.modules["app"] = app_pkg

    scraping_pkg = sys.modules.get("app.scraping")
    if scraping_pkg is None:
        scraping_pkg = ModuleType("app.scraping")
        scraping_pkg.__path__ = [str(repo_root / "app" / "scraping")]
        sys.modules["app.scraping"] = scraping_pkg

    common_pkg = sys.modules.get("app.scraping.common")
    if common_pkg is None:
        common_pkg = ModuleType("app.scraping.common")
        common_pkg.__path__ = [str(repo_root / "app" / "scraping" / "common")]
        sys.modules["app.scraping.common"] = common_pkg

    # Pre-load the common.base module so downloader can import ScrapeResult without importing app.scraping.__init__
    # Inject a minimal fake 'app.scraping.common.base' module with a lightweight
    # ScrapeResult class so downloader imports succeed without running the
    # repository's dataclass-heavy implementation.
    fake_base = ModuleType("app.scraping.common.base")

    class ScrapeResult:
        def __init__(self, *, source_url: str = "", file_path=None, suggested_filename: str = "", saved_filename: str = "", original_file_path=None, normalized_file_path=None, downloaded_at=None, target_date=None, conversion_succeeded: bool = False, conversion_error=None):
            self.source_url = source_url
            self.file_path = file_path
            self.suggested_filename = suggested_filename
            self.saved_filename = saved_filename
            self.original_file_path = original_file_path
            self.normalized_file_path = normalized_file_path
            self.downloaded_at = downloaded_at
            self.target_date = target_date
            self.conversion_succeeded = conversion_succeeded
            self.conversion_error = conversion_error

        def __str__(self):
            return f"ScrapeResult(file={self.file_path}, date={self.target_date}, url={self.source_url})"

    fake_base.ScrapeResult = ScrapeResult
    sys.modules["app.scraping.common.base"] = fake_base

    # Now load downloader module directly
    module_path = repo_root / "app" / "scraping" / "b3" / "downloader.py"
    spec = importlib.util.spec_from_file_location("app.scraping.b3.downloader", str(module_path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_normalize_csv_using_stdlib_detects_comma_and_skips_first_two_lines(tmp_path):
    mod = load_downloader_module()
    _normalize_csv_using_stdlib = mod._normalize_csv_using_stdlib

    src = tmp_path / "raw.csv"
    dest = tmp_path / "out.normalized.csv"

    # Build a file where the header is on the 3rd line and uses commas
    content = (
        "metadata line 1\n"
        "metadata line 2\n"
        "col1,col2,col3\n"
        "a1,b1,c1\n"
        "a2,b2,c2\n"
    )
    src.write_text(content, encoding="utf-8")

    # Run normalization
    _normalize_csv_using_stdlib(src_path=src, dest_path=dest, default_delimiter=";")

    # Read normalized file and assert header and delimiters
    out_text = dest.read_text(encoding="utf-8")
    lines = out_text.splitlines()

    # Header should be the 3rd line, converted to semicolon delimiter
    assert lines[0] == "col1;col2;col3"
    # Data rows should be converted to semicolons and not include the first two metadata lines
    assert lines[1] == "a1;b1;c1"
    assert lines[2] == "a2;b2;c2"


def test_save_download_creates_raw_and_normalized_and_returns_result(tmp_path):
    mod = load_downloader_module()
    save_download = mod.save_download

    # Prepare a fake Download object that mimics the Playwright Download interface
    class DummyDownload:
        def __init__(self, content_bytes: bytes, suggested_filename: str = "orig.csv", failure_msg: str | None = None):
            self.suggested_filename = suggested_filename
            self._content = content_bytes
            self._failure = failure_msg

        def failure(self):
            return self._failure

        def save_as(self, path):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(self._content)

    # CSV content with header on 3rd line and comma delimiter
    csv_content = (
        "meta1\n"
        "meta2\n"
        "c1,c2\n"
        "1,2\n"
    ).encode("utf-8")

    download = DummyDownload(csv_content, suggested_filename="downloaded.csv")

    target_date = date(2024, 6, 14)
    output_dir = tmp_path
    filename_template = "cadastro_instrumentos_{date}.csv"

    result = save_download(
        download=download,
        output_dir=output_dir,
        target_date=target_date,
        filename_template=filename_template,
        default_delimiter=";",
    )

    # Application-determined raw filename
    expected_raw = output_dir / "cadastro_instrumentos_20240614.csv"
    # Normalized filename has .normalized before the suffix
    expected_norm = output_dir / "cadastro_instrumentos_20240614.normalized.csv"

    assert expected_raw.exists(), "Raw/original file must exist"
    assert expected_norm.exists(), "Normalized file must exist"

    # file_path stays pointing at the raw file; normalized_file_path holds the normalized path
    assert result.file_path == expected_raw
    assert result.normalized_file_path == expected_norm
    assert result.original_file_path == expected_raw
    assert result.saved_filename == "cadastro_instrumentos_20240614.csv"
    assert result.suggested_filename == "downloaded.csv"

    # Conversion should have succeeded for this CSV
    assert result.conversion_succeeded is True

    # Check normalized content uses semicolon delimiter and skipped metadata lines
    norm_text = expected_norm.read_text(encoding="utf-8")
    norm_lines = norm_text.splitlines()
    assert norm_lines[0] == "c1;c2"
    assert norm_lines[1] == "1;2"


def test_normalize_negocios_preserves_header_with_blank_line(tmp_path):
    """Simulate a B3 NegociosConsolidados CSV that has a descriptive first line,
    a blank second line, and the real header on the third line. Ensure the
    normalized file begins with the header (not a data row).
    """
    mod = load_downloader_module()
    _normalize_csv_using_stdlib = mod._normalize_csv_using_stdlib

    src = tmp_path / "neg_raw.csv"
    dest = tmp_path / "neg.normalized.csv"

    content = (
        "Relatório diário de negociação B3 - metadata\n"
        "\n"
        "Instrumento financeiro;Código ISIN;Segmento;Preço de abertura;Preço mínimo\n"
        "BBSEO330W4;BRBBSE3O0EG2;EQUITY PUT;...;...\n"
    )
    src.write_text(content, encoding="utf-8")

    # Should not raise and should preserve the header as first line in normalized
    _normalize_csv_using_stdlib(src_path=src, dest_path=dest, default_delimiter=";", table_key="negocios_consolidados")

    out = dest.read_text(encoding="utf-8").splitlines()
    assert out[0].startswith("Instrumento financeiro"), "Normalized file must begin with header"


def test_normalize_negocios_raises_when_header_missing(tmp_path):
    """If the source file lacks a recognizable header, normalization for
    negocios_consolidados should raise a clear ValueError rather than
    silently producing a malformed normalized file.
    """
    mod = load_downloader_module()
    _normalize_csv_using_stdlib = mod._normalize_csv_using_stdlib

    src = tmp_path / "bad.csv"
    dest = tmp_path / "bad.normalized.csv"

    # Build a file where first non-empty line is a data row (no header tokens)
    content = (
        "meta line\n"
        "\n"
        "BBSEO330W4;BRBBSE3O0EG2;EQUITY PUT;100;99\n"
        "OTHER;ROW;DATA;101;98\n"
    )
    src.write_text(content, encoding="utf-8")

    try:
        _normalize_csv_using_stdlib(src_path=src, dest_path=dest, default_delimiter=";", table_key="negocios_consolidados")
        raised = False
    except ValueError as exc:
        raised = True
        assert "negocios_consolidados normalized file header" in str(exc) or "Normalized CSV does not start with a header-like row" in str(exc)

    assert raised, "Normalization must raise ValueError when header is missing for negocios_consolidados"

