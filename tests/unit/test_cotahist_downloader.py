"""Tests for COTAHIST ZIP download/extract (streaming I/O)."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.integrations.b3.cotahist_downloader import download_cotahist_zip, extract_cotahist_txt


def test_download_cotahist_zip_streams_chunks(tmp_path: Path) -> None:
    dest = tmp_path / "COTAHIST_A2020.zip"
    payload = b"zip-bytes-chunked"

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.iter_bytes = MagicMock(return_value=[payload[:4], payload[4:]])

    mock_cm = MagicMock()
    mock_cm.__enter__ = MagicMock(return_value=mock_resp)
    mock_cm.__exit__ = MagicMock(return_value=False)

    mock_client = MagicMock()
    mock_client.stream = MagicMock(return_value=mock_cm)
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)

    with patch("app.integrations.b3.cotahist_downloader.httpx.Client", return_value=mock_client):
        download_cotahist_zip(2020, dest, max_retries=1)

    assert dest.read_bytes() == payload
    mock_client.stream.assert_called_once()


def test_download_cotahist_zip_failure_unlinks_partial(tmp_path: Path) -> None:
    dest = tmp_path / "COTAHIST_A2020.zip"
    dest.write_bytes(b"partial")

    mock_resp = MagicMock()
    mock_resp.status_code = 500
    mock_resp.raise_for_status.side_effect = OSError("boom")

    mock_cm = MagicMock()
    mock_cm.__enter__ = MagicMock(return_value=mock_resp)
    mock_cm.__exit__ = MagicMock(return_value=False)

    mock_client = MagicMock()
    mock_client.stream = MagicMock(return_value=mock_cm)
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)

    with patch("app.integrations.b3.cotahist_downloader.httpx.Client", return_value=mock_client):
        with pytest.raises(OSError, match="boom"):
            download_cotahist_zip(2020, dest, max_retries=1)

    assert not dest.exists()


def test_extract_cotahist_txt_roundtrip_matches_member_bytes(tmp_path: Path) -> None:
    inner_name = "COTAHIST_A1999.TXT"
    content = b"00header\n" + b"01" + b"x" * 100 + b"\n"
    zip_path = tmp_path / "COTAHIST_A1999.zip"
    out_dir = tmp_path / "out"

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_STORED) as zf:
        zf.writestr(inner_name, content)
    zip_path.write_bytes(buf.getvalue())

    extracted = extract_cotahist_txt(zip_path, out_dir)
    assert extracted == out_dir / inner_name
    assert extracted.read_bytes() == content
