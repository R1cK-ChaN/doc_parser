"""Tests for doc_parser.google_drive — Google Drive client."""

from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from doc_parser.config import Settings
from doc_parser.google_drive import SUPPORTED_MIMES, DriveFile, GoogleDriveClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_settings() -> Settings:
    return Settings(
        textin_app_id="a",
        textin_secret_code="s",
        google_credentials_file="dummy.json",
        google_service_account=False,
    )


def _make_client_with_mock_service() -> tuple[GoogleDriveClient, MagicMock]:
    """Create a GoogleDriveClient with a mocked _service."""
    client = GoogleDriveClient.__new__(GoogleDriveClient)
    client._settings = _make_settings()
    mock_service = MagicMock()
    client._service = mock_service
    return client, mock_service


def _drive_file_dict(**overrides) -> dict:
    """Return a dict resembling a Google Drive API file resource."""
    d = {
        "id": "file-123",
        "name": "report.pdf",
        "mimeType": "application/pdf",
        "size": "1024",
        "createdTime": "2025-01-15T10:30:00+00:00",
        "parents": ["folder-abc"],
    }
    d.update(overrides)
    return d


# ---------------------------------------------------------------------------
# DriveFile dataclass
# ---------------------------------------------------------------------------

def test_drive_file_construction():
    """DriveFile can be constructed with all fields."""
    df = DriveFile(
        file_id="f1",
        name="test.pdf",
        mime_type="application/pdf",
        size=2048,
        created_time=datetime(2025, 1, 1),
        parents=["p1"],
    )
    assert df.file_id == "f1"
    assert df.name == "test.pdf"
    assert df.size == 2048


# ---------------------------------------------------------------------------
# SUPPORTED_MIMES
# ---------------------------------------------------------------------------

def test_supported_mimes_contains_pdf():
    """PDF is in SUPPORTED_MIMES."""
    assert "application/pdf" in SUPPORTED_MIMES


def test_supported_mimes_contains_images():
    """Common image types are in SUPPORTED_MIMES."""
    assert "image/png" in SUPPORTED_MIMES
    assert "image/jpeg" in SUPPORTED_MIMES


# ---------------------------------------------------------------------------
# list_files_sync
# ---------------------------------------------------------------------------

def test_list_files_sync_returns_drive_files():
    """list_files_sync returns a list of DriveFile objects."""
    client, svc = _make_client_with_mock_service()
    svc.files().list().execute.return_value = {
        "files": [_drive_file_dict(), _drive_file_dict(id="file-456", name="chart.png")],
    }
    result = client.list_files_sync("folder-abc")
    assert len(result) == 2
    assert all(isinstance(f, DriveFile) for f in result)
    assert result[0].file_id == "file-123"


def test_list_files_sync_empty_folder():
    """list_files_sync returns empty list for empty folder."""
    client, svc = _make_client_with_mock_service()
    svc.files().list().execute.return_value = {"files": []}
    result = client.list_files_sync("empty-folder")
    assert result == []


def test_list_files_sync_pagination():
    """list_files_sync handles pagination via nextPageToken."""
    client, svc = _make_client_with_mock_service()

    page1 = {
        "files": [_drive_file_dict(id="f1")],
        "nextPageToken": "token2",
    }
    page2 = {
        "files": [_drive_file_dict(id="f2")],
    }
    svc.files().list().execute.side_effect = [page1, page2]

    result = client.list_files_sync("folder-x")
    assert len(result) == 2
    assert result[0].file_id == "f1"
    assert result[1].file_id == "f2"


# ---------------------------------------------------------------------------
# get_file_metadata_sync
# ---------------------------------------------------------------------------

def test_get_file_metadata_sync():
    """get_file_metadata_sync returns a DriveFile."""
    client, svc = _make_client_with_mock_service()
    svc.files().get().execute.return_value = _drive_file_dict()
    result = client.get_file_metadata_sync("file-123")
    assert isinstance(result, DriveFile)
    assert result.file_id == "file-123"
    assert result.name == "report.pdf"


# ---------------------------------------------------------------------------
# download_file_sync
# ---------------------------------------------------------------------------

def test_download_file_sync(tmp_path: Path):
    """download_file_sync writes bytes to dest_path."""
    client, svc = _make_client_with_mock_service()
    dest = tmp_path / "downloaded.pdf"

    # Patch MediaIoBaseDownload to simulate download
    with patch("doc_parser.google_drive.MediaIoBaseDownload") as mock_dl_cls:
        mock_downloader = MagicMock()
        # Simulate: first chunk not done, second chunk done
        mock_downloader.next_chunk.side_effect = [
            (MagicMock(progress=lambda: 0.5), False),
            (MagicMock(progress=lambda: 1.0), True),
        ]
        mock_dl_cls.return_value = mock_downloader

        result = client.download_file_sync("file-123", dest)
        assert result == dest
        mock_dl_cls.assert_called_once()


# ---------------------------------------------------------------------------
# Async wrappers
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_list_files_delegates():
    """list_files() async wrapper delegates to list_files_sync."""
    client, svc = _make_client_with_mock_service()
    svc.files().list().execute.return_value = {
        "files": [_drive_file_dict()],
    }
    result = await client.list_files("folder-abc")
    assert len(result) == 1
    assert result[0].file_id == "file-123"
