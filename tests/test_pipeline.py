"""Tests for doc_parser.pipeline — orchestration logic."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from doc_parser.config import Settings
from doc_parser.google_drive import DriveFile
from doc_parser.models import DocFile
from doc_parser.pipeline import (
    ensure_doc_file,
    ensure_drive_doc_file,
    parse_drive_folder,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_settings(tmp_path: Path) -> Settings:
    return Settings(
        textin_app_id="test-app",
        textin_secret_code="test-secret",
        database_url="sqlite+aiosqlite://",
        data_dir=tmp_path / "data",
    )


# ---------------------------------------------------------------------------
# ensure_doc_file
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ensure_doc_file_creates_row(tmp_path: Path, async_engine, mock_get_session):
    """ensure_doc_file creates a DocFile with correct fields."""
    settings = _make_settings(tmp_path)
    pdf = tmp_path / "report.pdf"
    pdf.write_bytes(b"%PDF test content")

    doc_file_id = await ensure_doc_file(settings, pdf)
    assert doc_file_id is not None

    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        doc_files = (await session.execute(select(DocFile))).scalars().all()
        assert len(doc_files) == 1
        assert doc_files[0].source == "local"
        assert doc_files[0].file_name == "report.pdf"
        assert doc_files[0].local_path == str(pdf)
        assert doc_files[0].file_id == "local:report.pdf"


@pytest.mark.asyncio
async def test_ensure_doc_file_upserts(tmp_path: Path, async_engine, mock_get_session):
    """Calling ensure_doc_file twice updates sha256/local_path, doesn't duplicate."""
    settings = _make_settings(tmp_path)
    pdf = tmp_path / "report.pdf"
    pdf.write_bytes(b"%PDF v1")

    id1 = await ensure_doc_file(settings, pdf)

    # Change content → different sha
    pdf.write_bytes(b"%PDF v2")
    id2 = await ensure_doc_file(settings, pdf)

    assert id1 == id2

    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        doc_files = (await session.execute(select(DocFile))).scalars().all()
        assert len(doc_files) == 1


# ---------------------------------------------------------------------------
# ensure_drive_doc_file
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ensure_drive_doc_file_downloads_and_creates(tmp_path: Path, async_engine, mock_get_session):
    """ensure_drive_doc_file downloads the file and creates DocFile with local_path."""
    settings = _make_settings(tmp_path)
    settings.ensure_dirs()

    drive_meta = DriveFile(
        file_id="drive-f1",
        name="analysis.pdf",
        mime_type="application/pdf",
        size=4096,
        created_time=datetime(2025, 6, 1),
        parents=["folder-1"],
    )

    async def _fake_download(file_id, dest_path):
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_bytes(b"%PDF drive content")
        return dest_path

    with patch("doc_parser.pipeline.GoogleDriveClient") as MockDrive:
        mock_drive = AsyncMock()
        mock_drive.get_file_metadata = AsyncMock(return_value=drive_meta)
        mock_drive.download_file = AsyncMock(side_effect=_fake_download)
        MockDrive.return_value = mock_drive

        doc_file_id = await ensure_drive_doc_file(settings, "drive-f1")

    assert doc_file_id is not None

    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        doc_files = (await session.execute(select(DocFile))).scalars().all()
        assert len(doc_files) == 1
        df = doc_files[0]
        assert df.source == "drive"
        assert df.file_name == "analysis.pdf"
        assert df.local_path is not None
        assert "downloads" in df.local_path


# ---------------------------------------------------------------------------
# parse_drive_folder
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_parse_drive_folder_empty(tmp_path: Path, async_engine, mock_get_session):
    """parse_drive_folder returns [] for a folder with no supported files."""
    settings = _make_settings(tmp_path)

    with patch("doc_parser.pipeline.GoogleDriveClient") as MockDrive:
        mock_drive = AsyncMock()
        mock_drive.list_files = AsyncMock(return_value=[])
        MockDrive.return_value = mock_drive

        results = await parse_drive_folder(settings, "empty-folder")
        assert results == []
