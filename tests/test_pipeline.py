"""Tests for doc_parser.pipeline — orchestration logic."""

from __future__ import annotations

import base64
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from doc_parser.config import Settings
from doc_parser.google_drive import DriveFile
from doc_parser.models import Base, DocElement, DocFile, DocParse
from doc_parser.pipeline import process_folder, process_local_file, process_single_file
from doc_parser.textin_client import ParseResult, TextInAPIError


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


def _make_parse_result() -> ParseResult:
    return ParseResult(
        markdown="# Result",
        detail=[{"type": "text", "text": "Result", "page_number": 1}],
        pages=[{"page_number": 1}],
        excel_base64=base64.b64encode(b"xlsx-data").decode(),
        total_page_number=1,
        valid_page_number=1,
        duration_ms=200,
        request_id="req-test",
        has_chart=False,
    )


def _make_textin_mock(parse_result: ParseResult | None = None) -> MagicMock:
    """Create a mocked TextInClient.

    Uses MagicMock as base so sync methods (get_parse_config) return plain values,
    with async methods explicitly set to AsyncMock.
    """
    textin = MagicMock()
    textin.parse_file = AsyncMock(return_value=parse_result or _make_parse_result())
    textin.get_parse_config.return_value = {"parse_mode": "auto", "get_excel": "1"}
    textin.close = AsyncMock()
    return textin


# ---------------------------------------------------------------------------
# Local file success
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_local_file_success(tmp_path: Path, async_engine, mock_get_session):
    """Local file is parsed, creating DocFile + DocParse + DocElement rows."""
    settings = _make_settings(tmp_path)
    settings.ensure_dirs()
    pdf = tmp_path / "report.pdf"
    pdf.write_bytes(b"%PDF test content")

    textin = _make_textin_mock()

    result_id = await process_single_file(settings, textin, local_path=pdf)
    assert result_id is not None

    textin.parse_file.assert_called_once()

    # Verify DB rows were created
    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        doc_files = (await session.execute(select(DocFile))).scalars().all()
        assert len(doc_files) == 1
        assert doc_files[0].source == "local"

        doc_parses = (await session.execute(select(DocParse))).scalars().all()
        assert len(doc_parses) == 1
        assert doc_parses[0].status == "completed"

        elements = (await session.execute(select(DocElement))).scalars().all()
        assert len(elements) == 1


# ---------------------------------------------------------------------------
# Missing args
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_raises_without_file_or_drive(tmp_path: Path, async_engine, mock_get_session):
    """Raises ValueError when neither local_path nor drive_file is given."""
    settings = _make_settings(tmp_path)
    textin = _make_textin_mock()

    with pytest.raises(ValueError, match="Must provide"):
        await process_single_file(settings, textin)


# ---------------------------------------------------------------------------
# Dedup: skip already parsed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dedup_skip_already_parsed(tmp_path: Path, async_engine, mock_get_session):
    """File with existing completed parse is skipped when reparse=False."""
    settings = _make_settings(tmp_path)
    settings.ensure_dirs()
    pdf = tmp_path / "report.pdf"
    pdf.write_bytes(b"%PDF dedup test")

    textin = _make_textin_mock()

    # First parse
    result1 = await process_single_file(settings, textin, local_path=pdf)
    assert result1 is not None

    # Second parse — should skip
    result2 = await process_single_file(settings, textin, local_path=pdf, reparse=False)
    assert result2 is None

    # parse_file called only once (for the first parse)
    assert textin.parse_file.call_count == 1


# ---------------------------------------------------------------------------
# Re-parse creates second parse
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reparse_creates_second_parse(tmp_path: Path, async_engine, mock_get_session):
    """reparse=True creates a second DocParse for the same file."""
    settings = _make_settings(tmp_path)
    settings.ensure_dirs()
    pdf = tmp_path / "report.pdf"
    pdf.write_bytes(b"%PDF reparse test")

    textin = _make_textin_mock()

    result1 = await process_single_file(settings, textin, local_path=pdf)
    result2 = await process_single_file(settings, textin, local_path=pdf, reparse=True)

    assert result1 is not None
    assert result2 is not None
    assert result1 != result2

    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        parses = (await session.execute(select(DocParse))).scalars().all()
        assert len(parses) == 2


# ---------------------------------------------------------------------------
# TextIn failure
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_textin_failure_sets_status_failed(tmp_path: Path, async_engine, mock_get_session):
    """TextIn exception → status=failed, error_message stored."""
    settings = _make_settings(tmp_path)
    settings.ensure_dirs()
    pdf = tmp_path / "report.pdf"
    pdf.write_bytes(b"%PDF fail test")

    textin = _make_textin_mock()
    textin.parse_file = AsyncMock(side_effect=TextInAPIError(500, "Internal error"))

    result = await process_single_file(settings, textin, local_path=pdf)
    assert result is None

    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        doc_parse = (await session.execute(select(DocParse))).scalar_one()
        assert doc_parse.status == "failed"
        assert "Internal error" in doc_parse.error_message


# ---------------------------------------------------------------------------
# Drive file download + parse
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_drive_file_downloads_and_parses(tmp_path: Path, async_engine, mock_get_session):
    """Drive file is downloaded to temp, parsed, and temp is cleaned up."""
    settings = _make_settings(tmp_path)
    settings.ensure_dirs()

    drive_file = DriveFile(
        file_id="drive-f1",
        name="analysis.pdf",
        mime_type="application/pdf",
        size=4096,
        created_time=datetime(2025, 6, 1),
        parents=["folder-1"],
    )

    drive_client = AsyncMock()

    async def _fake_download(file_id, dest_path):
        dest_path.write_bytes(b"%PDF drive content")
        return dest_path

    drive_client.download_file = AsyncMock(side_effect=_fake_download)
    textin = _make_textin_mock()

    result_id = await process_single_file(
        settings, textin,
        drive_file=drive_file, drive_client=drive_client,
    )
    assert result_id is not None
    drive_client.download_file.assert_called_once()
    textin.parse_file.assert_called_once()


# ---------------------------------------------------------------------------
# process_local_file end-to-end
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_local_file_e2e(tmp_path: Path, async_engine, mock_get_session):
    """process_local_file() calls process_single_file and cleans up TextIn client."""
    settings = _make_settings(tmp_path)
    settings.ensure_dirs()
    pdf = tmp_path / "local.pdf"
    pdf.write_bytes(b"%PDF local e2e")

    mock_result = _make_parse_result()

    with patch("doc_parser.pipeline.TextInClient") as MockTextIn:
        mock_instance = MagicMock()
        mock_instance.parse_file = AsyncMock(return_value=mock_result)
        mock_instance.get_parse_config.return_value = {"parse_mode": "auto"}
        mock_instance.close = AsyncMock()
        MockTextIn.return_value = mock_instance

        result = await process_local_file(settings, pdf)
        assert result is not None
        mock_instance.close.assert_called_once()


# ---------------------------------------------------------------------------
# process_folder batch + empty
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_folder_empty(tmp_path: Path, async_engine, mock_get_session):
    """process_folder returns [] for a folder with no supported files."""
    settings = _make_settings(tmp_path)

    with (
        patch("doc_parser.pipeline.GoogleDriveClient") as MockDrive,
        patch("doc_parser.pipeline.TextInClient") as MockTextIn,
    ):
        mock_drive = AsyncMock()
        mock_drive.list_files = AsyncMock(return_value=[])
        MockDrive.return_value = mock_drive

        mock_textin = AsyncMock()
        mock_textin.close = AsyncMock()
        MockTextIn.return_value = mock_textin

        results = await process_folder(settings, "empty-folder")
        assert results == []
