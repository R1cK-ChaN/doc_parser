"""Tests for doc_parser.pipeline — orchestration logic."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from doc_parser.config import Settings
from doc_parser.google_drive import DriveFile
from doc_parser.pipeline import (
    process_drive_file,
    process_drive_folder,
    process_local,
    re_extract,
)
from doc_parser.storage import load_result, save_result
from doc_parser.textin_client import ExtractionResult, ParseResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_settings(tmp_path: Path) -> Settings:
    s = Settings(
        textin_app_id="test-app",
        textin_secret_code="test-secret",
        data_dir=tmp_path / "data",
        extraction_provider="textin",
    )
    s.ensure_dirs()
    return s


def _mock_parse_result(**overrides) -> ParseResult:
    defaults = dict(
        markdown="# Test\n\nContent here.",
        detail=[{"type": "text", "text": "Test", "page_number": 1}],
        pages=[{"page_number": 1}],
        total_page_number=1,
        valid_page_number=1,
        duration_ms=200,
        request_id="px-1",
        has_chart=False,
    )
    defaults.update(overrides)
    return ParseResult(**defaults)


def _mock_extraction_result(**overrides) -> ExtractionResult:
    defaults = dict(
        fields={
            "title": "Q4 Report",
            "broker": "Goldman Sachs",
            "authors": "John Doe",
            "publish_date": "2024-01-15",
            "market": "US",
            "asset_class": "Macro",
            "sector": "Technology",
            "document_type": "Research Report",
            "target_company": "Apple Inc",
            "ticker_symbol": "AAPL",
        },
        duration_ms=500,
        request_id="ext-1",
    )
    defaults.update(overrides)
    return ExtractionResult(**defaults)


# ---------------------------------------------------------------------------
# process_local
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_local_writes_json(tmp_path: Path):
    """process_local writes a result JSON with all expected fields."""
    settings = _make_settings(tmp_path)
    pdf = tmp_path / "report.pdf"
    pdf.write_bytes(b"%PDF test content")

    with (
        patch("doc_parser.pipeline.run_parse", new_callable=AsyncMock, return_value=_mock_parse_result()),
        patch("doc_parser.pipeline.run_extraction", new_callable=AsyncMock, return_value=_mock_extraction_result()),
    ):
        sha = await process_local(settings, pdf)

    assert sha is not None
    result = load_result(settings.extraction_path, sha)
    assert result is not None
    assert result["file_name"] == "report.pdf"
    assert result["source"] == "local"
    assert result["title"] == "Q4 Report"
    assert result["broker"] == "Goldman Sachs"
    assert result["ticker_symbol"] == "AAPL"
    assert result["markdown"] is not None
    assert result["parse_info"]["page_count"] == 1
    assert result["extraction_info"]["provider"] == "textin"


@pytest.mark.asyncio
async def test_process_local_skips_existing(tmp_path: Path):
    """process_local returns None if result already exists."""
    settings = _make_settings(tmp_path)
    pdf = tmp_path / "report.pdf"
    pdf.write_bytes(b"%PDF test content")

    # First run
    with (
        patch("doc_parser.pipeline.run_parse", new_callable=AsyncMock, return_value=_mock_parse_result()),
        patch("doc_parser.pipeline.run_extraction", new_callable=AsyncMock, return_value=_mock_extraction_result()),
    ):
        sha1 = await process_local(settings, pdf)

    # Second run without force
    with (
        patch("doc_parser.pipeline.run_parse", new_callable=AsyncMock) as mock_parse,
        patch("doc_parser.pipeline.run_extraction", new_callable=AsyncMock) as mock_extract,
    ):
        sha2 = await process_local(settings, pdf)

    assert sha1 is not None
    assert sha2 is None
    mock_parse.assert_not_awaited()
    mock_extract.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_local_force_reprocesses(tmp_path: Path):
    """process_local with force=True reprocesses even if result exists."""
    settings = _make_settings(tmp_path)
    pdf = tmp_path / "report.pdf"
    pdf.write_bytes(b"%PDF test content")

    # First run
    with (
        patch("doc_parser.pipeline.run_parse", new_callable=AsyncMock, return_value=_mock_parse_result()),
        patch("doc_parser.pipeline.run_extraction", new_callable=AsyncMock, return_value=_mock_extraction_result()),
    ):
        sha1 = await process_local(settings, pdf)

    # Second run with force
    with (
        patch("doc_parser.pipeline.run_parse", new_callable=AsyncMock, return_value=_mock_parse_result()),
        patch("doc_parser.pipeline.run_extraction", new_callable=AsyncMock, return_value=_mock_extraction_result(
            fields={"title": "Updated Report", "broker": "MS"}
        )),
    ):
        sha2 = await process_local(settings, pdf, force=True)

    assert sha2 is not None
    result = load_result(settings.extraction_path, sha2)
    assert result["title"] == "Updated Report"


# ---------------------------------------------------------------------------
# process_drive_file
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_drive_file(tmp_path: Path):
    """process_drive_file downloads, processes, and writes JSON."""
    settings = _make_settings(tmp_path)

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

    with (
        patch("doc_parser.pipeline.GoogleDriveClient") as MockDrive,
        patch("doc_parser.pipeline.run_parse", new_callable=AsyncMock, return_value=_mock_parse_result()),
        patch("doc_parser.pipeline.run_extraction", new_callable=AsyncMock, return_value=_mock_extraction_result()),
    ):
        mock_drive = AsyncMock()
        mock_drive.get_file_metadata = AsyncMock(return_value=drive_meta)
        mock_drive.download_file = AsyncMock(side_effect=_fake_download)
        MockDrive.return_value = mock_drive

        sha = await process_drive_file(settings, "drive-f1")

    assert sha is not None
    result = load_result(settings.extraction_path, sha)
    assert result["source"] == "drive"
    assert result["file_name"] == "analysis.pdf"
    assert result["drive_folder_id"] == "folder-1"


# ---------------------------------------------------------------------------
# process_drive_folder
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_drive_folder_empty(tmp_path: Path):
    """process_drive_folder returns [] for an empty folder."""
    settings = _make_settings(tmp_path)

    with patch("doc_parser.pipeline.GoogleDriveClient") as MockDrive:
        mock_drive = AsyncMock()
        mock_drive.list_files = AsyncMock(return_value=[])
        MockDrive.return_value = mock_drive

        results = await process_drive_folder(settings, "empty-folder")
        assert results == []


# ---------------------------------------------------------------------------
# re_extract
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_re_extract_updates_fields(tmp_path: Path):
    """re_extract reads existing JSON, re-runs extraction, updates fields."""
    settings = _make_settings(tmp_path)
    pdf = tmp_path / "report.pdf"
    pdf.write_bytes(b"%PDF test")

    sha = "a" * 64
    existing = {
        "sha256": sha,
        "file_name": "report.pdf",
        "source": "local",
        "local_path": str(pdf),
        "title": "Old Title",
        "broker": "Old Broker",
        "markdown": "# Original markdown",
        "parse_info": {"page_count": 1},
        "extraction_info": {"provider": "textin"},
    }
    save_result(settings.extraction_path, existing)

    new_ext = _mock_extraction_result(fields={"title": "New Title", "broker": "New Broker"})

    with patch("doc_parser.pipeline.run_extraction", new_callable=AsyncMock, return_value=new_ext):
        result = await re_extract(settings, sha)

    assert result is not None
    assert result["title"] == "New Title"
    assert result["broker"] == "New Broker"
    # Markdown preserved
    assert result["markdown"] == "# Original markdown"


@pytest.mark.asyncio
async def test_re_extract_missing_result(tmp_path: Path):
    """re_extract returns None if no existing result."""
    settings = _make_settings(tmp_path)
    result = await re_extract(settings, "nonexistent" + "0" * 55)
    assert result is None


@pytest.mark.asyncio
async def test_re_extract_passes_markdown_to_provider(tmp_path: Path):
    """re_extract passes stored markdown to run_extraction."""
    settings = _make_settings(tmp_path)
    pdf = tmp_path / "report.pdf"
    pdf.write_bytes(b"%PDF test")

    sha = "b" * 64
    existing = {
        "sha256": sha,
        "file_name": "report.pdf",
        "source": "local",
        "local_path": str(pdf),
        "markdown": "# My markdown content",
        "parse_info": {},
        "extraction_info": {},
    }
    save_result(settings.extraction_path, existing)

    mock_ext = _mock_extraction_result()

    with patch("doc_parser.pipeline.run_extraction", new_callable=AsyncMock, return_value=mock_ext) as mock_run:
        await re_extract(settings, sha)

    call_kwargs = mock_run.call_args.kwargs
    assert call_kwargs["markdown"] == "# My markdown content"
