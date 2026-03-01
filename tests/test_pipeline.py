"""Tests for doc_parser.pipeline — orchestration logic."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from doc_parser.config import Settings
from doc_parser.pipeline import (
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
            "institution": "Goldman Sachs",
            "authors": "John Doe",
            "publish_date": "2024-01-15",
            "data_period": "Q4 2024",
            "country": "US",
            "market": "US",
            "asset_class": "Macro",
            "sector": "Technology",
            "document_type": "Research Report",
            "event_type": None,
            "subject": "Apple Inc",
            "subject_id": "AAPL",
            "language": "en",
            "contains_commentary": True,
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
    assert result["institution"] == "Goldman Sachs"
    assert result["subject_id"] == "AAPL"
    assert result["markdown"] is not None
    assert result["parse_info"]["page_count"] == 1
    assert result["extraction_info"]["provider"] == "llm"


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
            fields={"title": "Updated Report", "institution": "MS"}
        )),
    ):
        sha2 = await process_local(settings, pdf, force=True)

    assert sha2 is not None
    result = load_result(settings.extraction_path, sha2)
    assert result["title"] == "Updated Report"


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
        "institution": "Old Institution",
        "markdown": "# Original markdown",
        "parse_info": {"page_count": 1},
        "extraction_info": {"provider": "llm"},
    }
    save_result(settings.extraction_path, existing)

    new_ext = _mock_extraction_result(fields={"title": "New Title", "institution": "New Institution"})

    with patch("doc_parser.pipeline.run_extraction", new_callable=AsyncMock, return_value=new_ext):
        result = await re_extract(settings, sha)

    assert result is not None
    assert result["title"] == "New Title"
    assert result["institution"] == "New Institution"
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
