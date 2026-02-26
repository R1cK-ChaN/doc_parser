"""Tests for doc_parser.steps — pure API call wrappers."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from doc_parser.steps.step2_parse import run_parse
from doc_parser.steps.step3_extract import parse_date_to_epoch, run_extraction
from doc_parser.textin_client import ExtractionResult, ParseResult, TextInAPIError


# ---------------------------------------------------------------------------
# parse_date_to_epoch
# ---------------------------------------------------------------------------

def test_parse_date_to_epoch_valid():
    """Valid date string is parsed to epoch."""
    result = parse_date_to_epoch("2024-01-15")
    assert isinstance(result, int)
    assert result > 0


def test_parse_date_to_epoch_none():
    assert parse_date_to_epoch(None) is None


def test_parse_date_to_epoch_empty():
    assert parse_date_to_epoch("") is None


def test_parse_date_to_epoch_invalid():
    assert parse_date_to_epoch("not-a-date") is None


def test_parse_date_to_epoch_various_formats():
    assert parse_date_to_epoch("January 15, 2024") is not None
    assert parse_date_to_epoch("2024/01/15") is not None
    assert parse_date_to_epoch("15 Jan 2024") is not None


# ---------------------------------------------------------------------------
# run_parse
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_parse_returns_parse_result(tmp_path: Path, test_settings):
    """run_parse returns a ParseResult from TextIn (no file writes)."""
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test content")

    mock_result = ParseResult(
        markdown="# Parsed",
        detail=[{"type": "text", "text": "Parsed", "page_number": 1}],
        pages=[{"page_number": 1}],
        total_page_number=1,
        valid_page_number=1,
        duration_ms=200,
        request_id="px-1",
    )

    with patch("doc_parser.steps.step2_parse.TextInClient") as MockTextIn:
        mock_instance = MagicMock()
        mock_instance.parse_file_x = AsyncMock(return_value=mock_result)
        mock_instance.close = AsyncMock()
        MockTextIn.return_value = mock_instance

        result = await run_parse(test_settings, pdf)

    assert isinstance(result, ParseResult)
    assert result.markdown == "# Parsed"
    assert result.total_page_number == 1
    assert result.duration_ms == 200


@pytest.mark.asyncio
async def test_run_parse_no_file_writes(tmp_path: Path, test_settings):
    """run_parse does not write any files to disk."""
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test")
    test_settings.ensure_dirs()

    mock_result = ParseResult(markdown="# Test", detail=[], pages=[])

    with patch("doc_parser.steps.step2_parse.TextInClient") as MockTextIn:
        mock_instance = MagicMock()
        mock_instance.parse_file_x = AsyncMock(return_value=mock_result)
        mock_instance.close = AsyncMock()
        MockTextIn.return_value = mock_instance

        await run_parse(test_settings, pdf)

    # No files should be written in parsed_path
    parsed_files = list(test_settings.parsed_path.rglob("*"))
    assert len(parsed_files) == 0


@pytest.mark.asyncio
async def test_run_parse_propagates_error(tmp_path: Path, test_settings):
    """run_parse propagates TextIn API errors."""
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test")

    with patch("doc_parser.steps.step2_parse.TextInClient") as MockTextIn:
        mock_instance = MagicMock()
        mock_instance.parse_file_x = AsyncMock(
            side_effect=TextInAPIError(500, "Parse error")
        )
        mock_instance.close = AsyncMock()
        MockTextIn.return_value = mock_instance

        with pytest.raises(TextInAPIError, match="Parse error"):
            await run_parse(test_settings, pdf)


@pytest.mark.asyncio
async def test_run_parse_closes_client(tmp_path: Path, test_settings):
    """run_parse always closes the TextIn client."""
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test")

    with patch("doc_parser.steps.step2_parse.TextInClient") as MockTextIn:
        mock_instance = MagicMock()
        mock_instance.parse_file_x = AsyncMock(
            side_effect=TextInAPIError(500, "fail")
        )
        mock_instance.close = AsyncMock()
        MockTextIn.return_value = mock_instance

        with pytest.raises(TextInAPIError):
            await run_parse(test_settings, pdf)

        mock_instance.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# run_extraction
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_extraction_returns_result(tmp_path: Path, test_settings):
    """run_extraction returns an ExtractionResult (no file writes)."""
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test")

    mock_result = ExtractionResult(
        fields={"title": "Q4 Report", "broker": "GS"},
        duration_ms=500,
        request_id="ext-1",
    )

    with patch("doc_parser.steps.step3_extract.create_extraction_provider") as mock_create:
        mock_provider = MagicMock()
        mock_provider.extract = AsyncMock(return_value=mock_result)
        mock_provider.close = AsyncMock()
        mock_create.return_value = mock_provider

        result = await run_extraction(test_settings, file_path=pdf)

    assert isinstance(result, ExtractionResult)
    assert result.fields["title"] == "Q4 Report"
    assert result.fields["broker"] == "GS"


@pytest.mark.asyncio
async def test_run_extraction_passes_markdown(tmp_path: Path, test_settings):
    """run_extraction passes markdown to the provider."""
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test")

    mock_result = ExtractionResult(fields={"title": "Report"}, duration_ms=100)

    with patch("doc_parser.steps.step3_extract.create_extraction_provider") as mock_create:
        mock_provider = MagicMock()
        mock_provider.extract = AsyncMock(return_value=mock_result)
        mock_provider.close = AsyncMock()
        mock_create.return_value = mock_provider

        await run_extraction(test_settings, file_path=pdf, markdown="# Test markdown")

    call_kwargs = mock_provider.extract.call_args.kwargs
    assert call_kwargs["markdown"] == "# Test markdown"


@pytest.mark.asyncio
async def test_run_extraction_propagates_error(tmp_path: Path, test_settings):
    """run_extraction propagates provider errors."""
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test")

    with patch("doc_parser.steps.step3_extract.create_extraction_provider") as mock_create:
        mock_provider = MagicMock()
        mock_provider.extract = AsyncMock(
            side_effect=TextInAPIError(500, "Extract error")
        )
        mock_provider.close = AsyncMock()
        mock_create.return_value = mock_provider

        with pytest.raises(TextInAPIError, match="Extract error"):
            await run_extraction(test_settings, file_path=pdf)
