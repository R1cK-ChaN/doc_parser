"""Tests for doc_parser.steps — decoupled 3-step pipeline functions."""

from __future__ import annotations

import base64
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from doc_parser.config import Settings
from doc_parser.models import (
    DocExtraction,
    DocFile,
    DocParse,
    DocWatermark,
    epoch_now,
)
from doc_parser.steps.step1_watermark import run_watermark_removal
from doc_parser.steps.step2_parse import run_parse
from doc_parser.extraction import TextInExtractionProvider
from doc_parser.steps.step3_extract import run_extraction, parse_date_to_epoch
from doc_parser.textin_client import (
    ExtractionResult,
    ParseResult,
    TextInAPIError,
    WatermarkResult,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_settings(tmp_path: Path) -> Settings:
    s = Settings(
        textin_app_id="test-app",
        textin_secret_code="test-secret",
        database_url="sqlite+aiosqlite://",
        data_dir=tmp_path / "data",
    )
    s.ensure_dirs()
    return s


async def _create_doc_file(async_engine, file_name: str = "test.pdf", local_path: str = "/tmp/test.pdf") -> int:
    """Create a DocFile row and return its ID."""
    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        df = DocFile(
            file_id=f"local:{file_name}",
            sha256="a" * 64,
            source="local",
            file_name=file_name,
            local_path=local_path,
        )
        session.add(df)
        await session.commit()
        return df.id


# ---------------------------------------------------------------------------
# parse_date_to_epoch
# ---------------------------------------------------------------------------

def test_parse_date_to_epoch_valid():
    """Valid date string is parsed to epoch."""
    result = parse_date_to_epoch("2024-01-15")
    assert isinstance(result, int)
    assert result > 0


def test_parse_date_to_epoch_none():
    """None input returns None."""
    assert parse_date_to_epoch(None) is None


def test_parse_date_to_epoch_empty():
    """Empty string returns None."""
    assert parse_date_to_epoch("") is None


def test_parse_date_to_epoch_invalid():
    """Invalid date string returns None."""
    assert parse_date_to_epoch("not-a-date") is None


def test_parse_date_to_epoch_various_formats():
    """Various date formats are handled."""
    assert parse_date_to_epoch("January 15, 2024") is not None
    assert parse_date_to_epoch("2024/01/15") is not None
    assert parse_date_to_epoch("15 Jan 2024") is not None


# ---------------------------------------------------------------------------
# Step 1: Watermark Removal
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_step1_watermark_success(tmp_path: Path, async_engine, mock_get_session):
    """Watermark removal creates DocWatermark row and writes cleaned file."""
    settings = _make_settings(tmp_path)

    # Create a local file that exists
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test content")
    doc_file_id = await _create_doc_file(async_engine, local_path=str(pdf))

    cleaned_b64 = base64.b64encode(b"cleaned-image-data").decode()

    with patch("doc_parser.steps.step1_watermark.TextInClient") as MockTextIn:
        mock_instance = MagicMock()
        mock_instance.remove_watermark = AsyncMock(
            return_value=WatermarkResult(image_base64=cleaned_b64, duration_ms=150)
        )
        mock_instance.close = AsyncMock()
        MockTextIn.return_value = mock_instance

        result = await run_watermark_removal(settings, doc_file_id)
        assert result is not None

    # Verify DB row
    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        wm = (await session.execute(select(DocWatermark))).scalar_one()
        assert wm.status == "completed"
        assert wm.duration_ms == 150
        assert wm.cleaned_file_path is not None

        # Verify file on disk
        full_path = settings.watermark_path / wm.cleaned_file_path
        assert full_path.exists()
        assert full_path.read_bytes() == b"cleaned-image-data"


@pytest.mark.asyncio
async def test_step1_watermark_skip_existing(tmp_path: Path, async_engine, mock_get_session):
    """Watermark removal is skipped if already completed."""
    settings = _make_settings(tmp_path)
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test")
    doc_file_id = await _create_doc_file(async_engine, local_path=str(pdf))

    # Create a completed watermark row
    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        wm = DocWatermark(
            doc_file_id=doc_file_id,
            status="completed",
            cleaned_file_path="some/path.jpg",
        )
        session.add(wm)
        await session.commit()

    with patch("doc_parser.steps.step1_watermark.TextInClient") as MockTextIn:
        mock_instance = MagicMock()
        mock_instance.close = AsyncMock()
        MockTextIn.return_value = mock_instance

        result = await run_watermark_removal(settings, doc_file_id)
        assert result is None  # skipped


@pytest.mark.asyncio
async def test_step1_watermark_failure(tmp_path: Path, async_engine, mock_get_session):
    """Watermark removal failure sets status=failed."""
    settings = _make_settings(tmp_path)
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test")
    doc_file_id = await _create_doc_file(async_engine, local_path=str(pdf))

    with patch("doc_parser.steps.step1_watermark.TextInClient") as MockTextIn:
        mock_instance = MagicMock()
        mock_instance.remove_watermark = AsyncMock(
            side_effect=TextInAPIError(500, "Server error")
        )
        mock_instance.close = AsyncMock()
        MockTextIn.return_value = mock_instance

        result = await run_watermark_removal(settings, doc_file_id)
        assert result is None

    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        wm = (await session.execute(select(DocWatermark))).scalar_one()
        assert wm.status == "failed"
        assert "Server error" in wm.error_message


@pytest.mark.asyncio
async def test_step1_watermark_not_found(tmp_path: Path, async_engine, mock_get_session):
    """Watermark removal returns None for nonexistent doc_file_id."""
    settings = _make_settings(tmp_path)

    with patch("doc_parser.steps.step1_watermark.TextInClient") as MockTextIn:
        mock_instance = MagicMock()
        mock_instance.close = AsyncMock()
        MockTextIn.return_value = mock_instance

        result = await run_watermark_removal(settings, 99999)
        assert result is None


# ---------------------------------------------------------------------------
# Step 2: Parse
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_step2_parse_success(tmp_path: Path, async_engine, mock_get_session):
    """Parse creates DocParse + DocElement rows and writes files."""
    settings = _make_settings(tmp_path)
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test content")
    doc_file_id = await _create_doc_file(async_engine, local_path=str(pdf))

    mock_result = ParseResult(
        markdown="# Parsed",
        detail=[{"type": "text", "text": "Parsed", "page_number": 1}],
        pages=[{"page_number": 1}],
        total_page_number=1,
        valid_page_number=1,
        duration_ms=200,
        request_id="px-1",
        src_page_count=3,
    )

    with patch("doc_parser.steps.step2_parse.TextInClient") as MockTextIn:
        mock_instance = MagicMock()
        mock_instance.parse_file_x = AsyncMock(return_value=mock_result)
        mock_instance.get_parsex_config.return_value = {"pdf_parse_mode": "auto"}
        mock_instance.close = AsyncMock()
        MockTextIn.return_value = mock_instance

        result = await run_parse(settings, doc_file_id)
        assert result is not None

    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        dp = (await session.execute(select(DocParse))).scalar_one()
        assert dp.status == "completed"
        assert dp.src_page_count == 3
        assert dp.markdown_path is not None


@pytest.mark.asyncio
async def test_step2_parse_skip_existing(tmp_path: Path, async_engine, mock_get_session):
    """Parse is skipped if already completed."""
    settings = _make_settings(tmp_path)
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test")
    doc_file_id = await _create_doc_file(async_engine, local_path=str(pdf))

    # Create a completed parse row
    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        dp = DocParse(doc_file_id=doc_file_id, status="completed")
        session.add(dp)
        await session.commit()

    with patch("doc_parser.steps.step2_parse.TextInClient") as MockTextIn:
        mock_instance = MagicMock()
        mock_instance.close = AsyncMock()
        MockTextIn.return_value = mock_instance

        result = await run_parse(settings, doc_file_id)
        assert result is None


@pytest.mark.asyncio
async def test_step2_parse_failure(tmp_path: Path, async_engine, mock_get_session):
    """Parse failure sets status=failed."""
    settings = _make_settings(tmp_path)
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test")
    doc_file_id = await _create_doc_file(async_engine, local_path=str(pdf))

    with patch("doc_parser.steps.step2_parse.TextInClient") as MockTextIn:
        mock_instance = MagicMock()
        mock_instance.parse_file_x = AsyncMock(
            side_effect=TextInAPIError(500, "Parse error")
        )
        mock_instance.get_parsex_config.return_value = {"pdf_parse_mode": "auto"}
        mock_instance.close = AsyncMock()
        MockTextIn.return_value = mock_instance

        result = await run_parse(settings, doc_file_id)
        assert result is None

    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        dp = (await session.execute(select(DocParse))).scalar_one()
        assert dp.status == "failed"
        assert "Parse error" in dp.error_message


# ---------------------------------------------------------------------------
# Step 3: Extraction
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_step3_extract_success(tmp_path: Path, async_engine, mock_get_session):
    """Extraction creates DocExtraction row, writes JSON, and backfills DocFile."""
    settings = _make_settings(tmp_path)
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test content")
    doc_file_id = await _create_doc_file(async_engine, local_path=str(pdf))

    mock_result = ExtractionResult(
        fields={
            "title": "Q4 Market Report",
            "broker": "Goldman Sachs",
            "authors": "John Doe",
            "publish_date": "2024-01-15",
            "market": "US",
            "sector": "Technology",
            "document_type": "Research Report",
            "target_company": "Apple Inc",
            "ticker_symbol": "AAPL",
        },
        page_count=10,
        duration_ms=500,
        request_id="ext-1",
    )

    mock_provider = MagicMock()
    mock_provider.extract = AsyncMock(return_value=mock_result)
    mock_provider.close = AsyncMock()

    with patch("doc_parser.steps.step3_extract.create_extraction_provider", return_value=mock_provider):
        result = await run_extraction(settings, doc_file_id)
        assert result is not None

    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        ext = (await session.execute(select(DocExtraction))).scalar_one()
        assert ext.status == "completed"
        assert ext.title == "Q4 Market Report"
        assert ext.broker == "Goldman Sachs"
        assert ext.ticker_symbol == "AAPL"
        assert ext.publish_date is not None
        assert ext.extraction_json_path is not None
        assert ext.provider == "textin"

        # Verify DocFile was backfilled
        df = (await session.execute(select(DocFile))).scalar_one()
        assert df.title == "Q4 Market Report"
        assert df.broker == "Goldman Sachs"
        assert df.ticker_symbol == "AAPL"
        assert df.market == "US"


@pytest.mark.asyncio
async def test_step3_extract_skip_existing(tmp_path: Path, async_engine, mock_get_session):
    """Extraction is skipped if already completed."""
    settings = _make_settings(tmp_path)
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test")
    doc_file_id = await _create_doc_file(async_engine, local_path=str(pdf))

    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        ext = DocExtraction(doc_file_id=doc_file_id, status="completed")
        session.add(ext)
        await session.commit()

    mock_provider = MagicMock()
    mock_provider.close = AsyncMock()

    with patch("doc_parser.steps.step3_extract.create_extraction_provider", return_value=mock_provider):
        result = await run_extraction(settings, doc_file_id)
        assert result is None


@pytest.mark.asyncio
async def test_step3_extract_failure(tmp_path: Path, async_engine, mock_get_session):
    """Extraction failure sets status=failed."""
    settings = _make_settings(tmp_path)
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test")
    doc_file_id = await _create_doc_file(async_engine, local_path=str(pdf))

    mock_provider = MagicMock()
    mock_provider.extract = AsyncMock(
        side_effect=TextInAPIError(500, "Extract error")
    )
    mock_provider.close = AsyncMock()

    with patch("doc_parser.steps.step3_extract.create_extraction_provider", return_value=mock_provider):
        result = await run_extraction(settings, doc_file_id)
        assert result is None

    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        ext = (await session.execute(select(DocExtraction))).scalar_one()
        assert ext.status == "failed"
        assert "Extract error" in ext.error_message


@pytest.mark.asyncio
async def test_step3_extract_links_to_parse(tmp_path: Path, async_engine, mock_get_session):
    """Extraction links to the latest completed parse."""
    settings = _make_settings(tmp_path)
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test")
    doc_file_id = await _create_doc_file(async_engine, local_path=str(pdf))

    # Create a completed parse
    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        dp = DocParse(doc_file_id=doc_file_id, status="completed")
        session.add(dp)
        await session.commit()
        parse_id = dp.id

    mock_result = ExtractionResult(
        fields={"title": "Report"},
        duration_ms=100,
        request_id="ext-2",
    )

    mock_provider = MagicMock()
    mock_provider.extract = AsyncMock(return_value=mock_result)
    mock_provider.close = AsyncMock()

    with patch("doc_parser.steps.step3_extract.create_extraction_provider", return_value=mock_provider):
        result = await run_extraction(settings, doc_file_id)
        assert result is not None

    async with factory() as session:
        ext = (await session.execute(select(DocExtraction))).scalar_one()
        assert ext.doc_parse_id == parse_id
