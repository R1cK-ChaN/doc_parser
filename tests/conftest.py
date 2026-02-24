"""Shared test fixtures for doc_parser test suite."""

from __future__ import annotations

import base64
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.postgresql import JSONB

from doc_parser.config import Settings
from doc_parser.models import Base
from doc_parser.textin_client import ParseResult


# ---------------------------------------------------------------------------
# JSONB → JSON on SQLite: register a compilation rule so that
# PostgreSQL's JSONB type compiles to plain "JSON" on SQLite.
# ---------------------------------------------------------------------------

@compiles(JSONB, "sqlite")
def _compile_jsonb_sqlite(element, compiler, **kw):
    return "JSON"


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

@pytest.fixture()
def test_settings(tmp_path: Path) -> Settings:
    """Settings with dummy creds and tmp_path-based data_dir."""
    return Settings(
        textin_app_id="test-app-id",
        textin_secret_code="test-secret",
        database_url="sqlite+aiosqlite://",
        data_dir=tmp_path / "data",
    )


# ---------------------------------------------------------------------------
# Async SQLite engine + session
# ---------------------------------------------------------------------------

@pytest.fixture()
async def async_engine():
    """In-memory SQLite engine with all tables created."""
    engine = create_async_engine("sqlite+aiosqlite://", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture()
async def async_session(async_engine):
    """AsyncSession from the test engine; rolls back after each test."""
    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        yield session
        await session.rollback()


@pytest.fixture()
def mock_get_session(async_engine):
    """Patch get_session in pipeline, db, and steps modules to use the test SQLite engine."""
    from contextlib import asynccontextmanager

    factory = async_sessionmaker(async_engine, expire_on_commit=False)

    @asynccontextmanager
    async def _fake_get_session():
        async with factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    with (
        patch("doc_parser.pipeline.get_session", _fake_get_session),
        patch("doc_parser.db.get_session", _fake_get_session),
        patch("doc_parser.steps.step1_watermark.get_session", _fake_get_session),
        patch("doc_parser.steps.step2_parse.get_session", _fake_get_session),
        patch("doc_parser.steps.step3_extract.get_session", _fake_get_session),
    ):
        yield _fake_get_session


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_parse_result() -> ParseResult:
    """ParseResult with markdown, 3 detail elements, excel, pages."""
    return ParseResult(
        markdown="# Title\n\nSome content here.",
        detail=[
            {"type": "text", "text": "Title", "page_number": 1, "position": {"x": 0, "y": 0}},
            {"type": "text", "text": "Some content", "page_number": 1, "position": {"x": 0, "y": 50}},
            {"type": "table", "text": "col1|col2", "page_number": 2, "table_cells": [{"r": 0, "c": 0}]},
        ],
        pages=[{"page_number": 1, "width": 612, "height": 792}, {"page_number": 2, "width": 612, "height": 792}],
        excel_base64=base64.b64encode(b"fake-xlsx-content").decode(),
        total_page_number=2,
        valid_page_number=2,
        duration_ms=1234,
        request_id="req-abc-123",
        has_chart=False,
    )


@pytest.fixture()
def sample_parse_result_no_excel(sample_parse_result: ParseResult) -> ParseResult:
    """Same as sample_parse_result but with excel_base64=None."""
    return ParseResult(
        markdown=sample_parse_result.markdown,
        detail=sample_parse_result.detail,
        pages=sample_parse_result.pages,
        excel_base64=None,
        total_page_number=sample_parse_result.total_page_number,
        valid_page_number=sample_parse_result.valid_page_number,
        duration_ms=sample_parse_result.duration_ms,
        request_id=sample_parse_result.request_id,
        has_chart=sample_parse_result.has_chart,
    )


@pytest.fixture()
def sample_pdf(tmp_path: Path) -> Path:
    """Write a fake PDF file to tmp_path and return its path."""
    pdf_path = tmp_path / "sample.pdf"
    # Minimal PDF-like content (not a valid PDF, but enough for hashing/parsing tests)
    pdf_path.write_bytes(b"%PDF-1.4 fake pdf content for testing\n%%EOF\n")
    return pdf_path
