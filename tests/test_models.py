"""Tests for doc_parser.models — ORM round-trips via async SQLite."""

from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from doc_parser.models import (
    DocElement,
    DocExtraction,
    DocFile,
    DocParse,
    epoch_now,
)


# ---------------------------------------------------------------------------
# epoch_now helper
# ---------------------------------------------------------------------------

def test_epoch_now_returns_int():
    """epoch_now() returns a positive integer."""
    ts = epoch_now()
    assert isinstance(ts, int)
    assert ts > 0


# ---------------------------------------------------------------------------
# DocFile
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_docfile_create_and_read(async_session: AsyncSession):
    """DocFile can be created and read back."""
    df = DocFile(
        file_id="local:test.pdf",
        sha256="a" * 64,
        source="local",
        file_name="test.pdf",
    )
    async_session.add(df)
    await async_session.flush()

    result = await async_session.execute(select(DocFile).where(DocFile.file_id == "local:test.pdf"))
    row = result.scalar_one()
    assert row.file_name == "test.pdf"
    assert row.sha256 == "a" * 64


@pytest.mark.asyncio
async def test_docfile_unique_file_id(async_session: AsyncSession):
    """DocFile.file_id has a unique constraint."""
    df1 = DocFile(file_id="dup-id", sha256="a" * 64, source="local", file_name="a.pdf")
    df2 = DocFile(file_id="dup-id", sha256="b" * 64, source="local", file_name="b.pdf")
    async_session.add(df1)
    await async_session.flush()
    async_session.add(df2)
    with pytest.raises(IntegrityError):
        await async_session.flush()


@pytest.mark.asyncio
async def test_docfile_new_columns(async_session: AsyncSession):
    """DocFile has new metadata columns for query convenience."""
    df = DocFile(
        file_id="local:meta.pdf",
        sha256="b" * 64,
        source="local",
        file_name="meta.pdf",
        market="US",
        sector="Technology",
        document_type="Research Report",
        target_company="Apple Inc",
        ticker_symbol="AAPL",
        authors="John Doe, Jane Smith",
    )
    async_session.add(df)
    await async_session.flush()

    result = await async_session.execute(select(DocFile).where(DocFile.file_id == "local:meta.pdf"))
    row = result.scalar_one()
    assert row.market == "US"
    assert row.ticker_symbol == "AAPL"
    assert row.authors == "John Doe, Jane Smith"


@pytest.mark.asyncio
async def test_docfile_epoch_timestamps(async_session: AsyncSession):
    """DocFile uses BigInteger epoch timestamps."""
    df = DocFile(
        file_id="local:epoch.pdf",
        sha256="c" * 64,
        source="local",
        file_name="epoch.pdf",
        publish_date=1700000000,
        created_at=1700000000,
    )
    async_session.add(df)
    await async_session.flush()

    result = await async_session.execute(select(DocFile).where(DocFile.file_id == "local:epoch.pdf"))
    row = result.scalar_one()
    assert row.publish_date == 1700000000
    assert isinstance(row.created_at, int)


# ---------------------------------------------------------------------------
# DocParse linked to DocFile
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_docparse_linked_to_docfile(async_session: AsyncSession):
    """DocParse references a DocFile via foreign key."""
    df = DocFile(file_id="f1", sha256="c" * 64, source="local", file_name="f1.pdf")
    async_session.add(df)
    await async_session.flush()

    dp = DocParse(doc_file_id=df.id, parse_mode="auto", status="completed")
    async_session.add(dp)
    await async_session.flush()

    result = await async_session.execute(select(DocParse).where(DocParse.doc_file_id == df.id))
    row = result.scalar_one()
    assert row.status == "completed"
    assert row.doc_file_id == df.id


@pytest.mark.asyncio
async def test_docparse_src_page_count(async_session: AsyncSession):
    """DocParse has src_page_count column."""
    df = DocFile(file_id="f-src", sha256="d" * 64, source="local", file_name="src.pdf")
    async_session.add(df)
    await async_session.flush()

    dp = DocParse(doc_file_id=df.id, parse_mode="auto", status="completed", src_page_count=10)
    async_session.add(dp)
    await async_session.flush()

    result = await async_session.execute(select(DocParse).where(DocParse.doc_file_id == df.id))
    row = result.scalar_one()
    assert row.src_page_count == 10


# ---------------------------------------------------------------------------
# DocElement with JSONB fields
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_docelement_jsonb_fields(async_session: AsyncSession):
    """DocElement stores position and table_cells as JSON (mapped from JSONB)."""
    df = DocFile(file_id="f2", sha256="d" * 64, source="local", file_name="f2.pdf")
    async_session.add(df)
    await async_session.flush()

    dp = DocParse(doc_file_id=df.id, parse_mode="auto", status="completed")
    async_session.add(dp)
    await async_session.flush()

    elem = DocElement(
        doc_parse_id=dp.id,
        page_number=1,
        element_type="table",
        position={"x": 10, "y": 20, "w": 100, "h": 50},
        table_cells=[{"r": 0, "c": 0, "text": "cell"}],
    )
    async_session.add(elem)
    await async_session.flush()

    result = await async_session.execute(select(DocElement).where(DocElement.doc_parse_id == dp.id))
    row = result.scalar_one()
    assert row.position == {"x": 10, "y": 20, "w": 100, "h": 50}
    assert row.table_cells == [{"r": 0, "c": 0, "text": "cell"}]


# ---------------------------------------------------------------------------
# DocExtraction
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_docextraction_create(async_session: AsyncSession):
    """DocExtraction can be created with all extracted fields."""
    df = DocFile(file_id="f-ext", sha256="f" * 64, source="local", file_name="ext.pdf")
    async_session.add(df)
    await async_session.flush()

    ext = DocExtraction(
        doc_file_id=df.id,
        status="completed",
        title="Market Analysis Report",
        broker="Goldman Sachs",
        authors="John Doe",
        publish_date=1700000000,
        market="US",
        sector="Technology",
        document_type="Research Report",
        target_company="Apple Inc",
        ticker_symbol="AAPL",
    )
    async_session.add(ext)
    await async_session.flush()

    result = await async_session.execute(select(DocExtraction).where(DocExtraction.doc_file_id == df.id))
    row = result.scalar_one()
    assert row.title == "Market Analysis Report"
    assert row.broker == "Goldman Sachs"
    assert row.ticker_symbol == "AAPL"
    assert row.publish_date == 1700000000


@pytest.mark.asyncio
async def test_docextraction_linked_to_parse(async_session: AsyncSession):
    """DocExtraction can optionally link to a DocParse."""
    df = DocFile(file_id="f-ext2", sha256="0" * 64, source="local", file_name="ext2.pdf")
    async_session.add(df)
    await async_session.flush()

    dp = DocParse(doc_file_id=df.id, parse_mode="auto", status="completed")
    async_session.add(dp)
    await async_session.flush()

    ext = DocExtraction(
        doc_file_id=df.id,
        doc_parse_id=dp.id,
        status="completed",
    )
    async_session.add(ext)
    await async_session.flush()

    result = await async_session.execute(select(DocExtraction).where(DocExtraction.doc_file_id == df.id))
    row = result.scalar_one()
    assert row.doc_parse_id == dp.id


# ---------------------------------------------------------------------------
# Relationship back-populates
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_relationship_back_populates(async_session: AsyncSession):
    """DocFile.parses and DocParse.elements back-populate correctly."""
    df = DocFile(file_id="f3", sha256="e" * 64, source="drive", file_name="f3.pdf")
    async_session.add(df)
    await async_session.flush()

    dp = DocParse(doc_file_id=df.id, parse_mode="auto", status="running")
    async_session.add(dp)
    await async_session.flush()

    elem = DocElement(doc_parse_id=dp.id, element_type="text", text="hello")
    async_session.add(elem)
    await async_session.flush()

    # Refresh to load relationships
    await async_session.refresh(df, ["parses"])
    await async_session.refresh(dp, ["elements"])

    assert len(df.parses) == 1
    assert df.parses[0].id == dp.id
    assert len(dp.elements) == 1
    assert dp.elements[0].text == "hello"


@pytest.mark.asyncio
async def test_extraction_relationships(async_session: AsyncSession):
    """DocFile.extractions back-populates."""
    df = DocFile(file_id="f-rel", sha256="1" * 64, source="local", file_name="rel.pdf")
    async_session.add(df)
    await async_session.flush()

    ext = DocExtraction(doc_file_id=df.id, status="completed")
    async_session.add(ext)
    await async_session.flush()

    await async_session.refresh(df, ["extractions"])
    assert len(df.extractions) == 1
