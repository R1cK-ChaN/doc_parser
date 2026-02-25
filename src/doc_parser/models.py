"""SQLAlchemy ORM models for the doc_parser pipeline."""

from __future__ import annotations

import time

from sqlalchemy import (
    BigInteger,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def epoch_now() -> int:
    """Return current time as Unix epoch seconds."""
    return int(time.time())


class Base(DeclarativeBase):
    pass


class DocFile(Base):
    """One row per unique source file."""

    __tablename__ = "doc_file"

    id: Mapped[int] = mapped_column(primary_key=True)
    file_id: Mapped[str] = mapped_column(String(255), unique=True, comment="Google Drive file ID or local identifier")
    sha256: Mapped[str] = mapped_column(String(64), index=True)
    source: Mapped[str] = mapped_column(String(50), comment="'drive' or 'local'")
    mime_type: Mapped[str | None] = mapped_column(String(127))
    file_name: Mapped[str] = mapped_column(String(512))
    file_size_bytes: Mapped[int | None] = mapped_column(Integer)
    publish_date: Mapped[int | None] = mapped_column(BigInteger)
    broker: Mapped[str | None] = mapped_column(String(255))
    title: Mapped[str | None] = mapped_column(String(1024))
    drive_folder_id: Mapped[str | None] = mapped_column(String(255))
    local_path: Mapped[str | None] = mapped_column(String(1024))
    created_at: Mapped[int | None] = mapped_column(BigInteger, default=epoch_now)
    updated_at: Mapped[int | None] = mapped_column(BigInteger, default=epoch_now, onupdate=epoch_now)

    # New fields for query convenience (backfilled from Step 3)
    market: Mapped[str | None] = mapped_column(String(255))
    sector: Mapped[str | None] = mapped_column(String(255))
    document_type: Mapped[str | None] = mapped_column(String(255))
    target_company: Mapped[str | None] = mapped_column(String(255))
    ticker_symbol: Mapped[str | None] = mapped_column(String(50))
    authors: Mapped[str | None] = mapped_column(String(1024))

    parses: Mapped[list[DocParse]] = relationship(back_populates="doc_file", cascade="all, delete-orphan")
    extractions: Mapped[list[DocExtraction]] = relationship(back_populates="doc_file", cascade="all, delete-orphan")


class DocParse(Base):
    """One row per TextIn parse invocation."""

    __tablename__ = "doc_parse"

    id: Mapped[int] = mapped_column(primary_key=True)
    doc_file_id: Mapped[int] = mapped_column(ForeignKey("doc_file.id", ondelete="CASCADE"))
    parse_mode: Mapped[str] = mapped_column(String(50), default="auto")
    status: Mapped[str] = mapped_column(String(20), default="pending", comment="pending/running/completed/failed")
    started_at: Mapped[int | None] = mapped_column(BigInteger)
    completed_at: Mapped[int | None] = mapped_column(BigInteger)
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    textin_request_id: Mapped[str | None] = mapped_column(String(255))

    # Storage paths (relative to data_dir/parsed/)
    markdown_path: Mapped[str | None] = mapped_column(String(1024))
    detail_json_path: Mapped[str | None] = mapped_column(String(1024))
    pages_json_path: Mapped[str | None] = mapped_column(String(1024))
    excel_path: Mapped[str | None] = mapped_column(String(1024))

    # Flags
    has_excel: Mapped[bool | None] = mapped_column(default=False)
    has_chart: Mapped[bool | None] = mapped_column(default=False)
    page_count: Mapped[int | None] = mapped_column(Integer)
    valid_page_count: Mapped[int | None] = mapped_column(Integer)
    src_page_count: Mapped[int | None] = mapped_column(Integer)
    error_message: Mapped[str | None] = mapped_column(Text)

    # Exact params sent to TextIn for reproducibility
    parse_config: Mapped[dict | None] = mapped_column(JSONB)

    doc_file: Mapped[DocFile] = relationship(back_populates="parses")
    elements: Mapped[list[DocElement]] = relationship(back_populates="doc_parse", cascade="all, delete-orphan")


class DocElement(Base):
    """One row per structural element extracted from a parse."""

    __tablename__ = "doc_element"
    __table_args__ = (
        Index("ix_doc_element_parse_page", "doc_parse_id", "page_number"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    doc_parse_id: Mapped[int] = mapped_column(ForeignKey("doc_parse.id", ondelete="CASCADE"))
    page_number: Mapped[int | None] = mapped_column(Integer)
    element_type: Mapped[str | None] = mapped_column(String(50))
    sub_type: Mapped[str | None] = mapped_column(String(50))
    text: Mapped[str | None] = mapped_column(Text)
    position: Mapped[dict | None] = mapped_column(JSONB, comment="Bounding box coordinates")
    char_pos_start: Mapped[int | None] = mapped_column(Integer)
    char_pos_end: Mapped[int | None] = mapped_column(Integer)
    outline_level: Mapped[int | None] = mapped_column(Integer)
    content_flag: Mapped[str | None] = mapped_column(String(50))
    image_url: Mapped[str | None] = mapped_column(String(1024))
    table_cells: Mapped[dict | None] = mapped_column(JSONB)

    doc_parse: Mapped[DocParse] = relationship(back_populates="elements")


class DocExtraction(Base):
    """One row per entity extraction invocation (Step 3)."""

    __tablename__ = "doc_extraction"

    id: Mapped[int] = mapped_column(primary_key=True)
    doc_file_id: Mapped[int] = mapped_column(ForeignKey("doc_file.id", ondelete="CASCADE"))
    doc_parse_id: Mapped[int | None] = mapped_column(ForeignKey("doc_parse.id", ondelete="SET NULL"))
    status: Mapped[str] = mapped_column(String(20), default="pending", comment="pending/running/completed/failed")
    started_at: Mapped[int | None] = mapped_column(BigInteger)
    completed_at: Mapped[int | None] = mapped_column(BigInteger)
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    textin_request_id: Mapped[str | None] = mapped_column(String(255))
    provider: Mapped[str | None] = mapped_column(String(50))
    llm_model: Mapped[str | None] = mapped_column(String(255))

    # Extracted fields
    title: Mapped[str | None] = mapped_column(String(1024))
    broker: Mapped[str | None] = mapped_column(String(255))
    authors: Mapped[str | None] = mapped_column(String(1024))
    publish_date: Mapped[int | None] = mapped_column(BigInteger)
    market: Mapped[str | None] = mapped_column(String(255))
    sector: Mapped[str | None] = mapped_column(String(255))
    document_type: Mapped[str | None] = mapped_column(String(255))
    target_company: Mapped[str | None] = mapped_column(String(255))
    ticker_symbol: Mapped[str | None] = mapped_column(String(50))

    # Storage
    extraction_json_path: Mapped[str | None] = mapped_column(String(1024))
    extraction_config: Mapped[dict | None] = mapped_column(JSONB)
    error_message: Mapped[str | None] = mapped_column(Text)

    doc_file: Mapped[DocFile] = relationship(back_populates="extractions")
