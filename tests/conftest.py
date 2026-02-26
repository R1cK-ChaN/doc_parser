"""Shared test fixtures for doc_parser test suite."""

from __future__ import annotations

from pathlib import Path

import pytest

from doc_parser.config import Settings
from doc_parser.textin_client import ParseResult


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

@pytest.fixture()
def test_settings(tmp_path: Path) -> Settings:
    """Settings with dummy creds and tmp_path-based data_dir."""
    return Settings(
        textin_app_id="test-app-id",
        textin_secret_code="test-secret",
        data_dir=tmp_path / "data",
    )


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_parse_result() -> ParseResult:
    """ParseResult with markdown, 3 detail elements, pages."""
    return ParseResult(
        markdown="# Title\n\nSome content here.",
        detail=[
            {"type": "text", "text": "Title", "page_number": 1, "position": {"x": 0, "y": 0}},
            {"type": "text", "text": "Some content", "page_number": 1, "position": {"x": 0, "y": 50}},
            {"type": "table", "text": "col1|col2", "page_number": 2, "table_cells": [{"r": 0, "c": 0}]},
        ],
        pages=[{"page_number": 1, "width": 612, "height": 792}, {"page_number": 2, "width": 612, "height": 792}],
        total_page_number=2,
        valid_page_number=2,
        duration_ms=1234,
        request_id="req-abc-123",
        has_chart=False,
    )


@pytest.fixture()
def sample_pdf(tmp_path: Path) -> Path:
    """Write a fake PDF file to tmp_path and return its path."""
    pdf_path = tmp_path / "sample.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 fake pdf content for testing\n%%EOF\n")
    return pdf_path
