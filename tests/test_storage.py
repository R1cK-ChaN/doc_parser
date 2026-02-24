"""Tests for doc_parser.storage — filesystem write logic."""

from __future__ import annotations

import base64
import json
from pathlib import Path

from doc_parser.storage import store_extraction_result, store_parse_result, store_watermark_result
from doc_parser.textin_client import ParseResult


# ---------------------------------------------------------------------------
# store_parse_result
# ---------------------------------------------------------------------------

def test_directory_structure(tmp_path: Path, sample_parse_result: ParseResult):
    """Creates <sha[:4]>/<sha>/<parse_id>/ directory layout."""
    sha = "abcdef1234567890" * 4  # 64-char hex
    store_parse_result(tmp_path, sha, 42, sample_parse_result)
    expected_dir = tmp_path / sha[:4] / sha / "42"
    assert expected_dir.is_dir()


def test_writes_markdown(tmp_path: Path, sample_parse_result: ParseResult):
    """Writes output.md with the markdown content."""
    sha = "a" * 64
    store_parse_result(tmp_path, sha, 1, sample_parse_result)
    md_path = tmp_path / sha[:4] / sha / "1" / "output.md"
    assert md_path.exists()
    assert md_path.read_text(encoding="utf-8") == sample_parse_result.markdown


def test_writes_detail_json(tmp_path: Path, sample_parse_result: ParseResult):
    """Writes detail.json with the detail list."""
    sha = "b" * 64
    store_parse_result(tmp_path, sha, 1, sample_parse_result)
    detail_path = tmp_path / sha[:4] / sha / "1" / "detail.json"
    assert detail_path.exists()
    data = json.loads(detail_path.read_text(encoding="utf-8"))
    assert data == sample_parse_result.detail


def test_writes_pages_json(tmp_path: Path, sample_parse_result: ParseResult):
    """Writes pages.json with the pages list."""
    sha = "c" * 64
    store_parse_result(tmp_path, sha, 1, sample_parse_result)
    pages_path = tmp_path / sha[:4] / sha / "1" / "pages.json"
    assert pages_path.exists()
    data = json.loads(pages_path.read_text(encoding="utf-8"))
    assert data == sample_parse_result.pages


def test_writes_xlsx_when_excel_present(tmp_path: Path, sample_parse_result: ParseResult):
    """Writes tables.xlsx when excel_base64 is provided."""
    sha = "d" * 64
    paths = store_parse_result(tmp_path, sha, 1, sample_parse_result)
    xlsx_path = tmp_path / sha[:4] / sha / "1" / "tables.xlsx"
    assert xlsx_path.exists()
    assert "excel_path" in paths


def test_skips_xlsx_when_no_excel(tmp_path: Path, sample_parse_result_no_excel: ParseResult):
    """Does not write tables.xlsx when excel_base64 is None."""
    sha = "e" * 64
    paths = store_parse_result(tmp_path, sha, 1, sample_parse_result_no_excel)
    xlsx_path = tmp_path / sha[:4] / sha / "1" / "tables.xlsx"
    assert not xlsx_path.exists()
    assert "excel_path" not in paths


def test_returns_relative_paths(tmp_path: Path, sample_parse_result: ParseResult):
    """Returns a dict of relative path strings."""
    sha = "f" * 64
    paths = store_parse_result(tmp_path, sha, 7, sample_parse_result)
    assert paths["markdown_path"] == f"{sha[:4]}/{sha}/7/output.md"
    assert paths["detail_json_path"] == f"{sha[:4]}/{sha}/7/detail.json"
    assert paths["pages_json_path"] == f"{sha[:4]}/{sha}/7/pages.json"
    assert paths["excel_path"] == f"{sha[:4]}/{sha}/7/tables.xlsx"


def test_idempotent_overwrite(tmp_path: Path, sample_parse_result: ParseResult):
    """Calling store twice on the same path overwrites without error."""
    sha = "0" * 64
    store_parse_result(tmp_path, sha, 1, sample_parse_result)
    store_parse_result(tmp_path, sha, 1, sample_parse_result)
    md_path = tmp_path / sha[:4] / sha / "1" / "output.md"
    assert md_path.exists()


# ---------------------------------------------------------------------------
# store_watermark_result
# ---------------------------------------------------------------------------

def test_store_watermark_result(tmp_path: Path):
    """Writes cleaned.jpg and returns relative path."""
    sha = "a" * 64
    image_b64 = base64.b64encode(b"fake-jpg-data").decode()
    rel_path = store_watermark_result(tmp_path, sha, 42, image_b64)

    assert rel_path == f"{sha[:4]}/{sha}/42/cleaned.jpg"
    full_path = tmp_path / rel_path
    assert full_path.exists()
    assert full_path.read_bytes() == b"fake-jpg-data"


def test_store_watermark_result_directory_layout(tmp_path: Path):
    """Creates <sha[:4]>/<sha>/<wm_id>/ directory layout."""
    sha = "b" * 64
    image_b64 = base64.b64encode(b"img").decode()
    store_watermark_result(tmp_path, sha, 1, image_b64)
    expected_dir = tmp_path / sha[:4] / sha / "1"
    assert expected_dir.is_dir()


# ---------------------------------------------------------------------------
# store_extraction_result
# ---------------------------------------------------------------------------

def test_store_extraction_result(tmp_path: Path):
    """Writes extraction.json and returns relative path."""
    sha = "c" * 64
    response_data = {"fields": {"title": "Report"}, "duration_ms": 100}
    rel_path = store_extraction_result(tmp_path, sha, 7, response_data)

    assert rel_path == f"{sha[:4]}/{sha}/7/extraction.json"
    full_path = tmp_path / rel_path
    assert full_path.exists()
    data = json.loads(full_path.read_text(encoding="utf-8"))
    assert data == response_data


def test_store_extraction_result_directory_layout(tmp_path: Path):
    """Creates <sha[:4]>/<sha>/<ext_id>/ directory layout."""
    sha = "d" * 64
    store_extraction_result(tmp_path, sha, 3, {"test": True})
    expected_dir = tmp_path / sha[:4] / sha / "3"
    assert expected_dir.is_dir()
