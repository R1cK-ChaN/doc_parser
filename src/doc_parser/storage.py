"""Save parsed outputs to the local filesystem."""

from __future__ import annotations

import json
from pathlib import Path

from doc_parser.textin_client import ParseResult, decode_excel
from doc_parser.watermark import strip_watermark_lines


def store_parse_result(
    base_dir: Path,
    sha256: str,
    parse_id: int,
    result: ParseResult,
) -> dict[str, str]:
    """Write parse outputs to disk and return relative paths.

    Layout: <base_dir>/<sha256[:4]>/<sha256>/<parse_id>/
        output.md, detail.json, pages.json, tables.xlsx
    """
    rel_root = Path(sha256[:4]) / sha256 / str(parse_id)
    out_dir = base_dir / rel_root
    out_dir.mkdir(parents=True, exist_ok=True)

    paths: dict[str, str] = {}

    # Markdown (strip watermark lines before persisting)
    md_path = out_dir / "output.md"
    md_path.write_text(strip_watermark_lines(result.markdown), encoding="utf-8")
    paths["markdown_path"] = str(rel_root / "output.md")

    # Detail JSON
    detail_path = out_dir / "detail.json"
    detail_path.write_text(json.dumps(result.detail, ensure_ascii=False, indent=2), encoding="utf-8")
    paths["detail_json_path"] = str(rel_root / "detail.json")

    # Pages JSON
    pages_path = out_dir / "pages.json"
    pages_path.write_text(json.dumps(result.pages, ensure_ascii=False, indent=2), encoding="utf-8")
    paths["pages_json_path"] = str(rel_root / "pages.json")

    # Excel (optional)
    if result.excel_base64:
        xlsx_path = out_dir / "tables.xlsx"
        xlsx_path.write_bytes(decode_excel(result.excel_base64))
        paths["excel_path"] = str(rel_root / "tables.xlsx")

    return paths


def store_enhanced_markdown(
    base_dir: Path,
    sha256: str,
    parse_id: int,
    content: str,
) -> str:
    """Write enhanced markdown to disk and return relative path.

    Layout: <base_dir>/<sha256[:4]>/<sha256>/<parse_id>/output_enhanced.md
    """
    rel_root = Path(sha256[:4]) / sha256 / str(parse_id)
    out_dir = base_dir / rel_root
    out_dir.mkdir(parents=True, exist_ok=True)

    md_path = out_dir / "output_enhanced.md"
    md_path.write_text(content, encoding="utf-8")

    return str(rel_root / "output_enhanced.md")


def store_extraction_result(
    base_dir: Path,
    sha256: str,
    extraction_id: int,
    response_data: dict,
) -> str:
    """Write full extraction API response to disk and return relative path.

    Layout: <base_dir>/<sha256[:4]>/<sha256>/<extraction_id>/extraction.json
    """
    rel_root = Path(sha256[:4]) / sha256 / str(extraction_id)
    out_dir = base_dir / rel_root
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / "extraction.json"
    json_path.write_text(json.dumps(response_data, ensure_ascii=False, indent=2), encoding="utf-8")

    return str(rel_root / "extraction.json")
