"""Step 2: ParseX (OCR to markdown) via TextIn API."""

from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy import select

from doc_parser.config import Settings
from doc_parser.db import get_session
from doc_parser.models import DocElement, DocFile, DocParse, epoch_now
from doc_parser.storage import store_parse_result
from doc_parser.textin_client import TextInClient

logger = logging.getLogger(__name__)


async def run_parse(
    settings: Settings,
    doc_file_id: int,
    *,
    force: bool = False,
    parse_mode: str | None = None,
    get_excel: bool = True,
    md_detail: int = 2,
) -> int | None:
    """Parse a document file via TextIn ParseX.

    1. Load DocFile
    2. Resolve file path
    3. Check for existing completed DocParse (skip if not force)
    4. Create DocParse row: status=running
    5. Call textin.parse_file_x()
    6. Store outputs via store_parse_result()
    7. Insert DocElement rows
    8. Update DocParse: status=completed

    Returns doc_parse.id or None on skip/failure.
    """
    textin = TextInClient(settings)
    try:
        return await _do_parse(
            settings, textin, doc_file_id,
            force=force,
            parse_mode=parse_mode, get_excel=get_excel,
            md_detail=md_detail,
        )
    finally:
        await textin.close()


async def _do_parse(
    settings: Settings,
    textin: TextInClient,
    doc_file_id: int,
    *,
    force: bool = False,
    parse_mode: str | None = None,
    get_excel: bool = True,
    md_detail: int = 2,
) -> int | None:
    async with get_session() as session:
        # Load DocFile
        doc_file = await session.get(DocFile, doc_file_id)
        if doc_file is None:
            logger.error("DocFile id=%d not found", doc_file_id)
            return None

        # Resolve file path
        file_path = _resolve_file_path(doc_file)

        if file_path is None or not file_path.exists():
            logger.error("Cannot resolve file path for DocFile id=%d", doc_file_id)
            return None

        # Check existing completed parse
        if not force:
            existing = await session.execute(
                select(DocParse).where(
                    DocParse.doc_file_id == doc_file_id,
                    DocParse.status == "completed",
                )
            )
            if existing.scalar_one_or_none():
                logger.info("Skipping parse for %s — already completed (use --force)", doc_file.file_name)
                return None

        # Create DocParse row
        parse_config = textin.get_parsex_config(parse_mode, get_excel, md_detail)
        doc_parse = DocParse(
            doc_file_id=doc_file_id,
            parse_mode=parse_config.get("pdf_parse_mode", "auto"),
            status="running",
            started_at=epoch_now(),
            parse_config=parse_config,
        )
        session.add(doc_parse)
        await session.flush()

        try:
            parse_result = await textin.parse_file_x(
                file_path,
                parse_mode=parse_mode,
                get_excel=get_excel,
                md_detail=md_detail,
            )
        except Exception as exc:
            doc_parse.status = "failed"
            doc_parse.completed_at = epoch_now()
            doc_parse.error_message = str(exc)
            logger.error("Parse failed for %s: %s", doc_file.file_name, exc)
            return None

        # Store outputs to disk
        paths = store_parse_result(
            settings.parsed_path, doc_file.sha256, doc_parse.id, parse_result
        )

        # Update DocParse
        doc_parse.status = "completed"
        doc_parse.completed_at = epoch_now()
        doc_parse.duration_ms = parse_result.duration_ms
        doc_parse.textin_request_id = parse_result.request_id
        doc_parse.markdown_path = paths.get("markdown_path")
        doc_parse.detail_json_path = paths.get("detail_json_path")
        doc_parse.pages_json_path = paths.get("pages_json_path")
        doc_parse.excel_path = paths.get("excel_path")
        doc_parse.has_excel = parse_result.excel_base64 is not None
        doc_parse.has_chart = parse_result.has_chart
        doc_parse.page_count = parse_result.total_page_number
        doc_parse.valid_page_count = parse_result.valid_page_number
        doc_parse.src_page_count = parse_result.src_page_count

        # Extract DocElement rows
        for elem in parse_result.detail:
            doc_elem = DocElement(
                doc_parse_id=doc_parse.id,
                page_number=elem.get("page_number"),
                element_type=elem.get("type"),
                sub_type=elem.get("sub_type"),
                text=elem.get("text"),
                position=elem.get("position"),
                char_pos_start=elem.get("char_pos_start"),
                char_pos_end=elem.get("char_pos_end"),
                outline_level=elem.get("outline_level"),
                content_flag=elem.get("content_flag"),
                image_url=elem.get("image_url"),
                table_cells=elem.get("table_cells"),
            )
            session.add(doc_elem)

        logger.info(
            "Parsed %s → %d elements, %d pages",
            doc_file.file_name,
            len(parse_result.detail),
            parse_result.total_page_number,
        )
        return doc_parse.id


def _resolve_file_path(doc_file: DocFile) -> Path | None:
    """Resolve the local file path for a DocFile."""
    if doc_file.local_path:
        return Path(doc_file.local_path)
    return None
