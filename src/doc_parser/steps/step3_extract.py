"""Step 3: Entity extraction via provider protocol."""

from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy import select

from doc_parser.config import Settings
from doc_parser.db import get_session
from doc_parser.extraction import (
    ExtractionProvider,
    create_extraction_provider,
)
from doc_parser.models import DocExtraction, DocFile, DocParse, epoch_now
from doc_parser.storage import store_extraction_result
from doc_parser.textin_client import EXTRACTION_FIELDS
from doc_parser.watermark import strip_watermark_lines

logger = logging.getLogger(__name__)


def parse_date_to_epoch(date_str: str | None) -> int | None:
    """Parse a date string to Unix epoch seconds.

    Returns None if parsing fails or input is empty.
    """
    if not date_str:
        return None
    try:
        from dateutil.parser import parse as dateparse
        return int(dateparse(date_str).timestamp())
    except (ValueError, OverflowError):
        return None


async def run_extraction(
    settings: Settings,
    doc_file_id: int,
    *,
    force: bool = False,
    fields: list[dict[str, str]] | None = None,
    provider: ExtractionProvider | None = None,
) -> int | None:
    """Extract structured entities from a document file.

    1. Load DocFile
    2. Check for existing completed DocExtraction (skip if not force)
    3. Resolve file path (prefer cleaned from Step 1)
    4. Create DocExtraction row: status=running
    5. Call provider.extract()
    6. Store full response JSON to disk
    7. Parse extracted fields into typed columns
    8. Backfill DocFile with extracted metadata
    9. Update DocExtraction: status=completed

    Returns doc_extraction.id or None on skip/failure.
    """
    if provider is None:
        provider = create_extraction_provider(settings)
    try:
        return await _do_extraction(settings, provider, doc_file_id, force=force, fields=fields)
    finally:
        await provider.close()


async def _do_extraction(
    settings: Settings,
    provider: ExtractionProvider,
    doc_file_id: int,
    *,
    force: bool = False,
    fields: list[dict[str, str]] | None = None,
) -> int | None:
    async with get_session() as session:
        # Load DocFile
        doc_file = await session.get(DocFile, doc_file_id)
        if doc_file is None:
            logger.error("DocFile id=%d not found", doc_file_id)
            return None

        # Check existing completed extraction
        if not force:
            existing = await session.execute(
                select(DocExtraction).where(
                    DocExtraction.doc_file_id == doc_file_id,
                    DocExtraction.status == "completed",
                )
            )
            if existing.scalar_one_or_none():
                logger.info("Skipping extraction for %s — already completed (use --force)", doc_file.file_name)
                return None

        # Resolve file path
        file_path = _resolve_file_path(doc_file)

        if file_path is None or not file_path.exists():
            logger.error("Cannot resolve file path for DocFile id=%d", doc_file_id)
            return None

        # Find latest completed parse for linking
        parse_result = await session.execute(
            select(DocParse).where(
                DocParse.doc_file_id == doc_file_id,
                DocParse.status == "completed",
            ).order_by(DocParse.id.desc()).limit(1)
        )
        latest_parse = parse_result.scalar_one_or_none()

        use_fields = fields or EXTRACTION_FIELDS

        # Load markdown for LLM provider
        markdown = None
        if settings.extraction_provider == "llm" and latest_parse:
            # Prefer enhanced markdown (VLM chart summaries) over raw parse output
            md_rel = latest_parse.enhanced_markdown_path or latest_parse.markdown_path
            if md_rel:
                md_file = settings.parsed_path / md_rel
                if md_file.exists():
                    markdown = md_file.read_text(encoding="utf-8")
                    markdown = strip_watermark_lines(markdown)

        # Create DocExtraction row
        extraction = DocExtraction(
            doc_file_id=doc_file_id,
            doc_parse_id=latest_parse.id if latest_parse else None,
            status="running",
            started_at=epoch_now(),
            provider=settings.extraction_provider,
            llm_model=settings.llm_model if settings.extraction_provider == "llm" else None,
            extraction_config={"fields": use_fields, "provider": settings.extraction_provider},
        )
        session.add(extraction)
        await session.flush()

        try:
            ext_result = await provider.extract(
                file_path=file_path,
                markdown=markdown,
                fields=use_fields,
            )
        except Exception as exc:
            extraction.status = "failed"
            extraction.completed_at = epoch_now()
            extraction.error_message = str(exc)
            logger.error("Extraction failed for %s: %s", doc_file.file_name, exc)
            return None

        # Store full response JSON to disk
        response_data = {
            "fields": ext_result.fields,
            "category": ext_result.category,
            "detail_structure": ext_result.detail_structure,
            "page_count": ext_result.page_count,
            "duration_ms": ext_result.duration_ms,
            "request_id": ext_result.request_id,
            "markdown": markdown,
        }
        rel_path = store_extraction_result(
            settings.extraction_path,
            doc_file.sha256,
            extraction.id,
            response_data,
        )

        # Parse extracted field values into typed columns
        extracted = ext_result.fields
        extraction.title = extracted.get("title")
        extraction.broker = extracted.get("broker")
        extraction.authors = extracted.get("authors")
        extraction.publish_date = parse_date_to_epoch(extracted.get("publish_date"))
        extraction.market = extracted.get("market")
        extraction.asset_class = extracted.get("asset_class")
        extraction.sector = extracted.get("sector")
        extraction.document_type = extracted.get("document_type")
        extraction.target_company = extracted.get("target_company")
        extraction.ticker_symbol = extracted.get("ticker_symbol")
        extraction.extraction_json_path = rel_path
        extraction.textin_request_id = ext_result.request_id
        extraction.duration_ms = ext_result.duration_ms
        extraction.status = "completed"
        extraction.completed_at = epoch_now()

        # Backfill DocFile for direct querying
        doc_file.title = extraction.title or doc_file.title
        doc_file.broker = extraction.broker or doc_file.broker
        doc_file.publish_date = extraction.publish_date or doc_file.publish_date
        doc_file.market = extraction.market or doc_file.market
        doc_file.asset_class = extraction.asset_class or doc_file.asset_class
        doc_file.sector = extraction.sector or doc_file.sector
        doc_file.document_type = extraction.document_type or doc_file.document_type
        doc_file.target_company = extraction.target_company or doc_file.target_company
        doc_file.ticker_symbol = extraction.ticker_symbol or doc_file.ticker_symbol
        doc_file.authors = extraction.authors or doc_file.authors

        logger.info(
            "Extracted entities for %s: title=%s, broker=%s, ticker=%s (provider=%s)",
            doc_file.file_name,
            extraction.title,
            extraction.broker,
            extraction.ticker_symbol,
            extraction.provider,
        )
        return extraction.id


def _resolve_file_path(doc_file: DocFile) -> Path | None:
    """Resolve the local file path for a DocFile."""
    if doc_file.local_path:
        return Path(doc_file.local_path)
    return None
