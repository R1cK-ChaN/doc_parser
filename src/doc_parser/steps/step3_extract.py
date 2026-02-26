"""Step 3: Entity extraction via provider protocol."""

from __future__ import annotations

import logging
from pathlib import Path

from doc_parser.config import Settings
from doc_parser.extraction import create_extraction_provider
from doc_parser.textin_client import ExtractionResult

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
    *,
    file_path: Path,
    markdown: str | None = None,
    fields: list[dict[str, str]] | None = None,
) -> ExtractionResult:
    """Extract entities. Returns in-memory result."""
    provider = create_extraction_provider(settings)
    try:
        result = await provider.extract(
            file_path=file_path,
            markdown=markdown,
            fields=fields or _default_fields(),
        )
        logger.info(
            "Extracted entities: title=%s, broker=%s (provider=%s)",
            result.fields.get("title"),
            result.fields.get("broker"),
            settings.extraction_provider,
        )
        return result
    finally:
        await provider.close()


def _default_fields() -> list[dict[str, str]]:
    from doc_parser.textin_client import EXTRACTION_FIELDS
    return EXTRACTION_FIELDS
